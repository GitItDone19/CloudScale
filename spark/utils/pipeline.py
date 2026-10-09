"""Read a Bronze partition and publish local Silver/DLQ partitions safely."""

import argparse
import json
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from spark.utils.quality import deduplicate_events, split_quality
from spark.utils.schemas import FIELDS, raw_schema


def read_bronze(spark, root, source, batch_date):
    folder = Path(root) / source / f"dt={batch_date}"
    extension = "json" if source == "carrier_events" else "csv"
    files = sorted(
        p for p in folder.glob(f"*.{extension}") if not p.name.endswith(".meta.json")
    )
    if not files:
        raise FileNotFoundError(f"No {extension} input files in {folder}")
    paths = [str(p.resolve()) for p in files]
    if source == "carrier_events":
        # Read text first: preserve the exact webhook payload, including malformed JSON.
        parsed = spark.read.text(paths).filter(F.length(F.trim("value")) > 0)
        parsed = parsed.withColumn("_source_file", F.input_file_name())
        parsed = parsed.withColumn(
            "_parsed",
            F.from_json(
                "value",
                raw_schema(source),
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
        return parsed.select(
            "_parsed.*", F.col("value").alias("_raw_record"), "_source_file"
        )
    parsed = (
        spark.read.schema(raw_schema(source))
        .option("header", True)
        .option("enforceSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(paths)
    )
    return parsed.withColumn("_source_file", F.input_file_name()).withColumn(
        "_raw_record",
        F.coalesce(
            "_corrupt_record", F.to_json(F.struct(*[F.col(c) for c in FIELDS[source]]))
        ),
    )


def publish_parquet(df, root, source, batch_date):
    parent = (Path(root) / source).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / f"dt={batch_date}"
    token = uuid.uuid4().hex
    staged = parent / f".stage-{token}"
    backup = parent / f".backup-{token}"
    # All rename/delete targets are direct children of this output table.
    for path in (target, staged, backup):
        if path.resolve().parent != parent:
            raise ValueError("Output path escapes its table directory")
    try:
        df.write.mode("errorifexists").option("compression", "snappy").parquet(
            str(staged)
        )
        if target.exists():
            target.rename(backup)
        try:
            staged.rename(target)
        except Exception:
            if backup.exists():
                backup.rename(target)
            raise
    finally:
        if staged.exists():
            shutil.rmtree(staged)
    if backup.exists():
        shutil.rmtree(backup)
    return str(target)


def validate_paths(bronze, silver, deadletter, batch_date):
    if date.fromisoformat(batch_date).isoformat() != batch_date:
        raise ValueError("Date must use YYYY-MM-DD")
    roots = [Path(p).resolve() for p in (bronze, silver, deadletter)]
    for i, root in enumerate(roots):
        for other in roots[i + 1 :]:
            if root == other or root in other.parents or other in root.parents:
                raise ValueError(
                    "Bronze, Silver and DLQ roots must be separate, non-overlapping directories"
                )


def write_audit(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        staged.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        staged.replace(path)
    finally:
        staged.unlink(missing_ok=True)


def run_sources(
    spark, transforms, batch_date, bronze_root, silver_root, deadletter_root, as_of=None
):
    validate_paths(bronze_root, silver_root, deadletter_root, batch_date)
    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        raise ValueError("as_of must include a timezone")
    summary = {
        "partition_date": batch_date,
        "as_of": as_of.isoformat(),
        "status": "running",
        "sources": {},
    }
    job_name = (
        "carrier_events" if list(transforms) == ["carrier_events"] else "shipments"
    )
    audit = Path(silver_root) / "_audit" / f"dt={batch_date}" / f"{job_name}.json"
    # Fail missing-input batches before changing any previously successful output.
    inputs = {
        source: read_bronze(spark, bronze_root, source, batch_date)
        for source in transforms
    }
    write_audit(audit, summary)
    try:
        for source, transform in transforms.items():
            frame = (
                inputs[source]
                .withColumn("_batch_date", F.lit(batch_date).cast("date"))
                .withColumn("_processed_at", F.lit(as_of).cast("timestamp"))
            )
            marked = transform(frame, as_of).cache()
            clean = None
            try:
                records_in = marked.count()
                clean, rejected = split_quality(marked)
                rejected_count = rejected.count()
                valid_count = records_in - rejected_count
                if source == "carrier_events":
                    clean = deduplicate_events(clean)
                clean = clean.cache()
                clean_count = clean.count()
                reasons = {
                    row["code"]: row["count"]
                    for row in rejected.select(F.explode("_error_codes").alias("code"))
                    .groupBy("code")
                    .count()
                    .collect()
                }
                silver_path = publish_parquet(clean, silver_root, source, batch_date)
                dlq_path = publish_parquet(
                    rejected, deadletter_root, source, batch_date
                )
                summary["sources"][source] = {
                    "records_in": records_in,
                    "records_clean": clean_count,
                    "records_rejected": rejected_count,
                    "duplicates_removed": valid_count - clean_count,
                    "error_counts": reasons,
                    "silver_path": silver_path,
                    "deadletter_path": dlq_path,
                }
            finally:
                if clean is not None:
                    clean.unpersist()
                marked.unpersist()
        summary["status"] = "success"
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        raise
    finally:
        write_audit(audit, summary)
    return summary


def cli(transforms, description):
    from spark.utils.spark_builder import build_spark

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date", required=True, help="Bronze partition date YYYY-MM-DD"
    )
    parser.add_argument("--bronze-root", default="data/bronze")
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--deadletter-root", default="data/deadletter")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument(
        "--as-of",
        help="Optional fixed ISO timestamp with timezone for replayable validation",
    )
    args = parser.parse_args()
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    validate_paths(args.bronze_root, args.silver_root, args.deadletter_root, args.date)
    spark = build_spark(description, args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(
            json.dumps(
                run_sources(
                    spark,
                    transforms,
                    args.date,
                    args.bronze_root,
                    args.silver_root,
                    args.deadletter_root,
                    as_of,
                ),
                indent=2,
            )
        )
    finally:
        spark.stop()
