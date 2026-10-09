"""Publish a validated Silver batch into immutable canonical GCS object names."""

import hashlib
import json
from datetime import date
from pathlib import Path

from warehouse.contracts import STRING_FIELDS, fields


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def inspect_batch(root, batch_date, max_bytes=52428800):
    import pyarrow as pa
    import pyarrow.parquet as pq

    if date.fromisoformat(batch_date).isoformat() != batch_date:
        raise ValueError("Batch date must use YYYY-MM-DD")
    if max_bytes <= 0:
        raise ValueError("Upload size limit must be positive")
    root = Path(root).resolve()
    expected_counts = {}
    for job in ("shipments", "carrier_events"):
        audit = json.loads(
            (root / "_audit" / f"dt={batch_date}" / f"{job}.json").read_text(
                encoding="utf-8"
            )
        )
        if (
            audit.get("status") != "success"
            or audit.get("partition_date") != batch_date
        ):
            raise ValueError("Only successfully audited batches can be uploaded")
        expected_counts.update(
            {
                source: stats["records_clean"]
                for source, stats in audit["sources"].items()
            }
        )
    uploads, total = [], 0
    for source in STRING_FIELDS:
        folder = root / source / f"dt={batch_date}"
        if not (folder / "_SUCCESS").is_file():
            raise ValueError(f"Missing Spark completion marker for {source}")
        files = sorted(folder.glob("*.parquet"))
        if not files:
            raise ValueError(f"No Parquet files for {source}")
        count = 0
        for i, path in enumerate(files):
            if root not in path.resolve().parents:
                raise ValueError("Parquet path escapes the Silver root")
            parquet = pq.ParquetFile(path)
            schema = parquet.schema_arrow
            if set(schema.names) != set(fields(source)):
                raise ValueError(f"Unexpected Silver schema for {source}")
            for name, kind in fields(source).items():
                typ = schema.field(name).type
                valid = {
                    "STRING": pa.types.is_string,
                    "FLOAT": pa.types.is_float64,
                    "DATE": pa.types.is_date32,
                    "TIMESTAMP": pa.types.is_timestamp,
                    "NUMERIC": lambda t: pa.types.is_decimal(t)
                    and t.precision == 12
                    and t.scale == 2,
                }[kind](typ)
                if not valid:
                    raise ValueError(f"Unexpected type for {source}.{name}")
            count += parquet.metadata.num_rows
            total += path.stat().st_size
            uploads.append(
                {
                    "source": source,
                    "path": path,
                    "relative": f"{source}/dt={batch_date}/part-{i:05d}.parquet",
                    "sha256": sha256(path),
                    "size": path.stat().st_size,
                }
            )
        if count != expected_counts.get(source):
            raise ValueError(f"Audit row count differs from Parquet for {source}")
    if total > max_bytes:
        raise ValueError("Batch exceeds the configured upload byte limit")
    return uploads


def publish(storage_client, config, uploads):
    bucket = storage_client.get_bucket(config.bucket)
    if bucket.location.upper() != config.location.upper():
        raise ValueError("Use a bucket in the exact same location as BigQuery")
    # Check all remote partitions before uploading any object. Reject changed snapshots.
    existing = {}
    for source in STRING_FIELDS:
        prefix = (
            f"{config.prefix}/{source}/" + uploads[0]["relative"].split("/")[1] + "/"
        )
        for blob in storage_client.list_blobs(bucket, prefix=prefix):
            existing[blob.name] = blob
    desired = {f"{config.prefix}/{item['relative']}": item for item in uploads}
    if set(existing) - set(desired):
        raise ValueError(
            "Remote partition contains unexpected files; use a new version prefix"
        )
    for name, blob in existing.items():
        item = desired[name]
        if (blob.metadata or {}).get("sha256") != item["sha256"] or blob.size != item[
            "size"
        ]:
            raise ValueError("Remote partition differs; use a new version prefix")
    results = []
    for name, item in desired.items():
        if name in existing:
            results.append({"object": name, "status": "skipped"})
            continue
        blob = bucket.blob(name)
        blob.metadata = {"sha256": item["sha256"]}
        blob.upload_from_filename(
            str(item["path"]), if_generation_match=0, checksum="auto"
        )
        results.append({"object": name, "status": "uploaded"})
    return results
