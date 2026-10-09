"""Bronze-to-Parquet integration: real files, malformed input, and partition reruns."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F
from spark.jobs.process_shipments import run as shipments
from spark.jobs.process_carrier_events import run as carriers
from ingestion.extract_postgres import run as land_postgres
from ingestion.extract_wms_csv import run as land_wms
from ingestion.extract_carrier_webhooks import run as land_carrier
from data_generator.generate_legacy_data import generate_data, load_config


def checksums(root):
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def test_end_to_end_silver_dlq_audit_and_partition_reruns(spark_session, tmp_path):
    date = "2026-10-01"
    generated = tmp_path / "generated"
    config = load_config(Path("data_generator/configs/anomaly_rates.json"))
    config["rates"]["carrier_events"]["network_duplicate_rate"] = 0.5
    config["rates"]["wms_picks"]["negative_or_zero_weight_rate"] = 0.4
    generate_data(30, generated, config, date)
    bronze, silver, dlq = [
        tmp_path / name for name in ("bronze", "silver", "deadletter")
    ]
    event_file = next(
        (generated / "raw" / "carrier_events" / f"dt={date}").glob("events*.json")
    )
    with event_file.open("a", encoding="utf-8") as f:
        f.write('{"event_id": broken JSON\n')
    for extractor in (land_postgres, land_wms, land_carrier):
        extractor(partition_date=date, source_raw=generated / "raw", bronze_root=bronze)

    original = checksums(bronze)
    sentinel = silver / "carrier_events" / "dt=2026-09-30" / "keep.txt"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("older partition")
    kwargs = dict(
        batch_date=date,
        bronze_root=bronze,
        silver_root=silver,
        deadletter_root=dlq,
        as_of=datetime(2026, 10, 9, tzinfo=timezone.utc),
    )
    result = shipments(spark_session, **kwargs)
    first = carriers(spark_session, **kwargs)
    assert result["status"] == first["status"] == "success"
    assert first["sources"]["carrier_events"]["duplicates_removed"] > 0
    assert result["sources"]["wms_picks"]["records_rejected"] > 0
    assert (
        first["sources"]["carrier_events"]["error_counts"]["ERR_MALFORMED_RECORD"] == 1
    )
    for summary in (result, first):
        for source, stats in summary["sources"].items():
            assert (
                stats["records_in"]
                == stats["records_clean"]
                + stats["records_rejected"]
                + stats["duplicates_removed"]
            )
            good = spark_session.read.parquet(stats["silver_path"])
            bad = spark_session.read.parquet(stats["deadletter_path"])
            assert good.count() == stats["records_clean"]
            assert bad.count() == stats["records_rejected"]
            assert bad.filter(F.col("_error_code").isNull()).count() == 0
            for key in {
                "postgres_orders": ["order_id", "merchant_id"],
                "postgres_merchants": ["merchant_id"],
                "wms_picks": ["wms_record_id", "order_id"],
                "carrier_events": ["event_id", "tracking_number"],
            }[source]:
                assert (
                    good.filter(F.col(key).isNull() | (F.trim(key) == "")).count() == 0
                )
            if source == "wms_picks":
                assert (
                    good.filter(
                        (F.col("parcel_weight_kg") <= 0.01)
                        | (F.col("parcel_weight_kg") >= 1000)
                    ).count()
                    == 0
                )
    before = (
        spark_session.read.parquet(first["sources"]["carrier_events"]["silver_path"])
        .orderBy("event_id")
        .collect()
    )
    second = carriers(spark_session, **kwargs)
    after = (
        spark_session.read.parquet(second["sources"]["carrier_events"]["silver_path"])
        .orderBy("event_id")
        .collect()
    )
    assert before == after
    assert checksums(bronze) == original
    assert sentinel.read_text() == "older partition"
    assert not list(silver.rglob(".stage-*"))
    assert not list(silver.rglob(".backup-*"))
    audit = json.loads(
        (silver / "_audit" / f"dt={date}" / "carrier_events.json").read_text()
    )
    assert audit["status"] == "success"
    rejected = spark_session.read.parquet(
        first["sources"]["carrier_events"]["deadletter_path"]
    )
    malformed = rejected.filter(F.col("_error_code") == "ERR_MALFORMED_RECORD").first()
    assert "broken JSON" in malformed._raw_record


def test_missing_input_fails_without_outputs(spark_session, tmp_path):
    with pytest.raises(FileNotFoundError):
        carriers(
            spark_session,
            batch_date="2026-10-01",
            bronze_root=tmp_path / "bronze",
            silver_root=tmp_path / "silver",
            deadletter_root=tmp_path / "dlq",
        )
    assert not (tmp_path / "silver").exists()


def test_failed_staged_write_preserves_previous_output(
    spark_session, tmp_path, monkeypatch
):
    from pyspark.sql.readwriter import DataFrameWriter
    from spark.utils.pipeline import publish_parquet

    root = tmp_path / "silver"
    original = spark_session.range(2)
    target = publish_parquet(original, root, "sample", "2026-10-01")
    original_writer = DataFrameWriter.parquet

    def fail_after_write(self, *args, **kwargs):
        original_writer(self, *args, **kwargs)
        raise OSError("simulated disk failure")

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_after_write)
    with pytest.raises(OSError, match="simulated disk failure"):
        publish_parquet(spark_session.range(5), root, "sample", "2026-10-01")
    assert spark_session.read.parquet(target).count() == 2
    assert not list(root.rglob(".stage-*"))
    assert not list(root.rglob(".backup-*"))


def test_publish_rename_failure_rolls_back(spark_session, tmp_path, monkeypatch):
    from spark.utils.pipeline import publish_parquet

    root = tmp_path / "silver"
    target = publish_parquet(spark_session.range(2), root, "sample", "2026-10-01")
    original_rename = Path.rename

    def fail_publish(path, destination):
        if path.name.startswith(".stage-"):
            raise OSError("simulated rename failure")
        return original_rename(path, destination)

    monkeypatch.setattr(Path, "rename", fail_publish)
    with pytest.raises(OSError, match="simulated rename failure"):
        publish_parquet(spark_session.range(5), root, "sample", "2026-10-01")
    assert spark_session.read.parquet(target).count() == 2
    assert not list(root.rglob(".stage-*"))
    assert not list(root.rglob(".backup-*"))
