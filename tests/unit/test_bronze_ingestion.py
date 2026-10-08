import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ingestion.bronze_lander import land_file
from ingestion.extract_postgres import run as postgres
from ingestion.extract_wms_csv import run as wms
from ingestion.extract_carrier_webhooks import run as carrier
from data_generator.generate_legacy_data import generate_data, load_config

DATE = "2026-10-01"


@pytest.fixture
def landing(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text('id,description\n1,"two\nlines"\n', encoding="utf-8")
    root = tmp_path / "bronze"
    return source, root


def land(source, root, **kwargs):
    return land_file(source, root, "orders", DATE, **kwargs)


def test_rerun_preserves_data_and_metadata(landing):
    source, root = landing
    first = land(source, root)
    target = Path(first["target"])
    sidecar = target.with_name(target.name + ".meta.json")
    original = (target.read_bytes(), sidecar.read_bytes(), target.stat().st_mtime_ns)
    second = land(source, root)
    assert first["status"] == "landed"
    assert first["records_landed"] == 1
    assert second["status"] == "skipped"
    assert second["ingestion_timestamp"] == first["ingestion_timestamp"]
    assert (
        target.read_bytes(),
        sidecar.read_bytes(),
        target.stat().st_mtime_ns,
    ) == original


def test_changed_source_requires_explicit_overwrite(landing):
    source, root = landing
    first = land(source, root)
    target = Path(first["target"])
    original = target.read_bytes()
    source.write_text("id\n2\n", encoding="utf-8")
    with pytest.raises(FileExistsError):
        land(source, root)
    assert target.read_bytes() == original
    assert land(source, root, overwrite=True)["status"] == "overwritten"
    assert target.read_bytes() == source.read_bytes()


def test_overwrite_on_first_landing_reports_landed(landing):
    assert land(*landing, overwrite=True)["status"] == "landed"


def test_stale_metadata_does_not_hide_data_corruption(landing):
    source, root = landing
    first = land(source, root)
    Path(first["target"]).write_text("damaged", encoding="utf-8")
    with pytest.raises(FileExistsError):
        land(source, root)


@pytest.mark.parametrize("metadata", [None, "broken JSON", "[]"])
def test_retry_repairs_missing_or_invalid_sidecar(landing, metadata):
    first = land(*landing)
    target = Path(first["target"])
    sidecar = target.with_name(target.name + ".meta.json")
    if metadata is None:
        sidecar.unlink()
    else:
        sidecar.write_text(metadata, encoding="utf-8")
    modified = target.stat().st_mtime_ns
    land(*landing)
    assert target.stat().st_mtime_ns == modified
    assert (
        json.loads(sidecar.read_text())["checksum_sha256"] == first["checksum_sha256"]
    )
    assert land(*landing)["status"] == "skipped"


def test_copy_failure_preserves_existing_partition_and_cleans_staging(landing):
    first = land(*landing)
    target = Path(first["target"])
    before = target.read_bytes()
    with patch(
        "ingestion.bronze_lander.shutil.copy2", side_effect=OSError("disk full")
    ):
        with pytest.raises(RuntimeError, match="disk full"):
            land(*landing, overwrite=True)
    assert target.read_bytes() == before
    assert not list(target.parent.glob(".landing-*"))


@pytest.mark.parametrize("date", ["2026-02-30", "2026-1-1", "../../escape"])
def test_rejects_invalid_partition_before_writing(landing, date):
    source, root = landing
    with pytest.raises(ValueError):
        land_file(source, root, "orders", date)
    assert not root.exists()


def test_generated_sources_land_and_rerun_without_mutation(tmp_path):
    config = load_config(Path("data_generator/configs/anomaly_rates.json"))
    generate_data(
        num_orders=20,
        output_dir=tmp_path / "source",
        config=config,
        partition_date=DATE,
    )
    source = tmp_path / "source" / "raw"
    bronze = tmp_path / "bronze"
    for extractor in (postgres, wms, carrier):
        first = extractor(partition_date=DATE, bronze_root=bronze, source_raw=source)
        assert first["files_landed"] > 0
        second = extractor(partition_date=DATE, bronze_root=bronze, source_raw=source)
        assert second["files_landed"] == 0
        assert second["files_skipped"] == first["files_landed"]
    assert (bronze / "postgres_merchants" / f"dt={DATE}" / "merchants.csv").exists()
    events = bronze / "carrier_events" / f"dt={DATE}" / "events_20261001.json"
    assert events.read_bytes() == (source / events.relative_to(bronze)).read_bytes()
