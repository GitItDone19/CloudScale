"""Actual local Parquet validation plus simulated immutable cloud uploads."""

import json
from unittest.mock import Mock

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa
import pyarrow.parquet as pq

from warehouse.contracts import WarehouseConfig, STRING_FIELDS, fields
from warehouse.publish_silver import inspect_batch, publish


@pytest.fixture
def silver(tmp_path):
    types = {
        "STRING": pa.string(),
        "NUMERIC": pa.decimal128(12, 2),
        "FLOAT": pa.float64(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
    }
    for source in STRING_FIELDS:
        folder = tmp_path / source / "dt=2026-10-01"
        folder.mkdir(parents=True)
        schema = pa.schema(
            [(name, types[kind]) for name, kind in fields(source).items()]
        )
        pq.write_table(
            pa.Table.from_pylist([], schema=schema),
            folder / "part-test.parquet",
            compression="snappy",
        )
        (folder / "_SUCCESS").touch()
    audit = tmp_path / "_audit" / "dt=2026-10-01"
    audit.mkdir(parents=True)
    for job, sources in [
        ("shipments", list(STRING_FIELDS)[:3]),
        ("carrier_events", ["carrier_events"]),
    ]:
        (audit / f"{job}.json").write_text(
            json.dumps(
                {
                    "status": "success",
                    "partition_date": "2026-10-01",
                    "sources": {s: {"records_clean": 0} for s in sources},
                }
            )
        )
    return tmp_path


def test_parquet_contract_and_canonical_names(silver):
    uploads = inspect_batch(silver, "2026-10-01")
    assert len(uploads) == 4
    assert all(item["relative"].endswith("/part-00000.parquet") for item in uploads)
    assert all(len(item["sha256"]) == 64 for item in uploads)


def test_failed_audit_rejected_before_upload(silver):
    audit = silver / "_audit" / "dt=2026-10-01" / "shipments.json"
    data = json.loads(audit.read_text())
    data["status"] = "failed"
    audit.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="successfully audited"):
        inspect_batch(silver, "2026-10-01")


def test_schema_drift_rejected(silver):
    pq.write_table(
        pa.table({"bad_column": [1]}),
        silver / "postgres_orders" / "dt=2026-10-01" / "part-test.parquet",
    )
    with pytest.raises(ValueError, match="schema"):
        inspect_batch(silver, "2026-10-01")


def test_row_counts_and_size_limit_enforced(silver):
    with pytest.raises(ValueError, match="byte limit"):
        inspect_batch(silver, "2026-10-01", max_bytes=1)
    audit = silver / "_audit" / "dt=2026-10-01" / "shipments.json"
    data = json.loads(audit.read_text())
    data["sources"]["postgres_orders"]["records_clean"] = 1
    audit.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="row count"):
        inspect_batch(silver, "2026-10-01")


def test_immutable_upload_and_rerun(silver):
    uploads = inspect_batch(silver, "2026-10-01")
    config = WarehouseConfig("cloudscale-test", "cloudscale-silver-test")
    client = Mock()
    client.get_bucket.return_value.location = "US"
    client.list_blobs.return_value = []
    result = publish(client, config, uploads)
    assert all(r["status"] == "uploaded" for r in result)
    for (
        call
    ) in (
        client.get_bucket.return_value.blob.return_value.upload_from_filename.call_args_list
    ):
        assert call.kwargs["if_generation_match"] == 0
    objects = [
        Mock(
            name=f"{config.prefix}/{item['relative']}",
            metadata={"sha256": item["sha256"]},
            size=item["size"],
        )
        for item in uploads
    ]
    for obj, item in zip(objects, uploads):
        obj.name = f"{config.prefix}/{item['relative']}"
    client.list_blobs.side_effect = [[obj] for obj in objects]
    client.get_bucket.return_value.blob.reset_mock()
    result = publish(client, config, uploads)
    assert all(r["status"] == "skipped" for r in result)
    client.get_bucket.return_value.blob.assert_not_called()


def test_remote_conflict_stops_all_uploads(silver):
    uploads = inspect_batch(silver, "2026-10-01")
    config = WarehouseConfig("cloudscale-test", "cloudscale-silver-test")
    client = Mock()
    client.get_bucket.return_value.location = "US"
    obj = Mock(metadata={"sha256": "changed"}, size=0)
    obj.name = f"{config.prefix}/{uploads[0]['relative']}"
    client.list_blobs.side_effect = [[obj], [], [], []]
    with pytest.raises(ValueError, match="differs"):
        publish(client, config, uploads)
    client.get_bucket.return_value.blob.assert_not_called()
