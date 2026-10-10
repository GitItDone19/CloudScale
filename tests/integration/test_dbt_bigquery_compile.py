"""Render BigQuery SQL offline with no database introspection or query execution."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("dbt.adapters.bigquery")
from dbt.cli.main import dbtRunner
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.bigquery.relation import BigQueryRelation

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("incremental", [False, True])
def test_bigquery_sql_compiles_without_network(tmp_path, monkeypatch, incremental):
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    monkeypatch.setenv("GCP_PROJECT_ID", "cloudscale-test")
    client = Mock()

    def open_offline(self, connection):
        connection.handle = client
        connection.state = ConnectionState.OPEN
        return connection

    def get_relation_offline(self, database, schema, identifier):
        if incremental:
            return BigQueryRelation.create(
                database=database, schema=schema, identifier=identifier, type="table"
            )
        return None

    with patch(
        "dbt.adapters.bigquery.connections.BigQueryConnectionManager.open",
        new=open_offline,
    ), patch(
        "dbt.adapters.bigquery.impl.BigQueryAdapter.get_relation",
        new=get_relation_offline,
    ):
        result = dbtRunner().invoke(
            [
                "compile",
                "--project-dir",
                str(ROOT / "dbt"),
                "--profiles-dir",
                str(ROOT / "dbt"),
                "--target",
                "bigquery",
                "--no-introspect",
                "--no-populate-cache",
                "--vars",
                '{run_date: "2026-10-05", history_start: "2026-10-01"}',
            ]
        )
    assert result.success, result.exception
    assert all(call[0] == "close" for call in client.mock_calls)
    compiled = tmp_path / "target" / "compiled" / "cloudscale" / "models"
    events = (compiled / "staging" / "stg_carrier_events.sql").read_text()
    expected_start = "2026-10-02" if incremental else "2026-10-01"
    assert (
        f"dt between cast('{expected_start}' as date) and cast('2026-10-05' as date)"
        in events
    )
    assert ("union all" in events) == incremental
    shipments = (compiled / "core" / "fct_shipments.sql").read_text().lower()
    assert "to_hex(md5" in shipments
    assert "datetime_diff" in shipments
    nodes = {item.node.name: item.node for item in result.result}
    assert nodes["fct_shipments"].config.incremental_strategy == "merge"
    assert nodes["fct_shipments"].config.get("partition_by")["field"] == "order_date"
