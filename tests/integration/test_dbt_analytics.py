"""Execute dbt models and incremental reconciliation against a real local database."""

import os
from pathlib import Path
import subprocess
import sys

import pytest

duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
from warehouse.contracts import fields

ROOT = Path(__file__).resolve().parents[2]


def build(database, day, full_refresh=False):
    env = {
        **os.environ,
        "CLOUDSCALE_DUCKDB_PATH": str(database),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_TARGET_PATH": str(database.parent / "dbt-target"),
        "DBT_LOG_PATH": str(database.parent / "dbt-logs"),
    }
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        str(ROOT / "dbt"),
        "--profiles-dir",
        str(ROOT / "dbt"),
        "--target",
        "local",
        "--vars",
        '{run_date: "' + day + '", history_start: "2026-10-01"}',
    ]
    if full_refresh:
        command.append("--full-refresh")
    result = subprocess.run(
        command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=180
    )
    assert result.returncode == 0, result.stdout + result.stderr


def insert(conn, source, **values):
    row = {name: None for name in fields(source)}
    row.update(
        _source_file="test.parquet",
        _batch_date="2026-10-01",
        _processed_at="2026-10-01 23:00:00",
        dt="2026-10-01",
    )
    row.update(values)
    conn.execute(
        f"insert into raw_staging.{source} ({', '.join(row)}) values ({', '.join('?' for _ in row)})",
        list(row.values()),
    )


def test_incremental_shipments_late_events_and_reruns(tmp_path):
    database = tmp_path / "analytics.duckdb"
    with duckdb.connect(str(database)) as conn:
        conn.execute("create schema raw_staging")
        for source in (
            "postgres_orders",
            "postgres_merchants",
            "wms_picks",
            "carrier_events",
        ):
            types = {**fields(source), "dt": "DATE"}
            types = {
                name: (
                    "DECIMAL(12,2)"
                    if kind == "NUMERIC"
                    else (
                        "VARCHAR"
                        if kind == "STRING"
                        else "DOUBLE" if kind == "FLOAT" else kind
                    )
                )
                for name, kind in types.items()
            }
            conn.execute(
                f"create table raw_staging.{source} ({', '.join(name + ' ' + kind for name, kind in types.items())})"
            )
        insert(
            conn,
            "postgres_merchants",
            merchant_id="M1",
            merchant_name="Merchant",
            merchant_tier="GOLD",
            country_code="FR",
        )
        insert(
            conn,
            "postgres_orders",
            order_id="O1",
            merchant_id="M1",
            customer_postal_code="75001",
            customer_country="FR",
            declared_value=100,
            currency="EUR",
            order_timestamp="2026-10-01 08:00:00",
        )
        insert(
            conn,
            "wms_picks",
            wms_record_id="W1",
            order_id="O1",
            warehouse_id="PARIS",
            parcel_weight_kg=1,
            picked_at="2026-10-01 09:00:00",
            packed_at="2026-10-01 09:30:00",
            dispatched_at="2026-10-01 10:00:00",
        )
        insert(
            conn,
            "carrier_events",
            event_id="E1",
            tracking_number="T1",
            order_id="O1",
            carrier_id="DHL_EXPRESS",
            event_type="IN_TRANSIT",
            scan_timestamp="2026-10-01 12:00:00",
            received_at="2026-10-01 12:01:00",
        )
        # Same tracking number at another carrier is a different shipment; orphan retained.
        insert(
            conn,
            "carrier_events",
            event_id="E2",
            tracking_number="T1",
            order_id="MISSING",
            carrier_id="FEDEX_EU",
            event_type="IN_TRANSIT",
            scan_timestamp="2026-10-01 12:00:00",
            received_at="2026-10-01 12:01:00",
        )
    build(database, "2026-10-01", full_refresh=True)
    build(database, "2026-10-01")
    with duckdb.connect(str(database)) as conn:
        assert (
            conn.execute(
                "select count(*) from analytics_core.fct_shipments"
            ).fetchone()[0]
            == 2
        )
        assert (
            conn.execute(
                "select count(*) from analytics_core.fct_shipments where missing_order"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "select on_time_delivery_rate from analytics_marts.mart_carrier_performance_daily where order_date is not null"
            ).fetchone()[0]
            is None
        )
        insert(
            conn,
            "carrier_events",
            event_id="E3",
            tracking_number="T1",
            order_id="O1",
            carrier_id="DHL_EXPRESS",
            event_type="DELIVERED",
            scan_timestamp="2026-10-02 17:00:00",
            received_at="2026-10-05 11:00:00",
            dt="2026-10-05",
            _batch_date="2026-10-05",
            _processed_at="2026-10-05 23:00:00",
        )
    build(database, "2026-10-05")
    with duckdb.connect(str(database)) as conn:
        assert conn.execute(
            "select current_status, delivery_duration_hours, is_sla_breached from analytics_core.fct_shipments where order_id = 'O1'"
        ).fetchone() == ("DELIVERED", 31.0, True)
        assert (
            conn.execute(
                "select count(*) from analytics_core.fct_delivery_events"
            ).fetchone()[0]
            == 3
        )
        assert (
            conn.execute(
                "select on_time_delivery_rate from analytics_marts.mart_carrier_performance_daily where order_date is not null"
            ).fetchone()[0]
            == 0
        )
        # Correct an existing event across partitions: key stays unique, metrics change.
        insert(
            conn,
            "carrier_events",
            event_id="E3",
            tracking_number="T1",
            order_id="O1",
            carrier_id="DHL_EXPRESS",
            event_type="DELIVERED",
            scan_timestamp="2026-10-01 17:00:00",
            received_at="2026-10-06 11:00:00",
            dt="2026-10-06",
            _batch_date="2026-10-06",
            _processed_at="2026-10-06 23:00:00",
        )
    build(database, "2026-10-06")
    build(database, "2026-10-01")
    with duckdb.connect(str(database)) as conn:
        assert conn.execute(
            "select delivery_duration_hours, is_sla_breached from analytics_core.fct_shipments where order_id = 'O1'"
        ).fetchone() == (7.0, False)
        assert (
            conn.execute(
                "select count(*) from analytics_core.fct_delivery_events"
            ).fetchone()[0]
            == 3
        )
        assert (
            conn.execute(
                "select on_time_delivery_rate from analytics_marts.mart_carrier_performance_daily where order_date is not null"
            ).fetchone()[0]
            == 100
        )
