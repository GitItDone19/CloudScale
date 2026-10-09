"""Business-rule regression cases executed by a real Spark JVM."""

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
from spark.utils.quality import (
    transform_orders,
    transform_wms,
    transform_carrier,
    split_quality,
    deduplicate_events,
)
from spark.utils.schemas import raw_schema
from spark.utils.pipeline import validate_paths

AS_OF = datetime(2026, 10, 9, tzinfo=timezone.utc)


def frame(spark, source, rows):
    return spark.createDataFrame(rows, raw_schema(source))


def test_orders_trim_cast_and_quarantine_all_errors(spark_session):
    base = dict(
        order_id=" O1 ",
        merchant_id=" M1 ",
        customer_postal_code="75001",
        customer_country=" fr ",
        currency=" eur ",
        declared_value="12.50",
        order_timestamp="2026-10-01 12:00:00",
    )
    cases = [
        base,
        {**base, "order_id": " "},
        {**base, "declared_value": "oops"},
        {**base, "order_timestamp": "2026-02-30 10:00:00"},
        {**base, "order_timestamp": "2027-01-01"},
        {**base, "customer_postal_code": "??INVALID##"},
        {**base, "currency": "XYZ"},
        {**base, "merchant_id": "", "declared_value": "-1"},
    ]
    marked = transform_orders(frame(spark_session, "postgres_orders", cases), AS_OF)
    clean, dlq = split_quality(marked)
    good = clean.collect()
    assert len(good) == 1
    assert (good[0].order_id, good[0].currency, good[0].customer_country) == (
        "O1",
        "EUR",
        "FR",
    )
    assert str(good[0].declared_value) == "12.50"
    rows = dlq.collect()
    assert len(rows) == 7
    codes = {r._error_code for r in rows}
    assert {
        "ERR_NULL_KEY",
        "ERR_INVALID_VALUE",
        "ERR_INVALID_TIMESTAMP",
        "ERR_FUTURE_TIMESTAMP",
        "ERR_INVALID_POSTAL_CODE",
        "ERR_INVALID_CURRENCY",
    } <= codes
    assert any(
        set(r._error_codes) == {"ERR_NULL_KEY", "ERR_INVALID_VALUE"} for r in rows
    )
    assert all(r._raw_record for r in rows)


def test_weight_boundaries_and_invalid_dates(spark_session):
    base = dict(
        wms_record_id="W1",
        order_id="O1",
        warehouse_id="WH1",
        parcel_weight_kg="1.25",
        picked_at="2026-10-01 12:00:00",
        packed_at="2026-10-01 12:30:00",
        dispatched_at="2026-10-01 13:00:00",
    )
    weights = ["-1", "0", "0.01", "1000", "9999", "NaN", "Infinity", "bad", None]
    cases = (
        [base]
        + [{**base, "parcel_weight_kg": w} for w in weights]
        + [
            {**base, "packed_at": "2026-10-01 11:00:00"},
            {**base, "picked_at": "invalid"},
            {**base, "order_id": " "},
        ]
    )
    clean, dlq = split_quality(
        transform_wms(frame(spark_session, "wms_picks", cases), AS_OF)
    )
    assert clean.count() == 1
    rows = dlq.collect()
    assert sum(r._error_code == "ERR_INVALID_WEIGHT" for r in rows) == len(weights)
    assert {"ERR_TIMESTAMP_SEQUENCE", "ERR_INVALID_TIMESTAMP", "ERR_NULL_KEY"} <= {
        r._error_code for r in rows
    }


def test_carrier_versions_late_arrivals_and_deterministic_dedup(spark_session):
    base = dict(
        event_id=" E1 ",
        tracking_number="T1",
        carrier_id="DHL",
        event_type="delivered",
        scan_timestamp="2026-10-01 12:00:00",
        received_at="2026-10-05 12:00:00",
    )
    rows = [
        base,
        {**base, "received_at": "2026-10-06 12:00:00"},
        {**base, "scan_timestamp": "bad", "received_at": "2026-10-07 12:00:00"},
        {
            **base,
            "event_id": "E2",
            "tracking_number": None,
            "tracking_code": "T2",
            "customs_fee": "14.50",
            "currency": "eur",
        },
        {**base, "event_id": " "},
        {**base, "event_id": "E3", "customs_fee": "garbage", "currency": "EUR"},
        {**base, "event_id": "E4", "received_at": "2026-09-30"},
    ]
    clean, rejected = split_quality(
        transform_carrier(frame(spark_session, "carrier_events", rows), AS_OF)
    )
    assert rejected.count() == 4
    deduped = deduplicate_events(clean).orderBy("event_id").collect()
    assert len(deduped) == 2
    assert deduped[0].received_at == datetime(2026, 10, 6, 12)
    assert deduped[0].scan_timestamp == datetime(2026, 10, 1, 12)
    assert deduped[1].tracking_number == "T2"
    assert str(deduped[1].customs_fee) == "14.50"
    assert deduped[1].currency == "EUR"
    assert (
        deduplicate_events(clean.repartition(3)).orderBy("event_id").collect()
        == deduped
    )


def test_dedup_ties_are_stable(spark_session):
    rows = [
        ("E1", "2026-10-01", "2026-10-02", "A"),
        ("E1", "2026-10-01", "2026-10-02", "B"),
    ]
    df = spark_session.createDataFrame(
        rows, ["event_id", "scan_timestamp", "received_at", "location_code"]
    )
    assert (
        deduplicate_events(df.repartition(2)).collect()
        == deduplicate_events(df.repartition(1)).collect()
    )


def test_output_roots_cannot_overlap_bronze(tmp_path):
    with pytest.raises(ValueError, match="non-overlapping"):
        validate_paths(
            tmp_path / "bronze",
            tmp_path / "bronze" / "silver",
            tmp_path / "dlq",
            "2026-10-01",
        )
    with pytest.raises(ValueError):
        validate_paths(
            tmp_path / "bronze", tmp_path / "silver", tmp_path / "dlq", "../../outside"
        )
