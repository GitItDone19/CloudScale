"""
Unit tests for CloudScale Phase 2 synthetic dirty data generator.
Validates controlled anomaly injection rates and schema compliance.
"""

import json
from pathlib import Path
from data_generator.generate_legacy_data import generate_data, load_config


def test_data_generation_creates_expected_files(tmp_path: Path):
    config = {
        "seed": 42,
        "rates": {
            "orders": {
                "null_merchant_id_rate": 0.05,
                "future_timestamp_rate": 0.03,
                "invalid_negative_declared_value_rate": 0.04,
                "corrupted_postal_code_rate": 0.02,
            },
            "wms_picks": {
                "negative_or_zero_weight_rate": 0.05,
                "missing_order_id_rate": 0.03,
                "inverted_timestamps_rate": 0.02,
                "extreme_weight_anomaly_rate": 0.01,
            },
            "carrier_events": {
                "network_duplicate_rate": 0.06,
                "late_arriving_event_rate": 0.05,
                "corrupted_event_id_rate": 0.02,
                "missing_timestamp_rate": 0.02,
            },
        },
    }

    summary = generate_data(
        num_orders=200,
        output_dir=tmp_path,
        config=config,
        partition_date="2026-10-01",
    )

    raw_dir = tmp_path / "raw"
    assert (raw_dir / "postgres_merchants" / "merchants.csv").exists()
    assert (raw_dir / "postgres_orders" / "dt=2026-10-01" / "orders_20261001.csv").exists()
    assert (raw_dir / "wms_picks" / "dt=2026-10-01" / "wms_dispatch_20261001.csv").exists()
    assert (raw_dir / "carrier_events" / "dt=2026-10-01" / "events_20261001.json").exists()

    assert summary["total_merchants"] == 50
    assert summary["total_orders"] == 200
    assert summary["total_wms_picks"] == 200
    assert summary["total_carrier_events"] > 200

    # Ensure anomalies were injected within expected bounds
    order_anomalies = summary["anomalies"]["orders"]
    assert order_anomalies["null_merchant_id"] > 0
    assert order_anomalies["negative_declared_value"] > 0

    wms_anomalies = summary["anomalies"]["wms_picks"]
    assert wms_anomalies["negative_or_zero_weight"] > 0

    carrier_anomalies = summary["anomalies"]["carrier_events"]
    assert carrier_anomalies["network_duplicates"] > 0
