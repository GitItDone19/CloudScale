#!/usr/bin/env python3
"""
CloudScale — Synthetic Legacy Data Generator (Phase 2)
Simulates enterprise legacy systems with deliberate, configurable anomalies:
  1. PostgreSQL OLTP Dumps (Merchants & Orders - CSV / SQL)
  2. Warehouse WMS Batch Drops (Pick & Pack operations - CSV)
  3. Carrier Webhook Streams (Mobile courier scans - NDJSON)

Anomalies injected to validate downstream PySpark & dbt quality gates:
  - Missing foreign keys (null merchant_id, orphan orders)
  - Corrupted numeric values (negative weights, zero weights, extreme outliers)
  - Temporal anomalies (future order timestamps, inverted pick/pack/dispatch times)
  - Network duplicates & late-arriving event scans (duplicated event_ids, 4-day lag)
"""

import argparse
import csv
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("generate_legacy_data")

# Fixed reference entities
CARRIERS = [
    {"carrier_id": "DHL_EXPRESS", "name": "DHL Express Europe", "service": "EXPRESS", "sla_hours": 24.0},
    {"carrier_id": "FEDEX_EU", "name": "FedEx European Logistics", "service": "EXPRESS", "sla_hours": 36.0},
    {"carrier_id": "CHRONOPOST", "name": "Chronopost France", "service": "STANDARD", "sla_hours": 48.0},
    {"carrier_id": "DPD_LOCAL", "name": "DPD Continental Network", "service": "GROUND", "sla_hours": 72.0},
]

WAREHOUSES = ["WH-PARIS-01", "WH-BERLIN-02", "WH-MADRID-03", "WH-MILAN-04", "WH-AMST-05"]
CURRENCIES = ["EUR", "EUR", "EUR", "USD", "GBP"]
MERCHANT_TIERS = ["PLATINUM", "GOLD", "SILVER", "BRONZE"]
BOX_TYPES = ["SMALL", "MEDIUM", "LARGE", "PALLET"]
COUNTRIES = ["FR", "DE", "ES", "IT", "NL", "BE"]


def load_config(config_path: Path) -> dict:
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "seed": 42,
        "default_record_count": 10000,
        "rates": {
            "orders": {
                "null_merchant_id_rate": 0.03,
                "future_timestamp_rate": 0.02,
                "invalid_negative_declared_value_rate": 0.02,
                "corrupted_postal_code_rate": 0.02,
            },
            "wms_picks": {
                "negative_or_zero_weight_rate": 0.035,
                "missing_order_id_rate": 0.02,
                "inverted_timestamps_rate": 0.025,
                "extreme_weight_anomaly_rate": 0.01,
            },
            "carrier_events": {
                "network_duplicate_rate": 0.05,
                "late_arriving_event_rate": 0.04,
                "corrupted_event_id_rate": 0.015,
                "missing_timestamp_rate": 0.01,
            },
        },
    }


def generate_merchants(fake: Faker, count: int = 50) -> list[dict]:
    merchants = []
    base_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for i in range(1, count + 1):
        created_at = base_date + timedelta(days=random.randint(0, 365))
        merchants.append({
            "merchant_id": f"MERCH-{i:04d}",
            "merchant_name": fake.company(),
            "merchant_tier": random.choice(MERCHANT_TIERS),
            "country_code": random.choice(COUNTRIES),
            "created_at": created_at.isoformat(),
        })
    return merchants


def generate_data(
    num_orders: int,
    output_dir: Path,
    config: dict,
    partition_date: str = None,
):
    seed = config.get("seed", 42)
    random.seed(seed)
    fake = Faker()
    fake.seed_instance(seed)

    order_rates = config.get("rates", {}).get("orders", {})
    wms_rates = config.get("rates", {}).get("wms_picks", {})
    carrier_rates = config.get("rates", {}).get("carrier_events", {})

    target_dt = partition_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base_timestamp = datetime.strptime(target_dt, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # Output paths (simulating Bronze landing buckets)
    orders_dir = output_dir / "raw" / "postgres_orders" / f"dt={target_dt}"
    merchants_dir = output_dir / "raw" / "postgres_merchants"
    wms_dir = output_dir / "raw" / "wms_picks" / f"dt={target_dt}"
    carrier_dir = output_dir / "raw" / "carrier_events" / f"dt={target_dt}"

    for d in [orders_dir, merchants_dir, wms_dir, carrier_dir]:
        d.mkdir(parents=True, exist_ok=True)

    logger.info(f"Generating synthetic legacy data for target_dt={target_dt}, count={num_orders} orders...")

    # 1. Generate Merchants (50 master merchants)
    merchants = generate_merchants(fake, count=50)
    merchant_ids = [m["merchant_id"] for m in merchants]
    merchants_file = merchants_dir / "merchants.csv"
    with open(merchants_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["merchant_id", "merchant_name", "merchant_tier", "country_code", "created_at"])
        writer.writeheader()
        writer.writerows(merchants)
    logger.info(f"Wrote {len(merchants)} merchants to {merchants_file}")

    # 2. Generate Orders with anomalies
    orders = []
    order_ids = []
    anomaly_counts = {
        "null_merchant_id": 0,
        "future_timestamp": 0,
        "negative_declared_value": 0,
        "corrupted_postal": 0,
    }

    now_utc = datetime.now(timezone.utc)

    for i in range(1, num_orders + 1):
        order_id = f"ORD-{target_dt.replace('-', '')}-{i:06d}"
        order_ids.append(order_id)

        # Anomaly 1: Null or blank merchant_id
        if random.random() < order_rates.get("null_merchant_id_rate", 0.03):
            merchant_id = ""
            anomaly_counts["null_merchant_id"] += 1
        else:
            merchant_id = random.choice(merchant_ids)

        # Base timestamp within the day
        order_seconds = random.randint(0, 86399)
        order_time = base_timestamp + timedelta(seconds=order_seconds)

        # Anomaly 2: Future order timestamp (simulating timezone misconfiguration or clock drift)
        if random.random() < order_rates.get("future_timestamp_rate", 0.02):
            order_time = now_utc + timedelta(days=random.randint(2, 30))
            anomaly_counts["future_timestamp"] += 1

        # Anomaly 3: Negative or zero declared value
        if random.random() < order_rates.get("invalid_negative_declared_value_rate", 0.02):
            declared_value = -round(random.uniform(5.0, 300.0), 2)
            anomaly_counts["negative_declared_value"] += 1
        else:
            declared_value = round(random.uniform(10.0, 850.0), 2)

        # Anomaly 4: Corrupted postal code
        if random.random() < order_rates.get("corrupted_postal_code_rate", 0.02):
            postal_code = "??INVALID##"
            anomaly_counts["corrupted_postal"] += 1
        else:
            postal_code = fake.postcode()

        orders.append({
            "order_id": order_id,
            "merchant_id": merchant_id,
            "customer_postal_code": postal_code,
            "customer_country": random.choice(COUNTRIES),
            "declared_value": declared_value,
            "currency": random.choice(CURRENCIES),
            "order_timestamp": order_time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    orders_file = orders_dir / f"orders_{target_dt.replace('-', '')}.csv"
    with open(orders_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["order_id", "merchant_id", "customer_postal_code", "customer_country", "declared_value", "currency", "order_timestamp"],
        )
        writer.writeheader()
        writer.writerows(orders)
    logger.info(f"Wrote {len(orders)} orders to {orders_file} | Injected anomalies: {anomaly_counts}")

    # 3. Generate WMS Picks & Packaging batch CSV
    wms_picks = []
    wms_anomaly_counts = {
        "negative_or_zero_weight": 0,
        "missing_order_id": 0,
        "inverted_timestamps": 0,
        "extreme_weight": 0,
    }

    for idx, order in enumerate(orders):
        wms_record_id = f"WMS-REC-{idx+1:07d}"
        
        # Anomaly 1: Missing / orphan order ID
        if random.random() < wms_rates.get("missing_order_id_rate", 0.02):
            pick_order_id = ""
            wms_anomaly_counts["missing_order_id"] += 1
        else:
            pick_order_id = order["order_id"]

        warehouse_id = random.choice(WAREHOUSES)

        # Anomaly 2: Negative or zero parcel weight
        if random.random() < wms_rates.get("negative_or_zero_weight_rate", 0.035):
            parcel_weight_kg = random.choice([0.0, -1.25, -0.5, -15.0])
            wms_anomaly_counts["negative_or_zero_weight"] += 1
        elif random.random() < wms_rates.get("extreme_weight_anomaly_rate", 0.01):
            parcel_weight_kg = round(random.uniform(2500.0, 9999.0), 2)  # Sensor glitch: thousands of kg for parcel
            wms_anomaly_counts["extreme_weight"] += 1
        else:
            parcel_weight_kg = round(random.uniform(0.15, 28.5), 2)

        try:
            order_dt = datetime.strptime(order["order_timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            order_dt = base_timestamp

        picked_at = order_dt + timedelta(minutes=random.randint(15, 180))
        packed_at = picked_at + timedelta(minutes=random.randint(10, 60))
        dispatched_at = packed_at + timedelta(minutes=random.randint(30, 240))

        # Anomaly 3: Inverted timestamps (dispatched before packed or picked)
        if random.random() < wms_rates.get("inverted_timestamps_rate", 0.025):
            dispatched_at = picked_at - timedelta(hours=2)
            wms_anomaly_counts["inverted_timestamps"] += 1

        wms_picks.append({
            "wms_record_id": wms_record_id,
            "order_id": pick_order_id,
            "warehouse_id": warehouse_id,
            "parcel_weight_kg": parcel_weight_kg,
            "box_type": random.choice(BOX_TYPES),
            "picked_at": picked_at.strftime("%Y-%m-%d %H:%M:%S"),
            "packed_at": packed_at.strftime("%Y-%m-%d %H:%M:%S"),
            "dispatched_at": dispatched_at.strftime("%Y-%m-%d %H:%M:%S"),
        })

    wms_file = wms_dir / f"wms_dispatch_{target_dt.replace('-', '')}.csv"
    with open(wms_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["wms_record_id", "order_id", "warehouse_id", "parcel_weight_kg", "box_type", "picked_at", "packed_at", "dispatched_at"],
        )
        writer.writeheader()
        writer.writerows(wms_picks)
    logger.info(f"Wrote {len(wms_picks)} WMS picks to {wms_file} | Injected anomalies: {wms_anomaly_counts}")

    # 4. Generate Carrier Webhook Events (NDJSON)
    carrier_events = []
    carrier_anomaly_counts = {
        "network_duplicates": 0,
        "late_arriving": 0,
        "corrupted_event_id": 0,
        "missing_timestamp": 0,
    }

    event_statuses = ["ACCEPTED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]

    for idx, wms_item in enumerate(wms_picks):
        if not wms_item["order_id"]:
            continue

        carrier = random.choice(CARRIERS)
        tracking_number = f"TRK-{carrier['carrier_id'][:3]}-{idx+1:08d}"

        try:
            dispatch_time = datetime.strptime(wms_item["dispatched_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            dispatch_time = base_timestamp + timedelta(hours=4)

        current_time = dispatch_time
        # Generate lifecycle events
        num_scans = random.randint(2, len(event_statuses))
        for step_idx in range(num_scans):
            status = event_statuses[step_idx]
            current_time = current_time + timedelta(hours=random.uniform(2.0, 14.0))

            event_id = str(uuid.uuid4())
            scan_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            received_time_str = (current_time + timedelta(seconds=random.randint(1, 120))).strftime("%Y-%m-%d %H:%M:%S")

            # Anomaly: Late-arriving event (synced 3 to 5 days after physical scan)
            if random.random() < carrier_rates.get("late_arriving_event_rate", 0.04):
                received_time_str = (current_time + timedelta(days=random.randint(3, 5))).strftime("%Y-%m-%d %H:%M:%S")
                carrier_anomaly_counts["late_arriving"] += 1

            # Anomaly: Corrupted event ID
            if random.random() < carrier_rates.get("corrupted_event_id_rate", 0.015):
                event_id = ""
                carrier_anomaly_counts["corrupted_event_id"] += 1

            # Anomaly: Missing timestamp
            if random.random() < carrier_rates.get("missing_timestamp_rate", 0.01):
                scan_time_str = None
                carrier_anomaly_counts["missing_timestamp"] += 1

            event_payload = {
                "event_id": event_id,
                "tracking_number": tracking_number,
                "order_id": wms_item["order_id"],
                "carrier_id": carrier["carrier_id"],
                "event_type": status,
                "scan_timestamp": scan_time_str,
                "received_at": received_time_str,
                "location_code": f"HUB-{random.choice(COUNTRIES)}-{random.randint(1, 9)}",
                "exception_reason": "CONSIGNEE_ABSENT" if status == "FAILED_ATTEMPT" else None,
            }
            carrier_events.append(event_payload)

            # Anomaly: Duplicate webhook re-transmissions (mobile courier network glitch)
            if random.random() < carrier_rates.get("network_duplicate_rate", 0.05):
                duplicate_count = random.randint(1, 3)
                for _ in range(duplicate_count):
                    dup_payload = dict(event_payload)
                    # Received a few seconds later, same event_id
                    dup_payload["received_at"] = (
                        current_time + timedelta(seconds=random.randint(120, 600))
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    carrier_events.append(dup_payload)
                    carrier_anomaly_counts["network_duplicates"] += 1

    carrier_file = carrier_dir / f"events_{target_dt.replace('-', '')}.json"
    with open(carrier_file, "w", encoding="utf-8") as f:
        for ev in carrier_events:
            f.write(json.dumps(ev) + "\n")
    logger.info(f"Wrote {len(carrier_events)} carrier events to {carrier_file} | Injected anomalies: {carrier_anomaly_counts}")

    # Summary metrics
    summary = {
        "partition_date": target_dt,
        "seed": seed,
        "total_merchants": len(merchants),
        "total_orders": len(orders),
        "total_wms_picks": len(wms_picks),
        "total_carrier_events": len(carrier_events),
        "anomalies": {
            "orders": anomaly_counts,
            "wms_picks": wms_anomaly_counts,
            "carrier_events": carrier_anomaly_counts,
        },
    }
    summary_file = output_dir / "raw" / f"generation_summary_{target_dt.replace('-', '')}.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Generation complete. Summary written to {summary_file}")
    return summary


def main():
    parser = argparse.ArgumentParser(description="CloudScale Synthetic Legacy Data Generator")
    parser.add_argument("--count", type=int, default=10000, help="Number of orders to generate (default: 10,000)")
    parser.add_argument("--date", type=str, default=None, help="Target partition date YYYY-MM-DD (default: today)")
    parser.add_argument("--output", type=str, default="data", help="Target output directory (default: data)")
    parser.add_argument("--config", type=str, default="data_generator/configs/anomaly_rates.json", help="Anomaly rates config path")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    output_dir = project_root / args.output
    config_path = project_root / args.config

    config = load_config(config_path)
    generate_data(num_orders=args.count, output_dir=output_dir, config=config, partition_date=args.date)


if __name__ == "__main__":
    main()
