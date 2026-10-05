#!/usr/bin/env python3
"""
CloudScale — Carrier Webhook JSON Extractor (Phase 4)
=====================================================
Simulates a carrier webhook event sink that reads mobile courier delivery
scan events (NDJSON format) and lands them idempotently into the Bronze zone.

In production this would:
    - Listen on an HTTP endpoint (Cloud Run / Cloud Functions)
    - Or pull from a Pub/Sub subscription (streaming ingestion)
    - Or drain a message queue buffer to NDJSON files

Locally it reads from the Phase 2 generated data:
    data/raw/carrier_events/dt={date}/events_{YYYYMMDD}.json

Landing strategy:
    - Partitioned by ingestion date (NOT scan event time) → data/raw/carrier_events/dt={date}/
    - Decouples ingestion_time from event_time to handle late-arriving scans (Phase 3 challenge).
    - Files are immutable once landed; deduplication and late-arrival reconciliation
      happen downstream in PySpark (Phase 5) and dbt incremental models (Phase 7).

Real-world challenges simulated:
    - Network duplicate re-transmissions (same event_id received 3-5x)
    - Late-arriving events (scanned 3-5 days before reaching the network)
    - Corrupted event IDs (empty string or null)
    - Missing scan timestamps
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.bronze_lander import land_directory  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("extract_carrier_webhooks")

SOURCE_NAME = "carrier_events"


def extract_carrier_events(
    source_raw: Path,
    bronze_root: Path,
    partition_date: str,
    overwrite: bool = False,
) -> list:
    """
    Extract and land carrier webhook NDJSON files for the given partition date.

    Args:
        source_raw:      Root of the raw source data (e.g. data/raw).
        bronze_root:     Bronze landing zone root.
        partition_date:  Target ingestion date string YYYY-MM-DD.
        overwrite:       Force re-land even if checksum matches.

    Returns:
        List of ingestion result dicts.
    """
    source_partition_dir = source_raw / SOURCE_NAME / f"dt={partition_date}"

    if not source_partition_dir.exists():
        logger.warning(
            f"[WARN] No carrier event data found for dt={partition_date} "
            f"at {source_partition_dir}. Run data_generator first."
        )
        return []

    logger.info(
        f"Extracting carrier webhook events for partition dt={partition_date} "
        f"from {source_partition_dir}"
    )

    results = land_directory(
        source_dir=source_partition_dir,
        bronze_root=bronze_root,
        source_name=SOURCE_NAME,
        partition_date=partition_date,
        file_extensions=(".json",),
        file_type="ndjson",
        overwrite=overwrite,
    )

    total_events = sum(
        r.get("records_landed", 0) for r in results
        if isinstance(r.get("records_landed"), int) and r["records_landed"] > 0
    )
    logger.info(
        f"Carrier webhook extraction complete: {len(results)} file(s) processed, "
        f"{total_events} events landed (including duplicates for downstream dedup)."
    )
    return results


def run(
    partition_date: str,
    bronze_root: Path,
    source_raw: Path,
    overwrite: bool = False,
) -> dict:
    """
    Run the carrier webhook extraction for a given logical date.

    Args:
        partition_date:  YYYY-MM-DD (Airflow logical_date).
        bronze_root:     Bronze landing zone root.
        source_raw:      Source raw data root.
        overwrite:       Force overwrite.

    Returns:
        Summary dict.
    """
    logger.info(
        f"=== Carrier Webhook Extractor START | partition_date={partition_date} ==="
    )

    results = extract_carrier_events(source_raw, bronze_root, partition_date, overwrite)

    summary = {
        "extractor": "extract_carrier_webhooks",
        "partition_date": partition_date,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "files_processed": len(results),
        "files_landed": sum(1 for r in results if r.get("status") in ("landed", "overwritten")),
        "files_skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "total_events": sum(
            r.get("records_landed", 0) for r in results
            if isinstance(r.get("records_landed"), int) and r["records_landed"] > 0
        ),
        "results": results,
    }

    logger.info(
        f"=== Carrier Webhook Extractor DONE | "
        f"landed={summary['files_landed']}, skipped={summary['files_skipped']}, "
        f"events={summary['total_events']} ==="
    )
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="CloudScale — Carrier Webhook Extractor (Bronze Landing)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Partition date YYYY-MM-DD (default: today UTC)",
    )
    parser.add_argument(
        "--bronze-root",
        type=str,
        default="data/raw",
        help="Bronze landing zone root directory (default: data/raw)",
    )
    parser.add_argument(
        "--source-raw",
        type=str,
        default="data/raw",
        help="Source raw data root (default: data/raw)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force re-land even if checksum matches",
    )
    args = parser.parse_args()

    bronze_root = PROJECT_ROOT / args.bronze_root
    source_raw = PROJECT_ROOT / args.source_raw

    summary = run(
        partition_date=args.date,
        bronze_root=bronze_root,
        source_raw=source_raw,
        overwrite=args.overwrite,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
