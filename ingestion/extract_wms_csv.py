#!/usr/bin/env python3
"""
CloudScale — Warehouse WMS CSV Extractor (Phase 4)
==================================================
Simulates a daily FTP batch pull of the Warehouse Management System (WMS)
pick-and-pack dispatch CSV exports from regional fulfillment centers.

In production this would SFTP-pull from:
    ftp://{wms_host}/exports/wms_dispatch_{YYYYMMDD}.csv

Locally it reads from the Phase 2 generated data:
    data/raw/wms_picks/dt={date}/wms_dispatch_{YYYYMMDD}.csv

Landing strategy:
    - Partitioned by dispatch date → data/bronze/wms_picks/dt={date}/
    - Each CSV file is landed atomically with checksum verification.
    - Re-running for the same partition date is idempotent (no-op if unchanged).

Real-world challenge simulated:
    - Silent schema drifts (extra/missing columns across WMS software versions)
    - Pipe vs. comma delimiter ambiguity (common with legacy WMS exports)
    - Files are NOT cleaned here — Bronze maintains raw fidelity.
      PySpark handles schema enforcement in Phase 5.
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
logger = logging.getLogger("extract_wms_csv")

SOURCE_NAME = "wms_picks"


def extract_wms_picks(
    source_raw: Path,
    bronze_root: Path,
    partition_date: str,
    overwrite: bool = False,
) -> list:
    """
    Extract and land WMS pick/dispatch CSV files for the given partition date.

    Args:
        source_raw:      Root of the raw source data (e.g. data/raw).
        bronze_root:     Bronze landing zone root.
        partition_date:  Target date string YYYY-MM-DD.
        overwrite:       Force re-land even if checksum matches.

    Returns:
        List of ingestion result dicts.
    """
    source_partition_dir = source_raw / SOURCE_NAME / f"dt={partition_date}"

    if not source_partition_dir.exists():
        logger.warning(
            f"[WARN] No WMS source data found for dt={partition_date} "
            f"at {source_partition_dir}. Run data_generator first."
        )
        return []

    logger.info(
        f"Extracting WMS dispatch CSV(s) for partition dt={partition_date} "
        f"from {source_partition_dir}"
    )

    results = land_directory(
        source_dir=source_partition_dir,
        bronze_root=bronze_root,
        source_name=SOURCE_NAME,
        partition_date=partition_date,
        file_extensions=(".csv",),
        file_type="csv",
        overwrite=overwrite,
    )

    total_records = sum(
        r.get("records_landed", 0)
        for r in results
        if isinstance(r.get("records_landed"), int) and r["records_landed"] > 0
    )
    logger.info(
        f"WMS extraction complete: {len(results)} file(s) processed, "
        f"{total_records} rows landed."
    )
    return results


def run(
    partition_date: str,
    bronze_root: Path,
    source_raw: Path,
    overwrite: bool = False,
) -> dict:
    """
    Run the WMS extraction for a given logical date.

    Args:
        partition_date:  YYYY-MM-DD (Airflow logical_date).
        bronze_root:     Bronze landing zone root.
        source_raw:      Source raw data root.
        overwrite:       Force overwrite.

    Returns:
        Summary dict.
    """
    logger.info(f"=== WMS CSV Extractor START | partition_date={partition_date} ===")

    results = extract_wms_picks(source_raw, bronze_root, partition_date, overwrite)

    summary = {
        "extractor": "extract_wms_csv",
        "partition_date": partition_date,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "files_processed": len(results),
        "files_landed": sum(
            1 for r in results if r.get("status") in ("landed", "overwritten")
        ),
        "files_skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "total_records": sum(
            r.get("records_landed", 0)
            for r in results
            if isinstance(r.get("records_landed"), int) and r["records_landed"] > 0
        ),
        "results": results,
    }

    logger.info(
        f"=== WMS CSV Extractor DONE | "
        f"landed={summary['files_landed']}, skipped={summary['files_skipped']}, "
        f"records={summary['total_records']} ==="
    )
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="CloudScale — WMS CSV Extractor (Bronze Landing)"
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
        default="data/bronze",
        help="Bronze landing zone root directory (default: data/bronze)",
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
