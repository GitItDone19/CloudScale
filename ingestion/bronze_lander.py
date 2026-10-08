"""
CloudScale — Bronze Lander Utility (Phase 4)
============================================
Handles idempotent, atomic writes from source files into the partitioned
Bronze (raw) landing zone, maintaining full immutability guarantees.

Design Rules:
  - Partitioned output path: {bronze_root}/{source}/{dt=YYYY-MM-DD}/{filename}
  - Idempotency: Re-running for the same partition is a no-op if the file
    already exists and its SHA-256 checksum matches the source.
  - Atomic write: Files are staged to a unique sibling before atomic publication
    so partial uploads never corrupt the landing zone.
  - Immutability: Existing partition files are NEVER modified; only new
    partitions are written unless overwrite is explicitly requested.
  - Metadata sidecar: Each landed file gets a `{filename}.meta.json` sidecar
    recording ingestion_timestamp, source_path, record_count, and checksum.

Storage support:
  - Local filesystem, used during Docker Compose development.
  - GCS uploads are planned; this utility currently supports local paths only.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("bronze_lander")


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    """Return hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _count_lines(path: Path, has_header: bool = True) -> int:
    """Count CSV records, including quoted fields containing newlines."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            total = sum(1 for _ in csv.reader(fh))
        return max(0, total - (1 if has_header else 0))
    except Exception:
        return -1


def _count_ndjson(path: Path) -> int:
    """Count non-empty lines in an NDJSON file."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return sum(1 for line in fh if line.strip())
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Core landing function
# ---------------------------------------------------------------------------


def land_file(
    source_path: Path,
    bronze_root: Path,
    source_name: str,
    partition_date: str,
    file_type: str = "csv",
    overwrite: bool = False,
) -> dict:
    """
    Idempotently land a single source file into the Bronze zone.

    Args:
        source_path:    Absolute path to the source file to ingest.
        bronze_root:    Root directory of the Bronze landing zone
                        (e.g. Path("data/bronze")).
        source_name:    Logical source name used as the sub-directory
                        (e.g. "postgres_orders", "wms_picks", "carrier_events").
        partition_date: ISO date string for the Hive partition (YYYY-MM-DD).
        file_type:      One of "csv", "json", "ndjson". Affects record counting.
        overwrite:      If True, overwrite existing files even if checksums match.
                        Default False (safe / idempotent mode).

    Returns:
        dict: Ingestion result metadata (status, records, checksum, path).
    """
    if (
        datetime.strptime(partition_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        != partition_date
    ):
        raise ValueError("partition_date must use YYYY-MM-DD")
    if (
        not source_name
        or source_name in (".", "..")
        or any(c in source_name for c in "/\\")
    ):
        raise ValueError("source_name must be a single directory name")
    if file_type not in ("csv", "json", "ndjson"):
        raise ValueError("file_type must be csv, json, or ndjson")
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    # Build target partition directory
    partition_dir = bronze_root / source_name / f"dt={partition_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    target_path = partition_dir / source_path.name
    meta_path = partition_dir / f"{source_path.name}.meta.json"
    existed = target_path.exists()

    source_checksum = _sha256(source_path)

    # Check the actual landed bytes rather than trusting a stale sidecar.
    if existed and not overwrite:
        if _sha256(target_path) != source_checksum:
            raise FileExistsError(
                f"Bronze conflict at {target_path}; use a new partition or explicit overwrite"
            )
        if meta_path.exists():
            try:
                existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                existing_meta = {}
            if (
                isinstance(existing_meta, dict)
                and existing_meta.get("checksum_sha256") == source_checksum
                and existing_meta.get("ingestion_timestamp")
            ):
                return {
                    "status": "skipped",
                    "source": str(source_path),
                    "target": str(target_path),
                    "partition_date": partition_date,
                    "checksum_sha256": source_checksum,
                    "ingestion_timestamp": existing_meta["ingestion_timestamp"],
                    "records_landed": existing_meta.get("records_landed", -1),
                }
        # A previous attempt may have published data before writing metadata.
        # Repair the sidecar without modifying the landed file.
    else:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=partition_dir, prefix=".landing-", delete=False
            ) as staged:
                tmp_path = Path(staged.name)
            shutil.copy2(source_path, tmp_path)
            if _sha256(tmp_path) != source_checksum:
                raise RuntimeError("Source changed during landing")
            if overwrite:
                tmp_path.replace(target_path)
            else:
                # Hard-link publication fails if another writer won the race.
                target_path.hardlink_to(tmp_path)
        except Exception as exc:
            raise RuntimeError(
                f"Atomic write failed for {source_path} -> {target_path}: {exc}"
            ) from exc
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

    # Count records for metadata
    if file_type in ("ndjson", "json"):
        records_landed = _count_ndjson(target_path)
    else:
        records_landed = _count_lines(target_path, has_header=True)

    # Write metadata sidecar
    meta = {
        "source_path": str(source_path),
        "target_path": str(target_path),
        "source_name": source_name,
        "partition_date": partition_date,
        "file_type": file_type,
        "checksum_sha256": source_checksum,
        "records_landed": records_landed,
        "ingestion_timestamp": ingestion_ts,
        "overwrite": overwrite,
    }
    meta_tmp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=partition_dir,
            prefix=".metadata-",
            delete=False,
        ) as f:
            meta_tmp = Path(f.name)
            json.dump(meta, f, indent=2)
        meta_tmp.replace(meta_path)
    finally:
        if meta_tmp is not None:
            meta_tmp.unlink(missing_ok=True)

    action = "overwritten" if (overwrite and existed) else "landed"
    logger.info(
        f"[OK] {action.upper()} {source_path.name} -> {target_path} "
        f"({records_landed} records, checksum={source_checksum[:12]}...)"
    )

    return {
        "status": action,
        "source": str(source_path),
        "target": str(target_path),
        "partition_date": partition_date,
        "checksum_sha256": source_checksum,
        "ingestion_timestamp": ingestion_ts,
        "records_landed": records_landed,
    }


def land_directory(
    source_dir: Path,
    bronze_root: Path,
    source_name: str,
    partition_date: str,
    file_extensions: tuple = (".csv", ".json"),
    file_type: str = "csv",
    overwrite: bool = False,
) -> list:
    """
    Land all matching files in a directory partition into Bronze.

    Args:
        source_dir:       Directory containing source files for one partition.
        bronze_root:      Root Bronze landing directory.
        source_name:      Logical source name (sub-directory under bronze_root).
        partition_date:   ISO date string (YYYY-MM-DD).
        file_extensions:  Tuple of file extensions to match.
        file_type:        "csv", "json", or "ndjson".
        overwrite:        Overwrite even if checksum matches.

    Returns:
        List of per-file result metadata dicts.
    """
    results = []
    matched = sorted(
        f for f in source_dir.iterdir() if f.is_file() and f.suffix in file_extensions
    )
    if not matched:
        logger.warning(f"[WARN] No {file_extensions} files found in {source_dir}")
        return results

    for source_file in matched:
        result = land_file(
            source_path=source_file,
            bronze_root=bronze_root,
            source_name=source_name,
            partition_date=partition_date,
            file_type=file_type,
            overwrite=overwrite,
        )
        results.append(result)

    return results
