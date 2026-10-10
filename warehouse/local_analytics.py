"""Load successfully audited Silver snapshots into a local dbt rehearsal database."""

import argparse
from pathlib import Path

from warehouse.contracts import STRING_FIELDS
from warehouse.publish_silver import inspect_batch


def load_silver(root, database):
    import duckdb

    root = Path(root).resolve()
    dates = sorted(p.name[3:] for p in (root / "_audit").glob("dt=*") if p.is_dir())
    if not dates:
        raise ValueError("No audited Silver batches found")
    uploads = [item for day in dates for item in inspect_batch(root, day)]
    database = Path(database).resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    with duckdb.connect(str(database)) as conn:
        conn.execute("begin transaction")
        conn.execute("create schema if not exists raw_staging")
        for source in STRING_FIELDS:
            paths = [str(item["path"]) for item in uploads if item["source"] == source]
            conn.read_parquet(paths, hive_partitioning=True).create_view(
                "_silver_input", replace=True
            )
            conn.execute(
                f"create or replace table raw_staging.{source} as select * from _silver_input"
            )
            counts[source] = conn.execute(
                f"select count(*) from raw_staging.{source}"
            ).fetchone()[0]
        conn.execute("commit")
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--database", default="data/analytics.duckdb")
    args = parser.parse_args()
    print(load_silver(args.silver_root, args.database))


if __name__ == "__main__":
    main()
