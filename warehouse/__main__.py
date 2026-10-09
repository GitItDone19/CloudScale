"""Offline plan by default; authenticated cloud operations require --apply."""

import argparse
import json
import os

from warehouse.contracts import WarehouseConfig, plan


def main():
    parser = argparse.ArgumentParser(description="CloudScale BigQuery staging")
    parser.add_argument("command", choices=("plan", "upload", "bootstrap", "verify"))
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--bucket", default=os.getenv("GCS_SILVER_BUCKET"))
    parser.add_argument("--location", default=os.getenv("BQ_LOCATION", "US"))
    parser.add_argument("--prefix", default=os.getenv("GCS_SILVER_PREFIX", "silver/v1"))
    parser.add_argument("--date")
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--max-upload-bytes", type=int, default=52428800)
    parser.add_argument("--max-query-bytes", type=int, default=104857600)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform uploads/setup or execute verification queries",
    )
    args = parser.parse_args()
    if not args.project or not args.bucket:
        parser.error("Provide --project and --bucket or their environment variables")
    try:
        config = WarehouseConfig(
            args.project, args.bucket, args.location, args.prefix, args.max_query_bytes
        )
        if args.command in ("upload", "verify") and not args.date:
            parser.error("--date is required for upload and verify")
        if args.command == "plan" or (args.command == "bootstrap" and not args.apply):
            output = plan(config)
        elif args.command == "upload":
            from warehouse.publish_silver import inspect_batch, publish

            uploads = inspect_batch(args.silver_root, args.date, args.max_upload_bytes)
            if args.apply:
                from google.cloud import storage

                output = publish(
                    storage.Client(project=config.project), config, uploads
                )
            else:
                output = [
                    {
                        **{k: v for k, v in item.items() if k != "path"},
                        "path": str(item["path"]),
                        "object": f"{config.prefix}/{item['relative']}",
                    }
                    for item in uploads
                ]
        elif args.command == "bootstrap":
            from google.cloud import bigquery, storage
            from warehouse.bigquery_setup import apply_setup

            bucket = storage.Client(project=config.project).get_bucket(config.bucket)
            if bucket.location.upper() != config.location.upper():
                raise ValueError("Bucket and dataset locations must match exactly")
            output = apply_setup(bigquery.Client(project=config.project), config)
        else:
            from google.cloud import bigquery
            from warehouse.bigquery_setup import verify

            output = verify(
                bigquery.Client(project=config.project),
                config,
                args.date,
                execute=args.apply,
            )
        print(json.dumps(output, indent=2))
    except (ValueError, FileNotFoundError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
