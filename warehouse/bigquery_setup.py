"""Idempotent dataset and external-table creation, with no destructive replacement."""

from google.cloud import bigquery

from warehouse.contracts import DATASETS, STRING_FIELDS, fields


def table_definition(config, source):
    schema = [
        bigquery.SchemaField(
            name,
            kind,
            precision=12 if kind == "NUMERIC" else None,
            scale=2 if kind == "NUMERIC" else None,
        )
        for name, kind in fields(source).items()
    ]
    external = bigquery.ExternalConfig("PARQUET")
    external.source_uris = [config.source_prefix(source) + "/*.parquet"]
    external.schema = schema
    hive = bigquery.HivePartitioningOptions()
    hive.mode = "CUSTOM"
    hive.source_uri_prefix = config.source_prefix(source) + "/{dt:DATE}"
    hive.require_partition_filter = True
    external.hive_partitioning = hive
    table = bigquery.Table(config.table_id(source))
    table.external_data_configuration = external
    table.description = "CloudScale Silver Parquet; filter dt for every query."
    return table


def external_signature(table):
    external = table.external_data_configuration
    if external is None or external.hive_partitioning is None:
        return None
    # BigQuery may return partition fields in table.schema separately.
    schema = external.schema or [f for f in table.schema if f.name != "dt"]
    return (
        external.source_format,
        tuple(sorted(external.source_uris)),
        external.hive_partitioning.mode,
        external.hive_partitioning.source_uri_prefix.rstrip("/"),
        external.hive_partitioning.require_partition_filter,
        tuple(
            sorted((f.name, f.field_type, f.mode, f.precision, f.scale) for f in schema)
        ),
    )


def apply_setup(client, config):
    results = {"datasets": [], "tables": []}
    for name in DATASETS:
        dataset = bigquery.Dataset(f"{config.project}.{name}")
        dataset.location = config.location
        dataset.description = "CloudScale " + name.replace("_", " ")
        created = client.create_dataset(dataset, exists_ok=True)
        if created.location.upper() != config.location.upper():
            raise ValueError(f"Dataset {name} exists in an incompatible location")
        results["datasets"].append(created.full_dataset_id)
    for source in STRING_FIELDS:
        desired = table_definition(config, source)
        existing = client.create_table(desired, exists_ok=True)
        if external_signature(existing) != external_signature(desired):
            raise ValueError(
                f"Existing table {desired.table_id} differs from the contract; review it manually"
            )
        results["tables"].append(existing.full_table_id)
    return results


def verification_sql(config, source):
    return f"SELECT COUNT(*) AS records, COUNTIF(_batch_date IS NULL OR _batch_date != dt) AS wrong_batch_date FROM `{config.table_id(source)}` WHERE dt = @batch_date"


def verify(client, config, batch_date, execute=False):
    from datetime import date

    parsed = date.fromisoformat(batch_date)
    if parsed.isoformat() != batch_date:
        raise ValueError("Batch date must use YYYY-MM-DD")
    results = {}
    for source in STRING_FIELDS:
        job_config = bigquery.QueryJobConfig(
            dry_run=not execute,
            use_query_cache=False,
            maximum_bytes_billed=config.maximum_bytes_billed,
            query_parameters=[
                bigquery.ScalarQueryParameter("batch_date", "DATE", parsed)
            ],
        )
        job = client.query(
            verification_sql(config, source),
            job_config=job_config,
            location=config.location,
        )
        if execute:
            rows = list(job.result())
            results[source] = {
                "records": rows[0]["records"],
                "wrong_batch_date": rows[0]["wrong_batch_date"],
                "bytes_processed": job.total_bytes_processed,
                "bytes_billed": job.total_bytes_billed,
            }
            if rows[0]["wrong_batch_date"]:
                raise ValueError(
                    f"Batch metadata does not match Hive date for {source}"
                )
        else:
            results[source] = {
                "dry_run": True,
                "estimated_bytes": job.total_bytes_processed,
            }
    return results
