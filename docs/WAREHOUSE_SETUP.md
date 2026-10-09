# BigQuery staging setup

This module connects the local Silver layer to the planned cloud warehouse.
It can generate a setup plan without credentials. Authenticated commands publish
validated Parquet files to an existing GCS bucket and create four datasets plus
four external staging tables. Live deployment and query verification remain
pending until a real project, bucket, and credentials are configured.

## What is created?

| Dataset | Purpose |
| --- | --- |
| `raw_staging` | External tables over Silver Parquet in GCS |
| `analytics_core` | Reserved for later dbt facts and dimensions |
| `analytics_marts` | Reserved for later business metrics |
| `ops_monitoring` | Reserved for later warehouse audit tables |

Staging tables are `postgres_orders`, `postgres_merchants`, `wms_picks`, and
`carrier_events`. Their explicit schemas match the Spark outputs, including
NUMERIC amounts, timestamps, and batch metadata. Monetary fields preserve their
original currency; no exchange-rate conversion is performed here.

An external table is a catalog entry pointing to files in Cloud Storage. It
does not copy those files into native BigQuery storage. The path
`silver/v1/carrier_events/dt=2026-10-01/part-00000.parquet` supplies a DATE partition
column named `dt`. Each query must filter that column. These are Hive partitions,
not native BigQuery table partitioning or clustering.

## Install and inspect offline

```powershell
py -3.11 -m venv .venv-warehouse
.\.venv-warehouse\Scripts\Activate.ps1
python -m pip install -r requirements-warehouse.txt
python -m warehouse plan --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME
```

Replace both placeholders with real names before applying. The plan lists the
datasets, source URIs, column contracts, and query scan cap. `plan` itself only
uses Python's standard library. The CLI does not automatically load `.env`.
It also accepts `GCP_PROJECT_ID`, `GCS_SILVER_BUCKET`, `BQ_LOCATION`, and
`GCS_SILVER_PREFIX` from the process environment.

After successfully running both Spark jobs, inspect an upload without contacting
Google Cloud:

```powershell
python -m warehouse upload --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME --date 2026-10-01 --silver-root data/silver
```

This validates successful audit summaries, Spark completion markers, Parquet
column types, row counts, and a default 50 MiB upload size limit. It lists the
canonical object names and checksums that would be uploaded. The uploader never
publishes dead-letter records or the local `_audit` folder.

## Configure Google Cloud

Use an existing project with the BigQuery and Cloud Storage APIs enabled, plus
an existing bucket in the exact same location as the datasets. The default is
`US`; this implementation intentionally requires an exact location match, even
though Google Cloud supports some additional colocation combinations.

If you manage your own project, install the Google Cloud CLI and adapt:

```powershell
gcloud auth login
gcloud auth application-default login
gcloud services enable bigquery.googleapis.com storage.googleapis.com --project YOUR_PROJECT_ID
gcloud storage buckets create gs://YOUR_BUCKET_NAME --project YOUR_PROJECT_ID --location US --uniform-bucket-level-access --public-access-prevention
```

Project creation, billing configuration, service account provisioning, and these
prerequisite commands are not executed by the Python module. Credentials are
resolved through Application Default Credentials. A configured service account
can also be used through `GOOGLE_APPLICATION_CREDENTIALS`; keep credential files
outside version control. Never put private keys in the README or command output.

Permissions needed include `bigquery.datasets.create`, dataset/table metadata
access, `bigquery.tables.create`, `bigquery.tables.getData`, and
`bigquery.jobs.create`, plus `storage.buckets.get`, `storage.objects.list`,
`storage.objects.get`, and `storage.objects.create` on the selected bucket.
An administrator should scope those permissions to this project and bucket.
Queries run under the caller's identity and require access to the referenced
GCS objects; the setup does not configure a BigLake connection or service account.

## Publish, bootstrap, and verify

```powershell
python -m warehouse upload --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME --date 2026-10-01 --silver-root data/silver --apply
python -m warehouse bootstrap --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME --apply
python -m warehouse verify --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME --date 2026-10-01
python -m warehouse verify --project YOUR_PROJECT_ID --bucket YOUR_BUCKET_NAME --date 2026-10-01 --apply
```

`upload --apply` writes objects. `bootstrap --apply` creates datasets and external
tables. Without `--apply`, bootstrap only prints the plan. Verification without
`--apply` submits BigQuery dry runs (authenticated, but it does not execute the
queries); verification with `--apply` executes counts for the requested date.

All verification queries use bound DATE parameters, disable the query cache,
and set `maximum_bytes_billed=104857600` (100 MiB). `--max-query-bytes` can reduce
that limit but cannot raise it. This guard applies to this module's verification
queries, not to all queries issued through the console, dbt, or other clients.
It does not cap GCS storage/operation charges or guarantee zero total cost.

Dry-run estimates over external sources can report zero bytes. A zero estimate
is not proof of no cost or of partition pruning. Live verification reports
processed/billed bytes, counts, and mismatched batch metadata. For a pruning
check, publish at least two dates, run the filtered verification for each date,
and inspect BigQuery job details and the scanned partitions. Local mocks do not
prove cloud authorization, SQL acceptance, or actual scan behavior.

## Safe reruns and conflicts

Canonical filenames avoid adding duplicate UUID-named Spark parts to a cloud
partition when uploading the same batch again. Identical remote objects are
skipped. Changed checksums or unexpected remote objects stop publication before
any new file is uploaded. Object writes use a generation precondition so an
existing object cannot be overwritten by a racing writer.

A locally rebuilt Spark batch can have new Parquet bytes or processing metadata,
even if its business rows are the same. If the remote snapshot differs, use a
new version prefix (for example `--prefix silver/v2`) and review which staging
tables should reference that version. The bootstrap refuses to replace existing
tables with a different schema or prefix; table migration requires deliberate
manual review. It also rejects existing datasets in another location.

Publication is not transactional across objects or tables. A network failure can
leave a partial upload; rerunning resumes matching objects. Until upload completes,
do not query that new batch or treat it as ready. Only one publisher should work
on a given batch at a time. The uploader assumes the validated local files remain
unchanged throughout publication; don't run Spark against the same batch during
upload. No existing cloud objects or datasets are deleted by these commands.

## Tests and references

Offline tests use real Google SDK table/query objects, simulated clients, and
actual PyArrow Parquet files. They cover schema and partition contracts, query
limits, dataset conflicts, upload preconditions, audit checks, and safe reruns.
Live cloud provisioning is not claimed by these tests.

Validation on 2026-10-10: the warehouse environment passed **32 tests**, with two
PySpark modules skipped because Spark is installed in its separate runner. This
includes **18 new warehouse tests**. An offline upload preview also validated all
seven Parquet parts from the existing local Silver demonstration batch. Formatting,
lint, and Git whitespace checks passed for the new code.

- [Google Cloud: external Hive partitions](https://docs.cloud.google.com/bigquery/docs/hive-partitioned-queries)
- [Google Cloud: create Cloud Storage external tables](https://docs.cloud.google.com/bigquery/docs/external-data-cloud-storage)
- [Google Cloud: query cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
- [Google Cloud: external-table limitations](https://docs.cloud.google.com/bigquery/docs/external-tables)
