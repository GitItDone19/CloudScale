# Phase 5 — PySpark cleansing and Silver datasets

CloudScale now has two local Spark jobs. They read the original Bronze files,
separate valid rows from rejected rows, and write compressed Parquet datasets.
No Bronze file is edited by these jobs.

## Run it

First generate and ingest a batch using the README's Bronze quick start. Then,
with Docker running, build the isolated Spark runner:

```powershell
docker compose --profile silver build spark-jobs
docker compose --profile silver run --rm spark-jobs -m spark.jobs.process_shipments --date 2026-10-01
docker compose --profile silver run --rm spark-jobs -m spark.jobs.process_carrier_events --date 2026-10-01
docker compose --profile silver run --rm spark-jobs -m pytest tests -q
```

This service runs Spark locally with two worker threads. It does not start the
Airflow or Spark master/worker services and does not require cloud credentials.
Spark's local mode executes the same DataFrame transformations used on a cluster;
it does not demonstrate multi-machine performance.

For a native setup, use Python 3.10 or 3.11, Java 17, and
`python -m pip install -r requirements-spark.txt`, then run:

```powershell
python -m spark.jobs.process_shipments --date 2026-10-01
python -m spark.jobs.process_carrier_events --date 2026-10-01
```

The Docker route provides a Linux filesystem and avoids native Windows Hadoop
utility requirements. `--master` selects Spark's execution target; the tested
runner uses `local[2]`. A remote master needs matching Python dependencies and
input/output paths available to its executors.

## Follow one parcel

Suppose a warehouse sends a parcel weight of `-1.25` kg. Bronze preserves that
original value. Spark safely converts the value into a number, applies the
weight rule, and sends the row to the dead-letter queue with
`ERR_INVALID_WEIGHT`. A valid parcel weighing `1.25` kg continues to Silver.

Suppose a courier sends the same delivery scan three times. Spark checks the
records first, then groups valid events by `event_id`, keeping the latest scan
timestamp, then latest received timestamp. A stable payload hash resolves ties.
The duplicates are counted in the audit summary; they are not mislabeled as
invalid records. Deduplication applies within the selected Bronze batch, not
across all historical dates. Global reconciliation is a later warehouse concern.

A scan received four days after it happened remains valid. Its physical scan
and receipt timestamps are retained separately. This phase does not implement
the planned dbt late-arrival merge or lookback window.

## What each job handles

| Job | Inputs | Clean outputs |
| --- | --- | --- |
| `process_shipments` | Orders, merchant snapshots, warehouse picks | Three separate typed Silver tables |
| `process_carrier_events` | Carrier NDJSON | One typed, deduplicated Silver event table |

The shipment job does not build the final joined shipment fact. Foreign-key
relationships, financial conversion, and dimensional modeling remain warehouse
work for later phases. Currency values remain in their original currencies;
no exchange rates are fabricated.

Every read uses an explicit schema. Raw fields begin as strings so invalid
numbers and dates can be detected and quarantined instead of crashing the
batch. Trimming removes surrounding whitespace, blank strings become null, and
country/currency/tier/event-type codes are standardized to uppercase.

Carrier payloads accept both `tracking_number` and the alternative
`tracking_code`, preferring `tracking_number` when both are present. Optional
`customs_fee` and `currency` fields are validated when supplied. This supports
the documented example of schema evolution, not arbitrary future payloads.

## Quality rules

| Error code | Meaning |
| --- | --- |
| `ERR_MALFORMED_RECORD` | Spark could not parse the input record |
| `ERR_NULL_KEY` | A required identifier is missing or blank |
| `ERR_INVALID_VALUE` | Invalid declared amount or supplied customs fee |
| `ERR_INVALID_WEIGHT` | Weight is not finite or not strictly between 0.01 and 1000 kg |
| `ERR_INVALID_TIMESTAMP` | A required timestamp is missing or cannot be parsed |
| `ERR_FUTURE_TIMESTAMP` | Order or merchant creation timestamp is later than the validation reference |
| `ERR_TIMESTAMP_SEQUENCE` | Warehouse activity times are reversed, or receipt precedes the scan |
| `ERR_INVALID_POSTAL_CODE` | Missing postal code or unsupported characters/length |
| `ERR_INVALID_COUNTRY` | Country code is not two letters |
| `ERR_INVALID_CURRENCY` | Currency is missing where required or outside EUR/USD/GBP |
| `ERR_MISSING_FIELD` | Required merchant name is missing |
| `ERR_INVALID_TIER` | Merchant tier is outside the documented set |
| `ERR_INVALID_EVENT_TYPE` | Carrier status is outside the documented set |

Country and postal checks validate format, not real-world existence. Warehouse
and carrier timestamps are checked for parseability and ordering; future-time
validation is specifically applied to orders and merchant creation timestamps.
CSV headers must match the contracted names and order. Incompatible headers fail
the job rather than silently assigning values to the wrong columns. Spark's CSV
parser does not flag every extra/missing token pattern as corrupt; required
fields still receive quality checks. JSON malformed records retain the exact
original line. CSV rejected rows retain parsed source fields as JSON (or Spark's
corrupt-record text when available). Bronze remains the original-byte reference.

## Outputs and audits

```text
data/
├── bronze/{source}/dt=2026-10-01/       # Unchanged originals
├── silver/{source}/dt=2026-10-01/       # Clean Snappy Parquet
├── silver/_audit/dt=2026-10-01/
│   ├── shipments.json
│   └── carrier_events.json
└── deadletter/{source}/dt=2026-10-01/   # Rejected Parquet + reasons
```

Each audit summary contains input, clean, rejected, and duplicate counts, plus
counts by error code. The accounting equation is:

`input = clean + rejected + duplicates_removed`

A rejected row can have several errors. `_error_codes` records all of them;
`_error_code` records the first rule that failed. Per-error counts can therefore
sum to more than the number of rejected rows.

Outputs include source filenames, batch date, and processing time. Event
`received_at` remains the source receipt time; processing time does not replace it.
Use `--as-of 2026-10-09T00:00:00+00:00` to fix the validation clock for replayable
tests. By default the clock is captured once at job start. Reruns without a fixed
clock may accept formerly future-dated input after time has passed.

## Reruns and failures

A rerun rebuilds only the selected output date partition. It replaces that
partition after a complete staged write; other dates remain intact. Publication
uses a temporary directory and backup/rollback on rename failure. Empty results
are also published, so old rejected rows do not survive a newly clean rerun.

Publication is per dataset, not one transaction across all sources and both
Silver and DLQ. The audit records running, success, or failure; a failed run can
leave a mix of previously successful and newly published outputs. Rerun the
same batch to reconcile it, and only consume a batch after a successful audit.
The jobs assume one writer per job and batch; overlapping concurrent runs are
not supported. A hard process or machine crash can leave staging/backup folders
for manual investigation. Output roots must not overlap each other or Bronze.

## Scope after phase 5

Local cleansing is implemented. BigQuery provisioning, dbt warehouse models,
full Airflow orchestration, cloud uploads, and business dashboards remain planned.
See the README for current verification results.

## Example run on 9 October 2026

A generated batch for `2026-10-01`, using 1,000 orders and the repository's
anomaly configuration, produced these local Spark results:

| Source | Input rows | Clean Silver rows | Rejected rows | Valid duplicates removed |
| --- | ---: | ---: | ---: | ---: |
| Orders | 1,000 | 902 | 98 | 0 |
| Merchants | 50 | 50 | 0 | 0 |
| Warehouse picks | 1,000 | 911 | 89 | 0 |
| Carrier events | 3,214 | 2,853 | 96 | 265 |

Both jobs completed with successful audit summaries and Snappy Parquet outputs.
These are synthetic demonstration results, not measurements from a real logistics
company. Exact generated data can vary with the clock and dependency versions.

## Verification checkpoint

On 9 October 2026, the isolated runner built successfully with Python 3.11,
Java 17, and Spark 3.5.1. The full suite completed with **23 tests passed**
in 29.55 seconds, with no skipped Spark tests. The nine new cases cover:

- Order validation, safe casts, normalization, and multiple rejection reasons.
- Parcel weight boundaries, malformed dates, missing keys, and activity ordering.
- Carrier payload aliases, late arrivals, invalid records, and stable deduplication.
- Rejection of overlapping output roots and invalid batch dates.
- Generated source ingestion through Silver/DLQ Parquet, malformed JSON handling,
  accounting, unchanged Bronze files, and reruns preserving other dates.
- Missing input handling, failed staged writes, and publication rename rollback.

Formatting, linting of the new Python code, README links, SVG syntax, and Docker
Compose configuration checks passed. The separate Airflow/master-worker stack
still has not been runtime-validated; this checkpoint verifies the isolated
local Spark runner and phase 5 jobs.
