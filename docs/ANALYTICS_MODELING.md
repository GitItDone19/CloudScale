# Analytics modeling with dbt

dbt turns SQL files into warehouse tables and tests their relationships. Spark
has already cleaned individual records; these models connect orders, merchants,
warehouse operations, and carrier events to answer business questions.

BigQuery is the deployment target. DuckDB runs the same project locally without
cloud credentials or cloud charges. A successful local build does not prove that
BigQuery permissions, SQL execution, MERGE behavior, or billing limits work live.

## What the project builds

| Layer | Models | Purpose |
| --- | --- | --- |
| Staging | Four `stg_*` tables | Keep the latest version of each business ID across ingestion batches |
| Intermediate | Latest WMS operation, shipment event summary | Avoid multiplying shipment rows when joining scans and warehouse records |
| Dimensions | Merchants, carriers, routes, dates | Describe the entities connected to the facts |
| Facts | Shipments, delivery events | One shipment per carrier/tracking pair; one event per event ID |
| Marts | Carrier performance, warehouse efficiency | Delivery SLA percentages and average handling times |

Stable hashes use length-prefixed values with distinct null markers, avoiding
ambiguous concatenations. A tracking number can appear at two carriers without
creating a key collision. Merchant and carrier dimensions include an unknown
member. Missing orders and warehouse operations are retained and flagged.
Routes use warehouse, full destination postal code, and country; no distance is
invented. Date keys use YYYYMMDD and cover dates observed in the staged data.
Dimensions describe current attributes (Type 1), not historical attribute versions.

## Run locally

From the repository root, using Python 3.11:

```powershell
python -m venv .venv-warehouse
.venv-warehouse/Scripts/python.exe -m pip install -r requirements-dbt.txt -r requirements-warehouse.txt
.venv-warehouse/Scripts/python.exe -m warehouse.local_analytics --silver-root data/silver --database data/analytics.duckdb
.venv-warehouse/Scripts/dbt.exe build --project-dir dbt --profiles-dir dbt --target local --vars '{run_date: "2026-10-01", history_start: "2026-10-01"}'
```

The loader validates all audited Silver batches before replacing local
`raw_staging` tables in one DuckDB transaction. It requires successful audits,
Spark completion markers, correct schemas, and matching row counts. It only
changes the local database. It rejects batches over 50 MiB each. The local dbt
profile defaults to `data/analytics.duckdb` relative to your working directory; set
`CLOUDSCALE_DUCKDB_PATH` to an absolute path when using another database.

Run `dbt build` again with the same variables to check reruns. After generating
and cleansing another batch, reload Silver and advance `run_date`. Use
`--full-refresh` with an appropriate `history_start` when rebuilding all history.
These commands do not generate Bronze or Silver; follow the README first.

## Late data and incremental updates

On the initial build, staging reads `dt` from `history_start` through `run_date`.
On later builds, it reads `run_date - lookback_days` through `run_date` (inclusive;
the default is three days back, covering four calendar dates). These are ingestion
partition dates, not scan timestamps. An event scanned on October 1 but ingested
on October 5 can update an October 1 shipment when the October 5 batch is processed.

Staging reconciles the selected source rows with its retained table and chooses
the latest version per ID. Events prefer the latest `received_at`, followed by
batch and processing metadata; other sources prefer the latest batch and
processing metadata. Older batch reruns retain newer versions already modeled.
This is current-state reconciliation, not an as-of historical reconstruction.
There is no delete/tombstone support. New records outside the ingestion lookback
need a wider lookback or full refresh. Dates beyond `run_date` are not read from
sources, but already-retained future batches remain on a historical rerun.
Carrier and tracking identity are assumed stable for an event ID. Corrections
that move an event to another shipment require a full refresh of the facts to
remove an obsolete shipment key; ordinary timestamp/status corrections merge.

Fact models recompute candidates from **all retained staging rows** each time.
BigQuery merges by unique key; DuckDB uses delete-and-insert by the same key.
This deliberately favors correct propagation of delayed orders, corrected events,
and WMS updates over minimum scan volume. It is not a large-scale optimization.
There is no destination-partition restriction that could duplicate an old shipment
when a new delivery arrives. Dimensions and marts rebuild completely.

## Metric definitions and limitations

- Delivered time is the earliest `DELIVERED` scan for the carrier/tracking pair.
  Current status comes from its latest scan, so later exceptions remain visible.
- Delivery duration is dispatch-to-delivery in hours. Impossible negative durations
  become null; `invalid_time_sequence` exposes cross-source chronology problems.
- SLA contracts are synthetic reference values matching the data generator.
  Unknown carriers and missing/invalid dispatch times have an unknown SLA outcome.
- On-time rate uses only delivered shipments with a measurable duration and known
  SLA. Pending and unmeasurable shipments are excluded from that denominator.
  No eligible shipments produces null, rather than a misleading zero.
- This is **on-time delivery**, not OTIF: there are no item quantities to measure
  whether delivery was in full. Open-shipment overdue alerts are not implemented.
- Warehouse metrics count dispatched orders and average pick/pack/dispatch times.
  Pending warehouse orders and backlog cannot be inferred from completed exports.
- Declared value stays in its original currency. Freight revenue, carrier cost,
  FX conversion, route distance, and shipping profitability are not available from
  the current inputs and are not fabricated.
- Multiple WMS operations per order select the latest dispatch. The source does
  not map an individual operation to an individual tracking number. Conflicting
  order IDs within a tracking pair need upstream investigation.

## BigQuery deployment

First complete the [warehouse setup](WAREHOUSE_SETUP.md) and publish Silver.
Set `GCP_PROJECT_ID` and `BQ_LOCATION` for the authenticated account, then:

```powershell
.venv-warehouse/Scripts/dbt.exe debug --project-dir dbt --profiles-dir dbt --target bigquery
.venv-warehouse/Scripts/dbt.exe build --project-dir dbt --profiles-dir dbt --target bigquery --vars '{run_date: "2026-10-01", history_start: "2026-10-01"}'
```

The profile uses Application Default Credentials; no secret is committed. Each
query has a 100 MiB maximum billed scan. This is a per-query guard, not a total
budget or promise of free execution. BigQuery fact and staging tables use date
partitioning and clustering; external inputs receive explicit `dt` filters.
Core tables allow full-history joins and tests, so required partition filtering
is not enabled on them. A cap failure requires reviewing scan scope and design.

The schema macro uses exactly `analytics_core` and `analytics_marts`, matching
the bootstrap. Use a separate project for development: concurrent users targeting
the same project would write the same tables. Run only one build at a time.

## Validation

The integration test executes the real dbt project repeatedly in a temporary
DuckDB database. It checks stable row counts on reruns, same tracking numbers
across carriers, retained orphan rows, late delivery, corrections across batches,
and protection against regression on older reruns. dbt also runs uniqueness,
not-null, relationship, duration, and percentage-range tests.

```powershell
.venv-warehouse/Scripts/python.exe -m pytest tests/integration/test_dbt_analytics.py -q
```

Validation on 2026-10-11: 36 Python tests passed, with two PySpark modules skipped
in the warehouse environment. Formatting, lint, dependency compatibility, and
Git whitespace checks passed. Live BigQuery execution remains pending cloud
credentials and configuration.

The offline BigQuery compilation test mocks only the connection and relation
inventory, disables introspection, and asserts that no queries are submitted.
It renders both initial and incremental branches with the actual BigQuery
adapter. This checks templating and configuration, not server-side SQL acceptance.

The existing Silver demonstration batch produced 982 shipments and 2,853 delivery
events locally. All 38 dbt data tests passed. The 162 shipments missing at least
one required join input remain visible with flags; passing structural tests does
not mean all source relationships are complete.

References: [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models),
[BigQuery model configuration](https://docs.getdbt.com/reference/resource-configs/bigquery-configs),
and [BigQuery connection and query caps](https://docs.getdbt.com/docs/local/connect-data-platform/bigquery-setup).
