<p align="center">
  <img src="docs/assets/cloudscale-banner.svg" alt="CloudScale: from messy logistics data to trusted analytics" width="100%">
</p>

<h1 align="center">CloudScale</h1>
<p align="center"><strong>Cloud data migration and logistics analytics — built step by step.</strong></p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square" alt="Python 3.10+">
  <img src="https://img.shields.io/badge/Current_stage-Local_Silver-0F766E?style=flat-square" alt="Current stage: local Silver">
  <img src="https://img.shields.io/badge/Infrastructure-Docker_Compose-2496ED?style=flat-square" alt="Docker Compose">
  <img src="https://img.shields.io/badge/Target-PySpark_%2B_dbt_%2B_BigQuery-7C3AED?style=flat-square" alt="Target: PySpark, dbt and BigQuery">
</p>

<p align="center">
  <a href="#architecture">Architecture</a> · <a href="#project-status">Status</a> · <a href="#quick-start">Quick start</a> · <a href="#learning-roadmap">Roadmap</a> · <a href="#documentation">Documentation</a>
</p>

## Why CloudScale exists

A logistics company needs to answer simple questions: **Which parcels arrived late? Where do warehouse delays happen? Which routes make money?** Its data makes those questions difficult: orders come from a database, warehouses send CSVs, and courier scanners resend delivery events over unreliable networks.

CloudScale is a hands-on data engineering project that brings these inputs into a shared platform. Its design moves from original files to clean datasets, a dimensional warehouse, and business metrics. Each phase introduces an engineering problem and a way to verify the solution.

| Source | Typical problem | Intended treatment |
| --- | --- | --- |
| Orders and merchants | Missing IDs, invalid values, future dates | Preserve originals; validate downstream |
| Warehouse CSVs | Invalid weights and reversed timestamps | Quarantine rejected rows with error reasons |
| Carrier JSON events | Duplicate scans and delayed messages | Deduplicate and handle late arrivals |

## Project status

**The local generator, Bronze ingestion, and PySpark Silver processing are implemented.** Planned components are labeled in the diagrams.

| Component | Current state |
| --- | --- |
| Architecture and source contracts | Documented |
| Synthetic dirty data generator | Implemented |
| Docker stack: Airflow, PostgreSQL, Spark | Configured; container startup and DAG execution unverified |
| Local Bronze ingestion | Implemented and tested |
| PySpark validation, deduplication, and Silver | Implemented locally — phase 5 |
| BigQuery, dbt, full orchestration, CI, dashboards | Planned — phases 6–12 |

Extractors currently read **generated local files**. Live PostgreSQL, FTP, webhook endpoints, and GCS uploads are future integrations. Checks run locally; automated CI is planned.

## Architecture

![Platform architecture: generated sources flow into implemented local Bronze, followed by local Silver processing and a planned warehouse and dashboards.](docs/assets/platform-architecture.svg)

The **medallion architecture** gives each data layer a specific purpose:

| Layer | Purpose | Tooling | Example |
| --- | --- | --- | --- |
| **Bronze** | Preserve original input for replay | Local files today; GCS planned | Warehouse CSV with its invalid weights intact |
| **Silver** | Validate, standardize, deduplicate | PySpark + Parquet, local | Events with valid IDs and timestamps |
| **Gold** | Connect business entities | BigQuery + dbt, planned | Shipment facts linked to carrier dimensions |
| **Analytics marts** | Produce focused business measures | dbt + BI, planned | Daily carrier performance and route margins |

A **dead-letter queue (DLQ)** stores rejected rows and their error reasons for investigation. Airflow will coordinate the full pipeline and retry failures. The current DAG is a small infrastructure healthcheck.

## How Bronze ingestion works

![Bronze ingestion: fingerprint the source, check the destination, land new files, skip identical input, and reject conflicts unless overwrite is explicit.](docs/assets/bronze-ingestion.svg)

**Collect original data safely before cleaning it.** Dirty values and duplicate events stay in Bronze; phase 5 processes a separate copy into Silver.

- **Partitions:** `dt=2026-10-01` groups files for a batch date.
- **Idempotency:** identical reruns preserve existing files and metadata.
- **Conflict protection:** changed input in an existing partition raises an error. Use a new partition for a new snapshot; `--overwrite` allows deliberate replacement.
- **Staged writes:** complete copies are verified before publication. A failed copy leaves existing data intact.
- **Metadata:** each file has a `.meta.json` sidecar recording its source, ingestion time, record count, and checksum — a fingerprint of its contents. Missing or invalid sidecars can be repaired on retry.

```text
data/
├── raw/                                  # Generated source files
│   ├── postgres_orders/dt=2026-10-01/
│   ├── postgres_merchants/merchants.csv
│   ├── wms_picks/dt=2026-10-01/
│   └── carrier_events/dt=2026-10-01/
└── bronze/                               # Collected originals + metadata
    ├── postgres_orders/dt=2026-10-01/
    ├── postgres_merchants/dt=2026-10-01/  # Daily merchant snapshot
    ├── wms_picks/dt=2026-10-01/
    └── carrier_events/dt=2026-10-01/
```

## Quick start

Use **Python 3.10+** from the repository root. This demo needs Faker and pytest; Docker and cloud credentials are not required. The destination filesystem must support hard links, as NTFS and ext4 do.

**1. Prepare your environment.**

```powershell
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# macOS / Linux, instead:
# source .venv/bin/activate
python -m pip install faker pytest
```

The full `requirements.txt` also includes dependencies for later phases. The smaller installation above is enough for this demo.

**2. Generate a batch and land all sources.**

```powershell
python -m data_generator.generate_legacy_data --count 1000 --date 2026-10-01
python -m ingestion.extract_postgres --date 2026-10-01
python -m ingestion.extract_wms_csv --date 2026-10-01
python -m ingestion.extract_carrier_webhooks --date 2026-10-01
```

Source files stay in `data/raw`; collected copies go to `data/bronze`. The PostgreSQL extractor handles both orders and merchants. Each extractor prints a JSON summary of landed and skipped files. Tune the seed and anomaly rates in [anomaly_rates.json](data_generator/configs/anomaly_rates.json).

**3. Check safe reruns and run the tests.**

Repeat the three extraction commands. Identical files should report `skipped`, keeping existing data and metadata intact.

```powershell
python -m pytest tests -q
```

**23 tests pass in the Spark runner**, including the original 14 checks and nine new Spark transformation and Parquet integration cases. Run the full suite using the Spark container below; without PySpark installed, Spark test modules are skipped. The DAG syntax check does not prove that Airflow services run successfully.

## Process Bronze into Silver — phase 5

The shipment job validates orders, merchant snapshots, and warehouse picks. The carrier job validates JSON events and removes duplicate scans. Both write **Snappy Parquet**: a compressed, column-oriented format designed for analytics.

After completing the Bronze quick start, start Docker and run:

```powershell
docker compose --profile silver build spark-jobs
docker compose --profile silver run --rm spark-jobs -m spark.jobs.process_shipments --date 2026-10-01
docker compose --profile silver run --rm spark-jobs -m spark.jobs.process_carrier_events --date 2026-10-01
docker compose --profile silver run --rm spark-jobs -m pytest tests -q
```

This isolated runner uses Spark's `local[2]` mode and does not start the Airflow stack. For native Python/Java setup, see the [phase 5 guide](docs/PHASE_5_REPORT.md).

| Output | Contents |
| --- | --- |
| `data/silver/{source}/dt=2026-10-01/` | Clean, typed records |
| `data/deadletter/{source}/dt=2026-10-01/` | Rejected records, original payload representation, and error codes |
| `data/silver/_audit/dt=2026-10-01/` | Input, clean, rejected, and removed-duplicate counts; job status |

A negative parcel weight goes to the dead-letter queue with `ERR_INVALID_WEIGHT`. A repeated valid courier scan is deduplicated by event ID within the batch. Late arrivals keep their scan and receipt timestamps. Both `tracking_number` and the alternative `tracking_code` are supported.

Reruns replace only the selected output date partition, leaving Bronze and other dates intact. Outputs publish per dataset; consume the batch only after its audit status is `success`. Cross-date event reconciliation, joined warehouse facts, currency conversion, and full Airflow orchestration remain later phases.

## Local development design

![Docker development design: shared project files, Airflow webserver and scheduler, PostgreSQL metadata, and Spark services. Runtime validation is pending.](docs/assets/local-development.svg)

Airflow's PostgreSQL database stores workflow state and history. It is separate from the legacy business database represented by generated orders.

Start Docker Desktop or Docker Engine before trying the containerized setup:

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

The configured Airflow UI is at [localhost:8080](http://localhost:8080), with local development credentials `admin` / `admin`. Inspect service health and trigger `healthcheck_dag` once the stack is running.

**Validation boundary:** Compose configuration validation has passed. Image builds, container startup, and healthcheck DAG execution remain unverified. Configuration validity alone does not guarantee a running stack.

## Learning roadmap

| Phase | Focus | Progress |
| --- | --- | --- |
| 1 | Architecture, business domain, contracts | Documented |
| 2 | Synthetic dirty data generation | Implemented |
| 3 | Local Docker development stack | Configured; runtime check pending |
| 4 | Bronze storage and ingestion | Implemented and tested locally |
| 5 | PySpark cleansing and Silver datasets | Implemented locally |
| **6 — next** | **BigQuery datasets and staging** | **Planned** |
| 7 | dbt dimensional models and marts | Planned |
| 8 | Full Airflow orchestration | Planned |
| 9 | Data quality and failure alerts | Planned |
| 10 | GitHub Actions CI/CD | Planned |
| 11 | Cost controls and security hardening | Planned |
| 12 | Analytics delivery and project presentation | Planned |

The cloud design aims to minimize cost through small datasets, partitioned storage, and query limits. Cloud services are not provisioned by this demo; a zero-cost deployment has not been verified.

## Repository map

```text
CloudScale/
├── airflow/
│   ├── dags/healthcheck_dag.py       # Infrastructure verification workflow
│   └── plugins/alert_handlers.py    # Alert callback code
├── data_generator/                  # Synthetic sources and anomaly configuration
├── ingestion/                       # Local Bronze landing and extractors
├── spark/                           # Silver jobs, schemas, quality rules, output helpers
├── docker/                          # Airflow and Spark image definitions
├── docs/
│   ├── assets/                      # README diagrams + regeneration script
│   ├── ARCHITECTURE_AND_ROADMAP.md
│   ├── data_dictionary.md
│   ├── PHASE_3_4_REPORT.md
│   └── PHASE_5_REPORT.md
├── tests/
│   ├── unit/                        # Generator and ingestion tests
│   └── integration/                 # DAG syntax validation
├── .env.example                     # Example local configuration
├── docker-compose.yml               # Local service topology
├── Makefile                         # Developer shortcuts
├── requirements.txt                 # Full project dependencies
└── requirements-spark.txt           # Minimal native Spark/test dependencies
```

The `dbt/` project and `.github/workflows/` are planned additions. Generated data and credentials are excluded from version control.

## Documentation

| Read this | To understand |
| --- | --- |
| [Architecture and roadmap](docs/ARCHITECTURE_AND_ROADMAP.md) | Target platform, business context, and phase-by-phase learning plan |
| [Data dictionary](docs/data_dictionary.md) | Source fields, validation rules, and intended warehouse relationships |
| [Phase 3 and 4 report](docs/PHASE_3_4_REPORT.md) | What was implemented, tested, and what remains |
| [Phase 5 guide](docs/PHASE_5_REPORT.md) | Spark setup, rules, outputs, reruns, and limitations |
| [Visual sources](docs/assets/generate_visuals.py) | Regenerate all four SVGs with `python docs/assets/generate_visuals.py` |

The architecture documents describe the target system. The status table above describes the current implementation.
