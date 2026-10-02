# CloudScale — Enterprise Cloud Data Migration & Analytics Platform

[![CI Pipeline](https://github.com/your-username/cloudscale/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/cloudscale/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg)](https://spark.apache.org/)
[![dbt-core](https://img.shields.io/badge/dbt-core-FF694B.svg)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg)](https://airflow.apache.org/)
[![GCP BigQuery](https://img.shields.io/badge/Google%20Cloud-BigQuery-4285F4.svg)](https://cloud.google.com/bigquery)

An enterprise-grade, zero-cost cloud data migration and analytics platform for cross-border logistics. Built with **PySpark**, **dbt Core**, **Apache Airflow**, **Google Cloud Platform (GCS & BigQuery)**, and **Docker Compose**.

---

## 🏗 Architecture & Medallion Design

CloudScale transitions a legacy logistics infrastructure (OLTP PostgreSQL, legacy WMS batch CSVs, noisy mobile courier webhooks) into a modern **Medallion Lakehouse**:

1. **Bronze (Raw):** Immutable landing in GCS (`gs://cloudscale-raw/{source}/dt=YYYY-MM-DD/`).
2. **Silver (Cleaned & Standardized):** Distributed processing via **PySpark** enforcing schemas, isolating corrupt data to a **Dead Letter Queue (DLQ)**, and writing columnar Snappy Parquet.
3. **Gold (Dimensional Warehouse):** Star Schema dimensional modeling in **BigQuery** using **dbt Core** (Surrogate keys, incremental merges with 3-day lookback for late-arriving data).
4. **Platinum (Marts & Analytics):** Aggregated metrics for logistics SLAs (OTIF, fulfillment dispatch lag, route margins).

```
Legacy Sources ──> Raw Ingestion ──> PySpark Cleansing ──> BigQuery Staging ──> dbt Star Schema ──> BI Dashboards
 (PG / CSV / JSON)   (GCS Bronze)     (Silver + DLQ)         (raw_staging)        (analytics_core)    (Looker Studio)
```

---

## 🚀 Key Engineering Highlights

- **Two-Tier Data Quality Defense:**
  - *Tier 1 (PySpark Gate):* Pre-warehouse quarantine of corrupt rows (invalid weights, future timestamps) to GCS DLQ without pipeline termination.
  - *Tier 2 (dbt Gate):* Post-load relational integrity, uniqueness, and custom business constraint assertions.
- **Idempotent Webhook Deduplication:** PySpark window functions eliminate duplicate carrier scanner pings over flaky networks.
- **Late-Arriving Data Handling:** Incremental dbt models equipped with a 3-day lookback window reconcile late courier event scans.
- **GCP Zero-Cost Architecture:** Engineered to run 100% within GCP's permanent Free Tier quotas (GCS 5 GB, BigQuery 10 GB storage + 1 TB/mo query scans with 100 MB max bytes billed guardrails).

---

## 📂 Repository Structure

```
cloudscale/
├── .github/workflows/          # CI/CD Workflows (PR lint, tests, dbt compile)
├── airflow/                    # Apache Airflow DAGs & plugins
├── data_generator/             # Synthetic dirty legacy data producer
├── ingestion/                  # Bronze extraction scripts (Postgres, WMS, Webhooks)
├── spark/                      # PySpark distributed processing & DLQ routing
├── dbt/                        # dbt dimensional models (staging, intermediate, marts)
├── docker/                     # Dockerfiles for Airflow and Spark runners
├── docs/                       # Architecture blueprint & Data Dictionary
│   ├── ARCHITECTURE_AND_ROADMAP.md
│   └── data_dictionary.md
├── tests/                      # PyTest unit tests & DAG integration tests
├── docker-compose.yml          # Local containerized development stack
└── Makefile                    # Developer shortcuts
```

---

## 📖 Documentation
- [Master Technical Architecture & Roadmap](docs/ARCHITECTURE_AND_ROADMAP.md)
- [Data Dictionary & Schema Contracts](docs/data_dictionary.md)

---

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
