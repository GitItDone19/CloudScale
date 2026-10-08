# Phase 3 and 4 learning report

Report date: 8 October 2026.

## What is on GitHub?

Phase 3 was already pushed in commit `90af724`. The initial phase 4 implementation
was already pushed in commit `d32e377`. This follow-up strengthens phase 4 and
adds automated regression tests. There was no missing local phase 3 commit to push.

## Phase 3: the local development environment

Docker Compose describes the services needed to work on CloudScale locally:

- PostgreSQL stores Airflow's operational information: task states, schedules,
  and execution history. It is the Airflow metadata database, not the legacy
  business database that contains customer orders.
- Airflow's webserver provides the interface where you inspect workflows.
- Airflow's scheduler decides when tasks should run.
- The initialization service prepares the Airflow database and development user.
- Spark's master and worker provide the environment for distributed data processing.

A Docker image is a packaged environment. A container is a running instance of
that image. Compose connects those containers and shares selected project folders
with them. This helps the different tools see the same source code and data.

The existing healthcheck DAG defines two tasks: print a start message and run a
Python function. It is a small workflow for checking the Airflow setup.

Validation here: `docker compose config --quiet` passed. This checks whether the
configuration can be interpreted. Docker's engine was unavailable, so the images
were not built, the services were not started, and the DAG was not executed.
The current DAG test checks Python syntax; it does not import Airflow or prove
that the scheduler works. Phase 3 is committed, but runtime validation remains open.

## Phase 4: safely collect the original data

Ingestion means moving data from its source into the platform. Bronze is the
first stored copy. It preserves the input so later processing can be repeated
or investigated.

The local implementation reads three families of generated source files:

- Orders and merchants, representing exports from a PostgreSQL business database.
- Warehouse CSV files, representing daily warehouse activity exports.
- Carrier JSON events, representing delivery scanner messages.

The generated source files live in `data/raw`. Extraction now defaults to a
separate destination, `data/bronze`. For example:

```text
data/raw/postgres_orders/dt=2026-10-01/orders_20261001.csv
    -> data/bronze/postgres_orders/dt=2026-10-01/orders_20261001.csv
```

The `dt=2026-10-01` folder is a date partition: it groups one date's input so a
pipeline can select a specific batch. Merchant snapshots also receive date
partitions, preserving different daily versions.

Each landed file has a metadata sidecar ending in `.meta.json`. It records where
the file came from, its ingestion time, record count, and SHA-256 checksum.
A checksum is a fingerprint of the file's contents, useful for detecting changes.

## What this follow-up fixed

Previously, changed input could reach the write operation even when overwrite
was disabled. That could replace existing Bronze data on some systems or fail
differently on others. The new behavior explicitly rejects the conflict.

An identical rerun skips the file and preserves its metadata and original
recorded ingestion time. This is idempotency: repeating the same operation has
the same stored result, rather than creating extra copies or changing history.

The checks compare the actual destination bytes with the source. They do not
trust an old checksum written in a sidecar when the destination may have changed.
Missing or invalid sidecars can be rebuilt without rewriting the data file.

Copies use unique temporary files. After a complete, verified copy, publication
uses a hard link for a new file or atomic replacement for explicit overwrite.
This prevents a partially copied file from becoming the final destination.
The temporary file is removed on failure. Metadata also uses a temporary file
and atomic replacement. Data and metadata are separate publications; if metadata
publication fails after the data succeeds, rerunning repairs the sidecar.
This requires a local filesystem that supports hard links, such as NTFS or ext4.

CSV record counting now handles a quoted field containing a newline correctly.
The status also distinguishes a first landing from replacement correctly.
The `make data-gen` command now uses the generator's supported `--count` argument.

## What the tests proved

All 14 tests passed: 12 ingestion cases, one existing generator test, and one
existing DAG syntax test. Ingestion checks cover identical reruns, changed input,
explicit overwrite, destination corruption, sidecar repair, copy failure, invalid
dates, and a generated-data run through all three extractors. The latter also
checks that carrier JSON is preserved byte for byte.

Formatting checks passed for ingestion code and the new tests. Git's whitespace
check passed. Docker Compose configuration validation passed with an existing
warning about the obsolete `version` field.

## What remains

Phase 4's local landing path is implemented and tested. These scripts simulate
extraction using generated files; they do not connect to a live PostgreSQL server,
FTP service, or webhook endpoint. GCS uploading is not implemented.

Bronze intentionally retains invalid values and duplicate messages. Phase 5 will
use PySpark to validate fields, remove duplicate carrier events, write clean
Parquet files to Silver, and send invalid rows to a dead-letter queue. A
dead-letter queue is a separate place to keep rejected records for investigation.

The next infrastructure check is to start Docker Desktop, run `docker compose
up -d --build`, inspect service health, and execute the healthcheck DAG. Passing
configuration validation alone does not establish that the containers will run.
