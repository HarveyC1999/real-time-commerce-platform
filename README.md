# Real-time Commerce Platform

[![CI](https://github.com/HarveyC1999/real-time-commerce-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/HarveyC1999/real-time-commerce-platform/actions/workflows/ci.yml)

A local side project for experimenting with commerce events, Kafka-compatible
streaming, PostgreSQL persistence, Spark transformations, dbt analytics, and
Airflow orchestration.

## Architecture

```text
                               +-> Python consumer -> PostgreSQL order_events
Python producer -> Redpanda ---|
                               +-> Spark Bronze -> Silver/Quarantine -> Gold Parquet
                                                                            |
Airflow (hourly) -------- schedules --------+                              |
                                            v                              v
                                      load_analytics.py -> PostgreSQL analytics
                                                             -> dbt staging -> mart
```

The core development loop only needs Redpanda, PostgreSQL, and the Python
consumer. Spark is optional because it uses more memory and takes longer to
start.

## What this project demonstrates

- Event production and consumption through a Kafka-compatible Redpanda broker.
- Idempotent PostgreSQL persistence and duplicate-event handling.
- A Spark Structured Streaming Bronze/Silver/Gold pipeline with quarantine,
  checkpoints, event-time windows, and watermarks.
- An idempotent Gold-to-PostgreSQL load followed by dbt transformations and 24
  data tests covering grains, accepted values, non-negative metrics, and
  reconciliation.
- Hourly Airflow orchestration of the bounded analytics load and dbt build.

This is a local portfolio project, not a production deployment. Spark jobs are
long-running streaming processes, while Airflow uses its single-container
standalone mode for local orchestration only.

## Prerequisites

- Docker Desktop with Docker Compose
- Python 3.12 or newer (the local `.venv` is tested with Python 3.14; Conda is
  not required)
- `make` on macOS/Linux (optional; the equivalent commands are listed below)

The current Docker setup pins Redpanda to `v26.2.1`, PostgreSQL to major
version 17, Spark to `4.1.3`, and Airflow to `3.3.0-python3.14`.

## Quick start on macOS/Linux

```bash
make setup
make up-core
make demo
```

Inspect the persisted events:

```bash
docker compose exec postgres psql -U commerce -d commerce \
  -c "SELECT event_id, event_type, order_status, order_amount, event_time FROM order_events ORDER BY ingested_at DESC LIMIT 10;"
```

Stop the services without deleting data:

```bash
make down
```

## Equivalent commands without Make

Create the environment and install the project:

```bash
cp -n .env.example .env
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[dev]'
```

Start the core infrastructure, wait for it to become healthy, apply all SQL
migrations, and then start the consumer:

```bash
docker compose up -d --wait broker postgres
.venv/bin/python scripts/apply_migrations.py
docker compose up -d --build consumer
docker compose ps
```

Publish sample events:

```bash
.venv/bin/python scripts/produce_orders.py --orders 25 --seed 42
```

This publishes ordered events for each generated order. Most orders progress
from `pending` through `paid` and `processing` to `completed`; a smaller share
ends at `cancelled`. Use `--count 100` when independent random events are more
useful for a quick load test.

On Windows PowerShell, use `.venv\Scripts\python.exe` in place of
`.venv/bin/python`; the Docker Compose commands are unchanged.

## Tests and code quality

The fast check does not require Docker:

```bash
make check
```

Equivalent commands:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest -m "not integration" -q
```

GitHub Actions runs the same lint and fast-test checks on pushes and pull
requests to `main`, then parses the dbt project and validates the Compose file.
It does not start Docker services or require repository secrets.

Integration tests require the complete core pipeline. The target starts and
checks its prerequisites before running the tests:

```bash
make test-integration
```

Equivalent commands:

```bash
docker compose up -d --wait broker postgres
.venv/bin/python scripts/apply_migrations.py
docker compose up -d --build consumer
.venv/bin/python -m pytest -m integration -q
```

Useful diagnostics:

```bash
docker compose ps
docker compose logs -f consumer
```

## Portfolio demo

The following path demonstrates the complete analytics slice after the Spark
Gold jobs have finalized at least one event-time window:

```bash
docker compose up -d --wait broker postgres
.venv/bin/python scripts/apply_migrations.py
docker compose up -d --build consumer
docker compose up -d spark-bronze spark-silver spark-gold
.venv/bin/python scripts/produce_orders.py --orders 25 --seed 42
docker compose up -d --build airflow
docker compose exec airflow airflow dags test commerce_analytics
```

Because Gold uses a one-day event-time watermark, newly generated current-time
events do not immediately finalize their hourly window. For a quick repeat of
an existing demo, keep the previously produced `lakehouse/gold/` files and run
only the Airflow command. A successful run ends with `state=success`.

Inspect the loaded source tables:

```bash
.venv/bin/python scripts/inspect_analytics.py
```

Both source row counts should be greater than zero. Verify that the dbt mart
contains exactly one row per `window_start + currency` grain:

```bash
docker compose exec postgres psql -U commerce -d commerce -c \
  "SELECT COUNT(*) AS mart_rows, COUNT(DISTINCT (window_start, currency)) AS unique_grains FROM analytics_dbt.fct_hourly_commerce_metrics;"
```

`mart_rows` and `unique_grains` should be equal and greater than zero. On the
validated sample dataset they are both 28; the source tables contain 28 metric
rows and 124 status rows.

Validation checkpoint for this version: 36 fast tests passed, `dbt build`
passed all 27 models/tests, and the manual Airflow DagRun completed with
`state=success`.

## Optional Spark analytics pipeline

Start all services, including Bronze, Silver, and Gold Spark jobs:

```bash
make up-all
```

Spark writes local Parquet data under `lakehouse/`. After Gold data has been
produced, install the analytics and dbt extras, load it into PostgreSQL, and
build the reporting mart:

```bash
make setup-dbt
make load-analytics
make dbt-debug
make dbt-build
make dbt-docs
```

The `lakehouse/` data and checkpoints are intentionally ignored by Git.

On Windows PowerShell, the equivalent commands are:

```powershell
.venv\Scripts\python.exe -m pip install -e ".[dev,analytics,dbt]"
.venv\Scripts\python.exe scripts\load_analytics.py
.venv\Scripts\dbt.exe debug --project-dir dbt --profiles-dir dbt
.venv\Scripts\dbt.exe build --project-dir dbt --profiles-dir dbt
.venv\Scripts\dbt.exe docs generate --project-dir dbt --profiles-dir dbt
```

## dbt analytics vertical slice

dbt reads the two idempotently loaded tables in the `analytics` schema. It
creates two staging views and one reporting table:

```text
analytics.order_metrics_hourly ─┐
                                ├─> analytics_dbt.fct_hourly_commerce_metrics
analytics.order_status_hourly  ─┘
```

The mart grain is one row per `window_start + currency`. Status fields are
named `*_event_count` because they describe lifecycle events, not the latest
state of unique orders.

`dbt build` verifies source nullability and accepted statuses, unique source
and mart grains, non-negative metrics, and reconciliation between total event
counts and the five status counts. PostgreSQL credentials are read from the
same `POSTGRES_*` environment variables as the application; the checked-in
profile contains only local-development defaults.

## Optional Airflow orchestration

Airflow runs separately in Docker, so the project's host `.venv` remains on
the locally installed Python version. Start the single-container development
instance after Gold Parquet data exists:

```bash
make up-airflow
docker compose logs airflow
```

Open <http://localhost:8080>. The standalone command prints the generated
admin credentials in the container logs. The `commerce_analytics` DAG runs
hourly and contains only two tasks:

```text
load_gold_analytics -> build_dbt_models
```

The first task idempotently loads finalized Gold aggregates into PostgreSQL;
the second runs `dbt build`, including all data tests. Airflow deliberately
does not start or stop the long-running Spark streaming jobs. To validate the
DAG once without waiting for its schedule:

```bash
docker compose exec airflow airflow dags test commerce_analytics
```

## Database migrations

SQL migrations live in `infrastructure/postgres/`. PostgreSQL's
`docker-entrypoint-initdb.d` scripts run only when its Docker volume is first
created, so always run the migration command after pulling new changes:

```bash
make migrate
```

Applied migrations are recorded with checksums and should not be edited.
Create a new numbered SQL file for later schema changes.
