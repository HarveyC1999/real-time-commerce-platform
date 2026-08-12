# Real-time Commerce Platform

A local side project for experimenting with commerce events, Kafka-compatible
streaming, PostgreSQL persistence, and a small Spark Bronze/Silver/Gold
pipeline.

## Architecture

```text
Python producer -> Redpanda -> Python consumer -> PostgreSQL order_events
                         |
                         +-> Spark Bronze -> Silver/Quarantine -> Gold Parquet
                                                                  |
                                              load_analytics.py -> PostgreSQL analytics
```

The core development loop only needs Redpanda, PostgreSQL, and the Python
consumer. Spark is optional because it uses more memory and takes longer to
start.

## Prerequisites

- Docker Desktop with Docker Compose
- Python 3.12 or newer
- `make` on macOS/Linux (optional; the equivalent commands are listed below)

The current Docker setup pins Redpanda to `v26.2.1`, PostgreSQL to major
version 17, and Spark to `4.1.3`.

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

## Optional Spark analytics pipeline

Start all services, including Bronze, Silver, and Gold Spark jobs:

```bash
make up-all
```

Spark writes local Parquet data under `lakehouse/`. After Gold data has been
produced, install the analytics extra and load it into PostgreSQL:

```bash
make setup-analytics
.venv/bin/python scripts/load_analytics.py
.venv/bin/python scripts/inspect_analytics.py
```

The `lakehouse/` data and checkpoints are intentionally ignored by Git.

## Database migrations

SQL migrations live in `infrastructure/postgres/`. PostgreSQL's
`docker-entrypoint-initdb.d` scripts run only when its Docker volume is first
created, so always run the migration command after pulling new changes:

```bash
make migrate
```

Applied migrations are recorded with checksums and should not be edited.
Create a new numbered SQL file for later schema changes.
