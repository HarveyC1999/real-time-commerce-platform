PYTHON ?= .venv/bin/python
DBT ?= .venv/bin/dbt

.PHONY: setup setup-analytics setup-dbt up-core up-all up-airflow migrate demo load-analytics dbt-debug dbt-build dbt-docs test test-integration lint check logs down

setup:
	@test -f .env || cp .env.example .env
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e '.[dev]'

setup-analytics:
	$(PYTHON) -m pip install -e '.[dev,analytics]'

setup-dbt:
	$(PYTHON) -m pip install -e '.[dev,analytics,dbt]'

up-core:
	docker compose up -d --wait broker postgres
	$(PYTHON) scripts/apply_migrations.py
	docker compose up -d --build consumer
	docker compose ps

up-all: up-core
	docker compose up -d spark-bronze spark-silver spark-gold

up-airflow:
	docker compose up -d --build airflow

migrate:
	$(PYTHON) scripts/apply_migrations.py

load-analytics:
	$(PYTHON) scripts/load_analytics.py

dbt-debug:
	$(DBT) debug --project-dir dbt --profiles-dir dbt

dbt-build:
	$(DBT) build --project-dir dbt --profiles-dir dbt

dbt-docs:
	$(DBT) docs generate --project-dir dbt --profiles-dir dbt

demo:
	$(PYTHON) scripts/produce_orders.py --orders 25 --seed 42

test:
	$(PYTHON) -m pytest -m 'not integration' -q

test-integration: up-core
	$(PYTHON) -m pytest -m integration -q

lint:
	$(PYTHON) -m ruff check .

check: lint test

logs:
	docker compose logs -f consumer

down:
	docker compose down
