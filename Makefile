PYTHON ?= .venv/bin/python

.PHONY: setup setup-analytics up-core up-all migrate demo test test-integration lint check logs down

setup:
	@test -f .env || cp .env.example .env
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e '.[dev]'

setup-analytics:
	$(PYTHON) -m pip install -e '.[dev,analytics]'

up-core:
	docker compose up -d --wait broker postgres
	$(PYTHON) scripts/apply_migrations.py
	docker compose up -d --build consumer
	docker compose ps

up-all: up-core
	docker compose up -d spark-bronze spark-silver spark-gold

migrate:
	$(PYTHON) scripts/apply_migrations.py

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
