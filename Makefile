.DEFAULT_GOAL := help

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
export PYTHONPATH := $(ROOT)

ifneq (,$(wildcard $(ROOT)/.env))
include $(ROOT)/.env
export
endif

INPUT ?= data/raw/2019-Oct.csv
SILVER_PATH ?= data/silver/events
SPARK_DRIVER_MEMORY ?= 8g
MONTH ?= 2019-10
LOG_STEM := $(basename $(notdir $(INPUT)))

.PHONY: help install fixtures test lint sqlfluff ci download ingest ingest-month sessionize \
	dbt-deps dbt-dev dbt-dev-window dbt-parse dbt-docs export reconcile run

help:
	@echo "Clickstream to Cohorts"
	@echo "  make install            pip install -r requirements-dev.txt"
	@echo "  make run                CSV to Parquet to DuckDB to data/exports CSV"
	@echo "  make fixtures           regenerate the synthetic CSV and seeds"
	@echo "  make test               pytest, including PySpark on the fixture"
	@echo "  make lint               ruff check and ruff format --check"
	@echo "  make sqlfluff           sqlfluff lint on dbt SQL"
	@echo "  make ci                 lint, pytest, dbt parse, DuckDB build, sqlfluff"
	@echo "  make download           Kaggle download of Oct and Nov 2019 CSV files"
	@echo "  make ingest             one CSV to silver Parquet (INPUT=...)"
	@echo "  make ingest-month       MONTH=2019-10 or 2019-11"
	@echo "  make sessionize         30 minute disagreement report"
	@echo "  make dbt-dev            full DuckDB build on the seed"
	@echo "  make dbt-dev-window     DuckDB build for the first 7 days"
	@echo "  make export             mart CSV files in data/exports"
	@echo "  make reconcile          Spark run logs vs local Parquet row count"

install:
	python -m pip install -r requirements-dev.txt

fixtures:
	python scripts/generate_fixtures.py

test:
	pytest

lint:
	ruff check .
	ruff format --check .

sqlfluff:
	cd dbt && sqlfluff lint models tests macros

ci: lint test dbt-parse dbt-dev sqlfluff

download:
	bash ingestion/download.sh

ingest:
	mkdir -p data/logs
	python -m spark.jobs.csv_to_parquet \
		--input $(INPUT) \
		--output $(SILVER_PATH) \
		--run-log data/logs/$(LOG_STEM).json \
		--driver-memory $(SPARK_DRIVER_MEMORY)

ingest-month:
	python scripts/ingest_month.py --month $(MONTH)

sessionize:
	mkdir -p data/logs
	python -m spark.jobs.sessionize_check \
		--input $(SILVER_PATH) \
		--report data/logs/session_disagreement.json \
		--pairs data/logs/session_disagreements \
		--driver-memory $(SPARK_DRIVER_MEMORY)

dbt-deps:
	cd dbt && dbt deps --profiles-dir .

dbt-dev: dbt-deps
	cd dbt && dbt build --target dev --profiles-dir . --full-refresh

dbt-dev-window: dbt-deps
	cd dbt && dbt build --target dev --profiles-dir . --full-refresh --vars '{dev_mode: true}'

dbt-parse: dbt-deps
	cd dbt && dbt parse --target dev --profiles-dir .

dbt-docs: dbt-deps
	cd dbt && dbt docs generate --target dev --profiles-dir . --empty-catalog --static
	cp dbt/target/static_index.html dbt/target/index.html

export:
	python scripts/export_marts.py

reconcile:
	python scripts/reconcile_counts.py

run:
	python scripts/run_local.py
