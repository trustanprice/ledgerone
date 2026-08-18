VENV := venv
PYTHON := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
PROJECT_ROOT := $(shell pwd)

export LEDGERONE_DB_PATH := $(PROJECT_ROOT)/data/processed/ledgerone.duckdb
export LEDGERONE_RAW_DIR := $(PROJECT_ROOT)/data/raw

.PHONY: all generate build report test clean

## Run the entire pipeline end to end: generate synthetic data, build the
## dbt warehouse (staging -> star schema -> reporting marts) with tests, and
## produce the report output (charts + CSV).
all: generate build report

generate:
	$(PYTHON) src/generate_data.py

build:
	$(DBT) build --project-dir dbt --profiles-dir dbt

test:
	$(DBT) test --project-dir dbt --profiles-dir dbt

report:
	$(PYTHON) src/generate_report.py

clean:
	rm -rf data/processed/*.duckdb reports/*.png reports/*.csv dbt/target dbt/logs
