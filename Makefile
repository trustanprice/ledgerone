VENV := venv
PYTHON := $(VENV)/bin/python
DBT := $(VENV)/bin/dbt
PROJECT_ROOT := $(shell pwd)

export LEDGERONE_DB_PATH := $(PROJECT_ROOT)/data/processed/ledgerone.duckdb
export LEDGERONE_RAW_DIR := $(PROJECT_ROOT)/data/raw

.PHONY: all generate generate-ledger generate-credit-risk build report test walkthrough-data clean

## Run the entire pipeline end to end: generate synthetic data, build the
## dbt warehouse (staging -> star schema -> reporting marts) with tests, and
## produce the report output (charts + CSV).
all: generate build report

generate: generate-ledger generate-credit-risk

generate-ledger:
	$(PYTHON) src/generate_data.py

generate-credit-risk:
	$(PYTHON) src/generate_credit_risk_data.py

build:
	$(DBT) build --project-dir dbt --profiles-dir dbt

test:
	$(DBT) test --project-dir dbt --profiles-dir dbt

report:
	$(PYTHON) src/generate_report.py

## Regenerate apps/walkthrough's static JSON from the current dbt marts.
## Not part of `all` — it's an on-demand step for that self-contained app,
## only needed after a credit-risk mart changes. See apps/walkthrough/README.md.
walkthrough-data:
	$(PYTHON) src/export_credit_risk_walkthrough_data.py

clean:
	rm -rf data/processed/*.duckdb reports/*.png reports/*.csv dbt/target dbt/logs
