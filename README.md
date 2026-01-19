# Queensland Revenue Office (QRO) — Fraud Case Management (Databricks App)

This repo contains a **Databricks App** for Queensland Revenue officers to **filter, investigate, and escalate** revenue/tax fraud cases. It also includes **SQL setup scripts** to generate demo data and build Bronze/Silver/Gold tables in Unity Catalog.

## Branding
- **Primary (QRO Maroon)**: `#6A0032`
- **Secondary (Gold)**: `#F5C400`
- **Neutral**: `#111827` (text), `#F9FAFB` (background)

## Contents
- `app/streamlit_app.py`: Databricks App UI (Streamlit)
- `qldrevenue/`: Python package (rules, formatting, calculations)
- `sql/`: setup scripts (Bronze → Silver → Gold)
- `tests/`: pytest unit tests (no Databricks required)
- `docs/`: guides you provided (developer + tester)

## Quick start (local dev + unit tests)

```bash
cd qld-revenue
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

## Databricks quick start (SQL)

Per your requirement, this repo uses:
- **Catalog**: `qldrevenue`
- **Schema**: `qro_fraud_detection`

Run the scripts in order in Databricks SQL:

```bash
databricks-sql < sql/00_bootstrap.sql
databricks-sql < sql/01_create_bronze_tables.sql
databricks-sql < sql/02_generate_test_data.sql
databricks-sql < sql/03_create_silver_table.sql
databricks-sql < sql/04_populate_silver.sql
databricks-sql < sql/05_create_gold.sql
```

## Databricks App

The app is implemented as a **Streamlit Databricks App** (`app/streamlit_app.py`). It:
- Loads cases from `qldrevenue.qro_fraud_detection.revenue_cases_gold_active`
- Persists officer rules in `qldrevenue.qro_fraud_detection.officer_case_rules`
- Shows time travel history via Delta CDF on `...revenue_cases_silver`

