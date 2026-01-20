# Queensland Revenue Office (QRO) — Fraud Case Management (Databricks App)

This repo contains a **Databricks App** for Queensland Revenue officers to **filter, investigate, and manage** revenue/tax fraud cases.

It includes:
- A **Streamlit Databricks App** (QRO branding)
- **Unity Catalog** tables (Bronze/Silver/Gold)
- **Operational case management write-back** tables (state + audit events)

## Branding
- **Primary (QRO Maroon)**: `#6A0032`
- **Secondary (Gold)**: `#F5C400`
- **Neutral**: `#111827` (text), `#F9FAFB` (background)

## Contents
- `app/streamlit_app.py`: Databricks App UI (Streamlit)
- `qldrevenue/`: Python helpers (formatting, rules, Databricks SQL parsing)
- `sql/`: setup scripts (Bronze → Silver → Gold, plus case management)
- `tests/`: pytest unit tests (no Databricks required)

## Quick start (local dev + unit tests)

```bash
cd qld-revenue
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

## Databricks quick start (SQL)

Per requirement:
- **Catalog**: `qldrevenue`
- **Schema**: `qro_fraud_detection`

Run the scripts in order in Databricks SQL:

```sql
-- 00
sql/00_bootstrap.sql

-- 01
sql/01_create_bronze_tables.sql

-- 02
sql/02_generate_test_data.sql

-- 03
sql/03_create_silver_table.sql

-- 04
sql/04_populate_silver.sql

-- 06 (case management writes)
sql/06_case_management.sql

-- 05 (gold overlay)
sql/05_create_gold.sql
```

## Architecture (Data + Operations)

### Data layers
- **Bronze (raw)**
  - `payroll_tax_lodgements_raw`
  - `land_tax_assessments_raw`
  - `transfer_duty_raw`

- **Silver (facts / source-of-truth for detection output)**
  - `revenue_cases_silver`

- **Gold (current active-case view)**
  - `revenue_cases_gold_active`
  - Gold is derived from Silver and overlays operational state (see below).

### Operational write-back (case management)
Operational actions should **not** directly mutate Gold and generally should not rewrite Silver facts.
Instead we write to dedicated operational tables:

- **`case_management_state`** (1 row per `case_id`)
  - Current operational state (status/assignment/officer)
  - Used by Gold overlay via `LEFT JOIN` and `COALESCE(...)`

- **`case_management_events`** (append-only)
  - Immutable audit trail of what happened and who did it
  - Event types include: `ASSIGN`, `UNASSIGN`, `STATUS_CHANGE`, `NOTE`

This separation is intentional:
- Silver stays a stable analytical fact table
- Operational changes remain auditable and reversible
- Gold stays a derived “current view” that changes as the state table changes

## Operational flow (what happens when an officer uses the app)

### 1) Officer creates a rule (narrower view)
- User selects filters (case type, thresholds, etc.) and clicks **Save rule**
- App writes one row into:
  - `qldrevenue.qro_fraud_detection.officer_case_rules`
- `filter_conditions` is stored as JSON

### 2) Officer applies a rule (read-only)
- App loads rule JSON from `officer_case_rules`
- App builds a SQL `WHERE` clause and queries:
  - `qldrevenue.qro_fraud_detection.revenue_cases_gold_active`
- App updates `officer_case_rules.last_used_at`
- **Important**: applying a rule **does not change any case data**

### 3) Officer performs case management writes (changes the dataset they see)
Actions are **operational writes**:

- **Assign to me / Unassign**
  - Upsert row in `case_management_state` (assignment)
  - Insert audit row in `case_management_events`

- **Change status** (Open/Under Review/Investigation/Compliance Action/Closed)
  - Upsert row in `case_management_state` (status)
  - Insert audit row in `case_management_events`

- **Add note**
  - Inserts an audit row in `case_management_events`

### 4) How these writes show up in Gold
`revenue_cases_gold_active` is rebuilt to overlay:
- `COALESCE(case_management_state.status, revenue_cases_silver.status)`
- `COALESCE(case_management_state.assigned_to, revenue_cases_silver.assigned_to)`
- `COALESCE(case_management_state.compliance_officer, revenue_cases_silver.compliance_officer)`

So operational writes immediately change what an officer sees in “Active Cases” (Gold), without rewriting Silver facts.

## App runtime: querying
The Streamlit app queries via a **Databricks SQL warehouse** (Statement Execution API). This avoids requiring Spark in the Apps runtime.

## Security / permissions
Minimum privileges for read-only users:
- `USE CATALOG` on `qldrevenue`
- `USE SCHEMA` on `qldrevenue.qro_fraud_detection`
- `SELECT` on tables (or schema-level grants)

For case management writes, principals need:
- `INSERT` on `case_management_events`
- `MODIFY` (or `INSERT` + `UPDATE`) on `case_management_state`

