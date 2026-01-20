import os
import sys

# Databricks Apps sync source under /app/python/source_code/... and does not necessarily install
# local packages. Ensure repo root is on sys.path so `import qldrevenue` works.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from qldrevenue.constants import GOLD_TABLE_ACTIVE, OFFICER_RULES_TABLE, SILVER_TABLE
from qldrevenue.formatting import format_abn
from qldrevenue.dbsql import dataframe_from_statement_response, state_str
from qldrevenue.metrics import kpis


APP_TITLE = "Queensland Revenue Office — Fraud Case Management"

# QRO branding
QRO_MAROON = "#6A0032"
QRO_GOLD = "#F5C400"
QRO_BG = "#F9FAFB"
QRO_TEXT = "#111827"

# Prefer using Databricks SQL warehouse for all querying
DEFAULT_WAREHOUSE_ID = "4b9b953939869799"


def _apply_branding() -> None:
    st.set_page_config(page_title="QRO Fraud Case Mgmt", layout="wide")
    st.markdown(
        f"""
<style>
  .stApp {{
    background: {QRO_BG};
    color: {QRO_TEXT};
  }}
  .qro-header {{
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:16px;
    padding: 14px 18px;
    border-radius: 14px;
    background: linear-gradient(90deg, {QRO_MAROON} 0%, {QRO_MAROON} 72%, {QRO_GOLD} 72%, {QRO_GOLD} 100%);
    color: white;
    margin-bottom: 14px;
    box-shadow: 0 6px 18px rgba(17,24,39,0.12);
  }}
  .qro-title {{
    font-size: 20px;
    font-weight: 800;
    letter-spacing: .2px;
  }}
  .qro-subtitle {{
    font-size: 12px;
    opacity: .9;
  }}
  .qro-badge {{
    background: rgba(255,255,255,0.18);
    border: 1px solid rgba(255,255,255,0.28);
    padding: 6px 10px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 700;
  }}
  .qro-card {{
    border-radius: 14px;
    padding: 12px 14px;
    background: white;
    border: 1px solid rgba(17,24,39,0.08);
  }}
  .qro-kpi {{
    font-size: 12px;
    color: rgba(17,24,39,0.7);
    margin-bottom: 4px;
  }}
  .qro-kpi-value {{
    font-size: 22px;
    font-weight: 800;
  }}
</style>
        """,
        unsafe_allow_html=True,
    )


def _sql_quote(val: str) -> str:
    return "'" + str(val).replace("'", "''") + "'"




def _state_str(state) -> str | None:
    """Normalize Databricks SDK enum/string state values to plain strings (e.g., 'SUCCEEDED')."""
    if state is None:
        return None
    if isinstance(state, str):
        return state
    # Databricks SDK uses enums for status.state
    if hasattr(state, 'value'):
        try:
            return str(state.value)
        except Exception:
            pass
    if hasattr(state, 'name'):
        try:
            return str(state.name)
        except Exception:
            pass
    s = str(state)
    # e.g. 'StatementState.SUCCEEDED' -> 'SUCCEEDED'
    return s.split('.')[-1]

def _get_ws_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _wait_for_statement(w, statement_id: str, timeout_s: int = 180):
    import time

    deadline = time.time() + timeout_s
    while True:
        resp = w.statement_execution.get_statement(statement_id)
        state = state_str(resp.status.state) if resp.status else None
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            return resp
        if time.time() > deadline:
            raise TimeoutError(f"SQL statement timed out (state={state})")
        time.sleep(1)


def _sql_fetch_df(statement: str, warehouse_id: str = DEFAULT_WAREHOUSE_ID) -> pd.DataFrame:
    """Execute SELECT and return a pandas DataFrame."""
    w = _get_ws_client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    st_id = resp.statement_id
    if not st_id:
        return pd.DataFrame()

    if (not resp.status) or (state_str(resp.status.state) in ("PENDING", "RUNNING")):
        resp = _wait_for_statement(w, st_id)

    state = state_str(resp.status.state) if resp.status else None
    if state != "SUCCEEDED":
        msg = resp.status.error.message if (resp.status and resp.status.error) else f"Statement failed: {state}"
        raise RuntimeError(msg)

    return dataframe_from_statement_response(resp)


def _sql_exec(statement: str, warehouse_id: str = DEFAULT_WAREHOUSE_ID) -> None:
    """Execute INSERT/UPDATE/DDL. Raises on failure."""
    w = _get_ws_client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    st_id = resp.statement_id
    if not st_id:
        return

    if (not resp.status) or (state_str(resp.status.state) in ("PENDING", "RUNNING")):
        resp = _wait_for_statement(w, st_id)

    state = state_str(resp.status.state) if resp.status else None
    if state != "SUCCEEDED":
        msg = resp.status.error.message if (resp.status and resp.status.error) else f"Statement failed: {state}"
        raise RuntimeError(msg)


@st.cache_data(ttl=10)
def _load_cases(limit: int = 2000) -> pd.DataFrame:
    stmt = f"""
      SELECT *
      FROM {GOLD_TABLE_ACTIVE}
      LIMIT {int(limit)}
    """
    return _sql_fetch_df(stmt)


@st.cache_data(ttl=10)
def _load_rules(officer_email: str) -> pd.DataFrame:
    stmt = f"""
      SELECT rule_id, officer_email, rule_name, filter_conditions, last_used_at
      FROM {OFFICER_RULES_TABLE}
      WHERE officer_email = {_sql_quote(officer_email)} AND is_active = true
    """
    try:
        return _sql_fetch_df(stmt)
    except Exception:
        return pd.DataFrame(columns=["rule_id", "officer_email", "rule_name", "filter_conditions", "last_used_at"])


def _save_rule(officer_email: str, rule_name: str, conds: Dict[str, Any]) -> str:
    rule_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    stmt = f"""
      INSERT INTO {OFFICER_RULES_TABLE}
      (rule_id, officer_email, rule_name, filter_conditions, created_at, is_active, last_used_at)
      VALUES (
        {_sql_quote(rule_id)},
        {_sql_quote(officer_email)},
        {_sql_quote(rule_name)},
        {_sql_quote(json.dumps(conds))},
        {_sql_quote(now)},
        true,
        NULL
      )
    """
    _sql_exec(stmt)
    return rule_id


def _mark_rule_used(rule_id: str) -> None:
    stmt = f"""
      UPDATE {OFFICER_RULES_TABLE}
      SET last_used_at = current_timestamp()
      WHERE rule_id = {_sql_quote(rule_id)}
    """
    _sql_exec(stmt)


@st.cache_data(ttl=30)
def _case_history(case_id: str) -> pd.DataFrame:
    stmt = f"""
      SELECT _commit_version, _commit_timestamp, status, risk_score, assigned_to, compliance_officer
      FROM table_changes('{SILVER_TABLE}', 0)
      WHERE case_id = {_sql_quote(case_id)}
      ORDER BY _commit_version DESC
      LIMIT 200
    """
    try:
        return _sql_fetch_df(stmt)
    except Exception:
        stmt2 = f"""
      SELECT _commit_version, _commit_timestamp, status, risk_score, assigned_to
      FROM table_changes('{SILVER_TABLE}', 0)
      WHERE case_id = {_sql_quote(case_id)}
      ORDER BY _commit_version DESC
      LIMIT 200
    """
        return _sql_fetch_df(stmt2)


def _kpis(df: pd.DataFrame) -> Dict[str, Any]:
    # Backwards-compatible wrapper
    return kpis(df)


def main() -> None:
    _apply_branding()

    st.markdown(
        f"""
<div class="qro-header">
  <div>
    <div class="qro-title">{APP_TITLE}</div>
    <div class="qro-subtitle">Databricks SQL warehouse querying • Rules • Active cases • Delta history</div>
  </div>
  <div class="qro-badge">Catalog: qldrevenue</div>
</div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("### Databricks SQL")
        st.code(f"Warehouse: {DEFAULT_WAREHOUSE_ID}")
        st.markdown("---")
        st.markdown("### Officer")
        officer_email = st.text_input("Email", value="revenue.officer1@qro.qld.gov.au")
        st.markdown("---")
        st.markdown("### My Rules")

        rules_df = _load_rules(officer_email)
        if not rules_df.empty:
            st.dataframe(rules_df[["rule_name", "last_used_at"]], use_container_width=True, height=160)

        rule_id_to_name = {}
        if not rules_df.empty:
            for _, r in rules_df.iterrows():
                rid = r.get("rule_id")
                nm = r.get("rule_name")
                if isinstance(rid, str) and rid:
                    rule_id_to_name[rid] = str(nm) if nm is not None else rid

        sorted_rule_ids = sorted(rule_id_to_name.keys(), key=lambda rid: rule_id_to_name[rid].lower())
        selected_rule_id = st.selectbox(
            "Select rule",
            options=[None] + sorted_rule_ids,
            format_func=lambda rid: "(none)" if rid is None else rule_id_to_name.get(rid, rid),
            key="selected_rule_id",
        )

        st.markdown("---")
        st.markdown("### Create New Rule")
        new_rule_name = st.text_input("Rule name", value="Mining Sector High Risk")
        case_types = st.multiselect("Case type", ["Payroll Tax", "Land Tax", "Transfer Duty"], default=["Payroll Tax"])
        industry_codes = st.multiselect("Industry code", ["0600", "3000", "4400", "4500", "6000"], default=["0600"])
        tax_shortfall_min = st.number_input("Min tax shortfall", min_value=0, value=50000, step=1000)
        risk_score_min = st.number_input("Min risk score", min_value=0, max_value=100, value=60, step=1)
        if st.button("Save rule", type="primary"):
            conds = {
                "case_types": case_types,
                "industry_codes": industry_codes,
                "tax_shortfall_min": float(tax_shortfall_min),
                "risk_score_min": int(risk_score_min),
            }
            rid = _save_rule(officer_email, new_rule_name, conds)
            st.success(f"Saved rule: {new_rule_name} ({rid})")
            st.session_state["selected_rule_id"] = rid
            st.cache_data.clear()

    # Cases (prefer warehouse)
    applied = None

    # If a rule is selected, apply filters in SQL (faster than fetching everything)
    if selected_rule_id and not rules_df.empty:
        row = rules_df[rules_df["rule_id"] == selected_rule_id]
        if not row.empty:
            r0 = row.iloc[0].to_dict()
            conds = json.loads(r0.get("filter_conditions") or "{}")
            where = ["1=1"]
            if conds.get("case_types"):
                vals = ",".join(_sql_quote(x) for x in conds["case_types"])
                where.append(f"case_type IN ({vals})")
            if conds.get("industry_codes"):
                # In this demo dataset, industry_code is populated for Payroll Tax cases.
                # If the rule excludes Payroll Tax, applying industry filter would yield 0 rows.
                ct = set(conds.get("case_types") or [])
                if not ct or ("Payroll Tax" in ct):
                    vals = ",".join(_sql_quote(x) for x in conds["industry_codes"])
                    where.append(f"industry_code IN ({vals})")
                else:
                    st.warning("Industry code filter applies to Payroll Tax cases in this demo; skipping industry filter for this rule.")
            if conds.get("tax_shortfall_min") is not None:
                where.append(f"tax_shortfall >= {float(conds['tax_shortfall_min'])}")
            if conds.get("risk_score_min") is not None:
                where.append(f"risk_score >= {int(conds['risk_score_min'])}")

            stmt = f"""
              SELECT *
              FROM {GOLD_TABLE_ACTIVE}
              WHERE {' AND '.join(where)}
              LIMIT 5000
            """
            applied = _sql_fetch_df(stmt)
            _mark_rule_used(selected_rule_id)

    if applied is None:
        applied = _load_cases()

    k = kpis(applied)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            '<div class="qro-card"><div class="qro-kpi">Total Cases</div>'
            f'<div class="qro-kpi-value">{k["total_cases"]}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div class="qro-card"><div class="qro-kpi">Total Exposure</div>'
            f'<div class="qro-kpi-value">${k["total_exposure"]:,.0f}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div class="qro-card"><div class="qro-kpi">Avg Shortfall</div>'
            f'<div class="qro-kpi-value">${k["avg_shortfall"]:,.0f}</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<div class="qro-card"><div class="qro-kpi">Unique Taxpayers</div>'
            f'<div class="qro-kpi-value">{k["unique_taxpayers"]}</div></div>',
            unsafe_allow_html=True,
        )

    st.markdown("### Active Cases")
    if applied.empty:
        st.error(
            "No cases loaded. Databricks SQL query returned 0 rows. "
            "Verify qldrevenue.qro_fraud_detection.revenue_cases_gold_active has data and this app has SELECT permissions."
        )
        return

    display_cols = [
        c
        for c in [
            "case_id",
            "case_type",
            "taxpayer_name",
            "taxpayer_abn",
            "tax_shortfall",
            "risk_score",
            "status",
            "severity",
            "regional_office",
            "financial_year",
        ]
        if c in applied.columns
    ]
    d = applied[display_cols].copy()
    if "taxpayer_abn" in d.columns:
        d["taxpayer_abn"] = d["taxpayer_abn"].map(format_abn)
    if "tax_shortfall" in d.columns:
        d["tax_shortfall"] = pd.to_numeric(d["tax_shortfall"], errors="coerce").fillna(0).map(lambda x: f"${float(x):,.0f}")
    st.dataframe(d, use_container_width=True, height=360)

    st.markdown("### Case Details")
    selected_case_id = st.text_input("Enter Case ID", value=str(d.iloc[0]["case_id"]))
    case_row = applied[applied["case_id"] == selected_case_id]
    if case_row.empty:
        st.warning("Case not found in current filtered view.")
        return

    r = case_row.iloc[0].to_dict()
    left, right = st.columns([2, 1])
    with left:
        st.json(r, expanded=False)
    with right:
        st.markdown("#### Actions")
        if st.button("Show Delta History"):
            hist = _case_history(selected_case_id)
            st.dataframe(hist, use_container_width=True, height=240)
        if st.button("Create ServiceNow Incident"):
            payload = {
                "short_description": f"QRO Revenue Case: {r.get('case_id')}",
                "description": f"ABN: {r.get('taxpayer_abn')}\nShortfall: {r.get('tax_shortfall')}",
                "urgency": 1 if r.get("severity") == "Critical" else 3,
                "assignment_group": "Revenue Compliance Team",
                "u_qro_case_id": r.get("case_id"),
            }
            st.success("Prepared ServiceNow payload (demo).")
            st.json(payload)


if __name__ == "__main__":
    main()
