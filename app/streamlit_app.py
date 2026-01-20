import os
import sys

# Databricks Apps run source under /app/python/source_code/...
# Ensure the repo root is on sys.path so `import qldrevenue` works without packaging.
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


APP_TITLE = "Queensland Revenue Office — Fraud Case Management"

# QRO branding
QRO_MAROON = "#6A0032"
QRO_GOLD = "#F5C400"
QRO_BG = "#F9FAFB"
QRO_TEXT = "#111827"


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


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401

        return True
    except Exception:
        return False


def _get_spark():
    try:
        return spark  # type: ignore[name-defined]
    except Exception:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


@st.cache_data(ttl=10)
def _load_cases(limit: int = 2000) -> pd.DataFrame:
    if not _spark_available():
        return pd.DataFrame()
    sp = _get_spark()
    return sp.table(GOLD_TABLE_ACTIVE).limit(int(limit)).toPandas()


@st.cache_data(ttl=10)
def _load_rules(officer_email: str) -> pd.DataFrame:
    if not _spark_available():
        return pd.DataFrame(columns=["rule_id", "officer_email", "rule_name", "filter_conditions", "last_used_at"])
    sp = _get_spark()
    return (
        sp.table(OFFICER_RULES_TABLE)
        .filter(f"officer_email = '{officer_email}'")
        .filter("is_active = true")
        .toPandas()
    )


def _save_rule(officer_email: str, rule_name: str, conds: Dict[str, Any]) -> str:
    rule_id = str(uuid.uuid4())
    if not _spark_available():
        return rule_id
    sp = _get_spark()
    now = datetime.utcnow().isoformat()
    row = [(rule_id, officer_email, rule_name, json.dumps(conds), now, True, None)]
    schema = "rule_id string, officer_email string, rule_name string, filter_conditions string, created_at string, is_active boolean, last_used_at string"
    df = sp.createDataFrame(row, schema=schema)
    df.write.mode("append").saveAsTable(OFFICER_RULES_TABLE)
    return rule_id


def _mark_rule_used(rule_id: str) -> None:
    if not _spark_available():
        return
    sp = _get_spark()
    sp.sql(
        f"""
        UPDATE {OFFICER_RULES_TABLE}
        SET last_used_at = current_timestamp()
        WHERE rule_id = '{rule_id}'
        """
    )


@st.cache_data(ttl=30)
def _case_history(case_id: str) -> pd.DataFrame:
    if not _spark_available():
        return pd.DataFrame()
    sp = _get_spark()
    q = f"""
      SELECT _commit_version, _commit_timestamp, status, risk_score, assigned_to, compliance_action
      FROM table_changes('{SILVER_TABLE}', 0)
      WHERE case_id = '{case_id}'
      ORDER BY _commit_version DESC
    """
    return sp.sql(q).toPandas()


def _kpis(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"total_cases": 0, "total_exposure": 0.0, "avg_shortfall": 0.0, "unique_taxpayers": 0}
    total_exposure = float(df["total_exposure"].fillna(0).sum()) if "total_exposure" in df.columns else 0.0
    avg_shortfall = float(df["tax_shortfall"].fillna(0).mean()) if "tax_shortfall" in df.columns else 0.0
    unique_taxpayers = int(df["taxpayer_abn"].nunique()) if "taxpayer_abn" in df.columns else 0
    return {
        "total_cases": int(len(df)),
        "total_exposure": total_exposure,
        "avg_shortfall": avg_shortfall,
        "unique_taxpayers": unique_taxpayers,
    }


def main() -> None:
    _apply_branding()

    st.markdown(
        f"""
<div class="qro-header">
  <div>
    <div class="qro-title">{APP_TITLE}</div>
    <div class="qro-subtitle">Filter rules • Active cases • Delta history • Escalations</div>
  </div>
  <div class="qro-badge">Catalog: qldrevenue</div>
</div>
         """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("### Officer")
        officer_email = st.text_input("Email", value="revenue.officer1@qro.qld.gov.au")
        st.markdown("---")
        st.markdown("### My Rules")
        rules_df = _load_rules(officer_email)
        rule_options = {"(none)": None}
        if not rules_df.empty:
            for _, r in rules_df.sort_values("rule_name").iterrows():
                rule_options[f"{r['rule_name']}"] = r["rule_id"]
        selected_rule_name = st.selectbox("Select rule", options=list(rule_options.keys()), index=0)
        selected_rule_id = rule_options[selected_rule_name]

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
            st.cache_data.clear()

    cases = _load_cases()
    applied = cases
    applied_rule_conds: Optional[Dict[str, Any]] = None

    if selected_rule_id and not rules_df.empty:
        row = rules_df[rules_df["rule_id"] == selected_rule_id].iloc[0].to_dict()
        applied_rule_conds = json.loads(row.get("filter_conditions") or "{}")
        if _spark_available():
            sp = _get_spark()
            df = sp.table(GOLD_TABLE_ACTIVE)
            if applied_rule_conds.get("case_types"):
                df = df.filter(f"case_type IN ({','.join([repr(x) for x in applied_rule_conds['case_types']])})")
            if applied_rule_conds.get("industry_codes"):
                df = df.filter(f"industry_code IN ({','.join([repr(x) for x in applied_rule_conds['industry_codes']])})")
            if applied_rule_conds.get("tax_shortfall_min") is not None:
                df = df.filter(f"tax_shortfall >= {float(applied_rule_conds['tax_shortfall_min'])}")
            if applied_rule_conds.get("risk_score_min") is not None:
                df = df.filter(f"risk_score >= {int(applied_rule_conds['risk_score_min'])}")
            applied = df.limit(5000).toPandas()
        _mark_rule_used(selected_rule_id)

    k = _kpis(applied)

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
        st.info("No cases loaded. If running locally, this is expected. In Databricks, ensure SQL scripts have been run.")
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
        d["tax_shortfall"] = d["tax_shortfall"].map(lambda x: f"${float(x):,.0f}")
    st.dataframe(d, use_container_width=True, height=360)

    st.markdown("### Case Details")
    selected_case_id = st.text_input("Enter Case ID", value=str(d.iloc[0]["case_id"]))
    if selected_case_id and "case_id" in applied.columns:
        case_row = applied[applied["case_id"] == selected_case_id]
        if case_row.empty:
            st.warning("Case not found in current filtered view.")
        else:
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
