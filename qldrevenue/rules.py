from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pandas as pd


@dataclass(frozen=True)
class OfficerRule:
    rule_id: str
    officer_email: str
    rule_name: str
    filter_conditions: Dict[str, Any]

    @staticmethod
    def from_json_row(row: Dict[str, Any]) -> "OfficerRule":
        conds = row.get("filter_conditions")
        if isinstance(conds, str):
            conds_dict = json.loads(conds) if conds.strip() else {}
        elif isinstance(conds, dict):
            conds_dict = conds
        else:
            conds_dict = {}

        return OfficerRule(
            rule_id=row["rule_id"],
            officer_email=row["officer_email"],
            rule_name=row.get("rule_name", ""),
            filter_conditions=conds_dict,
        )


def apply_rule_df(cases: pd.DataFrame, conds: Dict[str, Any]) -> pd.DataFrame:
    """Apply a subset of rule conditions to a cases DataFrame.

    Supported conditions (aligned to the QA guide):
    - case_types: list[str] on `case_type`
    - industry_codes: list[str] on `industry_code`
    - tax_shortfall_min: number on `tax_shortfall`
    - risk_score_min: number on `risk_score`
    - sla_breached: bool on `sla_breached`
    - financial_year: str on `financial_year`
    - regional_office: str on `regional_office`
    """

    df = cases.copy()

    def _maybe(col: str) -> bool:
        return col in df.columns

    if conds.get("case_types") and _maybe("case_type"):
        df = df[df["case_type"].isin(list(conds["case_types"]))]

    if conds.get("case_domains") and _maybe("case_domain"):
        df = df[df["case_domain"].isin(list(conds["case_domains"]))]

    if conds.get("industry_codes") and _maybe("industry_code"):
        df = df[df["industry_code"].isin(list(conds["industry_codes"]))]

    if conds.get("tax_shortfall_min") is not None and _maybe("tax_shortfall"):
        df = df[df["tax_shortfall"] >= float(conds["tax_shortfall_min"])]

    if conds.get("risk_score_min") is not None and _maybe("risk_score"):
        df = df[df["risk_score"] >= int(conds["risk_score_min"])]

    if conds.get("sla_breached") is not None and _maybe("sla_breached"):
        df = df[df["sla_breached"] == bool(conds["sla_breached"])]

    if conds.get("financial_year") and _maybe("financial_year"):
        df = df[df["financial_year"] == str(conds["financial_year"])]

    if conds.get("regional_office") and _maybe("regional_office"):
        df = df[df["regional_office"] == str(conds["regional_office"])]

    return df.reset_index(drop=True)


def to_rule_conditions(
    case_types: Optional[Iterable[str]] = None,
    case_domains: Optional[Iterable[str]] = None,
    industry_codes: Optional[Iterable[str]] = None,
    tax_shortfall_min: Optional[float] = None,
    risk_score_min: Optional[int] = None,
    sla_breached: Optional[bool] = None,
    financial_year: Optional[str] = None,
    regional_office: Optional[str] = None,
) -> Dict[str, Any]:
    conds: Dict[str, Any] = {}
    if case_types:
        conds["case_types"] = list(case_types)
    if case_domains:
        conds["case_domains"] = list(case_domains)
    if industry_codes:
        conds["industry_codes"] = list(industry_codes)
    if tax_shortfall_min is not None:
        conds["tax_shortfall_min"] = float(tax_shortfall_min)
    if risk_score_min is not None:
        conds["risk_score_min"] = int(risk_score_min)
    if sla_breached is not None:
        conds["sla_breached"] = bool(sla_breached)
    if financial_year:
        conds["financial_year"] = str(financial_year)
    if regional_office:
        conds["regional_office"] = str(regional_office)
    return conds
