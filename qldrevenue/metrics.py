from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def _num(series: pd.Series) -> pd.Series:
    # Databricks SQL often returns DECIMAL as strings. Coerce safely.
    return pd.to_numeric(series, errors="coerce").fillna(0)


def kpis(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"total_cases": 0, "total_exposure": 0.0, "avg_shortfall": 0.0, "unique_taxpayers": 0}

    total_exposure = 0.0
    avg_shortfall = 0.0
    if "total_exposure" in df.columns:
        total_exposure = float(_num(df["total_exposure"]).sum())
    if "tax_shortfall" in df.columns:
        avg_shortfall = float(_num(df["tax_shortfall"]).mean())

    unique_taxpayers = int(df["taxpayer_abn"].nunique()) if "taxpayer_abn" in df.columns else 0

    return {
        "total_cases": int(len(df)),
        "total_exposure": total_exposure,
        "avg_shortfall": avg_shortfall,
        "unique_taxpayers": unique_taxpayers,
    }
