from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CalculationInputs:
    tax_shortfall: float
    days_overdue: int
    is_fraud: bool


def penalty_amount(tax_shortfall: float, is_fraud: bool) -> float:
    """Penalty: 20% for standard review, 75% for fraud."""
    rate = 0.75 if is_fraud else 0.20
    return round(float(tax_shortfall) * rate, 2)


def interest_amount(tax_shortfall: float, days_overdue: int, annual_rate: float = 0.08) -> float:
    """Simple interest: shortfall * annual_rate * (days_overdue/365)."""
    return round(float(tax_shortfall) * float(annual_rate) * (int(days_overdue) / 365.0), 2)
