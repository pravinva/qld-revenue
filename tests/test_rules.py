import pandas as pd

from qldrevenue.rules import apply_rule_df, to_rule_conditions


def test_mining_sector_high_risk_rule_filters_correctly() -> None:
    cases = pd.DataFrame(
        [
            {
                "case_id": "CASE-PT-FRAUD-MINING-001",
                "case_type": "Payroll Tax",
                "industry_code": "0600",
                "tax_shortfall": 285000.0,
                "risk_score": 88,
            },
            {
                "case_id": "CASE-PT-00123",
                "case_type": "Payroll Tax",
                "industry_code": "0600",
                "tax_shortfall": 48000.0,
                "risk_score": 72,
            },
            {
                "case_id": "CASE-PT-00456",
                "case_type": "Payroll Tax",
                "industry_code": "4500",
                "tax_shortfall": 65000.0,
                "risk_score": 75,
            },
        ]
    )

    conds = to_rule_conditions(
        case_types=["Payroll Tax"],
        industry_codes=["0600"],
        tax_shortfall_min=50000,
        risk_score_min=60,
    )
    out = apply_rule_df(cases, conds)
    assert out["case_id"].tolist() == ["CASE-PT-FRAUD-MINING-001"]
