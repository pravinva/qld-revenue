from qldrevenue.calculations import interest_amount, penalty_amount


def test_penalty_rates() -> None:
    assert penalty_amount(100.0, is_fraud=False) == 20.0
    assert penalty_amount(100.0, is_fraud=True) == 75.0


def test_interest_formula() -> None:
    assert interest_amount(1000.0, days_overdue=365, annual_rate=0.08) == 80.0
