import pandas as pd

from qldrevenue.metrics import kpis


def test_kpis_handles_string_decimals() -> None:
    df = pd.DataFrame(
        {
            "total_exposure": ["10.50", "2.25"],
            "tax_shortfall": ["1.00", "3.00"],
            "taxpayer_abn": ["11", "22"],
        }
    )
    out = kpis(df)
    assert out["total_cases"] == 2
    assert out["total_exposure"] == 12.75
    assert out["avg_shortfall"] == 2.0
    assert out["unique_taxpayers"] == 2
