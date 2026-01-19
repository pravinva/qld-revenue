from datetime import date

from qldrevenue.formatting import financial_year_for_period, format_abn


def test_format_abn_spaces() -> None:
    assert format_abn("51824753556") == "51 824 753 556"
    assert format_abn("51 824 753 556") == "51 824 753 556"


def test_format_abn_nonstandard_passthrough() -> None:
    assert format_abn("123") == "123"


def test_financial_year_label() -> None:
    assert financial_year_for_period(date(2023, 7, 1)) == "2023-24"
    assert financial_year_for_period(date(2024, 6, 30)) == "2023-24"
    assert financial_year_for_period(date(2024, 7, 1)) == "2024-25"
