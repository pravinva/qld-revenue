import re
from datetime import date


def format_abn(abn: str) -> str:
    """Format ABN as 'XX XXX XXX XXX' (11 digits)."""
    if abn is None:
        return ""
    digits = re.sub(r"\D", "", str(abn))
    if len(digits) != 11:
        return str(abn)
    return re.sub(r"(\d{2})(\d{3})(\d{3})(\d{3})", r"\1 \2 \3 \4", digits)


def financial_year_for_period(period_start: date) -> str:
    """Australian FY label for a period start date. FY runs July -> June."""
    if period_start.month >= 7:
        start_year = period_start.year
    else:
        start_year = period_start.year - 1
    end_year_short = (start_year + 1) % 100
    return f"{start_year}-{end_year_short:02d}"
