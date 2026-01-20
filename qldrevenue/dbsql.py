from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Optional

import pandas as pd


def state_str(state: Any) -> Optional[str]:
    """Normalize SDK enum/string state values to plain strings (e.g., 'SUCCEEDED')."""
    if state is None:
        return None
    if isinstance(state, str):
        return state
    if hasattr(state, "value"):
        try:
            return str(state.value)
        except Exception:
            pass
    if hasattr(state, "name"):
        try:
            return str(state.name)
        except Exception:
            pass
    s = str(state)
    return s.split(".")[-1]


def dataframe_from_statement_response(resp: Any) -> pd.DataFrame:
    """Extract a pandas DataFrame from a databricks-sdk statement execution response.

    Newer SDK shape:
      resp.manifest.schema.columns + resp.result.data_array

    Older/alternate shapes are handled best-effort.
    """
    manifest = getattr(resp, "manifest", None)
    result = getattr(resp, "result", None)

    # Some older shapes might attach manifest under result
    if manifest is None and result is not None:
        manifest = getattr(result, "manifest", None)

    schema = getattr(manifest, "schema", None) if manifest is not None else None
    columns = getattr(schema, "columns", None) if schema is not None else None
    if not columns:
        return pd.DataFrame()

    col_names = [getattr(c, "name", None) for c in columns]
    col_names = [c if c is not None else "" for c in col_names]

    rows = []
    if result is not None:
        rows = getattr(result, "data_array", None) or []
    if not rows:
        rows = getattr(resp, "data_array", None) or []

    return pd.DataFrame(rows, columns=col_names)
