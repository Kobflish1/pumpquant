"""DuckDB helpers shared by the PumpAPI feature builders."""

from __future__ import annotations

from typing import Any

import duckdb


def one_row(result: duckdb.DuckDBPyConnection) -> tuple[Any, ...]:
    """Return the single row an aggregate query always produces."""
    row = result.fetchone()
    if row is None:
        raise RuntimeError("aggregate query returned no row")
    return row
