"""DuckDB connection helpers for the local pipeline."""

from __future__ import annotations

from pathlib import Path

import duckdb

from pipeline.schema import SCHEMA_STATEMENTS


def initialize_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create every pipeline table if it does not already exist."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)


def connect_warehouse(path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open (creating if needed) the DuckDB warehouse and ensure the schema.

    The parent directory is created on demand so callers can pass a path
    inside a pytest ``tmp_path`` without extra setup.
    """
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(resolved))
    initialize_schema(connection)
    return connection