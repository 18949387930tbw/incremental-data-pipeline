"""Shared fixtures for the pipeline test suite."""

from __future__ import annotations

import duckdb
import pytest

from pipeline.db import initialize_schema


@pytest.fixture
def connection(tmp_path):
    """An isolated DuckDB warehouse with the pipeline schema applied."""
    con = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    initialize_schema(con)
    yield con
    con.close()