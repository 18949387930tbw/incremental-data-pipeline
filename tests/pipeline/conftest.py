"""Shared fixtures for the pipeline test suite."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from pipeline.config import PipelinePaths, PipelineSettings
from pipeline.db import initialize_schema

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def connection(tmp_path):
    """An isolated DuckDB warehouse with the pipeline schema applied."""
    con = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    initialize_schema(con)
    yield con
    con.close()


@pytest.fixture
def pipeline_settings(tmp_path):
    """Settings pointing at the tiny fixtures and a throwaway warehouse."""
    paths = PipelinePaths(
        root=tmp_path,
        data_dir=tmp_path,
        movie_review_csv=FIXTURES / "tiny_movie_review.csv",
        user_purchase_csv=FIXTURES / "tiny_user_purchase.csv",
        behaviour_metrics_csv=tmp_path / "behaviour_metrics.csv",
        dashboard_html=tmp_path / "dashboard.html",
        warehouse=tmp_path / "warehouse.duckdb",
    )
    return PipelineSettings(paths=paths, batch_size=2)