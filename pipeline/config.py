"""Paths and settings for the local incremental pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PipelinePaths:
    """Filesystem layout of the local pipeline.

    Defaults resolve relative to the repository root so the pipeline behaves
    the same no matter which directory it is launched from.
    """

    root: Path = ROOT
    data_dir: Path = ROOT / "data"
    movie_review_csv: Path = ROOT / "data" / "movie_review.csv"
    user_purchase_csv: Path = ROOT / "data" / "OnlineRetail.csv"
    behaviour_metrics_csv: Path = ROOT / "data" / "behaviour_metrics.csv"
    dashboard_html: Path = ROOT / "dags" / "scripts" / "dashboard" / "dashboard.html"
    warehouse: Path = ROOT / "data" / "warehouse.duckdb"

    #: logical source identifiers used for checkpoints and quarantine rows
    movie_review_source: str = "movie_review"
    user_purchase_source: str = "user_purchase"


@dataclass(frozen=True)
class PipelineSettings:
    """Runtime configuration for a pipeline run."""

    paths: PipelinePaths = field(default_factory=PipelinePaths)
    batch_size: int = 5000
    pipeline_name: str = "user_analytics_local"

    @property
    def warehouse_path(self) -> Path:
        return self.paths.warehouse