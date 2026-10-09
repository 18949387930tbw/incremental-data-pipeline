"""FastAPI service for triggering runs and querying pipeline state.

The API is a thin read/write layer over the DuckDB warehouse:

* ``POST /pipeline/runs`` schedules a background run (only one at a time)
* the ``GET`` endpoints only read from DuckDB

Run it locally with::

    python -m uvicorn pipeline.service:app --reload --port 8001
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime

import duckdb
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

from pipeline.config import PipelineSettings
from pipeline.db import connect_warehouse
from pipeline.runner import run_pipeline

app = FastAPI(
    title="Incremental Data Pipeline",
    description="Checkpointed, idempotent, quality-aware local data pipeline.",
    version="0.1.0",
)

#: Only one pipeline run may be active at a time (process-level guard).
_run_lock = threading.Lock()


# ---------------------------------------------------------------------------
# schemas
# ---------------------------------------------------------------------------


class RunRequest(BaseModel):
    batch_size: int | None = None
    max_batches: int | None = None


class RunAccepted(BaseModel):
    run_id: str
    status: str = "accepted"


class PipelineRunResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    error_message: str | None = None


class CheckpointResponse(BaseModel):
    source_name: str
    cursor_value: str
    source_fingerprint: str
    updated_at: datetime | None = None


class QuarantineResponse(BaseModel):
    id: str
    run_id: str | None = None
    source_name: str
    source_row_id: str | None = None
    reason_code: str
    raw_payload: str | None = None
    created_at: datetime | None = None


class MetricResponse(BaseModel):
    customer_id: int
    amount_spent: float
    num_positive_reviews: int
    num_reviews: int
    updated_at: datetime | None = None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def get_settings() -> PipelineSettings:
    """Settings dependency (overridable in tests)."""
    return PipelineSettings()


def _fetch(connection: duckdb.DuckDBPyConnection, sql: str, params: list) -> list[dict]:
    result = connection.execute(sql, params)
    columns = [column[0] for column in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _fetch_one(
    connection: duckdb.DuckDBPyConnection, sql: str, params: list
) -> dict | None:
    rows = _fetch(connection, sql, params)
    return rows[0] if rows else None


def _execute_run(
    settings: PipelineSettings, run_id: str, batch_size: int | None, max_batches: int | None
) -> None:
    try:
        run_pipeline(
            settings,
            batch_size=batch_size,
            max_batches=max_batches,
            run_id=run_id,
        )
    finally:
        _run_lock.release()


# ---------------------------------------------------------------------------
# endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health(settings: PipelineSettings = Depends(get_settings)) -> dict:
    connection = connect_warehouse(settings.warehouse_path)
    try:
        last_run = _fetch_one(
            connection,
            "select run_id, status, finished_at from pipeline_run "
            "order by started_at desc limit 1",
            [],
        )
    finally:
        connection.close()
    return {
        "status": "ok",
        "warehouse": str(settings.warehouse_path),
        "last_run": last_run,
    }


@app.post("/pipeline/runs", status_code=202, response_model=RunAccepted)
def trigger_run(
    request: RunRequest,
    background: BackgroundTasks,
    settings: PipelineSettings = Depends(get_settings),
) -> RunAccepted:
    if not _run_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409, detail={"code": "PIPELINE_ALREADY_RUNNING"}
        )
    run_id = str(uuid.uuid4())
    background.add_task(
        _execute_run, settings, run_id, request.batch_size, request.max_batches
    )
    return RunAccepted(run_id=run_id)


@app.get("/pipeline/runs")
def list_runs(
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    settings: PipelineSettings = Depends(get_settings),
) -> dict:
    connection = connect_warehouse(settings.warehouse_path)
    try:
        items = _fetch(
            connection,
            "select run_id, pipeline_name, status, started_at, finished_at, "
            "rows_read, rows_written, rows_quarantined, error_message "
            "from pipeline_run order by started_at desc limit ? offset ?",
            [limit, offset],
        )
    finally:
        connection.close()
    return {"items": items, "limit": limit, "offset": offset}


@app.get("/pipeline/runs/{run_id}", response_model=PipelineRunResponse)
def get_run(run_id: str, settings: PipelineSettings = Depends(get_settings)):
    connection = connect_warehouse(settings.warehouse_path)
    try:
        row = _fetch_one(
            connection,
            "select run_id, pipeline_name, status, started_at, finished_at, "
            "rows_read, rows_written, rows_quarantined, error_message "
            "from pipeline_run where run_id = ?",
            [run_id],
        )
    finally:
        connection.close()
    if row is None:
        raise HTTPException(status_code=404, detail={"code": "RUN_NOT_FOUND"})
    return row


@app.get("/pipeline/checkpoints")
def list_checkpoints(settings: PipelineSettings = Depends(get_settings)) -> dict:
    connection = connect_warehouse(settings.warehouse_path)
    try:
        items = _fetch(
            connection,
            "select source_name, cursor_value, source_fingerprint, updated_at "
            "from source_checkpoint order by source_name",
            [],
        )
    finally:
        connection.close()
    return {"items": items}


@app.get("/pipeline/quarantine")
def list_quarantine(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    source_name: str | None = None,
    settings: PipelineSettings = Depends(get_settings),
) -> dict:
    connection = connect_warehouse(settings.warehouse_path)
    try:
        if source_name:
            items = _fetch(
                connection,
                "select id, run_id, source_name, source_row_id, reason_code, "
                "raw_payload, created_at from quarantine_record "
                "where source_name = ? order by created_at desc limit ? offset ?",
                [source_name, limit, offset],
            )
        else:
            items = _fetch(
                connection,
                "select id, run_id, source_name, source_row_id, reason_code, "
                "raw_payload, created_at from quarantine_record "
                "order by created_at desc limit ? offset ?",
                [limit, offset],
            )
    finally:
        connection.close()
    return {"items": items, "limit": limit, "offset": offset}


@app.get("/metrics/behaviour")
def list_metrics(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    settings: PipelineSettings = Depends(get_settings),
) -> dict:
    connection = connect_warehouse(settings.warehouse_path)
    try:
        items = _fetch(
            connection,
            "select customer_id, amount_spent, num_positive_reviews, "
            "num_reviews, updated_at from customer_behaviour_metrics "
            "order by customer_id limit ? offset ?",
            [limit, offset],
        )
    finally:
        connection.close()
    return {"items": items, "limit": limit, "offset": offset}