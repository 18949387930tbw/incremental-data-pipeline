"""Run orchestration: staging, idempotent upsert, checkpoints and metrics.

One ``run_pipeline`` call:

1. records a ``pipeline_run`` row with ``status=running``
2. for each source, reads batches from its checkpoint and processes them
3. each batch runs inside its own transaction: quarantine bad rows, upsert
   clean rows by ``source_row_id``, then advance the checkpoint. The checkpoint
   only moves when the transaction commits, so a crash resumes safely.
4. recomputes the full metrics table and re-renders the dashboard
5. marks the run ``succeeded`` (or ``failed`` with the error message)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd

from pipeline.config import PipelineSettings
from pipeline.db import connect_warehouse
from pipeline.quality import (
    QualityResult,
    validate_movie_review,
    validate_user_purchase,
)
from pipeline.sources import (
    SourceBatch,
    read_batch,
    read_checkpoint,
    source_row_count,
    update_checkpoint,
)
from pipeline.transforms import (
    build_metrics,
    classify_review,
    read_metrics,
    render_dashboard,
    write_metrics_csv,
)

#: CSV header -> warehouse column for the OnlineRetail source.
USER_PURCHASE_COLUMNS = {
    "InvoiceNo": "invoice_number",
    "StockCode": "stock_code",
    "Description": "detail",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}

#: Warehouse columns written per source, in insert order.
RAW_COLUMNS: dict[str, tuple[str, ...]] = {
    "movie_review": (
        "source_row_id",
        "cid",
        "review_str",
        "positive_review",
        "source_name",
    ),
    "user_purchase": (
        "source_row_id",
        "invoice_number",
        "stock_code",
        "detail",
        "quantity",
        "invoice_date",
        "unit_price",
        "customer_id",
        "country",
        "source_name",
    ),
}

_RAW_TABLE = {
    "movie_review": "raw_movie_review",
    "user_purchase": "raw_user_purchase",
}


@dataclass(frozen=True)
class _Source:
    name: str
    path: Path
    validate: Callable[[list[dict]], QualityResult]
    rename: Callable[[dict], dict]
    enrich: Callable[[dict, str], dict]


@dataclass
class RunStats:
    read: int = 0
    written: int = 0
    quarantined: int = 0


# ---------------------------------------------------------------------------
# per-source helpers
# ---------------------------------------------------------------------------


def _identity(row: dict) -> dict:
    return dict(row)


def _rename_user_purchase(row: dict) -> dict:
    return {USER_PURCHASE_COLUMNS.get(key, key): value for key, value in row.items()}


def _enrich_movie_review(row: dict, source_name: str) -> dict:
    enriched = dict(row)
    enriched["positive_review"] = classify_review(row.get("review_str"))
    enriched["source_name"] = source_name
    return enriched


def _enrich_user_purchase(row: dict, source_name: str) -> dict:
    enriched = dict(row)
    enriched["source_name"] = source_name
    return enriched


def _sources(settings: PipelineSettings) -> list[_Source]:
    paths = settings.paths
    return [
        _Source(
            name=paths.movie_review_source,
            path=paths.movie_review_csv,
            validate=validate_movie_review,
            rename=_identity,
            enrich=_enrich_movie_review,
        ),
        _Source(
            name=paths.user_purchase_source,
            path=paths.user_purchase_csv,
            validate=validate_user_purchase,
            rename=_rename_user_purchase,
            enrich=_enrich_user_purchase,
        ),
    ]


# ---------------------------------------------------------------------------
# staging + idempotent upsert
# ---------------------------------------------------------------------------


def _row_count(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    return connection.execute(f"select count(*) from {table}").fetchone()[0]


def upsert_raw_rows(
    connection: duckdb.DuckDBPyConnection, source_name: str, rows: list[dict]
) -> int:
    """Land clean rows into the raw table, ignoring duplicates.

    Rows are staged in a registered DataFrame view and copied with
    ``on conflict (source_row_id) do nothing`` so replaying a batch is a no-op.
    Returns the number of rows actually inserted.
    """
    if not rows:
        return 0

    table = _RAW_TABLE[source_name]
    columns = list(RAW_COLUMNS[source_name])
    frame = pd.DataFrame(rows, columns=columns)

    before = _row_count(connection, table)
    connection.register("_staging_rows", frame)
    try:
        connection.execute(
            f"insert into {table} ({', '.join(columns)}) "
            f"select {', '.join(columns)} from _staging_rows "
            "on conflict (source_row_id) do nothing"
        )
    finally:
        connection.unregister("_staging_rows")
    return _row_count(connection, table) - before


def _insert_quarantine(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    source_name: str,
    rows: list[dict],
) -> int:
    if not rows:
        return 0
    connection.executemany(
        """
        insert into quarantine_record
            (id, run_id, source_name, source_row_id, reason_code, raw_payload)
        values (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(uuid.uuid4()),
                run_id,
                source_name,
                row.get("source_row_id"),
                row["reason_code"],
                row["raw_payload"],
            )
            for row in rows
        ],
    )
    return len(rows)


# ---------------------------------------------------------------------------
# run bookkeeping
# ---------------------------------------------------------------------------


def _start_run(
    connection: duckdb.DuckDBPyConnection, run_id: str, pipeline_name: str
) -> None:
    connection.execute(
        """
        insert into pipeline_run (run_id, pipeline_name, status, started_at)
        values (?, ?, 'running', now())
        """,
        [run_id, pipeline_name],
    )


def _update_run_counts(
    connection: duckdb.DuckDBPyConnection, run_id: str, stats: RunStats
) -> None:
    connection.execute(
        """
        update pipeline_run
        set rows_read = ?, rows_written = ?, rows_quarantined = ?
        where run_id = ?
        """,
        [stats.read, stats.written, stats.quarantined, run_id],
    )


def _mark_succeeded(connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    connection.execute(
        "update pipeline_run set status = 'succeeded', finished_at = now() "
        "where run_id = ?",
        [run_id],
    )


def _mark_failed(
    connection: duckdb.DuckDBPyConnection, run_id: str, error: Exception
) -> None:
    connection.execute(
        "update pipeline_run set status = 'failed', finished_at = now(), "
        "error_message = ? where run_id = ?",
        [str(error)[:2000], run_id],
    )


# ---------------------------------------------------------------------------
# batch + run orchestration
# ---------------------------------------------------------------------------


def _process_batch(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    source: _Source,
    batch: SourceBatch,
) -> tuple[int, int]:
    """Stage, validate and commit one batch. Returns (written, quarantined)."""
    renamed = [source.rename(row) for row in batch.rows]
    result = source.validate(renamed)
    enriched = [source.enrich(row, source.name) for row in result.clean_rows]

    connection.begin()
    try:
        quarantined = _insert_quarantine(
            connection, run_id, source.name, result.quarantined_rows
        )
        written = upsert_raw_rows(connection, source.name, enriched)
        update_checkpoint(connection, source.name, batch.next_offset, batch.fingerprint)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return written, quarantined


def _process_source(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    source: _Source,
    batch_size: int,
    max_batches: int | None,
) -> RunStats:
    stats = RunStats()
    offset = read_checkpoint(connection, source.name)

    total_rows = source_row_count(source.path)
    if offset > total_rows:
        # The file was replaced with a shorter one: restart from the top.
        # Re-reading is safe because upserts key on source_row_id.
        offset = 0

    batches = 0
    while max_batches is None or batches < max_batches:
        batch = read_batch(
            source.path,
            offset=offset,
            batch_size=batch_size,
            source_name=source.name,
        )
        if not batch.rows:
            break
        written, quarantined = _process_batch(connection, run_id, source, batch)
        stats.read += batch.size
        stats.written += written
        stats.quarantined += quarantined
        offset = batch.next_offset
        batches += 1
    return stats


def run_pipeline(
    settings: PipelineSettings,
    batch_size: int | None = None,
    max_batches: int | None = None,
) -> str:
    """Run the incremental pipeline and return the run id.

    ``batch_size`` defaults to ``settings.batch_size``. By default the run
    drains every available batch for each source; pass ``max_batches=1`` to
    process a single batch per source (useful for tests and step-by-step demos).
    """
    effective_batch = batch_size or settings.batch_size
    run_id = str(uuid.uuid4())
    connection = connect_warehouse(settings.warehouse_path)
    try:
        _start_run(connection, run_id, settings.pipeline_name)

        stats = RunStats()
        for source in _sources(settings):
            source_stats = _process_source(
                connection, run_id, source, effective_batch, max_batches
            )
            stats.read += source_stats.read
            stats.written += source_stats.written
            stats.quarantined += source_stats.quarantined
        _update_run_counts(connection, run_id, stats)

        build_metrics(connection)
        metrics = read_metrics(connection)
        write_metrics_csv(settings.paths.behaviour_metrics_csv, metrics)
        render_dashboard(settings.paths.dashboard_html, metrics)

        _mark_succeeded(connection, run_id)
        return run_id
    except Exception as error:
        try:
            connection.rollback()
        except Exception:
            pass
        try:
            _mark_failed(connection, run_id, error)
        except Exception:
            pass
        raise
    finally:
        connection.close()