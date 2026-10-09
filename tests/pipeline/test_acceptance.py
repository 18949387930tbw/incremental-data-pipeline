"""Acceptance test for the whole incremental pipeline workflow.

Covers the end-to-end contract:

* the checkpoint only advances after a successful commit
* re-running the same rows never duplicates raw rows or metrics
* invalid rows land in quarantine
* the metrics table only contains clean data
"""

from __future__ import annotations

import pytest

from pipeline.db import connect_warehouse
from pipeline.runner import run_pipeline
from pipeline.sources import read_checkpoint


@pytest.fixture
def warehouse(pipeline_settings):
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    yield connection
    connection.close()


def test_checkpoint_advances_one_batch_at_a_time(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=2, max_batches=1)
    assert read_checkpoint(_con(pipeline_settings), "movie_review") == 2

    run_pipeline(pipeline_settings, batch_size=2, max_batches=1)
    assert read_checkpoint(_con(pipeline_settings), "movie_review") == 4


def test_full_workflow_is_idempotent(pipeline_settings):
    # first run processes step by step, second run drains the remainder
    run_pipeline(pipeline_settings, batch_size=2, max_batches=1)
    run_pipeline(pipeline_settings, batch_size=2)

    assert _count(pipeline_settings, "raw_movie_review") == 4
    assert _distinct(pipeline_settings, "raw_movie_review", "source_row_id") == 4

    # replay the same source rows: nothing should be inserted twice
    _reset_checkpoints(pipeline_settings)
    run_pipeline(pipeline_settings, batch_size=2)

    assert _count(pipeline_settings, "raw_movie_review") == 4
    assert _count(pipeline_settings, "customer_behaviour_metrics") == 1


def test_quarantine_holds_invalid_rows(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=10)

    reasons = _reasons(pipeline_settings)

    assert reasons == {
        "missing_required_field": 1,
        "negative_quantity": 1,
        "negative_unit_price": 1,
    }


def test_metrics_only_contain_clean_data(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=10)

    connection = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        rows = connection.execute(
            "select customer_id, amount_spent, num_reviews "
            "from customer_behaviour_metrics"
        ).fetchall()
    finally:
        connection.close()

    # only customer 17850 has a clean purchase (6 * 2.55 = 15.3)
    assert len(rows) == 1
    customer_id, amount_spent, num_reviews = rows[0]
    assert customer_id == 17850
    assert amount_spent == pytest.approx(15.3)
    assert num_reviews == 1


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _con(pipeline_settings):
    return connect_warehouse(pipeline_settings.warehouse_path)


def _count(pipeline_settings, table):
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        return connection.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        connection.close()


def _distinct(pipeline_settings, table, column):
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        return connection.execute(
            f"select count(distinct {column}) from {table}"
        ).fetchone()[0]
    finally:
        connection.close()


def _reasons(pipeline_settings):
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        rows = connection.execute(
            "select reason_code, count(*) from quarantine_record group by 1"
        ).fetchall()
    finally:
        connection.close()
    return {code: count for code, count in rows}


def _reset_checkpoints(pipeline_settings):
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        connection.execute("update source_checkpoint set cursor_value = '0'")
    finally:
        connection.close()