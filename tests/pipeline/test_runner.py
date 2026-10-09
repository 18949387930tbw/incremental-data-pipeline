"""End-to-end tests for the incremental runner."""

from __future__ import annotations

import pytest

from pipeline.db import connect_warehouse
from pipeline.runner import run_pipeline
from pipeline.sources import read_checkpoint


def _count(path, table):
    con = connect_warehouse(path)
    try:
        return con.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        con.close()


def test_running_same_batch_twice_does_not_duplicate_metrics(pipeline_settings):
    first_run = run_pipeline(pipeline_settings, batch_size=2)
    second_run = run_pipeline(pipeline_settings, batch_size=2)

    assert first_run != second_run
    assert _count(pipeline_settings.warehouse_path, "raw_movie_review") == 4
    assert _count(pipeline_settings.warehouse_path, "customer_behaviour_metrics") > 0


def test_incremental_rows_are_processed_once(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=2, max_batches=1)
    assert _count(pipeline_settings.warehouse_path, "raw_movie_review") == 2

    run_pipeline(pipeline_settings, batch_size=2, max_batches=1)
    assert _count(pipeline_settings.warehouse_path, "raw_movie_review") == 4


def test_quarantine_receives_invalid_rows(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=10)

    con = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        reasons = {
            row[0]
            for row in con.execute(
                "select distinct reason_code from quarantine_record"
            ).fetchall()
        }
    finally:
        con.close()

    # fixture has a negative quantity, a blank customer id and a negative price
    assert "negative_quantity" in reasons
    assert "missing_required_field" in reasons
    assert "negative_unit_price" in reasons


def test_dashboard_and_csv_are_written(pipeline_settings):
    run_pipeline(pipeline_settings, batch_size=10)

    assert pipeline_settings.paths.behaviour_metrics_csv.exists()
    assert pipeline_settings.paths.dashboard_html.exists()
    assert pipeline_settings.paths.dashboard_html.read_text(encoding="utf-8").startswith(
        "<html>"
    )


def test_run_history_is_recorded(pipeline_settings):
    run_id = run_pipeline(pipeline_settings, batch_size=10)

    con = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        row = con.execute(
            "select status, rows_read, rows_written, rows_quarantined "
            "from pipeline_run where run_id = ?",
            [run_id],
        ).fetchone()
    finally:
        con.close()

    status, rows_read, rows_written, rows_quarantined = row
    assert status == "succeeded"
    assert rows_read == 8  # 4 reviews + 4 purchases
    assert rows_quarantined == 3  # negative qty, blank customer, negative price


def test_checkpoint_is_not_advanced_on_failure(pipeline_settings, monkeypatch):
    import pipeline.runner as runner

    def boom(*args, **kwargs):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(runner, "upsert_raw_rows", boom)

    with pytest.raises(RuntimeError):
        run_pipeline(pipeline_settings, batch_size=2, max_batches=1)

    con = connect_warehouse(pipeline_settings.warehouse_path)
    try:
        assert read_checkpoint(con, "movie_review") == 0
        status = con.execute(
            "select status from pipeline_run order by started_at desc limit 1"
        ).fetchone()[0]
        assert status == "failed"
    finally:
        con.close()