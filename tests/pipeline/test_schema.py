import duckdb

from pipeline.db import initialize_schema


def test_schema_contains_pipeline_tables(tmp_path):
    connection = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    initialize_schema(connection)
    names = {
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables"
        ).fetchall()
    }
    assert {
        "pipeline_run",
        "source_checkpoint",
        "raw_movie_review",
        "raw_user_purchase",
        "quarantine_record",
        "customer_behaviour_metrics",
    }.issubset(names)


def test_initialize_schema_is_idempotent(tmp_path):
    connection = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    initialize_schema(connection)
    initialize_schema(connection)
    names = {
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables"
        ).fetchall()
    }
    assert "pipeline_run" in names