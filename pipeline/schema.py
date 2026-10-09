"""Idempotent DuckDB DDL for the incremental pipeline.

The warehouse keeps three kinds of tables:

* bookkeeping: ``pipeline_run`` and ``source_checkpoint``
* landed raw data: ``raw_movie_review`` and ``raw_user_purchase``
* quality + serving: ``quarantine_record`` and ``customer_behaviour_metrics``

Every statement uses ``create table if not exists`` so ``initialize_schema``
can be called on every connection without side effects.
"""

from __future__ import annotations

SCHEMA_STATEMENTS: tuple[str, ...] = (
    # --- bookkeeping -----------------------------------------------------
    """
    create table if not exists pipeline_run (
        run_id varchar primary key,
        pipeline_name varchar not null,
        status varchar not null,
        started_at timestamp not null,
        finished_at timestamp,
        rows_read bigint not null default 0,
        rows_written bigint not null default 0,
        rows_quarantined bigint not null default 0,
        error_message varchar,
        created_at timestamp not null default current_timestamp
    )
    """,
    """
    create table if not exists source_checkpoint (
        source_name varchar primary key,
        cursor_value varchar not null,
        source_fingerprint varchar not null,
        updated_at timestamp not null default current_timestamp
    )
    """,
    # --- landed raw data -------------------------------------------------
    """
    create table if not exists raw_movie_review (
        source_row_id varchar primary key,
        cid bigint,
        review_str varchar,
        positive_review boolean,
        source_name varchar not null,
        ingested_at timestamp not null default current_timestamp
    )
    """,
    """
    create table if not exists raw_user_purchase (
        source_row_id varchar primary key,
        invoice_number varchar,
        stock_code varchar,
        detail varchar,
        quantity bigint,
        invoice_date timestamp,
        unit_price double,
        customer_id bigint,
        country varchar,
        source_name varchar not null,
        ingested_at timestamp not null default current_timestamp
    )
    """,
    # --- quality + serving ----------------------------------------------
    """
    create table if not exists quarantine_record (
        id varchar primary key,
        run_id varchar,
        source_name varchar not null,
        source_row_id varchar,
        reason_code varchar not null,
        raw_payload varchar,
        created_at timestamp not null default current_timestamp
    )
    """,
    """
    create table if not exists customer_behaviour_metrics (
        customer_id bigint primary key,
        amount_spent double not null default 0,
        num_positive_reviews bigint not null default 0,
        num_reviews bigint not null default 0,
        updated_at timestamp not null default current_timestamp
    )
    """,
)

SCHEMA_TABLES: tuple[str, ...] = (
    "pipeline_run",
    "source_checkpoint",
    "raw_movie_review",
    "raw_user_purchase",
    "quarantine_record",
    "customer_behaviour_metrics",
)