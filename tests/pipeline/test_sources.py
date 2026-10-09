"""Tests for checkpointed, batched CSV reading."""

from __future__ import annotations

from pipeline.sources import (
    read_batch,
    read_checkpoint,
    source_fingerprint,
    update_checkpoint,
)


def test_read_batch_returns_requested_slice(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n2,bad\n3,good\n", encoding="utf-8")

    batch = read_batch(source, offset=1, batch_size=1)

    assert len(batch.rows) == 1
    assert batch.next_offset == 2
    assert batch.rows[0]["review_str"] == "bad"


def test_read_batch_never_exceeds_batch_size(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n2,bad\n3,good\n", encoding="utf-8")

    batch = read_batch(source, offset=0, batch_size=2)

    assert len(batch.rows) == 2
    assert batch.next_offset == 2


def test_read_batch_at_eof_returns_empty_batch(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n", encoding="utf-8")

    batch = read_batch(source, offset=5, batch_size=10)

    assert batch.rows == []
    assert batch.next_offset == 5


def test_source_row_ids_are_stable_and_unique(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n2,bad\n", encoding="utf-8")

    first = read_batch(source, offset=0, batch_size=10)
    second = read_batch(source, offset=0, batch_size=10)

    ids = [row["source_row_id"] for row in first.rows]
    assert ids == [row["source_row_id"] for row in second.rows]
    assert len(set(ids)) == len(ids)


def test_missing_checkpoint_starts_at_zero(connection):
    assert read_checkpoint(connection, "movie_review") == 0


def test_checkpoint_roundtrip(connection):
    update_checkpoint(connection, "movie_review", 5, "fingerprint-1")

    assert read_checkpoint(connection, "movie_review") == 5


def test_update_checkpoint_overwrites_previous_value(connection):
    update_checkpoint(connection, "movie_review", 5, "fingerprint-1")
    update_checkpoint(connection, "movie_review", 9, "fingerprint-2")

    assert read_checkpoint(connection, "movie_review") == 9


def test_fingerprint_changes_when_file_changes(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n", encoding="utf-8")
    first = source_fingerprint(source)

    source.write_text("cid,review_str\n1,good\n2,bad\n", encoding="utf-8")
    second = source_fingerprint(source)

    assert first != second