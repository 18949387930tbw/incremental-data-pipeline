"""Checkpointed, batched CSV reading.

Local sources are plain CSV files. Because they are append-only in practice we
track progress as a *row offset* plus a *file fingerprint* (size + mtime).

The fingerprint protects against silently skipping data: if the file is
replaced between runs the fingerprint changes and the runner must reconcile
before it trusts the stored offset.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class SourceBatch:
    """A single slice of a CSV source, ready for staging."""

    source_name: str
    rows: list[dict]
    next_offset: int
    fingerprint: str

    @property
    def size(self) -> int:
        return len(self.rows)


def compute_row_id(source_name: str, row_number: int, row: dict) -> str:
    """Hash the source name, row offset and raw values into a natural key."""
    payload = json.dumps(
        {"source": source_name, "row": row_number, "values": row},
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def source_fingerprint(path: str | Path) -> str:
    """Fingerprint a source file from its size and last-modified time."""
    resolved = Path(path)
    stat = resolved.stat()
    payload = f"{resolved.name}:{stat.st_size}:{int(stat.st_mtime)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_batch(
    source: str | Path,
    offset: int = 0,
    batch_size: int = 5000,
    *,
    source_name: str | None = None,
) -> SourceBatch:
    """Read at most ``batch_size`` rows starting at ``offset``.

    ``source`` may be a logical name or a path. Row numbers are zero-based and
    count data rows only (the CSV header is not counted). Each returned row is
    annotated with a stable ``source_row_id`` and its zero-based ``_row_number``.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    path = Path(source)
    name = source_name or path.stem
    fingerprint = source_fingerprint(path)

    rows: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for index, raw_row in enumerate(reader):
            if index < offset:
                continue
            if len(rows) >= batch_size:
                break
            row = dict(raw_row)
            row["_row_number"] = index
            row["source_row_id"] = compute_row_id(name, index, raw_row)
            rows.append(row)

    return SourceBatch(
        source_name=name,
        rows=rows,
        next_offset=offset + len(rows),
        fingerprint=fingerprint,
    )


def read_checkpoint(connection: duckdb.DuckDBPyConnection, source_name: str) -> int:
    """Return the stored row offset for a source, or 0 when none exists."""
    row = connection.execute(
        "select cursor_value from source_checkpoint where source_name = ?",
        [source_name],
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


def read_checkpoint_fingerprint(
    connection: duckdb.DuckDBPyConnection, source_name: str
) -> str | None:
    """Return the fingerprint stored next to a source checkpoint, if any."""
    row = connection.execute(
        "select source_fingerprint from source_checkpoint where source_name = ?",
        [source_name],
    ).fetchone()
    if row is None:
        return None
    return row[0]


def update_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    source_name: str,
    new_offset: int,
    fingerprint: str,
) -> None:
    """Persist the new row offset for a source (upsert)."""
    connection.execute(
        """
        insert into source_checkpoint
            (source_name, cursor_value, source_fingerprint, updated_at)
        values (?, ?, ?, now())
        on conflict (source_name) do update set
            cursor_value = excluded.cursor_value,
            source_fingerprint = excluded.source_fingerprint,
            updated_at = now()
        """,
        [source_name, str(int(new_offset)), fingerprint],
    )