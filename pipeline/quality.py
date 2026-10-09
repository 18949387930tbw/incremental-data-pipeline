"""Data quality rules and quarantine decisions.

Every rule returns a stable ``reason_code`` so the runner can persist a
machine-readable quarantine record. Bad rows are never silently dropped: they
are moved to ``quarantined_rows`` with their raw payload preserved.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

MISSING_REQUIRED_FIELD = "missing_required_field"
INVALID_INTEGER = "invalid_integer"
INVALID_AMOUNT = "invalid_amount"
NEGATIVE_QUANTITY = "negative_quantity"
NEGATIVE_UNIT_PRICE = "negative_unit_price"
INVALID_DATE = "invalid_date"
DUPLICATE_SOURCE_ROW = "duplicate_source_row"

_DATE_FORMATS = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%m/%d/%y %H:%M",
)


@dataclass(frozen=True)
class QualityResult:
    """Outcome of applying quality rules to a batch of rows."""

    clean_rows: list[dict]
    quarantined_rows: list[dict]

    @property
    def read_count(self) -> int:
        return len(self.clean_rows) + len(self.quarantined_rows)

    @property
    def reason_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.quarantined_rows:
            code = row["reason_code"]
            counts[code] = counts.get(code, 0) + 1
        return counts


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _parse_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            return int(text)
        except ValueError:
            try:
                as_float = float(text)
            except ValueError:
                return None
            return int(as_float) if as_float.is_integer() else None
    return None


def _parse_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_date(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _quarantine(row: dict, reason_code: str) -> dict:
    return {
        "source_row_id": row.get("source_row_id"),
        "reason_code": reason_code,
        "raw_payload": json.dumps(
            row, ensure_ascii=False, sort_keys=True, default=str
        ),
    }


def validate_movie_review(rows: list[dict]) -> QualityResult:
    """Validate raw movie review rows into clean and quarantined buckets."""
    clean: list[dict] = []
    quarantined: list[dict] = []
    seen: set[str] = set()

    for row in rows:
        row_id = row.get("source_row_id")
        if _is_blank(row_id):
            quarantined.append(_quarantine(row, MISSING_REQUIRED_FIELD))
            continue
        if row_id in seen:
            quarantined.append(_quarantine(row, DUPLICATE_SOURCE_ROW))
            continue
        seen.add(row_id)

        if _is_blank(row.get("review_str")):
            quarantined.append(_quarantine(row, MISSING_REQUIRED_FIELD))
            continue
        if _is_blank(row.get("cid")):
            quarantined.append(_quarantine(row, MISSING_REQUIRED_FIELD))
            continue

        cid = _parse_int(row.get("cid"))
        if cid is None:
            quarantined.append(_quarantine(row, INVALID_INTEGER))
            continue

        cleaned = dict(row)
        cleaned["cid"] = cid
        clean.append(cleaned)

    return QualityResult(clean, quarantined)


def _clean_purchase(row: dict) -> tuple[dict | None, str | None]:
    """Return ``(clean_row, None)`` or ``(None, reason_code)`` for one row."""
    for field in ("customer_id", "quantity", "unit_price", "invoice_date"):
        if _is_blank(row.get(field)):
            return None, MISSING_REQUIRED_FIELD

    customer_id = _parse_int(row.get("customer_id"))
    if customer_id is None:
        return None, INVALID_INTEGER

    quantity = _parse_int(row.get("quantity"))
    if quantity is None:
        return None, INVALID_INTEGER

    unit_price = _parse_float(row.get("unit_price"))
    if unit_price is None:
        return None, INVALID_AMOUNT

    invoice_date = _parse_date(row.get("invoice_date"))
    if invoice_date is None:
        return None, INVALID_DATE

    if quantity < 0:
        return None, NEGATIVE_QUANTITY
    if unit_price < 0:
        return None, NEGATIVE_UNIT_PRICE

    cleaned = dict(row)
    cleaned["customer_id"] = customer_id
    cleaned["quantity"] = quantity
    cleaned["unit_price"] = unit_price
    cleaned["invoice_date"] = invoice_date
    return cleaned, None


def validate_user_purchase(rows: list[dict]) -> QualityResult:
    """Validate raw purchase rows into clean and quarantined buckets."""
    clean: list[dict] = []
    quarantined: list[dict] = []
    seen: set[str] = set()

    for row in rows:
        row_id = row.get("source_row_id")
        if _is_blank(row_id):
            quarantined.append(_quarantine(row, MISSING_REQUIRED_FIELD))
            continue
        if row_id in seen:
            quarantined.append(_quarantine(row, DUPLICATE_SOURCE_ROW))
            continue
        seen.add(row_id)

        cleaned, reason = _clean_purchase(row)
        if reason is not None:
            quarantined.append(_quarantine(row, reason))
        else:
            clean.append(cleaned)

    return QualityResult(clean, quarantined)