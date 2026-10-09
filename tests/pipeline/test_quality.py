"""Tests for the data quality rules and quarantine decisions."""

from __future__ import annotations

from pipeline.quality import validate_movie_review, validate_user_purchase


def _purchase(**overrides):
    row = {
        "source_row_id": "row-1",
        "customer_id": "1",
        "quantity": "2",
        "unit_price": "10.0",
        "invoice_date": "12/1/2010 8:26",
    }
    row.update(overrides)
    return row


def test_valid_purchase_is_accepted():
    result = validate_user_purchase([_purchase()])

    assert len(result.clean_rows) == 1
    assert result.quarantined_rows == []
    clean = result.clean_rows[0]
    assert clean["customer_id"] == 1
    assert clean["quantity"] == 2
    assert clean["unit_price"] == 10.0


def test_negative_quantity_is_quarantined():
    result = validate_user_purchase([_purchase(quantity="-2")])

    assert result.clean_rows == []
    assert result.quarantined_rows[0]["reason_code"] == "negative_quantity"


def test_negative_unit_price_is_quarantined():
    result = validate_user_purchase([_purchase(unit_price="-4.0")])

    assert result.clean_rows == []
    assert result.quarantined_rows[0]["reason_code"] == "negative_unit_price"


def test_blank_customer_id_is_quarantined():
    result = validate_user_purchase([_purchase(customer_id="")])

    assert result.clean_rows == []
    assert result.quarantined_rows[0]["reason_code"] == "missing_required_field"


def test_unparseable_customer_id_is_quarantined():
    result = validate_user_purchase([_purchase(customer_id="not-a-number")])

    assert result.quarantined_rows[0]["reason_code"] == "invalid_integer"


def test_unparseable_amount_is_quarantined():
    result = validate_user_purchase([_purchase(unit_price="abc")])

    assert result.quarantined_rows[0]["reason_code"] == "invalid_amount"


def test_unparseable_date_is_quarantined():
    result = validate_user_purchase([_purchase(invoice_date="not-a-date")])

    assert result.quarantined_rows[0]["reason_code"] == "invalid_date"


def test_duplicate_source_row_is_quarantined():
    result = validate_user_purchase([_purchase(), _purchase()])

    assert len(result.clean_rows) == 1
    assert result.quarantined_rows[0]["reason_code"] == "duplicate_source_row"


def test_quarantine_keeps_raw_payload():
    result = validate_user_purchase([_purchase(quantity="-2")])

    payload = result.quarantined_rows[0]["raw_payload"]
    assert "quantity" in payload


def test_valid_movie_review_is_accepted():
    result = validate_movie_review(
        [{"source_row_id": "r1", "cid": "17850", "review_str": "good movie"}]
    )

    assert len(result.clean_rows) == 1
    assert result.clean_rows[0]["cid"] == 17850


def test_movie_review_invalid_cid_is_quarantined():
    result = validate_movie_review(
        [{"source_row_id": "r1", "cid": "abc", "review_str": "good"}]
    )

    assert result.clean_rows == []
    assert result.quarantined_rows[0]["reason_code"] == "invalid_integer"


def test_movie_review_missing_text_is_quarantined():
    result = validate_movie_review(
        [{"source_row_id": "r1", "cid": "1", "review_str": ""}]
    )

    assert result.quarantined_rows[0]["reason_code"] == "missing_required_field"