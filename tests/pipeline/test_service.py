"""Tests for the FastAPI trigger and query service."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import pipeline.service as service
from pipeline.service import app, get_settings


@pytest.fixture
def client(pipeline_settings):
    app.dependency_overrides[get_settings] = lambda: pipeline_settings
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_endpoint(client):
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "warehouse" in payload


def test_trigger_run_returns_202(client):
    response = client.post("/pipeline/runs", json={"batch_size": 10})

    assert response.status_code == 202
    assert "run_id" in response.json()


def test_run_history_can_be_queried(client):
    run_id = client.post("/pipeline/runs", json={"batch_size": 10}).json()["run_id"]

    single = client.get(f"/pipeline/runs/{run_id}")
    assert single.status_code == 200
    assert single.json()["run_id"] == run_id
    assert single.json()["status"] == "succeeded"

    listing = client.get("/pipeline/runs")
    assert listing.status_code == 200
    assert any(row["run_id"] == run_id for row in listing.json()["items"])


def test_unknown_run_returns_404(client):
    response = client.get("/pipeline/runs/does-not-exist")

    assert response.status_code == 404


def test_checkpoint_endpoint_lists_sources(client):
    client.post("/pipeline/runs", json={"batch_size": 10})

    response = client.get("/pipeline/checkpoints")

    assert response.status_code == 200
    names = {row["source_name"] for row in response.json()["items"]}
    assert {"movie_review", "user_purchase"}.issubset(names)


def test_quarantine_endpoint_returns_records(client):
    client.post("/pipeline/runs", json={"batch_size": 10})

    response = client.get("/pipeline/quarantine")

    assert response.status_code == 200
    items = response.json()["items"]
    assert items
    assert {"reason_code", "source_name"} .issubset(items[0].keys())


def test_metrics_endpoint_returns_behaviour_metrics(client):
    client.post("/pipeline/runs", json={"batch_size": 10})

    response = client.get("/metrics/behaviour")

    assert response.status_code == 200
    items = response.json()["items"]
    assert items
    assert {"customer_id", "amount_spent", "num_reviews"}.issubset(items[0].keys())


def test_concurrent_trigger_is_rejected(client):
    service._run_lock.acquire()
    try:
        response = client.post("/pipeline/runs", json={"batch_size": 10})
    finally:
        service._run_lock.release()

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "PIPELINE_ALREADY_RUNNING"