
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

import main


def test_request_id_is_preserved():
    with TestClient(main.app) as client:
        response = client.get(
            "/health",
            headers={
                "X-Request-ID": "day6-test-001"
            },
        )

    assert response.status_code == 200
    assert (
        response.headers["X-Request-ID"]
        == "day6-test-001"
    )


def test_missing_request_id_is_generated():
    with TestClient(main.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.headers["X-Request-ID"]


def test_prometheus_metrics_endpoint():
    with TestClient(main.app) as client:
        client.get("/health")
        response = client.get("/internal/metrics")

    assert response.status_code == 200
    assert "event_api_requests_total" in response.text
    assert (
        "event_api_request_duration_seconds"
        in response.text
    )


def test_readiness_returns_503_when_db_fails(
    monkeypatch,
):
    def fail_connect():
        raise OperationalError(
            "SELECT 1",
            {},
            RuntimeError("database unavailable"),
        )

    monkeypatch.setattr(
        main.engine,
        "connect",
        fail_connect,
    )

    with TestClient(main.app) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "postgres": "down",
    }
