import pytest

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from db import Base, get_db
from main import app
import cache
import models


class FakeRedis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, *, ex):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(cache, "redis_client", FakeRedis())
    engine = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    Base.metadata.create_all(bind=engine)

    def get_test_db():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
    engine.dispose()


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200


def test_save_and_read(client):
    response = client.post("/events", json={
        "source": "web",
        "event_type": "purchase",
        "value": 99.5
    })

    assert response.status_code == 201

    event_id = response.json()["id"]

    stored = client.get(f"/events/{event_id}")

    assert stored.status_code == 200
    assert stored.json()["value"] == 99.5

    metrics = client.get("/metrics").json()
    assert metrics["by_type"]["purchase"] == 1


def test_validation(client):
    response = client.post("/events", json={
        "source": "web",
        "event_type": "purchase",
        "value": -1
    })

    assert response.status_code == 422


def test_not_found(client):
    assert client.get("/events/999").status_code == 404


@pytest.fixture
def create_event(client):
    def create(event_type):
        payload = {"source": "web", "event_type": event_type, "value": 99.5}
        response = client.post("/events", json=payload)
        assert response.status_code == 201, response.text
        return response.json()

    return create


def test_filtered_and_unfiltered_metrics_use_separate_cache_keys(
    client, create_event
):
    create_event("purchase")
    create_event("click")

    cases = [
        ({}, {"total": 2, "by_type": {"purchase": 1, "click": 1}}),
        ({"event_type": "purchase"}, {"total": 1, "by_type": {"purchase": 1}}),
        ({"event_type": "click"}, {"total": 1, "by_type": {"click": 1}}),
    ]

    for params, expected in cases:
        first = client.get("/metrics", params=params)
        assert first.status_code == 200, first.text
        assert first.headers["X-Cache"] == "MISS"
        assert first.json() == expected

        cached = client.get("/metrics", params=params)
        assert cached.status_code == 200, cached.text
        assert cached.headers["X-Cache"] == "HIT"
        assert cached.json() == expected


def test_new_purchase_invalidates_purchase_and_unfiltered_metrics(
    client, create_event
):
    create_event("purchase")
    create_event("click")

    purchase_filter = {"event_type": "purchase"}
    click_filter = {"event_type": "click"}

    for params in ({}, purchase_filter, click_filter):
        warmed = client.get("/metrics", params=params)
        assert warmed.status_code == 200, warmed.text

    create_event("purchase")

    overall = client.get("/metrics")
    assert overall.status_code == 200, overall.text
    assert overall.headers["X-Cache"] == "MISS"
    assert overall.json() == {"total": 3, "by_type": {"purchase": 2, "click": 1}}

    purchases = client.get("/metrics", params=purchase_filter)
    assert purchases.status_code == 200, purchases.text
    assert purchases.headers["X-Cache"] == "MISS"
    assert purchases.json() == {"total": 2, "by_type": {"purchase": 2}}

    clicks = client.get("/metrics", params=click_filter)
    assert clicks.status_code == 200, clicks.text
    assert clicks.headers["X-Cache"] == "HIT"
    assert clicks.json() == {"total": 1, "by_type": {"click": 1}}