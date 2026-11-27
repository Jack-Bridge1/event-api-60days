
from unittest.mock import MagicMock

import json
from uuid import uuid4

import pytest

import jobs

@pytest.fixture
def activity_keys(monkeypatch):
    prefix = f"test:recent-events:{uuid4().hex}"
    recent_key = f"{prefix}:list"
    processed_key = f"{prefix}:processed"

    monkeypatch.setattr(jobs, "RECENT_EVENTS_KEY", recent_key)
    monkeypatch.setattr(jobs, "PROCESSED_EVENT_IDS_KEY", processed_key)

    try:
        yield recent_key
    finally:
        jobs.redis_client.delete(recent_key, processed_key)


def test_repeated_event_creates_one_activity_record(activity_keys):
    first = jobs.record_recent_event_job.run(42, "purchase", "pytest")
    second = jobs.record_recent_event_job.run(42, "purchase", "pytest")

    assert first == {"event_id": 42, "recorded": True}
    assert second == {"event_id": 42, "recorded": False}

    records = [
        json.loads(item)
        for item in jobs.redis_client.lrange(activity_keys, 0, -1)
    ]
    assert [record["event_id"] for record in records] == [42]


def test_feed_keeps_last_50_even_when_an_evicted_event_retries(activity_keys):
    for event_id in range(1, 52):
        jobs.record_recent_event_job.run(event_id, "click", "pytest")

    retry = jobs.record_recent_event_job.run(1, "click", "pytest")
    assert retry == {"event_id": 1, "recorded": False}

    records = [
        json.loads(item)
        for item in jobs.redis_client.lrange(activity_keys, 0, -1)
    ]
    assert [record["event_id"] for record in records] == list(range(51, 1, -1))

