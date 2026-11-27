
import pytest

import main


@pytest.fixture(autouse=True)
def isolate_external_services(monkeypatch):
    class FakeTask:
        id = "test-task-id"

    monkeypatch.setattr(
        main.record_recent_event_job,
        "delay",
        lambda *args, **kwargs: FakeTask(),
    )

    # monkeypatch.setattr(
    #     main,
    #     "get_cached_metrics",
    #     lambda: (None, "MISS"),
    # )
    # monkeypatch.setattr(
    #     main,
    #     "set_cached_metrics",
    #     lambda data: None,
    # )
    # monkeypatch.setattr(
    #     main,
    #     "invalidate_metrics_cache",
    #     lambda: None,
    # )
