
import os

from celery import Celery

celery_app = Celery(
    "event_api",
    broker=os.getenv(
        "CELERY_BROKER_URL",
        "redis://localhost:6379/1",
    ),
    backend=os.getenv(
        "CELERY_RESULT_BACKEND",
        "redis://localhost:6379/2",
    ),
)

celery_app.conf.update(
    imports=["jobs"],
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_publish_retry=False,
    result_expires=3600,
)
