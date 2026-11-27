from datetime import datetime
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db import get_db
from models import EventRecord

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from cache import (
    get_cached_metrics,
    set_cached_metrics,
    invalidate_metrics_cache,
)

from fastapi import BackgroundTasks
from tasks import record_recent_event

import logging
from celery_app import celery_app

from kombu.exceptions import OperationalError as BrokerOperationalError
from jobs import record_recent_event_job

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
EventType = Literal["click", "page_view", "purchase"]

class EventCreate(BaseModel):
    source: str = Field(min_length=2, max_length=100)
    event_type: EventType
    value: float = Field(ge=0)


class EventRead(EventCreate):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime


app = FastAPI(title="Event Platform API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", response_model=EventRead, status_code=201)
def create_event(
    payload: EventCreate,
    response: Response,
    session: Session = Depends(get_db)
):
    event = EventRecord(**payload.model_dump())

    session.add(event)
    session.commit()
    session.refresh(event)

    # Preserve Day 4 cache invalidation.
    invalidate_metrics_cache(event.event_type)

    # Day 5: publish background work to Celery.
    try:
        task = record_recent_event_job.delay(
            event.id,
            event.event_type,
            event.source,
        )
        response.headers["X-Task-ID"] = task.id
        response.headers["X-Task-Status"] = "QUEUED"

    except BrokerOperationalError:
        logger.exception(
            "Event %s saved, but job was not queued",
            event.id,
        )
        response.headers["X-Task-Status"] = "NOT_QUEUED"

    return event


@app.get("/events", response_model=list[EventRead])
def list_events(
    session: Session = Depends(get_db),
    event_type: EventType | None = None
):
    stmt = select(EventRecord).order_by(EventRecord.id)

    if event_type is not None:
        stmt = stmt.where(
            EventRecord.event_type == event_type
        )

    return session.scalars(stmt).all()


@app.get("/events/{event_id}", response_model=EventRead)
def get_event(
    event_id: int,
    session: Session = Depends(get_db)
):
    record = session.get(EventRecord, event_id)

    if record is None:
        raise HTTPException(
            status_code=404,
            detail="Event not found"
        )

    return record

@app.delete("/events/{event_id}", status_code=204, response_class=Response)
def delete_event(
    event_id: int,
    session: Session = Depends(get_db),
) -> Response:
    record = session.get(EventRecord, event_id)

    if record is None:
        raise HTTPException(status_code=404, detail="Event not found")

    affected_type = record.event_type
    session.delete(record)
    session.commit()
    invalidate_metrics_cache(affected_type)

    return Response(status_code=204)

@app.get("/metrics")
def get_metrics(
    response: Response,
    session: Session = Depends(get_db),
    event_type: EventType | None = None,
):
    cached, cache_status = get_cached_metrics(event_type)
    if cached is not None:
        response.headers["X-Cache"] = cache_status
        return cached

    stmt = select(
        EventRecord.event_type,
        func.count(EventRecord.id),
    ).group_by(EventRecord.event_type)

    if event_type is not None:
        stmt = stmt.where(EventRecord.event_type == event_type)

    by_type = dict(session.execute(stmt).all())
    metrics = {"total": sum(by_type.values()), "by_type": by_type}

    if cache_status == "MISS":
        cache_status = set_cached_metrics(metrics, event_type)

    response.headers["X-Cache"] = cache_status
    return metrics


@app.get("/db-health")
def db_health(session: Session = Depends(get_db)) -> dict[str, str]:
    try:
        session.execute(text("SELECT 1"))
    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        ) from exc

    return {"database": "connected"}

@app.get("/jobs/{task_id}")
def get_job_status(task_id: str):
    task = celery_app.AsyncResult(task_id)

    return {
        "task_id": task_id,
        "state": task.state,
    }
