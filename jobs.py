
import json
import logging
from datetime import datetime, timezone

from redis.exceptions import RedisError

from cache import redis_client
from celery_app import celery_app

logger = logging.getLogger(__name__)

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

RECENT_EVENTS_KEY = "events:recent"
PROCESSED_EVENT_IDS_KEY = "events:recent:processed_ids"

RECORD_RECENT_EVENT_SCRIPT = """
local recent_key = KEYS[1]
local processed_key = KEYS[2]
local event_id = ARGV[1]
local activity_json = ARGV[2]

if redis.call("SISMEMBER", processed_key, event_id) == 1 then
    return 0
end

redis.call("LPUSH", recent_key, activity_json)
redis.call("LTRIM", recent_key, 0, 49)
redis.call("SADD", processed_key, event_id)
return 1
"""

@celery_app.task(
    # Keep any explicit task name your existing decorator uses.
    autoretry_for=(RedisConnectionError, RedisTimeoutError),
    retry_backoff=True,
    max_retries=5,
    acks_late=True,
)
def record_recent_event_job(event_id: int, event_type: str, source: str) -> dict:
    activity = {
        "event_id": event_id,
        "event_type": event_type,
        "source": source,
    }

    logger.info(
        "Processing event %s in Celery worker",
        event_id,
    )
    
    inserted = redis_client.eval(
        RECORD_RECENT_EVENT_SCRIPT,
        2,
        RECENT_EVENTS_KEY,
        PROCESSED_EVENT_IDS_KEY,
        str(event_id),
        json.dumps(activity),
    )

    return {"event_id": event_id, "recorded": bool(inserted)}
