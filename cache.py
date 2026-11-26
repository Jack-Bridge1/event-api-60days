import json
import logging
import os

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "60"))

METRICS_CACHE_KEY = "metrics:counts:v1"

redis_client = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
)


def metrics_cache_key(event_type: str | None = None) -> str:
    if event_type is None:
        return METRICS_CACHE_KEY
    return f"{METRICS_CACHE_KEY}:{event_type}"


def get_cached_metrics(event_type: str | None = None):
    try:
        raw = redis_client.get(metrics_cache_key(event_type))

        if raw is None:
            return None, "MISS"

        return json.loads(raw), "HIT"

    except (RedisError, json.JSONDecodeError) as exc:
        logger.warning("Cache read failed: %s", exc)
        return None, "BYPASS"


def set_cached_metrics(data, event_type: str | None = None) -> str:
    try:
        redis_client.set(
            metrics_cache_key(event_type),
            json.dumps(data),
            ex=CACHE_TTL,
        )
    except RedisError as exc:
        logger.warning("Cache write failed: %s", exc)
        return "BYPASS"
    return "MISS"


def invalidate_metrics_cache(event_type: str | None = None) -> None:
    try:
        redis_client.delete(metrics_cache_key())
        if event_type is not None:
            redis_client.delete(metrics_cache_key(event_type))
    except RedisError:
        logger.warning("Could not invalidate metrics cache", exc_info=True)
