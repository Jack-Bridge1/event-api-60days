
import contextvars
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timezone

from prometheus_client import Counter, Histogram

request_id_context = contextvars.ContextVar(
    "request_id",
    default="-",
)

HTTP_REQUESTS = Counter(
    "event_api_requests_total",
    "HTTP requests served by the Event API",
    ["method", "route", "status_code"],
)

HTTP_DURATION = Histogram(
    "event_api_request_duration_seconds",
    "Time spent processing HTTP requests",
    ["method", "route"],
)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "service": "event-api",
            "message": record.getMessage(),
            "request_id": getattr(
                record,
                "request_id",
                request_id_context.get(),
            ),
        }

        for field in (
            "method",
            "route",
            "status_code",
            "duration_ms",
            "event_id",
            "task_id",
        ):
            if hasattr(record, field):
                data[field] = getattr(record, field)

        if record.exc_info:
            data["exception"] = self.formatException(
                record.exc_info
            )

        return json.dumps(data, default=str)


def configure_logging():
    logger = logging.getLogger("event_api")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(
        getattr(handler, "_event_api_handler", False)
        for handler in logger.handlers
    ):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler._event_api_handler = True
        logger.addHandler(handler)

    return logger


def get_request_id(header_value):
    if header_value and re.fullmatch(
        r"[A-Za-z0-9._-]{1,64}",
        header_value,
    ):
        return header_value

    return uuid.uuid4().hex
