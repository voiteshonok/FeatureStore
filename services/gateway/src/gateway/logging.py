"""JSON logs for the gateway. One object per line, metadata only."""

import json
import logging
from datetime import UTC, datetime

from schemas import mask_pans

_LOG_FIELDS = (
    "request_id",
    "client",
    "operator_id",
    "outcome",
    "method",
    "path",
    "status",
    "latency_ms",
)


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with request fields when present."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": mask_pans(record.getMessage()),
        }
        for key in _LOG_FIELDS:
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = mask_pans(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    """Attach the JSON formatter once."""
    logger = logging.getLogger("gateway")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
