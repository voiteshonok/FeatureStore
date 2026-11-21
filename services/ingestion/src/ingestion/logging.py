"""PAN-masking log filter and a one-line JSON formatter."""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from schemas import mask_pans

__all__ = ["JsonFormatter", "PanMaskFilter", "mask_pans"]


def _mask_value(value: object) -> object:
    if isinstance(value, str):
        return mask_pans(value)
    return value


def _mask_args(
    args: tuple[object, ...] | Mapping[str, object] | None,
) -> tuple[object, ...] | Mapping[str, object] | None:
    if isinstance(args, tuple):
        return tuple(_mask_value(item) for item in args)
    if isinstance(args, dict):
        masked: dict[str, object] = {}
        for key, value in args.items():
            masked[str(key)] = _mask_value(value)
        return masked
    return args


class PanMaskFilter(logging.Filter):
    """Mask 13–19 digit runs in the log message and its arguments."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = mask_pans(record.msg)
        record.args = _mask_args(record.args)
        return True


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record, with PAN digits masked."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, str] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": mask_pans(record.getMessage()),
        }
        if record.exc_info:
            payload["exception"] = mask_pans(self.formatException(record.exc_info))
        for key in ("request_id", "event_id"):
            value = getattr(record, key, None)
            if isinstance(value, str) and value:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)
