"""PAN-masking log filter and a one-line JSON formatter."""

import json
import logging
import re
from collections.abc import Mapping
from datetime import UTC, datetime

_PAN_RUN = re.compile(r"(?<!\d)\d{13,19}(?!\d)")


def mask_pans(text: str) -> str:
    """Mask maximal 13–19 digit runs, keeping the first 6 and last 4 digits."""

    def replace(match: re.Match[str]) -> str:
        digits = match.group(0)
        hidden = len(digits) - 10
        return f"{digits[:6]}{'*' * hidden}{digits[-4:]}"

    return _PAN_RUN.sub(replace, text)


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
        return json.dumps(payload, ensure_ascii=False)
