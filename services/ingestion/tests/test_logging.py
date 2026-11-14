from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from datetime import datetime

import pytest

from ingestion import JsonFormatter, PanMaskFilter
from ingestion.logging import mask_pans

PAN = "4111111111111111"
MASKED = "411111******1111"


@pytest.fixture
def masked_logger() -> Iterator[logging.Logger]:
    logger = logging.getLogger("ingestion.tests.pan_mask")
    logger.setLevel(logging.INFO)
    logger.propagate = True
    pan_filter = PanMaskFilter()
    logger.addFilter(pan_filter)
    yield logger
    logger.removeFilter(pan_filter)


def test_caplog_masks_pan_in_message_and_args(
    caplog: pytest.LogCaptureFixture,
    masked_logger: logging.Logger,
) -> None:
    with caplog.at_level(logging.INFO, logger=masked_logger.name):
        masked_logger.info("payment %s", PAN)
        masked_logger.info(f"inline {PAN}")
        masked_logger.info("mapped %(pan)s n=%(n)s", {"pan": PAN, "n": 7})
        masked_logger.info("n=%s card=%s", 7, PAN)
    assert PAN not in caplog.text
    assert MASKED in caplog.text
    assert [record.getMessage() for record in caplog.records] == [
        f"payment {MASKED}",
        f"inline {MASKED}",
        f"mapped {MASKED} n=7",
        f"n=7 card={MASKED}",
    ]
    assert caplog.records[2].args == {"pan": MASKED, "n": 7}
    assert caplog.records[3].args == (7, MASKED)


def test_mask_pans_keeps_short_and_long_runs() -> None:
    short = "123456789012"
    long = "12345678901234567890"
    pan13 = "1234567890123"
    pan19 = "1234567890123456789"
    text = f"{short}-{long}-{pan13}-{pan19}-{PAN}"
    assert mask_pans(text) == (f"{short}-{long}-123456***0123-123456*********6789-{MASKED}")
    assert mask_pans(MASKED) == MASKED


def test_json_formatter_masks_message_and_exception() -> None:
    formatter = JsonFormatter()
    plain = logging.LogRecord(
        name="ingestion",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="pan %s",
        args=(PAN,),
        exc_info=None,
    )
    rendered = formatter.format(plain)
    payload = json.loads(rendered)
    assert list(payload) == ["timestamp", "level", "logger", "message"]
    assert payload["message"] == f"pan {MASKED}"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "ingestion"
    assert PAN not in rendered
    parsed = datetime.fromisoformat(payload["timestamp"])
    assert parsed.tzinfo is not None

    try:
        raise RuntimeError(PAN)
    except RuntimeError:
        exc_info = sys.exc_info()
    failed = logging.LogRecord(
        name="ingestion",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="failed",
        args=(),
        exc_info=exc_info,
    )
    failed_rendered = formatter.format(failed)
    failed_payload = json.loads(failed_rendered)
    assert list(failed_payload) == ["timestamp", "level", "logger", "message", "exception"]
    assert PAN not in failed_rendered
    assert MASKED in failed_payload["exception"]
    assert failed_payload["level"] == "ERROR"


def test_filter_leaves_non_string_message_and_args() -> None:
    pan_filter = PanMaskFilter()
    with_args = logging.LogRecord(
        name="ingestion",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=123,
        args=(7, PAN),
        exc_info=None,
    )
    assert pan_filter.filter(with_args) is True
    assert with_args.msg == 123
    assert with_args.args == (7, MASKED)

    no_args = logging.LogRecord(
        name="ingestion",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=123,
        args=None,
        exc_info=None,
    )
    assert pan_filter.filter(no_args) is True
    assert no_args.msg == 123
    assert no_args.args is None
