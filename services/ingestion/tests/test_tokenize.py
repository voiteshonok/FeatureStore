from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ingestion import HmacKeyError, load_key, to_event, tokenize
from ingestion.tokenize import DEFAULT_KEY_PATH
from schemas import PaymentIn, TransactionEvent
from schemas.pan import is_luhn
from schemas.time import to_utc_milliseconds

_KEY = bytes(range(32))
_TEXT = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), blacklist_characters=""),
    min_size=1,
    max_size=32,
)


def test_same_pan_and_key_give_the_same_token() -> None:
    pan = "4111111111111111"
    token = tokenize(pan, _KEY)
    assert token == tokenize(pan, _KEY)
    assert token == token.lower()
    assert len(token) == 64


def test_different_pans_give_different_tokens() -> None:
    assert tokenize("4111111111111111", _KEY) != tokenize("4222222222222222", _KEY)


def test_different_keys_give_different_tokens() -> None:
    pan = "4111111111111111"
    assert tokenize(pan, _KEY) != tokenize(pan, bytes(range(1, 33)))


def test_load_key_accepts_hex_with_trailing_newline(tmp_path: Path) -> None:
    path = tmp_path / "hmac_key"
    path.write_text(_KEY.hex() + "\n", encoding="utf-8")
    assert load_key(path) == _KEY


def test_load_key_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "absent"
    with pytest.raises(HmacKeyError, match="not found") as exc_info:
        load_key(missing)
    assert str(missing) in str(exc_info.value)


def test_load_key_non_hex(tmp_path: Path) -> None:
    path = tmp_path / "hmac_key"
    path.write_text("zzzz", encoding="utf-8")
    with pytest.raises(HmacKeyError, match="not hexadecimal") as exc_info:
        load_key(path)
    assert "zzzz" not in str(exc_info.value)


def test_load_key_odd_length_hex(tmp_path: Path) -> None:
    path = tmp_path / "hmac_key"
    path.write_text("abc", encoding="utf-8")
    with pytest.raises(HmacKeyError, match="not hexadecimal"):
        load_key(path)


def test_load_key_wrong_length(tmp_path: Path) -> None:
    path = tmp_path / "hmac_key"
    material = "aa" * 16
    path.write_text(material, encoding="utf-8")
    with pytest.raises(HmacKeyError, match="got 16") as exc_info:
        load_key(path)
    assert material not in str(exc_info.value)


def test_load_key_rejects_non_text(tmp_path: Path) -> None:
    path = tmp_path / "hmac_key"
    path.write_bytes(b"\xff\xfe")
    with pytest.raises(HmacKeyError, match="not hexadecimal"):
        load_key(path)


def test_load_key_uses_env_when_path_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "hmac_key"
    path.write_text(_KEY.hex(), encoding="utf-8")
    monkeypatch.setenv("HMAC_KEY_FILE", str(path))
    assert load_key() == _KEY


def test_explicit_path_overrides_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_path = tmp_path / "env"
    env_path.write_text("not-hex", encoding="utf-8")
    explicit = tmp_path / "explicit"
    explicit.write_text(_KEY.hex() + "\n", encoding="utf-8")
    monkeypatch.setenv("HMAC_KEY_FILE", str(env_path))
    assert load_key(explicit) == _KEY


def test_load_key_defaults_to_docker_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert DEFAULT_KEY_PATH == Path("/run/secrets/hmac_key")
    monkeypatch.delenv("HMAC_KEY_FILE", raising=False)
    path = tmp_path / "hmac_key"
    path.write_text(_KEY.hex(), encoding="utf-8")
    # Package __init__ exports tokenize, which shadows the submodule attribute.
    monkeypatch.setattr(sys.modules["ingestion.tokenize"], "DEFAULT_KEY_PATH", path)
    assert load_key() == _KEY


def _with_luhn(body: str) -> str:
    for digit in "0123456789":
        candidate = body + digit
        if is_luhn(candidate):
            return candidate
    raise RuntimeError("Luhn check digit not found")


@st.composite
def _aware_datetimes(draw: st.DrawFn) -> datetime:
    tz = draw(
        st.sampled_from(
            [
                UTC,
                timezone(timedelta(hours=2)),
                timezone(timedelta(hours=-5)),
            ]
        )
    )
    naive = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 1, 1),
        )
    )
    return naive.replace(tzinfo=tz)


@st.composite
def _pans(draw: st.DrawFn) -> str:
    length = draw(st.integers(min_value=13, max_value=19))
    body = draw(st.text(alphabet="0123456789", min_size=length - 1, max_size=length - 1))
    return _with_luhn(body)


@st.composite
def _payments(draw: st.DrawFn) -> PaymentIn:
    has_coords = draw(st.booleans())
    lat = None
    lon = None
    if has_coords:
        lat = round(draw(st.floats(-90, 90, allow_nan=False, allow_infinity=False)), 6)
        lon = round(draw(st.floats(-180, 180, allow_nan=False, allow_infinity=False)), 6)
        lat = min(90.0, max(-90.0, lat))
        lon = min(180.0, max(-180.0, lon))
    return PaymentIn(
        event_id=UUID(int=draw(st.integers(min_value=0, max_value=2**128 - 1))),
        payment_id=draw(_TEXT),
        pan=draw(_pans()),
        merchant_id=draw(_TEXT),
        amount_minor=draw(st.integers(min_value=1, max_value=10**9)),
        currency="EUR",
        status=draw(st.sampled_from(("approved", "declined"))),
        event_time=draw(_aware_datetimes()),
        lat=lat,
        lon=lon,
    )


@given(_payments(), _aware_datetimes())
@settings(max_examples=50)
def test_to_event_drops_pan_and_validates(payment: PaymentIn, now: datetime) -> None:
    event = to_event(payment, _KEY, now)
    assert event == TransactionEvent.model_validate(event.model_dump())
    assert "pan" not in event.model_dump()
    assert event.card_token == tokenize(payment.pan, _KEY)
    assert event.schema_version == 1
    assert event.event_id == payment.event_id
    assert event.payment_id == payment.payment_id
    assert event.merchant_id == payment.merchant_id
    assert event.amount_minor == payment.amount_minor
    assert event.currency == payment.currency
    assert event.status == payment.status
    assert event.event_time == payment.event_time
    assert event.lat == payment.lat
    assert event.lon == payment.lon
    assert event.ingest_time == to_utc_milliseconds(now)
