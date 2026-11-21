"""Order-independent replay property (US-2.4 / ADR-005 / T4.1).

Stream features after any permutation-plus-duplicate delivery of E must equal a
pure-Python oracle over unique event_ids in E. Geo-velocity is out of scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from redis.asyncio import Redis

from features.windows import card_stream_features, merchant_stream_features
from features.worker import apply_event, load_script
from schemas import TransactionEvent

pytestmark = pytest.mark.integration

_CARDS = ("card-1", "card-2", "card-3")
_MERCHANTS = ("m-1", "m-2")
_MINUTE_MS = 60_000
_HOUR_MS = 60 * _MINUTE_MS
_DAY_MS = 24 * _HOUR_MS
_MERCHANT_BUCKETS = 1_440
_CARD_WINDOWS_MS = {
    "card_cnt_1m": _MINUTE_MS,
    "card_cnt_1h": _HOUR_MS,
    "card_cnt_24h": _DAY_MS,
    "card_sum_1m": _MINUTE_MS,
    "card_sum_1h": _HOUR_MS,
    "card_sum_24h": _DAY_MS,
}

# Relative offsets that do not need wall-clock T; plus named edges resolved at apply time.
_RELATIVE_BOUNDARIES_MS = (
    -_MINUTE_MS,
    -_MINUTE_MS - 1,
    -_MINUTE_MS + 1,
    -_HOUR_MS,
    -_HOUR_MS - 1,
    -_HOUR_MS + 1,
    -_DAY_MS,
    -_DAY_MS - 1,
    -_DAY_MS + 1,
)
EdgeKind = Literal[
    "minute_start",
    "minute_end_prev",
    "hour_start",
    "hour_end_prev",
]
DupKind = Literal["same_offset", "new_offset"]
SeqKind = Literal["primary", "same_offset", "new_offset"]
OffsetSpec = int | EdgeKind


@dataclass(frozen=True)
class EventSpec:
    """Event relative to T: ``event_time_ms = T_ms + resolved_offset``."""

    event_id: UUID
    card_token: str
    merchant_id: str
    amount_minor: int
    status: Literal["approved", "declined"]
    offset: OffsetSpec
    lat: float | None
    lon: float | None


def _epoch_ms(value: datetime) -> int:
    """Milliseconds since Unix epoch (oracle copy — not imported from windows)."""
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def _minute(event_time_ms: int) -> int:
    return event_time_ms // _MINUTE_MS


def _resolve_offset(offset: OffsetSpec, t_ms: int) -> int:
    if isinstance(offset, int):
        return offset
    minute = _minute(t_ms)
    hour = minute // 60
    absolute = {
        "minute_start": minute * _MINUTE_MS,
        "minute_end_prev": minute * _MINUTE_MS - 1,
        "hour_start": hour * _HOUR_MS,
        "hour_end_prev": hour * _HOUR_MS - 1,
    }[offset]
    resolved = absolute - t_ms
    return max(-26 * _HOUR_MS, min(0, resolved))


def _materialize(spec: EventSpec, t: datetime) -> TransactionEvent:
    t_ms = _epoch_ms(t)
    event_time = datetime.fromtimestamp((t_ms + _resolve_offset(spec.offset, t_ms)) / 1000, UTC)
    return TransactionEvent.model_validate(
        {
            "event_id": spec.event_id,
            "payment_id": f"pay-{spec.event_id}",
            "merchant_id": spec.merchant_id,
            "amount_minor": spec.amount_minor,
            "currency": "EUR",
            "status": spec.status,
            "event_time": event_time,
            "card_token": spec.card_token,
            "ingest_time": t,
            "lat": spec.lat,
            "lon": spec.lon,
        }
    )


def _assign_offsets(seq: list[tuple[int, SeqKind]]) -> list[tuple[int, int]]:
    """Map delivery steps to ``(event_index, kafka_offset)``."""
    next_offset = 0
    first_offset: dict[int, int] = {}
    out: list[tuple[int, int]] = []
    for event_index, kind in seq:
        if kind == "primary":
            first_offset[event_index] = next_offset
            out.append((event_index, next_offset))
            next_offset += 1
        elif kind == "same_offset":
            out.append((event_index, first_offset[event_index]))
        else:
            out.append((event_index, next_offset))
            next_offset += 1
    return out


def oracle_card_features(
    events: list[TransactionEvent],
    card_token: str,
    t_ms: int,
) -> dict[str, int]:
    """Card stream features: range ``(T − window, T]`` over unique event_ids."""
    seen: set[UUID] = set()
    unique: list[TransactionEvent] = []
    for event in events:
        if event.event_id in seen:
            continue
        seen.add(event.event_id)
        if event.card_token == card_token:
            unique.append(event)

    features: dict[str, int] = {}
    for name, window_ms in _CARD_WINDOWS_MS.items():
        start = t_ms - window_ms
        chosen = [e for e in unique if start < _epoch_ms(e.event_time) <= t_ms]
        if name.startswith("card_cnt_"):
            features[name] = len(chosen)
        else:
            features[name] = sum(e.amount_minor for e in chosen)
    return features


def oracle_merchant_features(
    events: list[TransactionEvent],
    merchant_id: str,
    t_ms: int,
) -> dict[str, int | float]:
    """Merchant stream features: minutes ``(minute(T) − 1440, minute(T)]``."""
    seen: set[UUID] = set()
    unique: list[TransactionEvent] = []
    for event in events:
        if event.event_id in seen:
            continue
        seen.add(event.event_id)
        if event.merchant_id == merchant_id:
            unique.append(event)

    current = _minute(t_ms)
    first = current - _MERCHANT_BUCKETS + 1
    in_window = [e for e in unique if first <= _minute(_epoch_ms(e.event_time)) <= current]
    count = len(in_window)
    declined = sum(1 for e in in_window if e.status == "declined")
    rate = 0.0 if count == 0 else declined / count
    return {"merchant_cnt_24h": count, "merchant_decline_rate_24h": rate}


@st.composite
def _event_specs(draw: st.DrawFn) -> list[EventSpec]:
    n = draw(st.integers(1, 60))
    offset_strategy: st.SearchStrategy[OffsetSpec] = st.one_of(
        st.integers(-26 * _HOUR_MS, 0),
        st.sampled_from(_RELATIVE_BOUNDARIES_MS),
        st.sampled_from(("minute_start", "minute_end_prev", "hour_start", "hour_end_prev")),
    )
    specs: list[EventSpec] = []
    ids: set[UUID] = set()
    for _ in range(n):
        event_id = uuid4()
        while event_id in ids:
            event_id = uuid4()
        ids.add(event_id)
        has_geo = draw(st.booleans())
        specs.append(
            EventSpec(
                event_id=event_id,
                card_token=draw(st.sampled_from(_CARDS)),
                merchant_id=draw(st.sampled_from(_MERCHANTS)),
                amount_minor=draw(st.integers(1, 10_000)),
                status=draw(st.sampled_from(("approved", "declined"))),
                offset=draw(offset_strategy),
                lat=(
                    draw(st.floats(-90, 90, allow_nan=False, allow_infinity=False))
                    if has_geo
                    else None
                ),
                lon=(
                    draw(st.floats(-180, 180, allow_nan=False, allow_infinity=False))
                    if has_geo
                    else None
                ),
            )
        )
    return specs


@st.composite
def _delivery_seq(draw: st.DrawFn, n_events: int) -> list[tuple[int, SeqKind]]:
    order = draw(st.permutations(range(n_events)))
    seq: list[tuple[int, SeqKind]] = [(index, "primary") for index in order]
    for _ in range(draw(st.integers(0, 5))):
        src = draw(st.integers(0, len(seq) - 1))
        event_index = seq[src][0]
        kind: DupKind = draw(st.sampled_from(("same_offset", "new_offset")))
        insert_at = draw(st.integers(src + 1, len(seq)))
        seq.insert(insert_at, (event_index, kind))
    return seq


@st.composite
def _replay_cases(
    draw: st.DrawFn,
) -> tuple[list[EventSpec], list[tuple[int, SeqKind]]]:
    specs = draw(_event_specs())
    seq = draw(_delivery_seq(len(specs)))
    return specs, seq


_EX_BOUNDARY_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0001")
_EX_DUP_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeee0002")

_EXAMPLE_BOUNDARY: tuple[tuple[EventSpec, ...], tuple[tuple[int, SeqKind], ...]] = (
    (
        EventSpec(
            event_id=_EX_BOUNDARY_ID,
            card_token="card-1",  # noqa: S106
            merchant_id="m-1",
            amount_minor=100,
            status="approved",
            offset=-_DAY_MS,
            lat=None,
            lon=None,
        ),
    ),
    ((0, "primary"),),
)

_EXAMPLE_DUP: tuple[tuple[EventSpec, ...], tuple[tuple[int, SeqKind], ...]] = (
    (
        EventSpec(
            event_id=_EX_DUP_ID,
            card_token="card-1",  # noqa: S106
            merchant_id="m-1",
            amount_minor=250,
            status="declined",
            offset=0,
            lat=None,
            lon=None,
        ),
    ),
    ((0, "primary"), (0, "new_offset")),
)


async def _assert_replay_matches_oracle(
    redis_url: str,
    specs: list[EventSpec] | tuple[EventSpec, ...],
    seq: list[tuple[int, SeqKind]] | tuple[tuple[int, SeqKind], ...],
    t: datetime,
) -> None:
    t_ms = _epoch_ms(t)
    events = [_materialize(spec, t) for spec in specs]
    delivery = _assign_offsets(list(seq))

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        await redis.flushdb()
        sha = await load_script(redis)
        for event_index, offset in delivery:
            await apply_event(
                redis,
                sha,
                events[event_index],
                partition=0,
                offset=offset,
                now_ms=t_ms,
            )

        cards = {events[i].card_token for i, _ in delivery}
        merchants = {events[i].merchant_id for i, _ in delivery}
        for card_token in cards:
            actual = await card_stream_features(redis, card_token, t_ms)
            expected = oracle_card_features(events, card_token, t_ms)
            assert actual == expected
        for merchant_id in merchants:
            actual = await merchant_stream_features(redis, merchant_id, t_ms)
            expected = oracle_merchant_features(events, merchant_id, t_ms)
            assert actual == expected
    finally:
        await redis.aclose()


type ReplayCase = tuple[
    list[EventSpec] | tuple[EventSpec, ...],
    list[tuple[int, SeqKind]] | tuple[tuple[int, SeqKind], ...],
]


@example(_EXAMPLE_BOUNDARY)
@example(_EXAMPLE_DUP)
@given(case=_replay_cases())
@settings(deadline=None)
async def test_replay_stream_features_match_oracle(
    session_redis_url: str,
    case: ReplayCase,
) -> None:
    """Shuffled delivery with Kafka/ADR-006 dups leaves stream features unchanged.

    Fixed examples: event exactly at T−24h (excluded from all windows); declined
    merchant event re-delivered at a new offset (pre-T4.1 counterexample: cnt
    would become 2 without ``dup_event``).
    """
    specs, seq = case
    t = datetime.now(UTC).replace(microsecond=0)
    await _assert_replay_matches_oracle(session_redis_url, specs, seq, t)
