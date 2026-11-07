from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from schemas import TransactionEvent

_TEXT = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), blacklist_characters=""),
    min_size=1,
    max_size=32,
)


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
def _transaction_events(draw: st.DrawFn) -> TransactionEvent:
    has_coords = draw(st.booleans())
    lat = None
    lon = None
    if has_coords:
        lat = round(draw(st.floats(-90, 90, allow_nan=False, allow_infinity=False)), 6)
        lon = round(draw(st.floats(-180, 180, allow_nan=False, allow_infinity=False)), 6)
        lat = min(90.0, max(-90.0, lat))
        lon = min(180.0, max(-180.0, lon))
    return TransactionEvent(
        schema_version=1,
        event_id=UUID(int=draw(st.integers(min_value=0, max_value=2**128 - 1))),
        payment_id=draw(_TEXT),
        merchant_id=draw(_TEXT),
        amount_minor=draw(st.integers(min_value=1, max_value=10**9)),
        currency="EUR",
        status=draw(st.sampled_from(("approved", "declined"))),
        event_time=draw(_aware_datetimes()),
        card_token=draw(_TEXT),
        ingest_time=draw(_aware_datetimes()),
        lat=lat,
        lon=lon,
    )


@given(_transaction_events())
@settings(max_examples=50)
def test_transaction_event_bytes_round_trip(event: TransactionEvent) -> None:
    assert TransactionEvent.from_bytes(event.to_bytes()) == event
