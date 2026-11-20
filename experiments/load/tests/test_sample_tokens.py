from sample_tokens import select_token_pairs

_EVENTS = [
    {
        "card_token": "c1",
        "merchant_id": "m1",
        "lat": 52.5,
        "lon": 13.4,
        "amount_minor": 100,
    },
    {
        "card_token": "c1",
        "merchant_id": "m1",
        "lat": 52.6,
        "lon": 13.5,
        "amount_minor": 200,
    },
    {
        "card_token": "c2",
        "merchant_id": "m2",
        "lat": None,
        "lon": None,
    },
    {
        "card_token": "c3",
        "merchant_id": "m3",
        "lat": 48.8,
        "lon": 2.3,
    },
    {
        "card_token": "c4",
        "merchant_id": "m4",
        "lat": 40.7,
        "lon": None,
    },
    {
        "card_token": "c5",
        "merchant_id": "m5",
        "lat": 41.9,
        "lon": 12.5,
        "extra": "drop-me",
    },
]


def test_selects_distinct_pairs_with_coords_only() -> None:
    rows = select_token_pairs(_EVENTS, n=10)
    assert rows == [
        {"card_token": "c1", "merchant_id": "m1", "lat": 52.5, "lon": 13.4},
        {"card_token": "c3", "merchant_id": "m3", "lat": 48.8, "lon": 2.3},
        {"card_token": "c5", "merchant_id": "m5", "lat": 41.9, "lon": 12.5},
    ]


def test_output_has_only_four_fields() -> None:
    rows = select_token_pairs(_EVENTS, n=10)
    assert all(set(row) == {"card_token", "merchant_id", "lat", "lon"} for row in rows)


def test_respects_n() -> None:
    rows = select_token_pairs(_EVENTS, n=1)
    assert len(rows) == 1
    assert rows[0]["card_token"] == _EVENTS[0]["card_token"]


def test_empty_when_n_zero() -> None:
    assert select_token_pairs(_EVENTS, n=0) == []
