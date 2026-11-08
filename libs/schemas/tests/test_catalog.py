from datetime import timedelta

import pytest
from pydantic import ValidationError

from schemas import CATALOG, FeatureSpec, by_name, for_entity

_MAX_WINDOW = timedelta(hours=24)


def test_catalog_names_are_unique() -> None:
    names = [spec.name for spec in CATALOG]
    assert len(names) == len(set(names))


def test_stream_features_have_window_and_stale_after() -> None:
    for spec in CATALOG:
        if spec.kind == "stream":
            assert spec.window is not None
            assert spec.stale_after is not None
        else:
            assert spec.window is None
            assert spec.stale_after is None


def test_no_window_exceeds_24h() -> None:
    for spec in CATALOG:
        if spec.window is not None:
            assert spec.window <= _MAX_WINDOW


def test_by_name_returns_each_spec() -> None:
    for spec in CATALOG:
        assert by_name(spec.name) is spec


def test_by_name_unknown_raises_key_error() -> None:
    with pytest.raises(KeyError):
        by_name("not_a_feature")


def test_for_entity_partitions_catalog() -> None:
    cards = for_entity("card")
    merchants = for_entity("merchant")
    assert all(spec.entity == "card" for spec in cards)
    assert all(spec.entity == "merchant" for spec in merchants)
    assert set(cards).isdisjoint(merchants)
    assert frozenset(cards) | frozenset(merchants) == frozenset(CATALOG)
    assert cards == tuple(spec for spec in CATALOG if spec.entity == "card")
    assert merchants == tuple(spec for spec in CATALOG if spec.entity == "merchant")


def _stream_kwargs(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": "card_cnt_1m",
        "entity": "card",
        "kind": "stream",
        "window": timedelta(minutes=1),
        "agg": "count",
        "stale_after": timedelta(seconds=5),
        "default": 0,
    }
    payload.update(overrides)
    return payload


def test_stream_without_window_rejected() -> None:
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(_stream_kwargs(window=None))


def test_stream_without_stale_after_rejected() -> None:
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(_stream_kwargs(stale_after=None))


def test_on_demand_with_stale_after_rejected() -> None:
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(
            _stream_kwargs(
                kind="on_demand",
                agg="geo_velocity",
                window=None,
                stale_after=timedelta(seconds=5),
                default=None,
            )
        )


def test_window_over_24h_rejected() -> None:
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(_stream_kwargs(window=timedelta(hours=24, milliseconds=1)))
