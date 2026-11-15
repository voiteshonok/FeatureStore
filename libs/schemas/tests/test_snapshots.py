from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from schemas import FeatureSnapshot, partition_for

EVENT_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)


def snapshot(**overrides: object) -> FeatureSnapshot:
    payload: dict[str, object] = {
        "entity_type": "card",
        "entity_id": "tok-1",
        "event_id": EVENT_ID,
        "event_time": NOW,
        "ingest_time": NOW,
        "features": {"card_cnt_1m": 1, "card_sum_1m": 1.0},
        "source_partition": 0,
        "source_offset": 4,
    }
    payload.update(overrides)
    return FeatureSnapshot.model_validate(payload)


def test_snapshot_bytes_round_trip_keeps_int_and_float() -> None:
    original = snapshot()
    restored = FeatureSnapshot.from_bytes(original.to_bytes())
    assert restored == original
    assert type(restored.features["card_cnt_1m"]) is int
    assert restored.features["card_cnt_1m"] == 1
    assert type(restored.features["card_sum_1m"]) is float
    assert restored.features["card_sum_1m"] == 1.0


def test_unknown_feature_name_rejected() -> None:
    with pytest.raises(ValidationError):
        snapshot(features={"not_a_feature": 1})


@pytest.mark.parametrize("name", ["merchant_cnt_24h", "geo_velocity_kmh"])
def test_feature_must_be_stream_feature_of_entity(name: str) -> None:
    with pytest.raises(ValidationError):
        snapshot(features={name: 1})


def test_partition_for_is_stable_and_in_range() -> None:
    for key in ("", "a", "ab", "abc", "card-token"):
        chosen = partition_for(key, 4)
        assert partition_for(key, 4) == chosen
        assert 0 <= chosen < 4
    assert partition_for("other", 1) == 0


def test_partition_for_rejects_non_positive_count() -> None:
    with pytest.raises(ValueError, match="n_partitions must be positive"):
        partition_for("card", 0)
    with pytest.raises(ValueError, match="n_partitions must be positive"):
        partition_for("card", -1)
