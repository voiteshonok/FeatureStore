"""Entity masking for the live stream."""

import pytest

from detection.live.mask import mask_entity_id


def test_mask_card_token() -> None:
    card = "abcdefghijklmnop"
    entity, entity_id = mask_entity_id(card_token=card, merchant_id=None)
    assert entity == "card"
    assert entity_id == "abcd…op"


def test_merchant_passthrough() -> None:
    entity, entity_id = mask_entity_id(card_token=None, merchant_id="m1")
    assert entity == "merchant"
    assert entity_id == "m1"


def test_short_token() -> None:
    card = "abc"
    entity, entity_id = mask_entity_id(card_token=card, merchant_id=None)
    assert entity == "card"
    assert entity_id == "…"


def test_missing_subject_raises() -> None:
    with pytest.raises(ValueError, match="neither"):
        mask_entity_id(card_token=None, merchant_id=None)
