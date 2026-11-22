"""Mask identifiers for the live stream (ADR-021)."""

from __future__ import annotations


def mask_entity_id(*, card_token: str | None, merchant_id: str | None) -> tuple[str, str]:
    """Return ``(entity, entity_id)`` with a masked card token when present.

    Card tokens become ``ab12…ef`` (first 4 + ellipsis + last 2). Merchant ids
    are passed through unchanged. Exactly one subject is expected (XOR in DDL).
    """
    if card_token is not None and card_token != "":
        return "card", _mask_token(card_token)
    if merchant_id is not None and merchant_id != "":
        return "merchant", merchant_id
    msg = "alert has neither card_token nor merchant_id"
    raise ValueError(msg)


def _mask_token(token: str) -> str:
    if len(token) <= 6:
        return "…"
    return f"{token[:4]}…{token[-2:]}"
