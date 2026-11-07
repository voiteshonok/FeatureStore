from enum import StrEnum
from uuid import UUID

from pydantic import Field

from schemas.coords import CoordinateMixin, StrictModel
from schemas.time import UtcMsDateTime


class FeatureStatus(StrEnum):
    fresh = "fresh"
    stale = "stale"
    fallback = "fallback"
    default = "default"


class FeatureValue(StrictModel):
    value: float | int | None = None
    as_of: UtcMsDateTime | None = None
    age_ms: int | None = Field(default=None, ge=0)
    status: FeatureStatus


class FeatureRequest(CoordinateMixin):
    card_token: str = Field(min_length=1)
    merchant_id: str = Field(min_length=1)
    ts: UtcMsDateTime


class FeatureResponse(StrictModel):
    request_id: UUID
    features: dict[str, FeatureValue]
