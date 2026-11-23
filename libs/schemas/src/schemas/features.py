from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import Field, model_validator
from pydantic_core import InitErrorDetails, PydanticCustomError, ValidationError

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
    card_token: str | None = Field(default=None, min_length=1)
    merchant_id: str | None = Field(default=None, min_length=1)
    ts: UtcMsDateTime

    @model_validator(mode="after")
    def one_entity(self) -> Self:
        if self.card_token is not None or self.merchant_id is not None:
            return self
        raise ValidationError.from_exception_data(
            self.__class__.__name__,
            [
                InitErrorDetails(
                    type=PydanticCustomError(
                        "entity_required",
                        "A card token or a merchant id is required",
                    ),
                    loc=("card_token",),
                    input=None,
                )
            ],
        )


class FeatureResponse(StrictModel):
    request_id: UUID
    features: dict[str, FeatureValue]
