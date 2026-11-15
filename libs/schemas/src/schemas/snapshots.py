from typing import Literal, Self
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from schemas.catalog import Entity, by_name
from schemas.coords import StrictModel
from schemas.time import UtcMsDateTime


class FeatureSnapshot(StrictModel):
    """Feature values published to ``features.updates`` after one applied event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    entity_type: Entity
    entity_id: str = Field(min_length=1)
    event_id: UUID
    event_time: UtcMsDateTime
    ingest_time: UtcMsDateTime
    features: dict[str, int | float]
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)

    @model_validator(mode="after")
    def features_belong_to_entity(self) -> Self:
        for name in self.features:
            try:
                spec = by_name(name)
            except KeyError as exc:
                msg = f"unknown feature {name}"
                raise ValueError(msg) from exc
            if spec.kind != "stream" or spec.entity != self.entity_type:
                msg = f"feature {name} is not a stream feature of {self.entity_type}"
                raise ValueError(msg)
        return self

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        return cls.model_validate_json(data)
