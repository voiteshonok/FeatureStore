from typing import Literal, Self
from uuid import UUID

from pydantic import ConfigDict, Field

from schemas.coords import CoordinateMixin
from schemas.time import UtcMsDateTime


class TransactionEvent(CoordinateMixin):
    """Kafka and downstream payment event. Structurally cannot carry a PAN."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    event_id: UUID
    payment_id: str = Field(min_length=1)
    merchant_id: str = Field(min_length=1)
    amount_minor: int = Field(gt=0)
    currency: Literal["EUR"]
    status: Literal["approved", "declined"]
    event_time: UtcMsDateTime
    card_token: str = Field(min_length=1)
    ingest_time: UtcMsDateTime

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        return cls.model_validate_json(data)
