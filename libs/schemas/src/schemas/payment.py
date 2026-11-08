from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from schemas.coords import CoordinateMixin
from schemas.pan import is_luhn
from schemas.time import UtcMsDateTime


class PaymentIn(CoordinateMixin):
    event_id: UUID
    payment_id: str = Field(min_length=1)
    pan: str
    merchant_id: str = Field(min_length=1)
    amount_minor: int = Field(gt=0)
    currency: Literal["EUR"]
    status: Literal["approved", "declined"]
    event_time: UtcMsDateTime

    @field_validator("pan")
    @classmethod
    def pan_digits_and_luhn(cls, pan: str) -> str:
        if not pan.isdigit() or not (13 <= len(pan) <= 19):
            msg = "PAN must be 13-19 digits"
            raise ValueError(msg)
        if not is_luhn(pan):
            msg = "PAN failed Luhn check"
            raise ValueError(msg)
        return pan
