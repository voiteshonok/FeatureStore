"""Simulator profile loaded from YAML."""

from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

_MAX_LATE_S = 23 * 60 * 60


class City(BaseModel):
    """A city centre used to place cards and merchants."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class SimulatorConfig(BaseModel):
    """Traffic profile. Numeric rates come from the YAML file, not from code."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    n_cards: int = Field(ge=1, le=1_000_000_000)
    n_merchants: int = Field(ge=1)
    bin: str = Field(pattern=r"^[0-9]{6,8}$")
    pan_length: int = Field(ge=13, le=19)
    tail_shape: float = Field(gt=0)
    max_weight_ratio: float = Field(gt=1)
    tps: float = Field(gt=0)
    card_home_sigma_km: float = Field(gt=0)
    merchant_sigma_km: float = Field(gt=0)
    amount_log_mean: float
    amount_log_sigma: float = Field(gt=0)
    decline_rate_mean: float = Field(ge=0, le=1)
    decline_rate_min: float = Field(ge=0, le=1)
    decline_rate_max: float = Field(ge=0, le=1)
    duplicate_rate: float = Field(ge=0, le=1)
    late_short_rate: float = Field(ge=0, le=1)
    late_short_min_s: float = Field(gt=0)
    late_short_max_s: float = Field(gt=0)
    late_long_rate: float = Field(ge=0, le=1)
    late_long_min_s: float = Field(gt=0)
    late_long_max_s: float = Field(gt=0, le=_MAX_LATE_S)
    retry_rate: float = Field(ge=0, le=1)
    cities: tuple[City, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def ranges_fit(self) -> Self:
        body = self.pan_length - len(self.bin) - 1
        if body < 1 or self.n_cards > 10**body:
            msg = "PAN body is too small for n_cards"
            raise ValueError(msg)
        if self.decline_rate_min > self.decline_rate_mean:
            msg = "decline_rate_min must be at most the mean"
            raise ValueError(msg)
        if self.decline_rate_mean > self.decline_rate_max:
            msg = "decline_rate_mean must be at most the max"
            raise ValueError(msg)
        if self.late_short_min_s > self.late_short_max_s:
            msg = "late_short window is inverted"
            raise ValueError(msg)
        if self.late_long_min_s > self.late_long_max_s:
            msg = "late_long window is inverted"
            raise ValueError(msg)
        if self.late_short_rate + self.late_long_rate > 1:
            msg = "late rates must sum to at most 1"
            raise ValueError(msg)
        if self.n_merchants < len(self.cities):
            msg = "each city needs at least one merchant"
            raise ValueError(msg)
        return self


def load_config(path: Path | None = None) -> SimulatorConfig:
    """Load the simulator profile. The packaged YAML is the default."""
    source = path if path is not None else Path(__file__).with_name("profile.yaml")
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"simulator profile must be a mapping: {source}"
        raise ValueError(msg)
    return SimulatorConfig.model_validate(payload)
