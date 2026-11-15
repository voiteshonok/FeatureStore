"""Response shapes for the gateway OpenAPI document.

Handlers proxy raw bytes or build the catalog from the registry row. These
models describe the contract and are not used to parse upstream bodies.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Status = Literal["open", "acknowledged", "resolved"]
Probe = Literal["ok", "down"]


class ErrorBody(BaseModel):
    """Error payload used by the gateway itself."""

    model_config = ConfigDict(extra="forbid")

    detail: str


class ReadyBody(BaseModel):
    """``GET /ready`` body."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ready", "not_ready"]
    features: Probe
    detection: Probe


class CatalogEntry(BaseModel):
    """One feature-registry version row."""

    model_config = ConfigDict(extra="forbid")

    name: str
    entity: str
    kind: str
    agg: str
    default_value: float | None
    version: int
    window_ms: int | None
    stale_after_ms: int | None


class AlertUpdate(BaseModel):
    """Body of ``PATCH /v1/alerts/{alert_id}``."""

    model_config = ConfigDict(extra="forbid")

    status: Status
    operator_id: str = Field(min_length=1)


class AlertOut(BaseModel):
    """One alert row, matching the detection API."""

    model_config = ConfigDict(extra="forbid")

    alert_id: UUID
    rule_id: UUID
    status: Status
    merchant_id: str | None
    card_token: str | None
    event_id: UUID
    feature_snapshot: dict[str, int | float]
    created_at: datetime
    resolved_at: datetime | None
    acknowledged_at: datetime | None
