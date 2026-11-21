"""Response shapes for the gateway OpenAPI document.

Handlers proxy raw bytes or build the catalog from the registry row. These
models describe the contract and are not used to parse upstream bodies.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Status = Literal["open", "acknowledged", "resolved"]
Sort = Literal["created_at", "-created_at"]
Probe = Literal["ok", "down"]
Entity = Literal["card", "merchant"]
Comparison = Literal["gt", "ge", "lt", "le"]
OperatorRole = Literal["viewer", "analyst", "admin"]


class FieldErrorOut(BaseModel):
    """One validation field failure."""

    model_config = ConfigDict(extra="forbid")

    field: str
    message: str


class ProblemDetails(BaseModel):
    """Gateway-owned error body (T7.2 / RFC 9457-shaped)."""

    model_config = ConfigDict(extra="forbid")

    type: str
    title: str
    status: int
    request_id: str
    errors: list[FieldErrorOut] = Field(default_factory=list)


# Backward-compatible alias used in a few OpenAPI helpers.
ErrorBody = ProblemDetails


class ReadyBody(BaseModel):
    """``GET /ready`` body."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ready", "not_ready"]
    features: Probe
    detection: Probe


class LoginBody(BaseModel):
    """Body of ``POST /v1/auth/login``."""

    model_config = ConfigDict(extra="forbid")

    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=128)


class MeOut(BaseModel):
    """Body of ``GET /v1/auth/me``."""

    model_config = ConfigDict(extra="forbid")

    operator_id: str
    email: str
    role: OperatorRole


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
    note: str | None = Field(default=None, min_length=3, max_length=500)


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
    resolution_note: str | None = None


class AlertListOut(BaseModel):
    """Keyset page of alerts."""

    model_config = ConfigDict(extra="forbid")

    items: list[AlertOut]
    next_cursor: str | None


class RuleConditionOut(BaseModel):
    """One comparison clause."""

    model_config = ConfigDict(extra="forbid")

    feature_name: str
    comparison: Comparison
    threshold: float


class RuleOut(BaseModel):
    """Rule with conditions."""

    model_config = ConfigDict(extra="forbid")

    rule_id: UUID
    name: str
    entity: Entity
    enabled: bool
    conditions: list[RuleConditionOut]


class AuditEntryOut(BaseModel):
    """One audit log row on an alert card."""

    model_config = ConfigDict(extra="forbid")

    audit_id: int
    occurred_at: datetime
    actor: str | None
    action: str
    detail: dict[str, object]


class AlertDetailOut(BaseModel):
    """Alert card: row, rule, and history."""

    model_config = ConfigDict(extra="forbid")

    alert: AlertOut
    rule: RuleOut
    history: list[AuditEntryOut]
