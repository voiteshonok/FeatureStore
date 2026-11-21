"""Pydantic request and response models for the detection API."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Status = Literal["open", "acknowledged", "resolved"]
Sort = Literal["created_at", "-created_at"]
Entity = Literal["card", "merchant"]
Comparison = Literal["gt", "ge", "lt", "le"]


class AlertUpdate(BaseModel):
    """Body of ``PATCH /v1/alerts/{alert_id}``."""

    model_config = ConfigDict(extra="forbid")

    status: Status
    note: str | None = Field(default=None, min_length=3, max_length=500)


class AlertOut(BaseModel):
    """One alert row."""

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
    """Rule with conditions — for filters and alert detail."""

    model_config = ConfigDict(extra="forbid")

    rule_id: UUID
    name: str
    entity: Entity
    enabled: bool
    conditions: list[RuleConditionOut]


class AuditEntryOut(BaseModel):
    """One ``ops.audit_log`` row for an alert."""

    model_config = ConfigDict(extra="forbid")

    audit_id: int
    occurred_at: datetime
    actor: str | None
    action: str
    detail: dict[str, object]


class AlertDetailOut(BaseModel):
    """Alert card: row, rule, and audit history."""

    model_config = ConfigDict(extra="forbid")

    alert: AlertOut
    rule: RuleOut
    history: list[AuditEntryOut]
