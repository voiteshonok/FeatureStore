"""SSE event payloads for the dashboard live stream (ADR-021)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

AlertOp = Literal["created", "updated"]
EntityKind = Literal["card", "merchant"]
AlertStatus = Literal["open", "acknowledged", "resolved"]


class AlertStreamEvent(BaseModel):
    """One alert create/update on the live stream."""

    model_config = ConfigDict(extra="forbid")

    id: int = Field(description="Monotonic stream id for Last-Event-ID resume")
    op: AlertOp
    alert_id: UUID
    rule_id: UUID
    rule_name: str
    status: AlertStatus
    entity: EntityKind
    entity_id: str = Field(description="Masked card_token or raw merchant_id")
    created_at: datetime


class StatsStreamEvent(BaseModel):
    """One-second stats snapshot over a 60 s sliding window."""

    model_config = ConfigDict(extra="forbid")

    events_per_s: float = Field(description="Events in the last completed second")
    events_per_s_avg_60s: float = Field(description="Mean events/s over 60 s")
    alerts_60s_by_rule: dict[str, int] = Field(default_factory=dict)
    latency_p50_ms: float | None = Field(
        default=None,
        description="p50 ingest_time → snapshot receive over 60 s",
    )
    latency_p95_ms: float | None = Field(
        default=None,
        description="p95 ingest_time → snapshot receive over 60 s",
    )
    active_cards_60s: int = Field(description="Distinct card entity ids in 60 s")


class ResetStreamEvent(BaseModel):
    """Resume impossible; client must reload alerts via REST."""

    model_config = ConfigDict(extra="forbid")

    reason: Literal["gap", "reconnect"] = "gap"
