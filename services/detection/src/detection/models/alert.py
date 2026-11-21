"""``detection.alerts`` row."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from detection.models.base import Base

if TYPE_CHECKING:
    from detection.models.rule import AlertRule


class Alert(Base):
    """One alert lifecycle row."""

    __tablename__ = "alerts"
    __table_args__ = {"schema": "detection"}

    alert_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    rule_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("detection.alert_rules.rule_id"),
        nullable=False,
    )
    merchant_id: Mapped[str | None] = mapped_column(Text)
    card_token: Mapped[str | None] = mapped_column(Text)
    event_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    feature_snapshot: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'open'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    acknowledged_by: Mapped[str | None] = mapped_column(Text)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_by: Mapped[str | None] = mapped_column(Text)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolution_note: Mapped[str | None] = mapped_column(Text)

    rule: Mapped[AlertRule] = relationship("AlertRule", lazy="raise")
