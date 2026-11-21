"""``ops.audit_log`` — read-only for detection (trigger writes)."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from detection.models.base import Base


class AuditLog(Base):
    """One alert status transition recorded by the SECURITY DEFINER trigger."""

    __tablename__ = "audit_log"
    __table_args__ = {"schema": "ops"}

    audit_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    actor: Mapped[str | None] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    alert_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    detail: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
