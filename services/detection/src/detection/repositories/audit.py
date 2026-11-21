"""Audit trail reads."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from detection.models import AuditLog


class AuditRepository:
    """Queries against ``ops.audit_log`` (SELECT grant only)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_for_alert(self, alert_id: UUID) -> list[AuditLog]:
        result = await self._session.execute(
            select(AuditLog)
            .where(AuditLog.alert_id == alert_id)
            .order_by(AuditLog.occurred_at.asc(), AuditLog.audit_id.asc())
        )
        return list(result.scalars().all())
