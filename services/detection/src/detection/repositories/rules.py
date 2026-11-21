"""Rule and clause access."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from detection.models import AlertRule


class RuleRepository:
    """Queries against ``detection.alert_rules`` / ``rule_features``."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_all(self) -> list[AlertRule]:
        result = await self._session.execute(
            select(AlertRule)
            .options(selectinload(AlertRule.features))
            .order_by(AlertRule.name.asc())
        )
        return list(result.scalars().all())

    async def get(self, rule_id: UUID) -> AlertRule | None:
        result = await self._session.execute(
            select(AlertRule)
            .where(AlertRule.rule_id == rule_id)
            .options(selectinload(AlertRule.features))
        )
        return result.scalar_one_or_none()
