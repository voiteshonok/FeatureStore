"""Alert row access."""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import ColumnElement, Select, and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from detection.models import Alert, AlertRule


class AlertRepository:
    """Queries against ``detection.alerts``."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, alert_id: UUID) -> Alert | None:
        result = await self._session.execute(
            select(Alert)
            .where(Alert.alert_id == alert_id)
            .options(selectinload(Alert.rule).selectinload(AlertRule.features))
        )
        return result.scalar_one_or_none()

    async def lock(self, alert_id: UUID) -> Alert | None:
        result = await self._session.execute(
            select(Alert).where(Alert.alert_id == alert_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def list_page(
        self,
        *,
        statuses: list[str] | None,
        entity: str | None,
        rule_id: UUID | None,
        created_from: datetime | None,
        created_to: datetime | None,
        sort_desc: bool,
        cursor_created_at: datetime | None,
        cursor_alert_id: UUID | None,
        limit: int,
    ) -> list[Alert]:
        stmt: Select[tuple[Alert]] = select(Alert)
        conditions: list[ColumnElement[bool]] = []
        if statuses:
            conditions.append(Alert.status.in_(statuses))
        if entity is not None:
            conditions.append(or_(Alert.merchant_id == entity, Alert.card_token == entity))
        if rule_id is not None:
            conditions.append(Alert.rule_id == rule_id)
        if created_from is not None:
            conditions.append(Alert.created_at >= created_from)
        if created_to is not None:
            conditions.append(Alert.created_at <= created_to)
        if cursor_created_at is not None and cursor_alert_id is not None:
            if sort_desc:
                conditions.append(
                    or_(
                        Alert.created_at < cursor_created_at,
                        and_(
                            Alert.created_at == cursor_created_at,
                            Alert.alert_id < cursor_alert_id,
                        ),
                    )
                )
            else:
                conditions.append(
                    or_(
                        Alert.created_at > cursor_created_at,
                        and_(
                            Alert.created_at == cursor_created_at,
                            Alert.alert_id > cursor_alert_id,
                        ),
                    )
                )
        if conditions:
            stmt = stmt.where(and_(*conditions))
        if sort_desc:
            stmt = stmt.order_by(Alert.created_at.desc(), Alert.alert_id.desc())
        else:
            stmt = stmt.order_by(Alert.created_at.asc(), Alert.alert_id.asc())
        stmt = stmt.limit(limit)
        rows = await self._session.scalars(stmt)
        return cast(list[Alert], list(rows.all()))
