"""Alert list, detail, and status transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from detection.models import Alert, AlertRule, AuditLog
from detection.repositories import AlertRepository, AuditRepository, RuleRepository
from detection.services.cursor import (
    AlertCursor,
    CursorError,
    decode_cursor,
    encode_cursor,
)
from detection.services.transitions import can_mutate, transition_allowed

Status = Literal["open", "acknowledged", "resolved"]
Sort = Literal["created_at", "-created_at"]
EntityType = Literal["card", "merchant"]


class AlertNotFoundError(Exception):
    """No row for the given alert id."""


class InvalidTransitionError(Exception):
    """Status change is not allowed from the current state."""


class ForbiddenTransitionError(Exception):
    """Operator role may not mutate alerts."""


class NoteRequiredError(Exception):
    """Resolution requires a note of 3–500 characters."""


@dataclass(frozen=True, slots=True)
class AlertPage:
    """One page of alerts plus an optional opaque cursor."""

    items: list[Alert]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class AlertDetail:
    """Alert row with rule metadata and audit history."""

    alert: Alert
    rule: AlertRule
    history: list[AuditLog]


class AlertService:
    """Orchestrates repositories; no SQL here."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._alerts = AlertRepository(session)
        self._rules = RuleRepository(session)
        self._audit = AuditRepository(session)

    async def list_alerts(
        self,
        *,
        statuses: list[Status] | None,
        entity: str | None,
        entity_type: EntityType | None,
        rule_id: UUID | None,
        created_from: datetime | None,
        created_to: datetime | None,
        sort: Sort,
        cursor: str | None,
        limit: int,
    ) -> AlertPage:
        sort_desc = sort == "-created_at"
        cursor_created_at: datetime | None = None
        cursor_alert_id: UUID | None = None
        if cursor is not None:
            try:
                parsed: AlertCursor = decode_cursor(cursor)
            except CursorError as exc:
                raise CursorError("invalid cursor") from exc
            cursor_created_at = parsed.created_at
            cursor_alert_id = parsed.alert_id
        rows = await self._alerts.list_page(
            statuses=list(statuses) if statuses else None,
            entity=entity,
            entity_type=entity_type,
            rule_id=rule_id,
            created_from=created_from,
            created_to=created_to,
            sort_desc=sort_desc,
            cursor_created_at=cursor_created_at,
            cursor_alert_id=cursor_alert_id,
            limit=limit + 1,
        )
        next_cursor: str | None = None
        if len(rows) > limit:
            last = rows[limit - 1]
            next_cursor = encode_cursor(last.created_at, last.alert_id)
            rows = rows[:limit]
        return AlertPage(items=rows, next_cursor=next_cursor)

    async def get_detail(self, alert_id: UUID) -> AlertDetail:
        alert = await self._alerts.get(alert_id)
        if alert is None:
            raise AlertNotFoundError
        history = await self._audit.list_for_alert(alert_id)
        return AlertDetail(alert=alert, rule=alert.rule, history=history)

    async def list_rules(self) -> list[AlertRule]:
        return await self._rules.list_all()

    async def transition(
        self,
        alert_id: UUID,
        *,
        new_status: Status,
        note: str | None,
        operator_id: str,
        role: str,
    ) -> Alert:
        if not can_mutate(role):
            raise ForbiddenTransitionError
        if new_status == "resolved":
            if note is None or not (3 <= len(note) <= 500):
                raise NoteRequiredError
        alert = await self._alerts.lock(alert_id)
        if alert is None:
            raise AlertNotFoundError
        if not transition_allowed(alert.status, new_status):
            raise InvalidTransitionError
        now = datetime.now(UTC)
        if new_status == "acknowledged":
            alert.status = "acknowledged"
            alert.acknowledged_by = operator_id
            alert.acknowledged_at = now
        elif new_status == "resolved":
            alert.status = "resolved"
            alert.resolved_by = operator_id
            alert.resolved_at = now
            alert.resolution_note = note
        else:
            raise InvalidTransitionError
        await self._session.commit()
        await self._session.refresh(alert)
        return alert
