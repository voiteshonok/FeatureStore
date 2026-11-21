"""LISTEN detection_alerts and hydrate rows for the broadcast hub (ADR-021)."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import UUID

from psycopg import AsyncConnection, Notify
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from detection.live.broadcast import BroadcastHub
from detection.live.mask import mask_entity_id
from detection.repositories.alerts import AlertRepository
from schemas import AlertStreamEvent

Sleep = Callable[[float], Awaitable[None]]
_CHANNEL = "detection_alerts"
_BACKOFF_S = 1.0
_BACKOFF_MAX_S = 30.0


class AlertNotifyListener:
    """Dedicated connection outside the pool; reconnects with reset on gap."""

    def __init__(
        self,
        database_url: str,
        session_factory: async_sessionmaker[AsyncSession],
        hub: BroadcastHub,
        *,
        sleep: Sleep = asyncio.sleep,
        on_delivered: Callable[[float], None] | None = None,
    ) -> None:
        self._database_url = database_url
        self._session_factory = session_factory
        self._hub = hub
        self._sleep = sleep
        self._on_delivered = on_delivered
        self._stopped = False

    async def run(self) -> None:
        """Listen until cancelled. On reconnect, reset all subscribers."""
        backoff = _BACKOFF_S
        first = True
        while not self._stopped:
            try:
                if not first:
                    await self._hub.publish_reset(reason="reconnect")
                first = False
                await self._listen_once()
                backoff = _BACKOFF_S
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("detection").warning(
                    "alert notify listener failed",
                    exc_info=True,
                )
                await self._sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX_S)

    def stop(self) -> None:
        self._stopped = True

    async def _listen_once(self) -> None:
        conn = await AsyncConnection.connect(self._database_url, autocommit=True)
        try:
            await conn.execute(f"LISTEN {_CHANNEL}")
            logging.getLogger("detection").info("listening on %s", _CHANNEL)
            async for notify in conn.notifies():
                if self._stopped:
                    return
                await self._handle_notify(notify)
        finally:
            await conn.close()

    async def _handle_notify(self, notify: Notify) -> None:
        started = datetime.now(UTC)
        try:
            payload = json.loads(notify.payload or "{}")
            alert_id = UUID(str(payload["alert_id"]))
            op = str(payload["op"])
            if op not in {"created", "updated"}:
                return
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            logging.getLogger("detection").warning(
                "invalid notify payload",
                extra={"payload": notify.payload},
            )
            return
        event = await self._hydrate(alert_id, cast(Literal["created", "updated"], op))
        if event is None:
            return
        await self._hub.publish_alert(event)
        if self._on_delivered is not None:
            elapsed = (datetime.now(UTC) - started).total_seconds()
            self._on_delivered(elapsed if elapsed > 0 else 0.0)

    async def _hydrate(
        self,
        alert_id: UUID,
        op: Literal["created", "updated"],
    ) -> AlertStreamEvent | None:
        async with self._session_factory() as session:
            repo = AlertRepository(session)
            alert = await repo.get(alert_id)
            if alert is None:
                return None
            entity, entity_id = mask_entity_id(
                card_token=alert.card_token,
                merchant_id=alert.merchant_id,
            )
            rule_name = alert.rule.name if alert.rule is not None else ""
            status = cast(Literal["open", "acknowledged", "resolved"], alert.status)
            return AlertStreamEvent(
                id=0,
                op=op,
                alert_id=alert.alert_id,
                rule_id=alert.rule_id,
                rule_name=rule_name,
                status=status,
                entity=cast(Literal["card", "merchant"], entity),
                entity_id=entity_id,
                created_at=alert.created_at,
            )


async def run_stats_ticker(
    hub: BroadcastHub,
    stats: object,
    *,
    interval_s: float = 1.0,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """Publish a stats event once per second."""
    from detection.live.stats import LiveStats

    live = cast(LiveStats, stats)
    while True:
        await sleep(interval_s)
        with suppress(Exception):
            await hub.publish_stats(live.snapshot())
