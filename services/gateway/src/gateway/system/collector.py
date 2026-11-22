"""Background system-state collector (ADR-022)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal

import httpx
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

from gateway.system.hub import SystemHub
from gateway.system.state import (
    ProbeBundle,
    ReadyProbe,
    WorkerProbe,
    build_snapshot,
    should_emit_change,
)
from schemas import SystemStreamEvent
from schemas.stream import ComponentStatus

READY_INTERVAL_S = 2.0
PROM_INTERVAL_S = 5.0
PROBE_TIMEOUT_S = 1.0

_DEP_MAP = {
    "kafka": "kafka",
    "redis": "redis",
    "postgres": "postgres",
    "postgresql": "postgres",
}

Sleep = Callable[[float], Awaitable[None]]


class SystemCollector:
    """Probe ``/ready`` and Prometheus; publish snapshot/change to the hub."""

    def __init__(
        self,
        hub: SystemHub,
        *,
        ingestion_url: str,
        features_url: str,
        detection_url: str,
        prometheus_url: str,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]] | None = None,
        probe_client: httpx.AsyncClient | None = None,
        ready_interval_s: float = READY_INTERVAL_S,
        prom_interval_s: float = PROM_INTERVAL_S,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._hub = hub
        self._urls = {
            "ingestion": ingestion_url.rstrip("/"),
            "features-api": features_url.rstrip("/"),
            "detection": detection_url.rstrip("/"),
        }
        self._prometheus_url = prometheus_url.rstrip("/")
        self._pool = pool
        self._owns_client = probe_client is None
        self._client = probe_client or httpx.AsyncClient(
            timeout=httpx.Timeout(PROBE_TIMEOUT_S),
        )
        self._ready_interval_s = ready_interval_s
        self._prom_interval_s = prom_interval_s
        self._sleep = sleep
        self._previous: SystemStreamEvent | None = None
        self._workers: dict[str, WorkerProbe] = {}
        self._prom_ok = False
        self._ready_probes = 0
        self._emit_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._prom_task: asyncio.Task[None] | None = None

    @property
    def ready_probe_count(self) -> int:
        """Number of ``/ready`` HTTP calls made (for tests)."""
        return self._ready_probes

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._ready_loop(), name="system-ready")
        self._prom_task = asyncio.create_task(self._prom_loop(), name="system-prom")

    async def aclose(self) -> None:
        for task in (self._task, self._prom_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._prom_task = None
        if self._owns_client:
            await self._client.aclose()

    async def tick_ready(self) -> SystemStreamEvent:
        """One ready-probe cycle; useful for tests."""
        return await self._emit_from_ready()

    async def tick_prom(self) -> None:
        """One Prometheus cycle then emit; useful for tests."""
        await self._refresh_prom()
        await self._emit_from_ready()

    async def _ready_loop(self) -> None:
        logger = logging.getLogger("gateway")
        while True:
            try:
                await self._emit_from_ready()
            except Exception:
                logger.exception("system ready tick failed")
            await self._sleep(self._ready_interval_s)

    async def _prom_loop(self) -> None:
        logger = logging.getLogger("gateway")
        while True:
            try:
                await self._refresh_prom()
                await self._emit_from_ready()
            except Exception:
                logger.exception("system prom tick failed")
            await self._sleep(self._prom_interval_s)

    async def _emit_from_ready(self) -> SystemStreamEvent:
        now = datetime.now(UTC)
        ready = await self._probe_all_ready(now)
        gateway_status = await self._gateway_status()
        async with self._emit_lock:
            bundle = ProbeBundle(
                checked_at=now,
                gateway_status=gateway_status,
                ready=ready,
                workers=dict(self._workers),
                prom_ok=self._prom_ok,
            )
            current = build_snapshot(bundle)
            event_name: Literal["snapshot", "change"]
            if self._previous is None:
                event_name = "snapshot"
                await self._hub.publish(event_name, current)
                self._previous = current
            elif should_emit_change(self._previous, current):
                event_name = "change"
                await self._hub.publish(event_name, current)
                self._previous = current
            return current

    async def _probe_all_ready(self, now: datetime) -> dict[str, ReadyProbe]:
        names: tuple[Literal["ingestion", "features-api", "detection"], ...] = (
            "ingestion",
            "features-api",
            "detection",
        )
        results = await asyncio.gather(
            *(self._probe_ready(name, self._urls[name], now) for name in names)
        )
        return {probe.service: probe for probe in results}

    async def _probe_ready(
        self,
        service: Literal["ingestion", "features-api", "detection"],
        base_url: str,
        now: datetime,
    ) -> ReadyProbe:
        self._ready_probes += 1
        url = f"{base_url}/ready"
        try:
            response = await self._client.get(url)
        except (httpx.HTTPError, TimeoutError, OSError):
            logging.getLogger("gateway").warning("%s ready failed", service)
            return ReadyProbe(service=service, status="down", checked_at=now, deps={})
        status: ComponentStatus = "up" if response.status_code == 200 else "down"
        deps: dict[Literal["kafka", "redis", "postgres"], ComponentStatus] = {}
        try:
            body = response.json()
        except ValueError:
            body = {}
        if isinstance(body, dict):
            for raw_key, mapped in _DEP_MAP.items():
                if raw_key not in body:
                    continue
                deps[mapped] = _normalize_dep(body[raw_key])  # type: ignore[index]
        return ReadyProbe(service=service, status=status, checked_at=now, deps=deps)

    async def _gateway_status(self) -> ComponentStatus:
        if self._pool is None:
            return "up"
        try:
            async with asyncio.timeout(PROBE_TIMEOUT_S):
                async with self._pool.connection() as conn:
                    await conn.execute("SELECT 1")
        except Exception:
            return "down"
        return "up"

    async def _refresh_prom(self) -> None:
        now = datetime.now(UTC)
        up_worker = await self._prom_query('up{job="features-worker"}')
        up_offline = await self._prom_query('up{job="features-offline"}')
        lag_worker = await self._prom_query("sum(features_consumer_lag)")
        lag_offline = await self._prom_query("sum(offline_consumer_lag)")
        if up_worker is None and up_offline is None and lag_worker is None and lag_offline is None:
            # Distinguish "Prom down" from "all zeros": try a trivial query.
            healthy = await self._prom_query("vector(1)")
            self._prom_ok = healthy is not None
        else:
            self._prom_ok = True
        self._workers = {
            "features-worker": WorkerProbe(
                name="features-worker",
                status=_up_status(up_worker, self._prom_ok),
                checked_at=now,
                lag=_lag_int(lag_worker) if self._prom_ok else None,
            ),
            "features-offline": WorkerProbe(
                name="features-offline",
                status=_up_status(up_offline, self._prom_ok),
                checked_at=now,
                lag=_lag_int(lag_offline) if self._prom_ok else None,
            ),
        }

    async def _prom_query(self, expr: str) -> float | None:
        url = f"{self._prometheus_url}/api/v1/query"
        try:
            response = await self._client.get(url, params={"query": expr})
        except (httpx.HTTPError, TimeoutError, OSError):
            return None
        if response.status_code != 200:
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict) or payload.get("status") != "success":
            return None
        results = payload.get("data", {}).get("result", [])
        if not results:
            return None
        value = results[0].get("value")
        if not value or len(value) < 2:
            return None
        raw = value[1]
        if raw in {"NaN", "+Inf", "-Inf"}:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None


def _normalize_dep(raw: object) -> ComponentStatus:
    if raw in {"ok", "up", "ready"}:
        return "up"
    if raw in {"down", "not_ready", "error"}:
        return "down"
    return "unknown"


def _up_status(value: float | None, prom_ok: bool) -> ComponentStatus:
    if not prom_ok or value is None:
        return "unknown"
    return "up" if value >= 1.0 else "down"


def _lag_int(value: float | None) -> int | None:
    if value is None:
        return None
    return int(value)
