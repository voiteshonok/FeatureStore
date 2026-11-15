"""Consumer commit, retry, and JSON log fields."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from uuid import UUID

import psycopg
import pytest
from aiokafka.structs import TopicPartition

from detection.consumer import DetectionConsumer, JsonFormatter, event_id_from_bytes
from detection.rules import Clause, Rule, RuleCache
from detection.settings import Settings
from schemas import FeatureSnapshot

_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
_ALERT = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_RULE = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
_EVENT = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
_TOPIC = "features.updates"


class StopLoop(Exception):
    pass


class Record:
    def __init__(self, value: bytes | None, *, offset: int = 0) -> None:
        self.value = value
        self.partition = 0
        self.offset = offset
        self.topic = _TOPIC


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    async def execute(self, _sql: str, _params: object = None) -> None:
        return None

    async def fetchone(self) -> tuple[object, ...] | None:
        if not self.rows:
            return None
        return self.rows[0]

    async def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Conn:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.rows = rows if rows is not None else [(_ALERT,)]
        self.fail_times = 0
        self.sql: list[str] = []
        self.cursor_obj = _Cursor([])

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    def transaction(self) -> _Conn:
        return self

    async def execute(self, sql: str, _params: object = None) -> _Cursor:
        if self.fail_times:
            self.fail_times -= 1
            raise psycopg.OperationalError("down")
        self.sql.append(sql)
        return _Cursor(self.rows)

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def connection(self) -> _Conn:
        return self.conn


class _Client:
    def __init__(self, result: bool | Exception) -> None:
        self.result = result

    async def force_metadata_update(self) -> bool:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _Kafka:
    def __init__(self, *, metadata: bool | Exception = True) -> None:
        self.client = _Client(metadata)
        self.commits: list[dict[object, int]] = []
        self.commit_failures = 0
        self.starts = 0
        self.stops = 0
        self.stop_error = False
        self._records: list[Record] = []

    async def start(self) -> None:
        self.starts += 1

    async def stop(self) -> None:
        self.stops += 1
        if self.stop_error:
            raise RuntimeError("stop")

    async def commit(self, offsets: dict[object, int]) -> None:
        if self.commit_failures:
            self.commit_failures -= 1
            raise ConnectionError("commit down")
        self.commits.append(offsets)

    def __aiter__(self) -> _Kafka:
        return self

    async def __anext__(self) -> Record:
        if self._records:
            return self._records.pop(0)
        await asyncio.sleep(60)
        raise StopAsyncIteration


def _rule() -> Rule:
    return Rule(
        rule_id=_RULE,
        name="card_velocity_burst",
        entity="card",
        clauses=(Clause("card_cnt_1m", "gt", 5),),
    )


def _snapshot(value: int = 6) -> FeatureSnapshot:
    return FeatureSnapshot(
        entity_type="card",
        entity_id="card-1",
        event_id=_EVENT,
        event_time=_NOW,
        ingest_time=_NOW,
        features={"card_cnt_1m": value, "card_cnt_1h": 3},
        source_partition=0,
        source_offset=4,
    )


def _service(
    kafka: _Kafka | None = None,
    conn: _Conn | None = None,
    *,
    sleep: object | None = None,
    cache: RuleCache | None = None,
) -> DetectionConsumer:
    rules = cache or RuleCache()
    if cache is None:
        rules.replace((_rule(),))
    kwargs: dict[str, object] = {}
    if sleep is not None:
        kwargs["sleep"] = sleep
    return DetectionConsumer(
        Settings(rule_reload_s=60),
        _Pool(conn or _Conn()),  # type: ignore[arg-type]
        rules,
        kafka or _Kafka(),  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


async def _noop(_seconds: float) -> None:
    return None


def test_settings_use_the_detection_group() -> None:
    settings = Settings()
    assert settings.group_id == "detection"
    assert settings.updates_topic == "features.updates"
    assert settings.rule_reload_s == 60


def test_settings_reject_a_non_positive_reload() -> None:
    with pytest.raises(ValueError, match="rule_reload_s"):
        Settings(rule_reload_s=0)


def test_from_env_reads_broker_and_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://svc_detection:featurestore@db:5432/featurestore"
    )
    monkeypatch.setenv("RULE_RELOAD_S", "15")
    settings = Settings.from_env()
    assert settings.kafka_bootstrap == "kafka:9092"
    assert settings.database_url.endswith("@db:5432/featurestore")
    assert settings.rule_reload_s == 15
    assert settings.group_id == "detection"


def test_json_log_includes_event_and_alert_id() -> None:
    record = logging.LogRecord(
        name="detection",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="alert fired",
        args=(),
        exc_info=None,
    )
    record.event_id = str(_EVENT)
    record.alert_id = str(_ALERT)
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "alert fired"
    assert payload["event_id"] == str(_EVENT)
    assert payload["alert_id"] == str(_ALERT)
    assert payload["logger"] == "detection"


def test_event_id_from_invalid_payload() -> None:
    assert event_id_from_bytes(b"not-json") is None
    assert event_id_from_bytes(b'{"event_id": "abc"}') == "abc"


async def test_matching_snapshot_commits_after_the_insert(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="detection")
    kafka = _Kafka()
    conn = _Conn()
    service = _service(kafka, conn, sleep=_noop)
    await service.handle(Record(_snapshot().to_bytes(), offset=3))  # type: ignore[arg-type]
    assert conn.sql
    assert "WHERE NOT EXISTS" in conn.sql[0]
    assert list(kafka.commits[0].values()) == [4]
    assert kafka.commits[0][TopicPartition(_TOPIC, 0)] == 4
    fired = [record for record in caplog.records if record.getMessage() == "alert fired"]
    assert len(fired) == 1
    assert fired[0].event_id == str(_EVENT)  # type: ignore[attr-defined]
    assert fired[0].alert_id == str(_ALERT)  # type: ignore[attr-defined]


async def test_redelivered_insert_still_commits_without_a_new_alert(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="detection")
    kafka = _Kafka()
    conn = _Conn(rows=[])
    service = _service(kafka, conn, sleep=_noop)
    await service.handle(Record(_snapshot().to_bytes()))  # type: ignore[arg-type]
    assert kafka.commits
    messages = [record.getMessage() for record in caplog.records if record.levelno == logging.INFO]
    assert messages == []


async def test_invalid_payload_is_committed() -> None:
    kafka = _Kafka()
    conn = _Conn()
    service = _service(kafka, conn, sleep=_noop)
    await service.handle(Record(b"not-a-snapshot", offset=1))  # type: ignore[arg-type]
    assert conn.sql == []
    assert list(kafka.commits[0].values()) == [2]


async def test_database_error_retries_then_commits() -> None:
    kafka = _Kafka()
    conn = _Conn()
    conn.fail_times = 1
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    service = _service(kafka, conn, sleep=sleep)
    await service.handle(Record(_snapshot().to_bytes()))  # type: ignore[arg-type]
    assert sleeps == [0.5]
    assert len(kafka.commits) == 1


async def test_non_retryable_apply_does_not_commit() -> None:
    kafka = _Kafka()

    class _Boom(_Conn):
        async def execute(self, sql: str, _params: object = None) -> _Cursor:
            raise RuntimeError(sql)

    service = _service(kafka, _Boom(), sleep=_noop)
    with pytest.raises(RuntimeError):
        await service.handle(Record(_snapshot().to_bytes()))  # type: ignore[arg-type]
    assert kafka.commits == []


async def test_commit_failure_retries() -> None:
    kafka = _Kafka()
    kafka.commit_failures = 1
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    service = _service(kafka, sleep=sleep)
    await service.handle(Record(b"nope"))  # type: ignore[arg-type]
    assert sleeps == [0.5]
    assert len(kafka.commits) == 1


async def test_ready_requires_a_started_consumer() -> None:
    kafka = _Kafka()
    service = _service(kafka, sleep=_noop)
    assert await service.ready() is False
    await service.start()
    assert await service.ready() is True
    kafka.client = _Client(TimeoutError("slow"))
    assert await service.ready() is False
    kafka.client = _Client(False)
    assert await service.ready() is False


async def test_aclose_is_idempotent() -> None:
    kafka = _Kafka()
    service = _service(kafka)
    await service.aclose()
    await service.aclose()
    assert kafka.stops == 1
    kafka.stop_error = True
    service._closed = False
    await service.aclose()
    assert kafka.stops == 2


async def test_failed_reload_keeps_the_previous_rules() -> None:
    cache = RuleCache()
    cache.replace((_rule(),))
    conn = _Conn()

    class _BadCursor(_Cursor):
        async def execute(self, _sql: str, _params: object = None) -> None:
            raise psycopg.OperationalError("down")

    conn.cursor_obj = _BadCursor([])
    sleeps = 0

    async def sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            raise StopLoop

    service = _service(conn=conn, cache=cache, sleep=sleep)
    with pytest.raises(StopLoop):
        await service.reload_loop()
    assert cache.rules == (_rule(),)


async def test_run_cancels_both_loops() -> None:
    kafka = _Kafka()
    sleeps = 0

    async def sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            raise StopLoop

    service = _service(kafka, sleep=sleep)
    with pytest.raises(StopLoop):
        await service.run()
    assert sleeps == 2
