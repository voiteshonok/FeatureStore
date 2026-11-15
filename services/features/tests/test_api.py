import json
import logging

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError as RedisConnectionError

from features.api import JsonFormatter, _partition_count, create_app
from features.settings import Settings
from schemas import CATALOG


class _RedisDown:
    async def ping(self) -> bool:
        raise RedisConnectionError("down")

    def pipeline(self, *, transaction: bool = False) -> object:
        raise RedisConnectionError("down")


class _RedisUp:
    async def ping(self) -> bool:
        return True


class _Connection:
    def __init__(self, *, fail: bool) -> None:
        self.fail = fail

    async def __aenter__(self) -> "_Connection":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, _query: str) -> None:
        if self.fail:
            raise OSError("down")


class _Pool:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def connection(self) -> _Connection:
        return _Connection(fail=self.fail)


def _app(redis: object, pool: object | None = None) -> object:
    app = create_app(Settings())
    app.state.redis = redis
    app.state.pool = pool or _Pool()
    return app


def _body() -> dict[str, object]:
    return {
        "card_token": "card-1",
        "merchant_id": "m-1",
        "ts": "2026-06-01T12:00:00Z",
    }


async def test_health_does_not_check_dependencies() -> None:
    app = create_app(Settings())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_names_the_failed_probe() -> None:
    redis_down = _app(_RedisDown(), _Pool())
    transport = ASGITransport(app=redis_down)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 503
    assert response.json()["redis"] == "down"
    assert response.json()["postgres"] == "ok"

    postgres_down = _app(_RedisUp(), _Pool(fail=True))
    transport = ASGITransport(app=postgres_down)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 503
    assert response.json()["redis"] == "ok"
    assert response.json()["postgres"] == "down"


async def test_ready_is_ok_when_both_probes_pass() -> None:
    app = _app(_RedisUp(), _Pool())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "redis": "ok", "postgres": "ok"}


async def test_redis_outage_on_read_is_503() -> None:
    app = _app(_RedisDown())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/features", json=_body())
    assert response.status_code == 503
    assert response.json()["detail"] == "redis unavailable"


async def test_request_log_carries_the_request_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="features")
    app = _app(FakeAsyncRedis(decode_responses=True))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/features", json=_body())
    assert response.status_code == 200
    request_id = response.json()["request_id"]
    assert set(response.json()["features"]) == {spec.name for spec in CATALOG}
    matched = [
        record for record in caplog.records if getattr(record, "request_id", None) == request_id
    ]
    assert matched
    record = matched[0]
    assert getattr(record, "card_token", None) == "card-1"
    assert getattr(record, "merchant_id", None) == "m-1"
    assert isinstance(getattr(record, "latency_ms", None), int)


def test_formatter_masks_a_pan() -> None:
    record = logging.LogRecord(
        name="features",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="pan 4111111111111111",
        args=(),
        exc_info=None,
    )
    payload = json.loads(JsonFormatter().format(record))
    assert "4111111111111111" not in payload["message"]
    assert "411111" in payload["message"]
    assert "1111" in payload["message"]


def test_partition_count_ignores_an_unknown_topic() -> None:
    described = [
        {
            "topic": "transactions.raw",
            "error_code": 0,
            "partitions": [{}, {}, {}, {}],
        }
    ]
    assert _partition_count(described, "transactions.raw") == 4
    missing = [{"topic": "transactions.raw", "error_code": 3, "partitions": []}]
    assert _partition_count(missing, "transactions.raw") is None
    assert _partition_count([], "transactions.raw") is None


def test_partition_count_must_be_positive() -> None:
    with pytest.raises(ValueError, match="n_partitions"):
        Settings(n_partitions=0)
