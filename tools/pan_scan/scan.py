"""Scan Kafka, Postgres, Redis, ClickHouse, and Compose logs for Luhn-valid PANs."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
import psycopg
import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from psycopg import sql

from features.windows import card_key, geo_key, merchant_features_key, watermark_key
from schemas.pan import is_luhn

# Hex on either side is a card token. A leading dot is the fraction of a coordinate.
_PAN_RUN = re.compile(r"(?<![0-9A-Fa-f.])\d{13,19}(?![0-9A-Fa-f])")
_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _ROOT / "infra" / "docker-compose.yml"
_TOPICS = ("transactions.raw", "features.updates")
_LOG_SERVICES = (
    "ingestion",
    "features-worker",
    "features-offline",
    "features-api",
    "detection",
    "gateway",
    "baseline-writer",
    "baseline-api",
)
_CARD_PREFIX = card_key("")
_MFEAT_PREFIX = merchant_features_key("")
_GEO_PREFIX = geo_key("")
_WM_PREFIX = watermark_key(0).split(":", 1)[0] + ":"
_CH_TABLES = ("events", "feature_snapshots")
_STRING_TYPES = frozenset({"String", "LowCardinality(String)"})


class ScanError(Exception):
    """A source could not be read."""


@dataclass(frozen=True)
class Finding:
    """One Luhn-valid number, with the PAN already masked."""

    source: str
    location: str
    masked: str


def find_pans(text: str) -> list[str]:
    """Return 13–19 digit Luhn runs that stand alone, not inside a token or a fraction."""
    return [match.group(0) for match in _PAN_RUN.finditer(text) if is_luhn(match.group(0))]


def mask_pan(digits: str) -> str:
    """Keep the first 6 and last 4 digits."""
    hidden = len(digits) - 10
    return f"{digits[:6]}{'*' * hidden}{digits[-4:]}"


def exit_code(findings: Iterable[Finding]) -> int:
    """Return 1 when any card number was found."""
    for _finding in findings:
        return 1
    return 0


def scan_records(source: str, records: Iterable[tuple[str, str]]) -> list[Finding]:
    """Scan already-loaded text. ``records`` are ``(location, text)`` pairs."""
    findings: list[Finding] = []
    for location, text in records:
        findings.extend(Finding(source, location, mask_pan(pan)) for pan in find_pans(text))
    return findings


def format_report(
    counts: list[tuple[str, int | None]],
    findings: list[Finding],
) -> str:
    """Render per-source scanned-record counts and masked findings. ``None`` means skipped."""
    lines: list[str] = []
    for name, count in counts:
        if count is None:
            lines.append(f"{name}: skipped")
        else:
            lines.append(f"{name}: {count}")
    for finding in findings:
        lines.append(f"{finding.location}: {finding.masked}")
    total = sum(count for _name, count in counts if count is not None)
    lines.append(f"total: {total}")
    return "\n".join(lines)


def redis_scan_texts(
    key: str,
    key_type: str,
    *,
    string_value: str | None = None,
    hash_items: dict[str, str] | None = None,
    zset_members: list[tuple[str, float]] | None = None,
    list_values: list[str] | None = None,
    set_values: list[str] | None = None,
) -> list[str]:
    """Return texts to scan for one Redis key, applying known-prefix skip rules."""
    kind = _redis_kind(key)
    texts: list[str] = [key]
    normalized = key_type.lower()

    if kind == "card":
        if normalized == "zset" and zset_members is not None:
            texts.extend(member for member, _score in zset_members)
        return texts

    if kind == "mfeat":
        if normalized == "hash" and hash_items is not None:
            texts.extend(hash_items)
        return texts

    if kind == "geo":
        if string_value is not None:
            texts.extend(_geo_scan_fields(string_value))
        return texts

    if kind == "wm":
        return texts

    if normalized == "string" and string_value is not None:
        texts.append(string_value)
    elif normalized == "hash" and hash_items is not None:
        for field, value in hash_items.items():
            texts.append(field)
            texts.append(value)
    elif normalized == "zset" and zset_members is not None:
        for member, score in zset_members:
            texts.append(member)
            texts.append(_score_text(score))
    elif normalized == "list" and list_values is not None:
        texts.extend(list_values)
    elif normalized == "set" and set_values is not None:
        texts.extend(set_values)
    return texts


def _redis_kind(key: str) -> str | None:
    if key.startswith(_CARD_PREFIX):
        return "card"
    if key.startswith(_MFEAT_PREFIX):
        return "mfeat"
    if key.startswith(_GEO_PREFIX):
        return "geo"
    if key.startswith(_WM_PREFIX):
        return "wm"
    return None


def _geo_scan_fields(value: str) -> list[str]:
    """Scan geo value fields except the format_geo time field (index 2)."""
    parts = value.split("|")
    scanned: list[str] = []
    for index, part in enumerate(parts):
        if index == 2:
            continue
        scanned.append(part)
    return scanned


def _score_text(score: float) -> str:
    if score.is_integer():
        return str(int(score))
    return str(score)


def main(argv: list[str] | None = None) -> int:
    """Scan the configured sources and print the report."""
    parser = argparse.ArgumentParser(description="Scan stores and logs for Luhn-valid PANs.")
    parser.add_argument(
        "--sources",
        help="Comma-separated source names (default: all)",
    )
    args = parser.parse_args(argv)
    try:
        selected = _parse_sources(args.sources)
        counts: list[tuple[str, int | None]] = []
        findings: list[Finding] = []
        for source in _sources():
            if source.name not in selected:
                continue
            if not source.enabled:
                counts.append((source.name, None))
                continue
            records = list(source.records())
            found = scan_records(source.name, records)
            counts.append((source.name, len(records)))
            findings.extend(found)
    except ScanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_report(counts, findings))
    return exit_code(findings)


def _parse_sources(raw: str | None) -> set[str]:
    available = {source.name for source in _sources()}
    if raw is None or raw.strip() == "":
        return available
    wanted = {part.strip() for part in raw.split(",") if part.strip()}
    unknown = sorted(wanted - available)
    if unknown:
        msg = f"unknown sources: {', '.join(unknown)}"
        raise ScanError(msg)
    return wanted


def _sources() -> list[_LiveSource]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
    return [
        *[KafkaSource(topic, bootstrap) for topic in _TOPICS],
        PostgresSource(),
        RedisSource(),
        ClickHouseSource(),
        LogSource(),
    ]


class _LiveSource:
    name: str
    enabled: bool

    def records(self) -> Iterator[tuple[str, str]]:
        raise NotImplementedError


class KafkaSource(_LiveSource):
    """Read one topic from the start."""

    def __init__(self, topic: str, bootstrap: str) -> None:
        self.name = topic
        self.enabled = True
        self._topic = topic
        self._bootstrap = bootstrap

    def records(self) -> Iterator[tuple[str, str]]:
        consumer = KafkaConsumer(
            bootstrap_servers=self._bootstrap,
            enable_auto_commit=False,
            consumer_timeout_ms=10_000,
        )
        try:
            yield from _read_topic(consumer, self._topic)
        except KafkaError as exc:
            msg = f"kafka {self._topic} at {self._bootstrap}: {exc}"
            raise ScanError(msg) from exc
        finally:
            consumer.close()


def _read_topic(consumer: KafkaConsumer, topic: str) -> Iterator[tuple[str, str]]:
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        msg = f"topic {topic} not found"
        raise ScanError(msg)
    assigned = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assigned)
    end_offsets = consumer.end_offsets(assigned)
    for part in assigned:
        consumer.seek(part, 0)
    done = {part: end_offsets[part] == 0 for part in assigned}
    idle = 0
    while not all(done.values()):
        batches = consumer.poll(timeout_ms=1000, max_records=500)
        if not batches:
            idle += 1
            for part in assigned:
                if consumer.position(part) >= end_offsets[part]:
                    done[part] = True
            if idle >= 10:
                msg = f"timed out reading {topic}"
                raise ScanError(msg)
            continue
        idle = 0
        for part, messages in batches.items():
            for message in messages:
                payload = b"" if message.value is None else message.value
                text = payload.decode("utf-8", errors="replace")
                location = f"{topic} partition {part.partition} offset {message.offset}"
                yield location, text
                if message.offset + 1 >= end_offsets[part]:
                    done[part] = True


class PostgresSource(_LiveSource):
    """Read every user table."""

    def __init__(self) -> None:
        self.name = "postgres"
        self.enabled = True

    def records(self) -> Iterator[tuple[str, str]]:
        try:
            with psycopg.connect(
                host=os.environ.get("POSTGRES_HOST", "localhost"),
                port=int(os.environ.get("POSTGRES_PORT", "5432")),
                user=os.environ.get("POSTGRES_USER", "featurestore"),
                password=os.environ.get("POSTGRES_PASSWORD", "featurestore"),
                dbname=os.environ.get("POSTGRES_DB", "featurestore"),
                connect_timeout=5,
            ) as conn:
                yield from _read_database(conn)
        except psycopg.OperationalError as exc:
            msg = f"postgres: {exc}"
            raise ScanError(msg) from exc


def _read_database(conn: psycopg.Connection[tuple[object, ...]]) -> Iterator[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
            """
        )
        tables = [(str(schema), str(name)) for schema, name in cur.fetchall()]
    for schema, name in tables:
        query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(name),
        )
        with conn.cursor() as cur:
            cur.execute(query)
            for index, row in enumerate(cur):
                text = " ".join(_cell(value) for value in row)
                yield f"postgres {schema}.{name} row {index}", text


def _cell(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


def redis_url_from_env() -> str:
    """Redis URL for a host-side client.

    ``REDIS_URL`` wins. Otherwise a set ``REDIS_PASSWORD`` is applied to
    localhost:6379. With neither, the URL has no password so test containers
    keep working.
    """
    explicit = os.environ.get("REDIS_URL")
    if explicit:
        return explicit
    password = os.environ.get("REDIS_PASSWORD")
    if password:
        return f"redis://:{password}@localhost:6379/0"
    return "redis://localhost:6379/0"


class RedisSource(_LiveSource):
    """SCAN Redis keys and apply known-prefix skip rules."""

    def __init__(self) -> None:
        self.name = "redis"
        self.enabled = True
        self._url = redis_url_from_env()

    def records(self) -> Iterator[tuple[str, str]]:
        try:
            client = redis.Redis.from_url(self._url, decode_responses=True)
            client.ping()
        except redis.RedisError as exc:
            msg = f"redis at {self._url}: {exc}"
            raise ScanError(msg) from exc
        try:
            for raw_key in client.scan_iter(count=200):
                key = str(raw_key)
                key_type = str(client.type(key))
                texts = redis_scan_texts(
                    key,
                    key_type,
                    string_value=_redis_string(client, key, key_type),
                    hash_items=_redis_hash(client, key, key_type),
                    zset_members=_redis_zset(client, key, key_type),
                    list_values=_redis_list(client, key, key_type),
                    set_values=_redis_set(client, key, key_type),
                )
                yield f"redis key {key}", " ".join(texts)
        except redis.RedisError as exc:
            msg = f"redis at {self._url}: {exc}"
            raise ScanError(msg) from exc
        finally:
            client.close()


def _redis_string(client: redis.Redis, key: str, key_type: str) -> str | None:
    if key_type.lower() != "string":
        return None
    value = client.get(key)
    return None if value is None else str(value)


def _redis_hash(client: redis.Redis, key: str, key_type: str) -> dict[str, str] | None:
    if key_type.lower() != "hash":
        return None
    raw = client.hgetall(key)
    return {str(field): str(value) for field, value in raw.items()}


def _redis_zset(client: redis.Redis, key: str, key_type: str) -> list[tuple[str, float]] | None:
    if key_type.lower() != "zset":
        return None
    raw = client.zrange(key, 0, -1, withscores=True)
    if not isinstance(raw, list):
        return []
    out: list[tuple[str, float]] = []
    for item in raw:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            continue
        member, score = item
        out.append((str(member), float(score)))
    return out


def _redis_list(client: redis.Redis, key: str, key_type: str) -> list[str] | None:
    if key_type.lower() != "list":
        return None
    return [str(item) for item in client.lrange(key, 0, -1)]


def _redis_set(client: redis.Redis, key: str, key_type: str) -> list[str] | None:
    if key_type.lower() != "set":
        return None
    return [str(item) for item in client.smembers(key)]


class ClickHouseSource(_LiveSource):
    """Scan string columns and map keys in events / feature_snapshots."""

    def __init__(self) -> None:
        self.name = "clickhouse"
        self.enabled = True
        parsed = urlparse(os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"))
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or 8123
        self._user = os.environ.get("CLICKHOUSE_USER", "svc_offline")
        self._password = os.environ.get(
            "CLICKHOUSE_PASSWORD",
            os.environ.get("SVC_OFFLINE_PASSWORD", "svc_offline_dev"),
        )
        self._database = os.environ.get("CLICKHOUSE_DB", "featurestore")

    def records(self) -> Iterator[tuple[str, str]]:
        try:
            client = clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                username=self._user,
                password=self._password,
                database=self._database,
                connect_timeout=5,
                send_receive_timeout=30,
            )
        except Exception as exc:
            msg = f"clickhouse at {self._host}:{self._port}: {exc}"
            raise ScanError(msg) from exc
        try:
            yield from _read_clickhouse(client, self._database)
        except Exception as exc:
            msg = f"clickhouse at {self._host}:{self._port}: {exc}"
            raise ScanError(msg) from exc
        finally:
            client.close()


def _read_clickhouse(
    client: clickhouse_connect.driver.Client, database: str
) -> Iterator[tuple[str, str]]:
    columns = client.query(
        """
        SELECT table, name, type
        FROM system.columns
        WHERE database = {db:String}
          AND table IN ('events', 'feature_snapshots')
        ORDER BY table, name
        """,
        parameters={"db": database},
    ).result_rows
    by_table: dict[str, list[tuple[str, str]]] = {table: [] for table in _CH_TABLES}
    for table, name, col_type in columns:
        by_table[str(table)].append((str(name), str(col_type)))

    for table in _CH_TABLES:
        expressions: list[str] = []
        for name, col_type in by_table[table]:
            if not name.isidentifier():
                continue
            if col_type in _STRING_TYPES:
                expressions.append(name)
            elif col_type.startswith("Map("):
                expressions.append(f"mapKeys({name})")
        if not expressions:
            continue
        select_list = ", ".join(expressions)
        if not database.isidentifier():
            msg = f"invalid clickhouse database name: {database!r}"
            raise ScanError(msg)
        # Names are filtered to identifiers from system.columns.
        row_sql = f"SELECT {select_list} FROM {database}.{table}"  # noqa: S608
        rows = client.query(row_sql).result_rows
        for index, row in enumerate(rows):
            parts: list[str] = []
            for value in row:
                parts.extend(_ch_cell_parts(value))
            yield f"clickhouse {database}.{table} row {index}", " ".join(parts)


def _ch_cell_parts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)]


class LogSource(_LiveSource):
    """Read Compose logs for application services."""

    def __init__(self) -> None:
        self.name = "logs"
        self.enabled = True

    def records(self) -> Iterator[tuple[str, str]]:
        docker = shutil.which("docker")
        if docker is None:
            msg = "docker not found"
            raise ScanError(msg)
        try:
            completed = subprocess.run(  # noqa: S603
                [
                    docker,
                    "compose",
                    "-f",
                    str(_COMPOSE_FILE),
                    "logs",
                    "--no-color",
                    *_LOG_SERVICES,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            msg = f"docker compose logs: {exc}"
            raise ScanError(msg) from exc
        for index, line in enumerate(completed.stdout.splitlines(), start=1):
            service = line.split("|", 1)[0].strip() if "|" in line else "compose"
            yield f"logs {service} line {index}", line
