"""Scan Kafka, Postgres, and Compose logs for Luhn-valid card numbers."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from psycopg import sql

from schemas.pan import is_luhn

# Hex on either side is a card token. A leading dot is the fraction of a coordinate.
_PAN_RUN = re.compile(r"(?<![0-9A-Fa-f.])\d{13,19}(?![0-9A-Fa-f])")
_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_FILE = _ROOT / "infra" / "docker-compose.yml"
_TOPICS = ("transactions.raw", "features.updates")


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
    """Render per-source counts and masked findings. ``None`` means skipped."""
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


def main() -> int:
    """Scan the configured sources and print the report."""
    counts: list[tuple[str, int | None]] = []
    findings: list[Finding] = []
    try:
        for source in _sources():
            if not source.enabled:
                counts.append((source.name, None))
                continue
            records = list(source.records())
            found = scan_records(source.name, records)
            counts.append((source.name, len(found)))
            findings.extend(found)
    except ScanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_report(counts, findings))
    return exit_code(findings)


def _sources() -> list[_LiveSource]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
    return [KafkaSource(topic, bootstrap) for topic in _TOPICS] + [
        PostgresSource(),
        LogSource(),
        DisabledSource("redis"),
        DisabledSource("clickhouse"),
    ]


class _LiveSource:
    name: str
    enabled: bool

    def records(self) -> Iterator[tuple[str, str]]:
        raise NotImplementedError


class DisabledSource(_LiveSource):
    """A store that is not scanned until a later stage."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.enabled = False

    def records(self) -> Iterator[tuple[str, str]]:
        return iter(())


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


class LogSource(_LiveSource):
    """Read Compose logs for every service."""

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
