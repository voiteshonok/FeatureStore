"""Harvest known card/merchant pairs with coordinates from transactions.raw."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, TopicPartition

from schemas import TransactionEvent

_DEFAULT_BOOTSTRAP = "localhost:29092"
_DEFAULT_TOPIC = "transactions.raw"
_DEFAULT_N = 500


def select_token_pairs(
    events: Iterable[Mapping[str, Any] | TransactionEvent],
    n: int,
) -> list[dict[str, str | float]]:
    """Return up to ``n`` distinct ``(card_token, merchant_id)`` rows with coords.

    Only the four fields ``card_token``, ``merchant_id``, ``lat``, ``lon`` are kept.
    Events missing either coordinate are skipped. The first occurrence of a pair wins.
    """
    if n < 0:
        msg = "n must be non-negative"
        raise ValueError(msg)
    chosen: list[dict[str, str | float]] = []
    seen: set[tuple[str, str]] = set()
    for event in events:
        if len(chosen) >= n:
            break
        card_token, merchant_id, lat, lon = _fields(event)
        if lat is None or lon is None:
            continue
        key = (card_token, merchant_id)
        if key in seen:
            continue
        seen.add(key)
        chosen.append(
            {
                "card_token": card_token,
                "merchant_id": merchant_id,
                "lat": float(lat),
                "lon": float(lon),
            }
        )
    return chosen


def _fields(
    event: Mapping[str, Any] | TransactionEvent,
) -> tuple[str, str, float | None, float | None]:
    if isinstance(event, TransactionEvent):
        return event.card_token, event.merchant_id, event.lat, event.lon
    lat = event.get("lat")
    lon = event.get("lon")
    return (
        str(event["card_token"]),
        str(event["merchant_id"]),
        None if lat is None else float(lat),
        None if lon is None else float(lon),
    )


def read_events(bootstrap: str, topic: str, partitions: int = 4) -> list[TransactionEvent]:
    """Read all records currently on ``topic`` from beginning to end."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        auto_offset_reset="earliest",
    )
    assigned = [TopicPartition(topic, index) for index in range(partitions)]
    consumer.assign(assigned)
    consumer.seek_to_beginning(*assigned)
    events: list[TransactionEvent] = []
    try:
        for record in consumer:
            if record.value is None:
                continue
            events.append(TransactionEvent.from_bytes(record.value))
    finally:
        consumer.close()
    return events


def write_tokens(path: Path, rows: Sequence[Mapping[str, str | float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(rows), indent=2) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sample distinct card_token/merchant_id pairs from transactions.raw",
    )
    parser.add_argument("--n", type=int, default=_DEFAULT_N, help="max distinct pairs")
    parser.add_argument(
        "--bootstrap",
        default=_DEFAULT_BOOTSTRAP,
        help="Kafka bootstrap servers",
    )
    parser.add_argument("--topic", default=_DEFAULT_TOPIC, help="Kafka topic name")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "tokens.json",
        help="output JSON path",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=4,
        help="topic partition count",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    events = read_events(args.bootstrap, args.topic, partitions=args.partitions)
    rows = select_token_pairs(events, args.n)
    if not rows:
        print("no token pairs with coordinates found", file=sys.stderr)
        return 1
    write_tokens(args.output, rows)
    print(f"wrote {len(rows)} pairs to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
