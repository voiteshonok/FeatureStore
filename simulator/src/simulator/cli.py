"""Command line for the payment sender."""

import argparse
import asyncio
import json
import sys
from uuid import uuid4

from simulator.sender import (
    MAX_BACKFILL_S,
    parse_duration,
    run_backfill,
    run_burst,
    run_sender,
)


def main(argv: list[str] | None = None) -> None:
    args = argv if argv is not None else sys.argv[1:]
    if args and args[0] == "burst":
        _burst(args[1:])
        return
    _run(args)


def _run(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(prog="simulator")
    parser.add_argument("--tps", type=float, required=True)
    parser.add_argument("--duration", help="60, 60s, 10m, or 1h. Not used with --backfill")
    parser.add_argument("--backfill", help="History depth, for example 23h. Must be under 24h")
    parser.add_argument("--send-rate", type=float, help="Posts per second while backfilling")
    parser.add_argument("--max-in-flight", type=int, default=64)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--run-id", default=None, help="Defaults to a random id")
    parser.add_argument("--url", required=True)
    parser.add_argument(
        "--anomalies",
        action="store_true",
        help="Enable anomaly scenarios from profile.yaml (burst, bad merchant, travel)",
    )
    parsed = parser.parse_args(argv)
    if parsed.tps <= 0:
        parser.error("--tps must be positive")
    run_id = parsed.run_id if parsed.run_id else str(uuid4())
    if parsed.backfill is not None:
        if parsed.duration is not None:
            parser.error("--duration is not used with --backfill")
        if parsed.send_rate is None:
            parser.error("--send-rate is required with --backfill")
        if parsed.send_rate <= 0:
            parser.error("--send-rate must be positive")
        if parsed.max_in_flight < 1:
            parser.error("--max-in-flight must be positive")
        depth_s = parse_duration(parsed.backfill)
        if depth_s >= MAX_BACKFILL_S:
            parser.error("--backfill must be shorter than 24h")
        stats = asyncio.run(
            run_backfill(
                url=parsed.url,
                tps=parsed.tps,
                depth_s=depth_s,
                send_rate=parsed.send_rate,
                seed=parsed.seed,
                run_id=run_id,
                max_in_flight=parsed.max_in_flight,
                anomalies=parsed.anomalies,
            )
        )
    else:
        if parsed.send_rate is not None:
            parser.error("--send-rate requires --backfill")
        if parsed.duration is None:
            parser.error("--duration is required")
        stats = asyncio.run(
            run_sender(
                url=parsed.url,
                tps=parsed.tps,
                duration_s=parse_duration(parsed.duration),
                seed=parsed.seed,
                run_id=run_id,
                anomalies=parsed.anomalies,
            )
        )
    print(json.dumps(stats.to_json()))


def _burst(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(prog="simulator burst")
    parser.add_argument("--card-index", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--run-id", default=None, help="Defaults to a random id")
    parsed = parser.parse_args(argv)
    if parsed.card_index < 0:
        parser.error("--card-index must be non-negative")
    if parsed.count < 1:
        parser.error("--count must be positive")
    run_id = parsed.run_id if parsed.run_id else str(uuid4())
    stats = asyncio.run(
        run_burst(
            url=parsed.url,
            seed=parsed.seed,
            card_index=parsed.card_index,
            count=parsed.count,
            run_id=run_id,
        )
    )
    print(json.dumps(stats.to_json()))
