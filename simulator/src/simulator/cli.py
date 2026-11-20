"""Command line for the payment sender."""

import argparse
import asyncio
import json
from uuid import uuid4

from simulator.sender import MAX_BACKFILL_S, parse_duration, run_backfill, run_sender


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="simulator")
    parser.add_argument("--tps", type=float, required=True)
    parser.add_argument("--duration", help="60, 60s, 10m, or 1h. Not used with --backfill")
    parser.add_argument("--backfill", help="History depth, for example 23h. Must be under 24h")
    parser.add_argument("--send-rate", type=float, help="Posts per second while backfilling")
    parser.add_argument("--max-in-flight", type=int, default=64)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--run-id", default=None, help="Defaults to a random id")
    parser.add_argument("--url", required=True)
    args = parser.parse_args(argv)
    if args.tps <= 0:
        parser.error("--tps must be positive")
    run_id = args.run_id if args.run_id else str(uuid4())
    if args.backfill is not None:
        if args.duration is not None:
            parser.error("--duration is not used with --backfill")
        if args.send_rate is None:
            parser.error("--send-rate is required with --backfill")
        if args.send_rate <= 0:
            parser.error("--send-rate must be positive")
        if args.max_in_flight < 1:
            parser.error("--max-in-flight must be positive")
        depth_s = parse_duration(args.backfill)
        if depth_s >= MAX_BACKFILL_S:
            parser.error("--backfill must be shorter than 24h")
        stats = asyncio.run(
            run_backfill(
                url=args.url,
                tps=args.tps,
                depth_s=depth_s,
                send_rate=args.send_rate,
                seed=args.seed,
                run_id=run_id,
                max_in_flight=args.max_in_flight,
            )
        )
    else:
        if args.send_rate is not None:
            parser.error("--send-rate requires --backfill")
        if args.duration is None:
            parser.error("--duration is required")
        stats = asyncio.run(
            run_sender(
                url=args.url,
                tps=args.tps,
                duration_s=parse_duration(args.duration),
                seed=args.seed,
                run_id=run_id,
            )
        )
    print(json.dumps(stats.to_json()))
