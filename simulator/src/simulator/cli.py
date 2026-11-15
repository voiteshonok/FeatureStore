"""Command line for the payment sender."""

import argparse
import asyncio
import json
from uuid import uuid4

from simulator.sender import parse_duration, run_sender


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="simulator")
    parser.add_argument("--tps", type=float, required=True)
    parser.add_argument("--duration", required=True, help="60, 60s, 10m, or 1h")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--run-id", default=None, help="Defaults to a random id")
    parser.add_argument("--url", required=True)
    args = parser.parse_args(argv)
    if args.tps <= 0:
        parser.error("--tps must be positive")
    run_id = args.run_id if args.run_id else str(uuid4())
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
