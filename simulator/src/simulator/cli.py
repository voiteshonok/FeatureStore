"""Command line for the payment sender."""

import argparse
import asyncio
import json

from simulator.sender import parse_duration, run_sender


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="simulator")
    parser.add_argument("--tps", type=float, required=True)
    parser.add_argument("--duration", required=True, help="60, 60s, 10m, or 1h")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--url", required=True)
    args = parser.parse_args(argv)
    if args.tps <= 0:
        parser.error("--tps must be positive")
    stats = asyncio.run(
        run_sender(
            url=args.url,
            tps=args.tps,
            duration_s=parse_duration(args.duration),
            seed=args.seed,
        )
    )
    print(json.dumps(stats.to_json()))
