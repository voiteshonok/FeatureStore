"""Orchestrate the first K1/K2 + Redis memory experiment."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from prometheus import (
    PrometheusError,
    current_lag,
    wait_for_zero_lag,
    window_metrics,
)
from redis_memory import capture_redis_memory
from report import ReportError, assess_validity, build_report, error_classes

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-f", str(_ROOT / "infra" / "docker-compose.yml")]
_RESULTS = _ROOT / "experiments" / "results"
_LOAD_JS = _ROOT / "experiments" / "load" / "features.js"
_SAMPLE_TOKENS = _ROOT / "experiments" / "load" / "sample_tokens.py"
_TARGETS = ("direct", "gateway", "baseline")


def _redis_url() -> str:
    """Host Redis URL. ``REDIS_URL`` wins; else ``REDIS_PASSWORD`` on localhost."""
    explicit = os.environ.get("REDIS_URL")
    if explicit:
        return explicit
    password = os.environ.get("REDIS_PASSWORD")
    if password:
        return f"redis://:{password}@localhost:6379/0"
    return "redis://localhost:6379/0"


def main(argv: list[str] | None = None) -> int:
    _ = argv
    mode = os.environ.get("MODE", "full")
    short = mode == "short"

    backfill = os.environ.get("BACKFILL", "30s")
    tps = os.environ.get("TPS", "200")
    send_rate = os.environ.get("SEND_RATE", "2000")
    seed = os.environ.get("SEED", "42")
    warm_run_id = os.environ.get("WARM_RUN_ID", "warm")
    live_run_id = os.environ.get("LIVE_RUN_ID", "live")
    # Short mode uses a gentler RATE so a colocated stand is not saturated;
    # override with RATE=... when measuring for real.
    rate = os.environ.get("RATE", "50" if short else "200")
    duration = "1m" if short else os.environ.get("DURATION", "10m")
    pause_s = int(os.environ.get("PAUSE_S", "10" if short else "60"))
    gateway_limit = os.environ.get("GATEWAY_RATE_LIMIT", "400/second")
    prom_url = os.environ.get("PROM_URL", "http://127.0.0.1:9090")
    host = os.environ.get("HOST", "localhost")
    api_key = os.environ.get("API_KEY", "dev-scoring-key")
    sim_duration = _sim_duration(duration, pause_s, short=short)

    stamp = datetime.now(UTC).strftime("%Y%m%d")
    out_dir = _RESULTS / f"{stamp}-k1-k2"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"results → {out_dir}", flush=True)

    env = os.environ.copy()
    env["GATEWAY_RATE_LIMIT"] = gateway_limit

    skip_setup = os.environ.get("SKIP_SETUP", "") == "1"
    achieved: float | None = None
    if skip_setup:
        print("== SKIP_SETUP=1: reuse running stand ==", flush=True)
        _ensure_gateway_rate_limit(env, gateway_limit)
    else:
        print("== clean volumes + secret + stand ==", flush=True)
        _run(["make", "clean"], cwd=_ROOT, env=env, check=False)
        _run(["make", "secret"], cwd=_ROOT, env=env)
        print("== build images ==", flush=True)
        _run(
            [
                *_COMPOSE,
                "--profile",
                "baseline",
                "--profile",
                "obs",
                "--profile",
                "sim",
                "build",
            ],
            cwd=_ROOT,
            env=env,
        )
        _run(["make", "up"], cwd=_ROOT, env=env)
        _run(["make", "obs-up"], cwd=_ROOT, env=env)
        _run(["make", "baseline-up"], cwd=_ROOT, env=env)
        # Recreate gateway with the raised rate limit.
        _run(
            [*_COMPOSE, "up", "-d", "--force-recreate", "--wait", "gateway"],
            cwd=_ROOT,
            env=env,
        )

        print(f"== warm-up backfill {backfill} ==", flush=True)
        backfill_out = _run(
            [
                "make",
                "backfill",
                f"BACKFILL={backfill}",
                f"TPS={tps}",
                f"SEND_RATE={send_rate}",
                f"SEED={seed}",
                f"RUN_ID={warm_run_id}",
            ],
            cwd=_ROOT,
            env=env,
            capture=True,
        )
        achieved = _parse_achieved_tps(backfill_out.stdout + backfill_out.stderr)

    print("== wait consumer lag == 0 ==", flush=True)
    lag_timeout = float(os.environ.get("LAG_TIMEOUT_S", "3600"))
    wait_for_zero_lag(prom_url, timeout_s=lag_timeout)

    print("== redis memory ==", flush=True)
    redis_url = _redis_url()
    redis_payload = capture_redis_memory(url=redis_url)
    _write_json(out_dir / "redis-memory.json", redis_payload)

    print(f"== live simulator {tps} TPS for {sim_duration} ==", flush=True)
    sim_name = "k1k2-simulator"
    _run(["docker", "rm", "-f", sim_name], check=False, env=env)
    _run(
        [
            *_COMPOSE,
            "--profile",
            "sim",
            "run",
            "-d",
            "--build",
            "--name",
            sim_name,
            "--rm",
            "simulator",
            "--tps",
            tps,
            "--duration",
            sim_duration,
            "--seed",
            seed,
            "--run-id",
            live_run_id,
            "--url",
            "http://ingestion:8000",
        ],
        cwd=_ROOT,
        env=env,
    )

    print("== sample tokens ==", flush=True)
    _run(
        [
            "uv",
            "run",
            "python",
            str(_SAMPLE_TOKENS),
            "--output",
            str(_ROOT / "experiments" / "load" / "tokens.json"),
        ],
        cwd=_ROOT,
        env=env,
    )

    k6_summaries: dict[str, dict[str, Any] | None] = {
        "direct": None,
        "gateway": None,
        "baseline": None,
    }
    windows: dict[str, Any] = {}
    status_accum: dict[str, float] = {}
    run_windows: list[dict[str, Any]] = []

    try:
        for index, target in enumerate(_TARGETS):
            if index:
                print(f"== pause {pause_s}s ==", flush=True)
                time.sleep(pause_s)
            print(f"== k6 {target} rate={rate} duration={duration} ==", flush=True)
            summary_path = out_dir / f"k6-{target}.json"
            started = time.time()
            tokens_path = _ROOT / "experiments" / "load" / "tokens.json"
            result = _run(
                [
                    "k6",
                    "run",
                    "--no-thresholds",
                    "-e",
                    f"TARGET={target}",
                    "-e",
                    f"HOST={host}",
                    "-e",
                    f"API_KEY={api_key}",
                    "-e",
                    f"RATE={rate}",
                    "-e",
                    f"DURATION={duration}",
                    "-e",
                    f"TOKENS_FILE={tokens_path}",
                    "--summary-export",
                    str(summary_path),
                    str(_LOAD_JS),
                ],
                cwd=_ROOT,
                env=env,
                check=False,
            )
            ended = time.time()
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            k6_summaries[target] = summary
            metrics = window_metrics(prom_url, end_s=ended, range_s=duration)
            # K2 for online path uses features_* for direct/gateway; baseline uses baseline_*.
            window = {
                "target": target,
                "started_unix": started,
                "ended_unix": ended,
                "range": duration,
                "k6_exit_code": result.returncode,
                "features_event_to_online_p95_seconds": metrics[
                    "features_event_to_online_p95_seconds"
                ],
                "features_event_to_online_p95_at_inf": metrics[
                    "features_event_to_online_p95_at_inf"
                ],
                "baseline_event_to_store_p95_seconds": metrics[
                    "baseline_event_to_store_p95_seconds"
                ],
                "baseline_event_to_store_p95_at_inf": metrics["baseline_event_to_store_p95_at_inf"],
                "status_counts": metrics["status_counts"],
                "status_shares": metrics["status_shares"],
                "error_classes": error_classes(summary),
            }
            windows[target] = window
            run_windows.append(window)
            for name, count in metrics["status_counts"].items():
                status_accum[name] = status_accum.get(name, 0.0) + float(count)
    finally:
        print("== stop simulator ==", flush=True)
        _run(["docker", "stop", sim_name], check=False, env=env)

    total_status = sum(status_accum.values()) or 0.0
    status_shares = (
        {name: count / total_status for name, count in status_accum.items()} if total_status else {}
    )
    feat_lag, base_lag = current_lag(prom_url)
    lag_payload = {"features": feat_lag, "baseline": base_lag}
    prom_payload = {
        "windows": windows,
        "status_shares": status_shares,
        "status_counts": status_accum,
        "runs": run_windows,
        "lag": lag_payload,
    }
    _write_json(out_dir / "prometheus.json", prom_payload)

    validity, validity_reasons = assess_validity(
        k6_by_target={name: summary for name, summary in k6_summaries.items() if summary},
        prometheus=prom_payload,
    )
    config = {
        "git_sha": _git_sha(),
        "mode": mode,
        "seed": int(seed),
        "warm_run_id": warm_run_id,
        "live_run_id": live_run_id,
        "backfill": {
            "depth": backfill,
            "tps": float(tps),
            "send_rate": float(send_rate),
            "achieved_tps": achieved,
        },
        "k6": {
            "rate": int(rate),
            "duration": duration,
            "pause_s": pause_s,
            "targets": list(_TARGETS),
        },
        "GATEWAY_RATE_LIMIT": gateway_limit,
        "container_memory_limits": _compose_memory_limits(),
        "images": _compose_images(env),
        "host": _host_info(),
        "load_generator": ("k6 ran on the same host as the Docker Compose stand (colocated)"),
        "measurement_limits": (
            f"Warm-up depth {backfill} is not a full-day ADR-002 replay; "
            "24h windows are only partially filled; Redis memory is not "
            "comparable to the ~1.5 GB day estimate."
        ),
        "simulator_duration": sim_duration,
        "prom_url": prom_url,
        "lag": lag_payload,
        "validity": validity,
        "validity_reasons": validity_reasons,
        "created_at": datetime.now(UTC).isoformat(),
    }
    _write_json(out_dir / "config.json", config)

    try:
        report = build_report(
            k6_by_target=k6_summaries,
            prometheus=prom_payload,
            redis_memory=redis_payload,
            config=config,
        )
    except ReportError as exc:
        print(f"report failed: {exc}", file=sys.stderr)
        return 1
    (out_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {out_dir} validity={validity}", flush=True)
    return 0


def _ensure_gateway_rate_limit(env: Mapping[str, str], gateway_limit: str) -> None:
    """Recreate gateway when the live rate limit differs from the experiment."""
    current = _gateway_env_rate_limit()
    if current == gateway_limit:
        print(f"gateway rate limit already {gateway_limit}", flush=True)
        return
    print(
        f"== recreate gateway GATEWAY_RATE_LIMIT {current!r} → {gateway_limit!r} ==",
        flush=True,
    )
    _run(
        [*_COMPOSE, "up", "-d", "--force-recreate", "--wait", "gateway"],
        cwd=_ROOT,
        env=env,
    )


def _gateway_env_rate_limit() -> str | None:
    completed = subprocess.run(  # noqa: S603
        [
            "docker",
            "inspect",
            "-f",
            "{{range .Config.Env}}{{println .}}{{end}}",
            "infra-gateway-1",
        ],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    for line in completed.stdout.splitlines():
        if line.startswith("GATEWAY_RATE_LIMIT="):
            return line.split("=", 1)[1]
    return None


def _sim_duration(k6_duration: str, pause_s: int, *, short: bool) -> str:
    # Three k6 windows + two pauses + margin.
    seconds = _duration_seconds(k6_duration) * 3 + pause_s * 2 + (60 if short else 300)
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _duration_seconds(raw: str) -> int:
    match = re.fullmatch(r"(\d+)(s|m|h)?", raw.strip())
    if not match:
        msg = f"bad duration {raw!r}"
        raise ValueError(msg)
    value = int(match.group(1))
    unit = match.group(2) or "s"
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    return value * 3600


def _parse_achieved_tps(output: str) -> float | None:
    for line in reversed(output.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "achieved_tps" in payload:
            return float(payload["achieved_tps"])
    match = re.search(r"achieved_tps=([0-9.]+)", output)
    if match:
        return float(match.group(1))
    return None


def _compose_memory_limits() -> dict[str, str]:
    compose_path = _ROOT / "infra" / "docker-compose.yml"
    data = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = data.get("services", {})
    out: dict[str, str] = {}
    for name, spec in services.items():
        limits = (
            spec.get("deploy", {}).get("resources", {}).get("limits", {})
            if isinstance(spec, Mapping)
            else {}
        )
        memory = limits.get("memory")
        if memory:
            out[name] = str(memory)
    return out


def _compose_images(env: Mapping[str, str]) -> list[dict[str, str]]:
    completed = _run(
        [*_COMPOSE, "--profile", "baseline", "--profile", "obs", "images", "--format", "json"],
        cwd=_ROOT,
        env=dict(env),
        capture=True,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return []
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return []
    if isinstance(payload, list):
        return payload
    return []


def _host_info() -> dict[str, Any]:
    mem_total = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                mem_total = f"{parts[1]} {parts[2]}"
                break
    cpus = os.cpu_count()
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": cpus,
        "mem_total": mem_total,
        "hostname": platform.node(),
    }


def _git_sha() -> str:
    completed = subprocess.run(  # noqa: S603
        ["git", "rev-parse", "HEAD"],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip()


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        env=None if env is None else dict(env),
        check=check,
        text=True,
        capture_output=capture,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PrometheusError, ReportError, subprocess.CalledProcessError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc
