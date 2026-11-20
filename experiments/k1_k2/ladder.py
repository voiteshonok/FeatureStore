"""Direct-only read-path load ladder (no warm-up, no simulator)."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prometheus import current_lag
from report import error_classes, error_rate, k1_p50_ms, k1_p99_ms

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-f", str(_ROOT / "infra" / "docker-compose.yml")]
_RESULTS = _ROOT / "experiments" / "results"
_LOAD_JS = _ROOT / "experiments" / "load" / "features.js"
_RATES = (20, 50, 100, 200)
_STEP_DURATION = "2m"
_SAMPLE_EVERY_S = 5.0
_K1_P99_MS = 20.0
_ERR_INVALID = 0.01


def main(argv: list[str] | None = None) -> int:
    _ = argv
    prom_url = os.environ.get("PROM_URL", "http://127.0.0.1:9090")
    host = os.environ.get("HOST", "localhost")
    api_key = os.environ.get("API_KEY", "dev-scoring-key")
    duration = os.environ.get("DURATION", _STEP_DURATION)
    rates_raw = os.environ.get("RATES", "")
    rates = (
        tuple(int(part) for part in rates_raw.split(",") if part.strip())
        if rates_raw.strip()
        else _RATES
    )

    stamp = datetime.now(UTC).strftime("%Y%m%d")
    out_dir = _RESULTS / f"{stamp}-ladder"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"results → {out_dir}", flush=True)

    env = os.environ.copy()
    skip_setup = os.environ.get("SKIP_SETUP", "") == "1"
    if skip_setup:
        print("== SKIP_SETUP=1: reuse running stand ==", flush=True)
    else:
        print("== ensure stand (no backfill, simulator off) ==", flush=True)
        _run(["make", "secret"], cwd=_ROOT, env=env, check=False)
        _run(["make", "up"], cwd=_ROOT, env=env)
        _run(["make", "obs-up"], cwd=_ROOT, env=env)

    tokens_path = _ROOT / "experiments" / "load" / "tokens.json"
    if not tokens_path.exists():
        print("== write synthetic tokens ==", flush=True)
        _write_synthetic_tokens(tokens_path)

    host_info = _host_info()
    docker_info = _docker_info()
    steps: list[dict[str, Any]] = []

    for rate in rates:
        print(f"== ladder step rate={rate} duration={duration} ==", flush=True)
        step_dir = out_dir / f"rps-{rate}"
        step_dir.mkdir(parents=True, exist_ok=True)
        summary_path = step_dir / "k6-direct.json"
        samples_path = step_dir / "samples.jsonl"

        stop = threading.Event()
        sampler = threading.Thread(
            target=_sample_loop,
            kwargs={
                "prom_url": prom_url,
                "out_path": samples_path,
                "stop": stop,
                "every_s": _SAMPLE_EVERY_S,
            },
            daemon=True,
        )
        sampler.start()
        started = time.time()
        result = _run(
            [
                "k6",
                "run",
                "--no-thresholds",
                "-e",
                "TARGET=direct",
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
        stop.set()
        sampler.join(timeout=15.0)

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        samples = _read_jsonl(samples_path)
        step = {
            "rate": rate,
            "duration": duration,
            "started_unix": started,
            "ended_unix": ended,
            "k6_exit_code": result.returncode,
            "p50_ms": k1_p50_ms(summary),
            "p99_ms": k1_p99_ms(summary),
            "error_rate": error_rate(summary),
            "error_classes": error_classes(summary),
            "samples": _summarize_samples(samples),
        }
        steps.append(step)
        _write_json(step_dir / "step.json", step)
        samples_summary = step["samples"]
        ops = (
            samples_summary.get("redis_ops_per_sec_avg")
            if isinstance(samples_summary, Mapping)
            else None
        )
        print(
            f"rate={rate} p50={step['p50_ms']:.2f}ms p99={step['p99_ms']:.2f}ms "
            f"err={step['error_rate']:.4%} redis_ops≈{ops}",
            flush=True,
        )

    knee = _find_knee(steps)
    config = {
        "git_sha": _git_sha(),
        "target": "direct",
        "rates": list(rates),
        "duration": duration,
        "no_backfill": True,
        "simulator": "off",
        "host": host_info,
        "docker_info": docker_info,
        "knee": knee,
        "hypothesis": (
            "merchant read path: ADR-016 two-level buckets (~23 hourly + ≤120 minute "
            "HMGET/req). Pre-ADR-016 baseline was 1440 HMGET/req."
        ),
        "created_at": datetime.now(UTC).isoformat(),
    }
    _write_json(out_dir / "config.json", config)
    _write_json(out_dir / "steps.json", {"steps": steps, "knee": knee})
    ladder_md = _render_ladder(steps=steps, knee=knee, config=config)
    (out_dir / "ladder.md").write_text(ladder_md, encoding="utf-8")
    print(ladder_md)
    print(f"wrote {out_dir}", flush=True)
    return 0


def _sample_loop(
    *,
    prom_url: str,
    out_path: Path,
    stop: threading.Event,
    every_s: float,
) -> None:
    with out_path.open("w", encoding="utf-8") as handle:
        while not stop.is_set():
            row = {
                "ts": time.time(),
                "docker_stats": _docker_stats(("redis", "features-api")),
                "redis_stats": _redis_info_stats(),
                "redis_commandstats": _redis_commandstats(),
                "features_consumer_lag": current_lag(prom_url)[0],
            }
            handle.write(json.dumps(row) + "\n")
            handle.flush()
            stop.wait(every_s)


def _docker_stats(names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in names:
        container = _container_name(name)
        if container is None:
            out[name] = {"cpu_pct": None, "mem_usage": None}
            continue
        completed = subprocess.run(  # noqa: S603
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}}\t{{.MemUsage}}",
                container,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0 or not completed.stdout.strip():
            out[name] = {"cpu_pct": None, "mem_usage": None}
            continue
        cpu_text, _, mem = completed.stdout.strip().partition("\t")
        out[name] = {
            "cpu_pct": _parse_pct(cpu_text),
            "mem_usage": mem.strip() or None,
        }
    return out


def _container_name(service: str) -> str | None:
    completed = subprocess.run(  # noqa: S603
        [*_COMPOSE, "ps", "-q", service],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    cid = completed.stdout.strip().splitlines()
    if not cid:
        return None
    inspect = subprocess.run(  # noqa: S603
        ["docker", "inspect", "-f", "{{.Name}}", cid[0]],
        check=False,
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        return None
    return inspect.stdout.strip().lstrip("/")


def _redis_info_stats() -> dict[str, Any]:
    text = _redis_cli("INFO", "stats")
    return {
        "instantaneous_ops_per_sec": _info_int(text, "instantaneous_ops_per_sec"),
        "total_commands_processed": _info_int(text, "total_commands_processed"),
    }


def _redis_commandstats() -> dict[str, Any]:
    text = _redis_cli("INFO", "commandstats")
    # cmdstat_hmget:calls=123,usec=456,...
    match = re.search(r"cmdstat_hmget:calls=(\d+)", text)
    return {"cmdstat_hmget_calls": int(match.group(1)) if match else None}


def _redis_cli(*args: str) -> str:
    completed = subprocess.run(  # noqa: S603
        [*_COMPOSE, "exec", "-T", "redis", "redis-cli", *args],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout


def _info_int(text: str, key: str) -> int | None:
    match = re.search(rf"^{re.escape(key)}:(\d+)\s*$", text, re.MULTILINE)
    if not match:
        return None
    return int(match.group(1))


def _parse_pct(raw: str) -> float | None:
    cleaned = raw.strip().rstrip("%")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {}
    ops = [
        row["redis_stats"]["instantaneous_ops_per_sec"]
        for row in samples
        if isinstance(row.get("redis_stats"), Mapping)
        and row["redis_stats"].get("instantaneous_ops_per_sec") is not None
    ]
    redis_cpu = [
        row["docker_stats"]["redis"]["cpu_pct"]
        for row in samples
        if isinstance(row.get("docker_stats"), Mapping)
        and isinstance(row["docker_stats"].get("redis"), Mapping)
        and row["docker_stats"]["redis"].get("cpu_pct") is not None
    ]
    api_cpu = [
        row["docker_stats"]["features-api"]["cpu_pct"]
        for row in samples
        if isinstance(row.get("docker_stats"), Mapping)
        and isinstance(row["docker_stats"].get("features-api"), Mapping)
        and row["docker_stats"]["features-api"].get("cpu_pct") is not None
    ]
    hmget_vals = [
        row["redis_commandstats"]["cmdstat_hmget_calls"]
        for row in samples
        if isinstance(row.get("redis_commandstats"), Mapping)
        and row["redis_commandstats"].get("cmdstat_hmget_calls") is not None
    ]
    hmget_rate = None
    if len(hmget_vals) >= 2 and len(samples) >= 2:
        dt = samples[-1]["ts"] - samples[0]["ts"]
        if dt > 0:
            hmget_rate = (hmget_vals[-1] - hmget_vals[0]) / dt
    lags = [
        row["features_consumer_lag"]
        for row in samples
        if row.get("features_consumer_lag") is not None
    ]
    return {
        "n_samples": len(samples),
        "redis_ops_per_sec_avg": _avg(ops),
        "redis_ops_per_sec_max": max(ops) if ops else None,
        "redis_cpu_pct_avg": _avg(redis_cpu),
        "redis_cpu_pct_max": max(redis_cpu) if redis_cpu else None,
        "features_api_cpu_pct_avg": _avg(api_cpu),
        "features_api_cpu_pct_max": max(api_cpu) if api_cpu else None,
        "hmget_calls_per_sec": hmget_rate,
        "features_consumer_lag_max": max(lags) if lags else None,
    }


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _find_knee(steps: list[dict[str, Any]]) -> dict[str, Any] | None:
    for step in steps:
        samples = step.get("samples") or {}
        redis_cpu = samples.get("redis_cpu_pct_max")
        if (
            step["p99_ms"] > _K1_P99_MS * 5
            or step["error_rate"] > _ERR_INVALID
            or (redis_cpu is not None and redis_cpu >= 95.0)
        ):
            return {
                "rate": step["rate"],
                "reason": _knee_reason(step),
            }
    return None


def _knee_reason(step: dict[str, Any]) -> str:
    samples = step.get("samples") or {}
    parts: list[str] = []
    if step["p99_ms"] > _K1_P99_MS * 5:
        parts.append(f"p99={step['p99_ms']:.1f}ms ≫ {_K1_P99_MS:g}ms")
    if step["error_rate"] > _ERR_INVALID:
        parts.append(f"error_rate={step['error_rate']:.2%}")
    redis_cpu = samples.get("redis_cpu_pct_max")
    if redis_cpu is not None and redis_cpu >= 95.0:
        parts.append(f"redis_cpu_max={redis_cpu:.1f}%")
    return ", ".join(parts) or "unknown"


def _render_ladder(
    *,
    steps: list[dict[str, Any]],
    knee: dict[str, Any] | None,
    config: Mapping[str, Any],
) -> str:
    lines = [
        "# Read-path load ladder (direct :8001)",
        "",
        "No backfill, simulator off. Isolates merchant HMGET read path.",
        "",
        "| RPS | p50 | p99 | errors | Redis ops/s | HMGET/s | Redis CPU | API CPU | lag max |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for step in steps:
        s = step.get("samples") or {}
        lines.append(
            "| "
            + " | ".join(
                [
                    str(step["rate"]),
                    f"{step['p50_ms']:.2f} ms",
                    f"{step['p99_ms']:.2f} ms",
                    f"{step['error_rate'] * 100:.3f}%",
                    _fmt_num(s.get("redis_ops_per_sec_avg")),
                    _fmt_num(s.get("hmget_calls_per_sec")),
                    _fmt_pct_cpu(s.get("redis_cpu_pct_avg")),
                    _fmt_pct_cpu(s.get("features_api_cpu_pct_avg")),
                    _fmt_num(s.get("features_consumer_lag_max")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Error classes", ""])
    lines.append("| RPS | 2xx_check_fail | 4xx | 429 | 503 | timeout | other |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|")
    for step in steps:
        classes = step.get("error_classes") or {}
        lines.append(
            "| "
            + " | ".join(
                [
                    str(step["rate"]),
                    str(classes.get("2xx_check_fail", 0)),
                    str(classes.get("4xx", 0)),
                    str(classes.get("429", 0)),
                    str(classes.get("503", 0)),
                    str(classes.get("timeout", 0)),
                    str(classes.get("other", 0)),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Knee", ""])
    if knee:
        lines.append(f"- First bad step: **{knee['rate']} RPS** ({knee['reason']}).")
    else:
        lines.append("- No knee within ladder (all steps below gates).")
    host_raw = config.get("host")
    docker_raw = config.get("docker_info")
    host: Mapping[str, Any] = host_raw if isinstance(host_raw, Mapping) else {}
    docker: Mapping[str, Any] = docker_raw if isinstance(docker_raw, Mapping) else {}
    lines.extend(
        [
            "",
            "## Host / Docker",
            "",
            f"- host CPUs: {host.get('cpu_count', '?')}, mem: {host.get('mem_total', '?')}",
            f"- docker CPUs: {docker.get('ncpu', '?')}, mem: {docker.get('mem_total', '?')}",
            f"- hypothesis: {config.get('hypothesis', 'n/a')}",
            "",
        ]
    )
    return "\n".join(lines)


def _fmt_num(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return "n/a"
    return f"{float(value):.1f}"


def _fmt_pct_cpu(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return "n/a"
    return f"{float(value):.1f}%"


def _write_synthetic_tokens(path: Path) -> None:
    rows = [
        {
            "card_token": f"synth-card-{index:04d}",
            "merchant_id": f"synth-merchant-{index % 50:04d}",
            "lat": 52.52,
            "lon": 13.405,
        }
        for index in range(200)
    ]
    path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


def _docker_info() -> dict[str, Any]:
    completed = subprocess.run(  # noqa: S603
        ["docker", "info", "--format", "{{.NCPU}}\t{{.MemTotal}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return {}
    ncpu, _, mem = completed.stdout.strip().partition("\t")
    return {"ncpu": ncpu or None, "mem_total": mem or None}


def _host_info() -> dict[str, Any]:
    mem_total = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                mem_total = f"{parts[1]} {parts[2]}"
                break
    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
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
) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        env=None if env is None else dict(env),
        check=check,
        text=True,
        capture_output=False,
    )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (subprocess.CalledProcessError, OSError, ValueError) as exc:
        print(exc, flush=True)
        raise SystemExit(1) from exc
