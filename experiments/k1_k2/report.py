"""Build report.md for a K1/K2 experiment pack."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class ReportError(ValueError):
    """Raised when a required run artifact is missing or malformed."""


_TARGETS = ("baseline", "direct", "gateway")
_ADR_REDIS_GB = 1.5
_K1_P99_MS = 20.0
_K1_P50_MS = 5.0
_K2_P95_S = 2.0
_ERR_RATE = 0.001


def k1_p99_ms(summary: Mapping[str, Any]) -> float:
    return _duration_percentile_ms(summary, "p(99)")


def k1_p50_ms(summary: Mapping[str, Any]) -> float:
    return _duration_percentile_ms(summary, "p(50)")


def error_rate(summary: Mapping[str, Any]) -> float:
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        msg = "k6 summary missing metrics"
        raise ReportError(msg)
    failed = metrics.get("http_req_failed")
    if not isinstance(failed, Mapping):
        msg = "k6 summary missing http_req_failed"
        raise ReportError(msg)
    values = failed.get("values")
    if isinstance(values, Mapping) and "rate" in values:
        return float(values["rate"])
    if "value" in failed:
        # k6 summary-export Rate: value = share of non-zero samples (failures).
        return float(failed["value"])
    msg = "k6 summary missing http_req_failed rate"
    raise ReportError(msg)


def require_targets(
    k6_by_target: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Mapping[str, Any]]:
    """Return all three target summaries or raise naming the missing one."""
    out: dict[str, Mapping[str, Any]] = {}
    for name in _TARGETS:
        summary = k6_by_target.get(name)
        if summary is None:
            msg = f"missing k6 run for target {name}"
            raise ReportError(msg)
        out[name] = summary
    return out


def build_report(
    *,
    k6_by_target: Mapping[str, Mapping[str, Any] | None],
    prometheus: Mapping[str, Any],
    redis_memory: Mapping[str, Any],
    config: Mapping[str, Any],
) -> str:
    """Render report.md from experiment artifacts."""
    summaries = require_targets(k6_by_target)
    windows = prometheus.get("windows")
    if not isinstance(windows, Mapping):
        msg = "prometheus.json missing windows"
        raise ReportError(msg)

    rows = [
        (
            "K1: p99 /v1/features, 200 RPS",
            f"≤ {_K1_P99_MS:g} мс",
            _fmt_ms(k1_p99_ms(summaries["baseline"])),
            _fmt_ms(k1_p99_ms(summaries["direct"])),
            _fmt_ms(k1_p99_ms(summaries["gateway"])),
        ),
        (
            "K1: p50",
            f"≤ {_K1_P50_MS:g} мс",
            _fmt_ms(k1_p50_ms(summaries["baseline"])),
            _fmt_ms(k1_p50_ms(summaries["direct"])),
            _fmt_ms(k1_p50_ms(summaries["gateway"])),
        ),
        (
            "K2: p95 событие → хранилище",
            f"≤ {_K2_P95_S:g} с",
            _fmt_s(_k2(windows, "baseline")),
            _fmt_s(_k2(windows, "direct")),
            _fmt_s(_k2(windows, "gateway")),
        ),
        (
            "Доля ошибок",
            f"< {_ERR_RATE * 100:g}%",
            _fmt_pct(error_rate(summaries["baseline"])),
            _fmt_pct(error_rate(summaries["direct"])),
            _fmt_pct(error_rate(summaries["gateway"])),
        ),
    ]

    lines = [
        "# K1 / K2 report",
        "",
        "| Показатель | Цель | Baseline | Online напрямую | Online через gateway |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")

    used_bytes = int(redis_memory.get("used_memory", 0))
    used_gb = used_bytes / (1024**3)
    backfill = config.get("backfill", {})
    depth = backfill.get("depth", "?") if isinstance(backfill, Mapping) else "?"
    achieved = backfill.get("achieved_tps") if isinstance(backfill, Mapping) else None
    achieved_s = f"{float(achieved):.1f} posts/s" if achieved is not None else "n/a"

    status = prometheus.get("status_shares", {})
    if not isinstance(status, Mapping):
        status = {}

    lines.extend(
        [
            "",
            "## Notes",
            "",
            f"- Redis `used_memory` = **{used_gb:.3f} GB** "
            f"(`{redis_memory.get('used_memory_human', '?')}`), "
            f"DBSIZE = {redis_memory.get('dbsize', '?')}. "
            f"ADR-002 day-scale estimate is ~{_ADR_REDIS_GB} GB; "
            f"this run used warm-up depth **{depth}**, so memory is not "
            "comparable to the full-day figure and 24h windows are only "
            "partially filled.",
            f"- Warm-up achieved ingest rate: **{achieved_s}** "
            f"(TPS={_cfg(backfill, 'tps')}, send-rate={_cfg(backfill, 'send_rate')}).",
            f"- Feature value statuses during runs: "
            f"fresh={_share(status, 'fresh')}, "
            f"stale={_share(status, 'stale')}, "
            f"default={_share(status, 'default')}.",
            f"- Load generator placement: {config.get('load_generator', 'unknown')}.",
            f"- Measurement limits: {config.get('measurement_limits', 'n/a')}.",
        ]
    )

    misses = _threshold_misses(summaries, windows)
    if misses:
        lines.extend(["", "## Causes", "", "Threshold misses:"])
        lines.extend(f"- {item}" for item in misses)
        lines.append("- Stage 4 must not start until these are understood and accepted.")

    lines.append("")
    return "\n".join(lines)


def _duration_percentile_ms(summary: Mapping[str, Any], key: str) -> float:
    """Return a duration percentile in milliseconds.

    k6 ``--summary-export`` (v0.54+) stores trend stats in milliseconds on the
    metric object. Older nested ``values`` maps used seconds.
    """
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        msg = "k6 summary missing metrics"
        raise ReportError(msg)
    duration = metrics.get("http_req_duration")
    if not isinstance(duration, Mapping):
        msg = "k6 summary missing http_req_duration"
        raise ReportError(msg)
    values = duration.get("values")
    if isinstance(values, Mapping) and key in values:
        return float(values[key]) * 1000.0
    if key in duration:
        return float(duration[key])
    msg = f"k6 summary missing http_req_duration.{key}"
    raise ReportError(msg)


def _k2(windows: Mapping[str, Any], target: str) -> float:
    window = windows.get(target)
    if not isinstance(window, Mapping):
        msg = f"prometheus windows missing target {target}"
        raise ReportError(msg)
    if target == "baseline":
        key = "baseline_event_to_store_p95_seconds"
    else:
        key = "features_event_to_online_p95_seconds"
    if key not in window:
        msg = f"prometheus window {target} missing {key}"
        raise ReportError(msg)
    raw = window[key]
    if raw is None:
        msg = f"prometheus window {target} has null {key} (no histogram samples in range)"
        raise ReportError(msg)
    return float(raw)


def _threshold_misses(
    summaries: Mapping[str, Mapping[str, Any]],
    windows: Mapping[str, Any],
) -> list[str]:
    misses: list[str] = []
    for name in ("direct", "gateway"):
        p99 = k1_p99_ms(summaries[name])
        p50 = k1_p50_ms(summaries[name])
        if p99 > _K1_P99_MS:
            misses.append(f"{name} K1 p99 = {p99:.2f} ms > {_K1_P99_MS:g} ms")
        if p50 > _K1_P50_MS:
            misses.append(f"{name} K1 p50 = {p50:.2f} ms > {_K1_P50_MS:g} ms")
        err = error_rate(summaries[name])
        if err >= _ERR_RATE:
            misses.append(f"{name} error rate = {err:.4%} ≥ {_ERR_RATE:.1%}")
    for name in _TARGETS:
        k2 = _k2(windows, name)
        if k2 > _K2_P95_S:
            misses.append(f"{name} K2 p95 = {k2:.3f} s > {_K2_P95_S:g} s")
    return misses


def _fmt_ms(value: float) -> str:
    return f"{value:.2f} мс"


def _fmt_s(value: float) -> str:
    return f"{value:.3f} с"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.3f}%"


def _share(status: Mapping[str, Any], name: str) -> str:
    raw = status.get(name)
    if raw is None:
        return "n/a"
    return f"{float(raw) * 100:.2f}%"


def _cfg(backfill: object, key: str) -> object:
    if isinstance(backfill, Mapping):
        return backfill.get(key, "?")
    return "?"
