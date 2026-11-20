"""Build report.md for a K1/K2 experiment pack."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from prometheus import K2_FINITE_CEILING_S


class ReportError(ValueError):
    """Raised when a required run artifact is missing or malformed."""


_TARGETS = ("baseline", "direct", "gateway")
_K1_TARGETS = ("direct", "gateway")
_ADR_REDIS_GB = 1.5
_K1_P99_MS = 20.0
_K1_P50_MS = 5.0
_K2_P95_S = 2.0
_ERR_RATE_GOAL = 0.001
# Pack validity: any K1 target above this → INVALID (stricter goal stays 0.1%).
_ERR_RATE_INVALID = 0.01
_INVALID_CELL = "— (INVALID)"
_ERROR_CLASS_KEYS = (
    "2xx_check_fail",
    "4xx",
    "429",
    "503",
    "timeout",
    "other",
)


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


def error_classes(summary: Mapping[str, Any]) -> dict[str, int]:
    """Return per-class error counts from k6 custom counters."""
    metrics = summary.get("metrics")
    if not isinstance(metrics, Mapping):
        return {key: 0 for key in _ERROR_CLASS_KEYS}
    out: dict[str, int] = {}
    for key in _ERROR_CLASS_KEYS:
        metric = metrics.get(f"err_{key}")
        out[key] = _counter_count(metric)
    return out


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


def assess_validity(
    *,
    k6_by_target: Mapping[str, Mapping[str, Any]],
    prometheus: Mapping[str, Any],
    lag: Mapping[str, Any] | None = None,
) -> tuple[str, list[str]]:
    """Return ``(VALID|INVALID, reasons)``.

    Gates: non-zero consumer lag at measurement, K1-target error rate > 1%,
    or missing in-window K2 for any target.
    """
    reasons: list[str] = []
    lag_payload = lag if isinstance(lag, Mapping) else prometheus.get("lag", {})
    if isinstance(lag_payload, Mapping):
        for name in ("features", "baseline"):
            raw = lag_payload.get(name)
            if raw is None:
                continue
            if float(raw) != 0.0:
                reasons.append(f"consumer lag {name}={raw} ≠ 0")
    for name in _K1_TARGETS:
        summary = k6_by_target.get(name)
        if summary is None:
            continue
        rate = error_rate(summary)
        if rate > _ERR_RATE_INVALID:
            reasons.append(f"{name} error rate {rate:.4%} > {_ERR_RATE_INVALID:.0%} INVALID gate")
    windows = prometheus.get("windows")
    if isinstance(windows, Mapping):
        for name in _TARGETS:
            try:
                _k2_value(windows, name)
            except ReportError as exc:
                reasons.append(str(exc))
    validity = "INVALID" if reasons else "VALID"
    return validity, reasons


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

    validity, validity_reasons = assess_validity(
        k6_by_target=summaries,
        prometheus=prometheus,
        lag=config.get("lag") if isinstance(config.get("lag"), Mapping) else None,
    )
    invalid = validity == "INVALID"

    def cell_ms(target: str, fn: Any) -> str:
        if invalid:
            return _INVALID_CELL
        return _fmt_ms(fn(summaries[target]))

    def cell_pct(target: str) -> str:
        if invalid:
            return _INVALID_CELL
        return _fmt_pct(error_rate(summaries[target]))

    def cell_k2(target: str) -> str:
        if invalid:
            return _INVALID_CELL
        value, at_inf = _k2_value(windows, target)
        return _fmt_s(value, at_inf=at_inf)

    rows = [
        (
            "K1: p99 /v1/features, 200 RPS",
            f"≤ {_K1_P99_MS:g} мс",
            cell_ms("baseline", k1_p99_ms),
            cell_ms("direct", k1_p99_ms),
            cell_ms("gateway", k1_p99_ms),
        ),
        (
            "K1: p50",
            f"≤ {_K1_P50_MS:g} мс",
            cell_ms("baseline", k1_p50_ms),
            cell_ms("direct", k1_p50_ms),
            cell_ms("gateway", k1_p50_ms),
        ),
        (
            "K2: p95 событие → хранилище",
            f"≤ {_K2_P95_S:g} с",
            cell_k2("baseline"),
            cell_k2("direct"),
            cell_k2("gateway"),
        ),
        (
            "Доля ошибок",
            f"< {_ERR_RATE_GOAL * 100:g}%",
            cell_pct("baseline"),
            cell_pct("direct"),
            cell_pct("gateway"),
        ),
    ]

    title = "# K1 / K2 report" + (" — INVALID" if invalid else "")
    lines = [
        title,
        "",
    ]
    if invalid:
        lines.extend(
            [
                f"**validity: {validity}**",
                "",
                "Goal table cells are omitted from pass/fail interpretation.",
                "",
            ]
        )
        if validity_reasons:
            lines.append("Validity gates failed:")
            lines.extend(f"- {reason}" for reason in validity_reasons)
            lines.append("")

    lines.extend(
        [
            "| Показатель | Цель | Baseline | Online напрямую | Online через gateway |",
            "|---|---|---|---|---|",
        ]
    )
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
            "## Error classes (k6)",
            "",
            "| Target | 2xx_check_fail | 4xx | 429 | 503 | timeout | other |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for name in _TARGETS:
        classes = error_classes(summaries[name])
        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    *(str(classes[key]) for key in _ERROR_CLASS_KEYS),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            f"- validity: **{validity}**.",
            f"- K2 finite histogram ceiling: **{K2_FINITE_CEILING_S:g} s**; "
            f"mass beyond that is reported as `> {K2_FINITE_CEILING_S:g} с`, "
            "never as a fake finite p95. K2 uses only the k6 window "
            "(`increase(...[DURATION])`); no cumulative fallback.",
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
            f"- GATEWAY_RATE_LIMIT: {config.get('GATEWAY_RATE_LIMIT', 'n/a')}.",
            f"- Load generator placement: {config.get('load_generator', 'unknown')}.",
            f"- Measurement limits: {config.get('measurement_limits', 'n/a')}.",
        ]
    )

    if not invalid:
        misses = _threshold_misses(summaries, windows)
        if misses:
            lines.extend(["", "## Causes", "", "Threshold misses:"])
            lines.extend(f"- {item}" for item in misses)
            lines.append("- Stage 4 must not start until these are understood and accepted.")
    else:
        lines.extend(["", "## Causes", ""])
        lines.extend(f"- {reason}" for reason in validity_reasons)
        lines.append("- Pack is INVALID: do not compare these cells to K1/K2 diploma goals.")

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


def _k2_value(windows: Mapping[str, Any], target: str) -> tuple[float, bool]:
    """Return ``(seconds, at_inf)`` for the target's in-window K2, or raise."""
    window = windows.get(target)
    if not isinstance(window, Mapping):
        msg = f"prometheus windows missing target {target}"
        raise ReportError(msg)
    if target == "baseline":
        key = "baseline_event_to_store_p95_seconds"
        inf_key = "baseline_event_to_store_p95_at_inf"
    else:
        key = "features_event_to_online_p95_seconds"
        inf_key = "features_event_to_online_p95_at_inf"
    if key not in window:
        msg = f"prometheus window {target} missing {key}"
        raise ReportError(msg)
    raw = window[key]
    if raw is None:
        msg = f"prometheus window {target} has null {key} (no in-window K2 samples; INVALID)"
        raise ReportError(msg)
    at_inf = bool(window.get(inf_key, False))
    return float(raw), at_inf


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
        if err >= _ERR_RATE_GOAL:
            misses.append(f"{name} error rate = {err:.4%} ≥ {_ERR_RATE_GOAL:.1%}")
    for name in _TARGETS:
        value, at_inf = _k2_value(windows, name)
        if at_inf or value > _K2_P95_S:
            shown = _fmt_s(value, at_inf=at_inf)
            misses.append(f"{name} K2 p95 = {shown} > {_K2_P95_S:g} s")
    return misses


def _counter_count(metric: object) -> int:
    if not isinstance(metric, Mapping):
        return 0
    values = metric.get("values")
    if isinstance(values, Mapping) and "count" in values:
        return int(float(values["count"]))
    if "count" in metric:
        return int(float(metric["count"]))
    if "value" in metric:
        return int(float(metric["value"]))
    return 0


def _fmt_ms(value: float) -> str:
    return f"{value:.2f} мс"


def _fmt_s(value: float, *, at_inf: bool = False) -> str:
    if at_inf or value >= K2_FINITE_CEILING_S - 1e-9:
        return f"> {K2_FINITE_CEILING_S:g} с"
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
