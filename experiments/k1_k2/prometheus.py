"""Query Prometheus for K2 and consumer lag during the experiment."""

from __future__ import annotations

import time
from typing import Any

import httpx


class PrometheusError(RuntimeError):
    """Prometheus query or lag wait failed."""


def query(prom_url: str, expr: str, *, time_s: float | None = None) -> float | None:
    """Instant query; returns the first result value or None."""
    results = _query_result(prom_url, expr, time_s=time_s)
    if not results:
        return None
    value = results[0].get("value")
    if not value or len(value) < 2:
        return None
    raw = value[1]
    if raw in {"NaN", "+Inf", "-Inf"}:
        return None
    return float(raw)


def query_vector(prom_url: str, expr: str, *, time_s: float | None = None) -> dict[str, float]:
    """Instant query returning a map of label `status` → value."""
    out: dict[str, float] = {}
    for item in _query_result(prom_url, expr, time_s=time_s):
        metric = item.get("metric", {})
        status = metric.get("status")
        value = item.get("value")
        if status is None or not value or len(value) < 2:
            continue
        raw = value[1]
        if raw in {"NaN", "+Inf", "-Inf"}:
            continue
        out[str(status)] = float(raw)
    return out


def wait_for_zero_lag(
    prom_url: str,
    *,
    timeout_s: float = 3600.0,
    poll_s: float = 5.0,
) -> None:
    """Block until features and baseline consumer lag gauges sum to zero."""
    deadline = time.monotonic() + timeout_s
    last_print = 0.0
    while True:
        features = query(prom_url, "sum(features_consumer_lag)")
        baseline = query(prom_url, "sum(baseline_consumer_lag)")
        feat_ok = features is not None and features <= 0
        base_ok = baseline is not None and baseline <= 0
        if feat_ok and base_ok:
            return
        now = time.monotonic()
        if now - last_print >= 30.0:
            print(
                f"waiting for lag=0 features={features!r} baseline={baseline!r}",
                flush=True,
            )
            last_print = now
        if now >= deadline:
            msg = (
                "consumer lag did not reach zero: "
                f"features={features!r} baseline={baseline!r} "
                "(is features-worker :9000 scraped? rebuild images if up=0)"
            )
            raise PrometheusError(msg)
        time.sleep(poll_s)


def window_metrics(
    prom_url: str,
    *,
    end_s: float,
    range_s: str,
) -> dict[str, Any]:
    """K2 p95 and status increases over ``range_s`` ending at ``end_s``."""
    # Prefer increase() over the k6 window; fall back to a longer lookback, then
    # to the cumulative histogram if the worker produced no samples in-window
    # (common when colocated k6 saturates Redis/CPU and the writer stalls).
    online = _k2_quantile(
        prom_url,
        "features_event_to_online_seconds_bucket",
        range_s=range_s,
        time_s=end_s,
    )
    baseline = _k2_quantile(
        prom_url,
        "baseline_event_to_store_seconds_bucket",
        range_s=range_s,
        time_s=end_s,
    )
    status_expr = f"sum by (status) (increase(features_values_total[{range_s}]))"
    status_counts = query_vector(prom_url, status_expr, time_s=end_s)
    total = sum(status_counts.values()) or 0.0
    shares = {name: count / total for name, count in status_counts.items()} if total else {}
    return {
        "features_event_to_online_p95_seconds": online,
        "baseline_event_to_store_p95_seconds": baseline,
        "status_counts": status_counts,
        "status_shares": shares,
    }


def _k2_quantile(
    prom_url: str,
    bucket_metric: str,
    *,
    range_s: str,
    time_s: float,
) -> float | None:
    candidates = [
        f"histogram_quantile(0.95, sum by (le) (increase({bucket_metric}[{range_s}])))",
        f"histogram_quantile(0.95, sum by (le) (increase({bucket_metric}[15m])))",
        f"histogram_quantile(0.95, sum by (le) ({bucket_metric}))",
    ]
    for expr in candidates:
        value = query(prom_url, expr, time_s=time_s)
        if value is not None:
            return value
    return None


def _query_result(
    prom_url: str,
    expr: str,
    *,
    time_s: float | None,
) -> list[dict[str, Any]]:
    params: dict[str, str | float] = {"query": expr}
    if time_s is not None:
        params["time"] = time_s
    response = httpx.get(f"{prom_url.rstrip('/')}/api/v1/query", params=params, timeout=30.0)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "success":
        msg = f"prometheus query failed: {payload}"
        raise PrometheusError(msg)
    results = payload.get("data", {}).get("result", [])
    if not isinstance(results, list):
        return []
    return results
