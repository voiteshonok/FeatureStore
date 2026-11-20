"""Query Prometheus for K2 and consumer lag during the experiment."""

from __future__ import annotations

import time
from typing import Any

import httpx

# Last finite freshness bucket (features/baseline FRESHNESS_BUCKETS_S).
K2_FINITE_CEILING_S = 300.0


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
        features, baseline = current_lag(prom_url)
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


def current_lag(prom_url: str) -> tuple[float | None, float | None]:
    """Return ``(features_lag, baseline_lag)`` sums, or None when missing."""
    return (
        query(prom_url, "sum(features_consumer_lag)"),
        query(prom_url, "sum(baseline_consumer_lag)"),
    )


def window_metrics(
    prom_url: str,
    *,
    end_s: float,
    range_s: str,
) -> dict[str, Any]:
    """K2 p95 and status increases over ``range_s`` ending at ``end_s`` only.

    No cumulative / longer-lookback fallback: missing in-window samples stay
    null so the report can mark the pack INVALID.
    """
    online, online_inf = _k2_window(
        prom_url,
        "features_event_to_online_seconds",
        range_s=range_s,
        time_s=end_s,
    )
    baseline, baseline_inf = _k2_window(
        prom_url,
        "baseline_event_to_store_seconds",
        range_s=range_s,
        time_s=end_s,
    )
    status_expr = f"sum by (status) (increase(features_values_total[{range_s}]))"
    status_counts = query_vector(prom_url, status_expr, time_s=end_s)
    total = sum(status_counts.values()) or 0.0
    shares = {name: count / total for name, count in status_counts.items()} if total else {}
    return {
        "features_event_to_online_p95_seconds": online,
        "features_event_to_online_p95_at_inf": online_inf,
        "baseline_event_to_store_p95_seconds": baseline,
        "baseline_event_to_store_p95_at_inf": baseline_inf,
        "status_counts": status_counts,
        "status_shares": shares,
    }


def _k2_window(
    prom_url: str,
    metric_prefix: str,
    *,
    range_s: str,
    time_s: float,
) -> tuple[float | None, bool]:
    """Return (p95_seconds, at_inf) for one histogram over the k6 window only."""
    bucket = f"{metric_prefix}_bucket"
    count_metric = f"{metric_prefix}_count"
    count = query(
        prom_url,
        f"sum(increase({count_metric}[{range_s}]))",
        time_s=time_s,
    )
    if count is None or count <= 0:
        return None, False

    ceiling = f"{K2_FINITE_CEILING_S:g}"
    inf_increase = query(
        prom_url,
        f'sum(increase({bucket}{{le="+Inf"}}[{range_s}]))',
        time_s=time_s,
    )
    finite_increase = query(
        prom_url,
        f'sum(increase({bucket}{{le="{ceiling}"}}[{range_s}]))',
        time_s=time_s,
    )
    beyond = 0.0
    if inf_increase is not None and finite_increase is not None:
        beyond = max(0.0, inf_increase - finite_increase)

    quantile = query(
        prom_url,
        f"histogram_quantile(0.95, sum by (le) (increase({bucket}[{range_s}])))",
        time_s=time_s,
    )
    # Prometheus returns +Inf (→ None) when the quantile lands past the last
    # finite bucket; treat that as the ceiling with at_inf=True.
    if quantile is None:
        if beyond > 0 or (inf_increase is not None and inf_increase > 0):
            return K2_FINITE_CEILING_S, True
        return None, False
    if quantile >= K2_FINITE_CEILING_S - 1e-9:
        return K2_FINITE_CEILING_S, True
    # p95 still finite but ≥5% of observations sit in +Inf → report ceiling.
    if inf_increase and beyond / inf_increase >= 0.05 and quantile >= K2_FINITE_CEILING_S * 0.5:
        return K2_FINITE_CEILING_S, True
    return quantile, False


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
