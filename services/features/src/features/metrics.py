"""Prometheus series for the feature API and the online writer.

The two processes do not share a registry. Each scrape shows only the series
that process records (ADR-014).
"""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

# 5 ms and 20 ms are the K1 bounds (ADR-014).
API_LATENCY_BUCKETS_S = (
    0.0005,
    0.001,
    0.002,
    0.003,
    0.005,
    0.0075,
    0.01,
    0.015,
    0.02,
    0.03,
    0.05,
    0.1,
    0.25,
)

# 2 s is the K2 bound (ADR-014).
FRESHNESS_BUCKETS_S = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)

_started_ports: set[int] = set()


class ApiMetrics:
    """Feature-read series."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.request_duration = Histogram(
            "features_request_duration_seconds",
            "Feature read latency.",
            labelnames=("outcome",),
            buckets=API_LATENCY_BUCKETS_S,
            registry=registry,
        )
        self.values = Counter(
            "features_values_total",
            "Feature values returned.",
            labelnames=("feature", "status"),
            registry=registry,
        )


class WorkerMetrics:
    """Online-writer series."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.event_to_online = Histogram(
            "features_event_to_online_seconds",
            "Time from ingest to a successful online apply.",
            buckets=FRESHNESS_BUCKETS_S,
            registry=registry,
        )
        self.events = Counter(
            "features_events_total",
            "Records handled by the online writer.",
            labelnames=("outcome",),
            registry=registry,
        )
        self.consumer_lag = Gauge(
            "features_consumer_lag",
            "Records between the log end and the consumer position.",
            labelnames=("partition",),
            registry=registry,
        )
        self.apply_retries = Counter(
            "features_apply_retries_total",
            "Apply attempts retried after a retryable failure.",
            registry=registry,
        )


def api_metrics(registry: CollectorRegistry | None = None) -> ApiMetrics:
    """Build API series on ``registry``, or on a fresh one."""
    return ApiMetrics(CollectorRegistry() if registry is None else registry)


def worker_metrics(registry: CollectorRegistry | None = None) -> WorkerMetrics:
    """Build writer series on ``registry``, or on a fresh one."""
    return WorkerMetrics(CollectorRegistry() if registry is None else registry)


API_METRICS = api_metrics()
WORKER_METRICS = worker_metrics()


def start_metrics_server(port: int, registry: CollectorRegistry) -> None:
    """Serve ``registry``. ``0`` leaves the server off. A second start is a no-op."""
    if port <= 0 or port in _started_ports:
        return
    start_http_server(port, registry=registry)
    _started_ports.add(port)
