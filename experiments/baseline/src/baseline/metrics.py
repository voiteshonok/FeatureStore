"""Prometheus series for the SQL stand.

The API process and the writer process do not share a registry. Each scrape
shows only the series that process records (ADR-014, ADR-015).
"""

from prometheus_client import CollectorRegistry, Gauge, Histogram, start_http_server

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

# 2 s is the K2 bound (ADR-014). Finite ceiling 300 s; beyond that is +Inf.
FRESHNESS_BUCKETS_S = (
    0.01,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

_started_ports: set[int] = set()


class ApiMetrics:
    """Baseline read latency."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.request_duration = Histogram(
            "baseline_request_duration_seconds",
            "SQL baseline feature read latency.",
            labelnames=("outcome",),
            buckets=API_LATENCY_BUCKETS_S,
            registry=registry,
        )


class WriterMetrics:
    """Baseline writer freshness and lag."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.event_to_store = Histogram(
            "baseline_event_to_store_seconds",
            "Time from ingest to the commit of the batch that stored the event.",
            buckets=FRESHNESS_BUCKETS_S,
            registry=registry,
        )
        self.consumer_lag = Gauge(
            "baseline_consumer_lag",
            "Records between the log end and the consumer position.",
            labelnames=("partition",),
            registry=registry,
        )


def api_metrics(registry: CollectorRegistry | None = None) -> ApiMetrics:
    """Build API series on ``registry``, or on a fresh one."""
    return ApiMetrics(CollectorRegistry() if registry is None else registry)


def writer_metrics(registry: CollectorRegistry | None = None) -> WriterMetrics:
    """Build writer series on ``registry``, or on a fresh one."""
    return WriterMetrics(CollectorRegistry() if registry is None else registry)


API_METRICS = api_metrics()
WRITER_METRICS = writer_metrics()


def start_metrics_server(port: int, registry: CollectorRegistry) -> None:
    """Serve ``registry``. ``0`` leaves the server off. A second start is a no-op."""
    if port <= 0 or port in _started_ports:
        return
    start_http_server(port, registry=registry)
    _started_ports.add(port)
