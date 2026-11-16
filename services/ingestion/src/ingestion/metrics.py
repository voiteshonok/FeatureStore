"""Prometheus series for event acceptance (ADR-014)."""

from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

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

_started_ports: set[int] = set()


class IngestionMetrics:
    """Event-accept series."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.request_duration = Histogram(
            "ingestion_request_duration_seconds",
            "Event accept latency.",
            labelnames=("status",),
            buckets=API_LATENCY_BUCKETS_S,
            registry=registry,
        )
        self.events = Counter(
            "ingestion_events_total",
            "Event accept results.",
            labelnames=("result",),
            registry=registry,
        )


def ingestion_metrics(registry: CollectorRegistry | None = None) -> IngestionMetrics:
    """Build series on ``registry``, or on a fresh one."""
    return IngestionMetrics(CollectorRegistry() if registry is None else registry)


METRICS = ingestion_metrics()


def start_metrics_server(port: int, registry: CollectorRegistry) -> None:
    """Serve ``registry``. ``0`` leaves the server off. A second start is a no-op."""
    if port <= 0 or port in _started_ports:
        return
    start_http_server(port, registry=registry)
    _started_ports.add(port)
