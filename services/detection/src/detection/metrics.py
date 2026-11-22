"""Prometheus series for snapshot evaluation (ADR-014)."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

# 2 s is the K2 bound (ADR-014).
FRESHNESS_BUCKETS_S = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)

_started_ports: set[int] = set()


class DetectionMetrics:
    """Snapshot and alert series."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self.snapshots = Counter(
            "detection_snapshots_total",
            "Snapshots handled by the detection consumer.",
            labelnames=("outcome",),
            registry=registry,
        )
        self.alerts = Counter(
            "detection_alerts_fired_total",
            "Alerts inserted.",
            labelnames=("rule",),
            registry=registry,
        )
        self.event_to_alert = Histogram(
            "detection_event_to_alert_seconds",
            "Time from ingest to an inserted alert.",
            buckets=FRESHNESS_BUCKETS_S,
            registry=registry,
        )
        self.stream_subscribers = Gauge(
            "detection_stream_subscribers",
            "Open SSE subscribers on this detection process.",
            registry=registry,
        )
        self.stream_dropped = Counter(
            "detection_stream_dropped_total",
            "Subscribers closed because their queue overflowed.",
            registry=registry,
        )
        self.stream_delivery = Histogram(
            "detection_stream_delivery_seconds",
            "Time from alert notify handling to fan-out.",
            buckets=FRESHNESS_BUCKETS_S,
            registry=registry,
        )


def detection_metrics(registry: CollectorRegistry | None = None) -> DetectionMetrics:
    """Build series on ``registry``, or on a fresh one."""
    return DetectionMetrics(CollectorRegistry() if registry is None else registry)


METRICS = detection_metrics()


def start_metrics_server(port: int, registry: CollectorRegistry) -> None:
    """Serve ``registry``. ``0`` leaves the server off. A second start is a no-op."""
    if port <= 0 or port in _started_ports:
        return
    start_http_server(port, registry=registry)
    _started_ports.add(port)
