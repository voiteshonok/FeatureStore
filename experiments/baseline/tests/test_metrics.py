from prometheus_client import CollectorRegistry

from baseline.metrics import (
    API_LATENCY_BUCKETS_S,
    FRESHNESS_BUCKETS_S,
    _started_ports,
    start_metrics_server,
)
from features.metrics import API_LATENCY_BUCKETS_S as FEATURES_LATENCY_BUCKETS_S
from features.metrics import FRESHNESS_BUCKETS_S as FEATURES_FRESHNESS_BUCKETS_S


def test_buckets_match_the_feature_api() -> None:
    assert API_LATENCY_BUCKETS_S == FEATURES_LATENCY_BUCKETS_S
    assert FRESHNESS_BUCKETS_S == FEATURES_FRESHNESS_BUCKETS_S


def test_zero_metrics_port_does_not_listen() -> None:
    start_metrics_server(0, CollectorRegistry())
    assert 0 not in _started_ports
