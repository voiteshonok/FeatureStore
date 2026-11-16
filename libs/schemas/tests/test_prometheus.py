"""Compose metrics ports match the Prometheus scrape targets."""

from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[3]
_COMPOSE = _ROOT / "infra" / "docker-compose.yml"
_PROMETHEUS = _ROOT / "infra" / "prometheus" / "prometheus.yml"


def _metrics_port(environment: object) -> str | None:
    if isinstance(environment, dict):
        value = environment.get("METRICS_PORT")
        if value is None:
            return None
        return str(value)
    if isinstance(environment, list):
        for item in environment:
            if isinstance(item, str) and item.startswith("METRICS_PORT="):
                return item.split("=", 1)[1]
    return None


def test_metrics_ports_are_prometheus_targets() -> None:
    compose = yaml.safe_load(_COMPOSE.read_text(encoding="utf-8"))
    prometheus = yaml.safe_load(_PROMETHEUS.read_text(encoding="utf-8"))
    expected: dict[str, str] = {}
    for name, spec in compose["services"].items():
        if not isinstance(spec, dict):
            continue
        port = _metrics_port(spec.get("environment"))
        if port is not None:
            expected[name] = port

    jobs: dict[str, list[str]] = {}
    for job in prometheus["scrape_configs"]:
        targets: list[str] = []
        for group in job["static_configs"]:
            targets.extend(str(target) for target in group["targets"])
        jobs[str(job["job_name"])] = targets

    assert set(jobs) == set(expected)
    found = {target for targets in jobs.values() for target in targets}
    assert found == {f"{name}:{port}" for name, port in expected.items()}
    for name, port in expected.items():
        assert jobs[name] == [f"{name}:{port}"]
