import json
from pathlib import Path

import pytest

from report import ReportError, build_report, require_targets

_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def test_build_report_table_from_fixtures() -> None:
    report = build_report(
        k6_by_target={
            "direct": _load("k6-direct.json"),
            "gateway": _load("k6-gateway.json"),
            "baseline": _load("k6-baseline.json"),
        },
        prometheus=_load("prometheus.json"),
        redis_memory=_load("redis-memory.json"),
        config=_load("config.json"),
    )
    assert "| Показатель | Цель | Baseline | Online напрямую | Online через gateway |" in report
    assert "| K1: p99 /v1/features, 200 RPS | ≤ 20 мс | 200.00 мс | 8.00 мс | 12.00 мс |" in report
    assert "| K1: p50 | ≤ 5 мс | 50.00 мс | 2.00 мс | 3.00 мс |" in report
    assert "| K2: p95 событие → хранилище | ≤ 2 с | 0.450 с | 0.120 с | 0.150 с |" in report
    assert "| Доля ошибок | < 0.1% | 0.000% | 0.010% | 0.020% |" in report
    assert "50.00M" in report
    assert "128.5 posts/s" in report
    assert "colocated" in report


def test_missing_target_is_explicit_error() -> None:
    with pytest.raises(ReportError, match="missing k6 run for target gateway"):
        require_targets(
            {
                "direct": _load("k6-direct.json"),
                "gateway": None,
                "baseline": _load("k6-baseline.json"),
            }
        )


def test_build_report_rejects_missing_run() -> None:
    with pytest.raises(ReportError, match="missing k6 run for target baseline"):
        build_report(
            k6_by_target={
                "direct": _load("k6-direct.json"),
                "gateway": _load("k6-gateway.json"),
                "baseline": None,
            },
            prometheus=_load("prometheus.json"),
            redis_memory=_load("redis-memory.json"),
            config=_load("config.json"),
        )
