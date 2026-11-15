"""Rule comparison, multi-feature matches, and on-demand skips."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

import pytest

from detection.rules import (
    Clause,
    Comparison,
    Rule,
    apply_snapshot,
    entity_ids,
    holds,
    load_rules,
    matched_values,
    rules_from_rows,
)
from schemas import FeatureSnapshot

_BURST = UUID("c0ffee00-0000-4000-8000-000000000001")
_TRAVEL = UUID("c0ffee00-0000-4000-8000-000000000002")
_MIXED = UUID("c0ffee00-0000-4000-8000-000000000003")
_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)

_MATRIX: list[tuple[Comparison, float, bool]] = [
    ("gt", 4.0, False),
    ("gt", 5.0, False),
    ("gt", 6.0, True),
    ("ge", 4.0, False),
    ("ge", 5.0, True),
    ("ge", 6.0, True),
    ("lt", 4.0, True),
    ("lt", 5.0, False),
    ("lt", 6.0, False),
    ("le", 4.0, True),
    ("le", 5.0, True),
    ("le", 6.0, False),
]


def _rule(*clauses: Clause, entity: str = "card", name: str = "card_velocity_burst") -> Rule:
    return Rule(
        rule_id=_BURST,
        name=name,
        entity=entity,  # type: ignore[arg-type]
        clauses=clauses,
    )


def _snapshot(**features: int) -> FeatureSnapshot:
    return FeatureSnapshot(
        entity_type="card",
        entity_id="card-1",
        event_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        event_time=_NOW,
        ingest_time=_NOW,
        features=dict(features),
        source_partition=0,
        source_offset=0,
    )


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.sql = ""

    async def execute(self, sql: str, _params: object = None) -> None:
        self.sql = sql

    async def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    async def fetchone(self) -> tuple[object, ...] | None:
        if not self.rows:
            return None
        return self.rows[0]

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Conn:
    def __init__(self, rows: list[tuple[object, ...]] | None = None) -> None:
        self.rows = rows or []
        self.sql: list[str] = []
        self.cursor_obj = _Cursor(self.rows)

    def cursor(self) -> _Cursor:
        return self.cursor_obj

    def transaction(self) -> _Conn:
        return self

    async def execute(self, sql: str, _params: object = None) -> _Cursor:
        self.sql.append(sql)
        return _Cursor(self.rows)

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def connection(self) -> _Conn:
        return self.conn


@pytest.mark.parametrize(("comparison", "value", "expected"), _MATRIX)
def test_comparison_boundary(comparison: Comparison, value: float, expected: bool) -> None:
    assert holds(value, comparison, 5.0) is expected


def test_multi_feature_rule_needs_every_comparison() -> None:
    rule = _rule(
        Clause("card_cnt_1m", "gt", 5),
        Clause("card_sum_1m", "gt", 100),
    )
    assert matched_values(rule, {"card_cnt_1m": 6, "card_sum_1m": 50}) is None
    assert matched_values(rule, {"card_cnt_1m": 6, "card_sum_1m": 150}) == {
        "card_cnt_1m": 6,
        "card_sum_1m": 150,
    }


def test_missing_feature_skips_the_rule() -> None:
    rule = _rule(Clause("card_cnt_1m", "gt", 5), Clause("card_sum_1m", "gt", 100))
    assert matched_values(rule, {"card_cnt_1m": 6}) is None


def test_equal_boundary_does_not_fire_gt() -> None:
    rule = _rule(Clause("card_cnt_1m", "gt", 5))
    assert matched_values(rule, {"card_cnt_1m": 5}) is None
    assert matched_values(rule, {"card_cnt_1m": 6}) == {"card_cnt_1m": 6}


def test_on_demand_rule_is_skipped_with_one_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="detection")
    rows = [
        (_TRAVEL, "impossible_travel", "card", "geo_velocity_kmh", "gt", 900, "on_demand"),
        (_MIXED, "mixed", "card", "card_cnt_1m", "gt", 5, "stream"),
        (_MIXED, "mixed", "card", "geo_velocity_kmh", "gt", 900, "on_demand"),
        (_BURST, "card_velocity_burst", "card", "card_cnt_1m", "gt", 5, "stream"),
    ]
    rules = rules_from_rows(rows)
    assert [rule.name for rule in rules] == ["card_velocity_burst"]
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "skipped on-demand rules: impossible_travel, mixed"


async def test_load_rules_uses_the_catalog_join(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="detection")
    conn = _Conn(
        [
            (_TRAVEL, "impossible_travel", "card", "geo_velocity_kmh", "gt", 900, "on_demand"),
        ]
    )
    rules = await load_rules(_Pool(conn))  # type: ignore[arg-type]
    assert rules == ()
    assert "features.feature_definitions" in conn.cursor_obj.sql
    assert len(caplog.records) == 1


def test_card_and_merchant_ids() -> None:
    card = _snapshot(card_cnt_1m=6)
    assert entity_ids(card) == (None, "card-1")
    merchant = FeatureSnapshot(
        entity_type="merchant",
        entity_id="m-1",
        event_id=card.event_id,
        event_time=_NOW,
        ingest_time=_NOW,
        features={"merchant_cnt_24h": 1},
        source_partition=0,
        source_offset=1,
    )
    assert entity_ids(merchant) == ("m-1", None)


async def test_below_threshold_does_not_insert() -> None:
    conn = _Conn([(UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),)])
    rule = _rule(Clause("card_cnt_1m", "gt", 5))
    fired = await apply_snapshot(conn, (rule,), _snapshot(card_cnt_1m=5))  # type: ignore[arg-type]
    assert fired == ()
    assert conn.sql == []


async def test_card_insert_is_idempotent_and_keeps_referenced_values() -> None:
    alert_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    conn = _Conn([(alert_id,)])
    rule = _rule(Clause("card_cnt_1m", "gt", 5))
    fired = await apply_snapshot(
        conn,  # type: ignore[arg-type]
        (rule,),
        _snapshot(card_cnt_1m=6, card_cnt_1h=3),
    )
    assert fired == (alert_id,)
    assert len(conn.sql) == 1
    assert "WHERE NOT EXISTS" in conn.sql[0]
    assert "ref.merchants" not in conn.sql[0]


async def test_merchant_alert_inserts_the_merchant_id() -> None:
    alert_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    conn = _Conn([(alert_id,)])
    rule = _rule(Clause("merchant_cnt_24h", "gt", 10), entity="merchant", name="merchant_burst")
    snapshot = FeatureSnapshot(
        entity_type="merchant",
        entity_id="m-9",
        event_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        event_time=_NOW,
        ingest_time=_NOW,
        features={"merchant_cnt_24h": 11},
        source_partition=1,
        source_offset=2,
    )
    fired = await apply_snapshot(conn, (rule,), snapshot)  # type: ignore[arg-type]
    assert fired == (alert_id,)
    assert any("ref.merchants" in sql for sql in conn.sql)
    assert any("WHERE NOT EXISTS" in sql for sql in conn.sql)


async def test_other_entity_is_not_evaluated() -> None:
    conn = _Conn([(UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),)])
    rule = _rule(Clause("merchant_cnt_24h", "gt", 1), entity="merchant", name="merchant_burst")
    fired = await apply_snapshot(conn, (rule,), _snapshot(card_cnt_1m=6))  # type: ignore[arg-type]
    assert fired == ()
    assert conn.sql == []
