"""Alert rules loaded from Postgres and evaluated against a feature snapshot.

Snapshots carry stream features only (ADR-013). A rule that references an
on-demand feature is dropped on each reload, with one warning for that reload.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, NamedTuple
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

from schemas import FeatureSnapshot
from schemas.catalog import Entity

Comparison = Literal["gt", "ge", "lt", "le"]

_ENTITIES: dict[str, Entity] = {"card": "card", "merchant": "merchant"}
_COMPARISONS: dict[str, Comparison] = {"gt": "gt", "ge": "ge", "lt": "lt", "le": "le"}

_LOAD_RULES = """
SELECT r.rule_id, r.name, r.entity, rf.feature_name, rf.comparison, rf.threshold, d.kind
FROM detection.alert_rules AS r
JOIN detection.rule_features AS rf ON rf.rule_id = r.rule_id
JOIN features.feature_definitions AS d ON d.name = rf.feature_name
WHERE r.enabled
ORDER BY r.rule_id, rf.feature_name
"""

_UPSERT_MERCHANT = """
INSERT INTO ref.merchants (merchant_id)
VALUES (%s)
ON CONFLICT (merchant_id) DO NOTHING
"""

# One open alert per rule and entity. A redelivery finds the open row and inserts nothing.
_INSERT_ALERT = """
INSERT INTO detection.alerts (rule_id, merchant_id, card_token, event_id, feature_snapshot)
SELECT %s, %s, %s, %s, %s::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM detection.alerts
    WHERE rule_id = %s
      AND status = 'open'
      AND card_token IS NOT DISTINCT FROM %s
      AND merchant_id IS NOT DISTINCT FROM %s
)
RETURNING alert_id
"""


@dataclass(frozen=True)
class Clause:
    """One feature comparison on a rule."""

    feature: str
    comparison: Comparison
    threshold: float


@dataclass(frozen=True)
class Rule:
    """An enabled rule whose clauses are all stream features."""

    rule_id: UUID
    name: str
    entity: Entity
    clauses: tuple[Clause, ...]


@dataclass
class _Draft:
    rule_id: UUID
    name: str
    entity: Entity
    clauses: list[Clause]
    on_demand: bool


class RuleCache:
    """Latest enabled rules. Replaced as a whole on each reload."""

    def __init__(self) -> None:
        self.rules: tuple[Rule, ...] = ()

    def replace(self, rules: tuple[Rule, ...]) -> None:
        self.rules = rules


def holds(value: float, comparison: Comparison, threshold: float) -> bool:
    """Return whether ``value`` meets ``comparison`` against ``threshold``.

    Equality matches ``ge`` and ``le`` only.
    """
    if comparison == "gt":
        return value > threshold
    if comparison == "ge":
        return value >= threshold
    if comparison == "lt":
        return value < threshold
    return value <= threshold


def matched_values(
    rule: Rule,
    features: dict[str, int | float],
) -> dict[str, int | float] | None:
    """Return the referenced values when every comparison holds.

    ``None`` means a referenced feature is missing or a comparison failed.
    The rule is then not fired for this snapshot.
    """
    picked: dict[str, int | float] = {}
    for clause in rule.clauses:
        if clause.feature not in features:
            return None
        value = features[clause.feature]
        if not holds(float(value), clause.comparison, clause.threshold):
            return None
        picked[clause.feature] = value
    return picked


def rules_from_rows(rows: Sequence[Sequence[object]]) -> tuple[Rule, ...]:
    """Build stream rules from a rule join. Warn once if any rule is on-demand."""
    rules, skipped = _assemble(rows)
    if skipped:
        logging.getLogger("detection").warning(
            "skipped on-demand rules: %s",
            ", ".join(skipped),
        )
    return rules


async def load_rules(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
) -> tuple[Rule, ...]:
    """Load enabled rules. On-demand rules are omitted."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(_LOAD_RULES)
        rows = await cur.fetchall()
    return rules_from_rows(rows)


def entity_ids(snapshot: FeatureSnapshot) -> tuple[str | None, str | None]:
    """Return ``(merchant_id, card_token)`` for the snapshot entity."""
    if snapshot.entity_type == "merchant":
        return snapshot.entity_id, None
    return None, snapshot.entity_id


class FiredAlert(NamedTuple):
    """An alert row inserted for one rule. ``rule`` is the rule name."""

    rule: str
    alert_id: UUID


async def apply_snapshot(
    conn: AsyncConnection[TupleRow],
    rules: tuple[Rule, ...],
    snapshot: FeatureSnapshot,
) -> tuple[FiredAlert, ...]:
    """Insert one open alert per matching rule. Return the new alerts."""
    merchant_id, card_token = entity_ids(snapshot)
    fired: list[FiredAlert] = []
    async with conn.transaction():
        if merchant_id is not None:
            await conn.execute(_UPSERT_MERCHANT, (merchant_id,))
        for rule in rules:
            if rule.entity != snapshot.entity_type:
                continue
            values = matched_values(rule, snapshot.features)
            if values is None:
                continue
            alert_id = await _insert_alert(
                conn,
                rule.rule_id,
                merchant_id,
                card_token,
                snapshot.event_id,
                values,
            )
            if alert_id is not None:
                fired.append(FiredAlert(rule.name, alert_id))
    return tuple(fired)


async def _insert_alert(
    conn: AsyncConnection[TupleRow],
    rule_id: UUID,
    merchant_id: str | None,
    card_token: str | None,
    event_id: UUID,
    values: dict[str, int | float],
) -> UUID | None:
    cursor = await conn.execute(
        _INSERT_ALERT,
        (
            rule_id,
            merchant_id,
            card_token,
            event_id,
            json.dumps(values),
            rule_id,
            card_token,
            merchant_id,
        ),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    found = row[0]
    if not isinstance(found, UUID):
        msg = "alert_id must be a uuid"
        raise TypeError(msg)
    return found


def _assemble(rows: Sequence[Sequence[object]]) -> tuple[tuple[Rule, ...], tuple[str, ...]]:
    drafts: dict[UUID, _Draft] = {}
    order: list[UUID] = []
    for row in rows:
        if len(row) != 7:
            msg = "rule row is incomplete"
            raise TypeError(msg)
        rule_id = _uuid(row[0])
        name = _text(row[1])
        entity = _entity(_text(row[2]))
        feature = _text(row[3])
        comparison = _comparison(_text(row[4]))
        threshold = _number(row[5])
        kind = _text(row[6])
        draft = drafts.get(rule_id)
        if draft is None:
            draft = _Draft(rule_id, name, entity, [], False)
            drafts[rule_id] = draft
            order.append(rule_id)
        if kind != "stream":
            draft.on_demand = True
            continue
        draft.clauses.append(Clause(feature, comparison, threshold))
    rules: list[Rule] = []
    skipped: list[str] = []
    for rule_id in order:
        draft = drafts[rule_id]
        if draft.on_demand:
            skipped.append(draft.name)
            continue
        if not draft.clauses:
            continue
        rules.append(
            Rule(
                rule_id=draft.rule_id,
                name=draft.name,
                entity=draft.entity,
                clauses=tuple(draft.clauses),
            )
        )
    return tuple(rules), tuple(skipped)


def _uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    msg = "rule_id must be a uuid"
    raise TypeError(msg)


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    msg = "expected text"
    raise TypeError(msg)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = "threshold must be a number"
        raise TypeError(msg)
    return float(value)


def _entity(value: str) -> Entity:
    try:
        return _ENTITIES[value]
    except KeyError as exc:
        msg = f"unknown entity {value}"
        raise ValueError(msg) from exc


def _comparison(value: str) -> Comparison:
    try:
        return _COMPARISONS[value]
    except KeyError as exc:
        msg = f"unknown comparison {value}"
        raise ValueError(msg) from exc
