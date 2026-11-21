"""Map ORM rows to response schemas."""

from __future__ import annotations

from typing import cast

from detection.api.schemas import (
    AlertDetailOut,
    AlertOut,
    AuditEntryOut,
    Comparison,
    Entity,
    RuleConditionOut,
    RuleOut,
    Status,
)
from detection.models import Alert, AlertRule, AuditLog
from detection.services.alerts import AlertDetail


def alert_out(alert: Alert) -> AlertOut:
    return AlertOut(
        alert_id=alert.alert_id,
        rule_id=alert.rule_id,
        status=cast(Status, alert.status),
        merchant_id=alert.merchant_id,
        card_token=alert.card_token,
        event_id=alert.event_id,
        feature_snapshot=_features(alert.feature_snapshot),
        created_at=alert.created_at,
        resolved_at=alert.resolved_at,
        acknowledged_at=alert.acknowledged_at,
        resolution_note=alert.resolution_note,
    )


def rule_out(rule: AlertRule) -> RuleOut:
    conditions = [
        RuleConditionOut(
            feature_name=clause.feature_name,
            comparison=cast(Comparison, clause.comparison),
            threshold=float(clause.threshold),
        )
        for clause in sorted(rule.features, key=lambda item: item.feature_name)
    ]
    return RuleOut(
        rule_id=rule.rule_id,
        name=rule.name,
        entity=cast(Entity, rule.entity),
        enabled=rule.enabled,
        conditions=conditions,
    )


def audit_out(row: AuditLog) -> AuditEntryOut:
    return AuditEntryOut(
        audit_id=row.audit_id,
        occurred_at=row.occurred_at,
        actor=row.actor,
        action=row.action,
        detail=dict(row.detail),
    )


def detail_out(detail: AlertDetail) -> AlertDetailOut:
    return AlertDetailOut(
        alert=alert_out(detail.alert),
        rule=rule_out(detail.rule),
        history=[audit_out(row) for row in detail.history],
    )


def _features(value: object) -> dict[str, int | float]:
    if not isinstance(value, dict):
        msg = "feature_snapshot must be an object"
        raise TypeError(msg)
    parsed: dict[str, int | float] = {}
    for key, item in value.items():
        if not isinstance(key, str) or isinstance(item, bool) or not isinstance(item, int | float):
            msg = "feature_snapshot values must be numbers"
            raise TypeError(msg)
        parsed[key] = item
    return parsed
