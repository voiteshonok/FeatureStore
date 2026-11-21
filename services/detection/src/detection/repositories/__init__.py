"""Persistence for detection CRUD."""

from detection.repositories.alerts import AlertRepository
from detection.repositories.audit import AuditRepository
from detection.repositories.rules import RuleRepository

__all__ = ["AlertRepository", "AuditRepository", "RuleRepository"]
