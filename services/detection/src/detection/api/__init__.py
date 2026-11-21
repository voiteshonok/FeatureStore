"""HTTP layer and application assembly for detection (ADR-019)."""

from detection.api.app import app, create_app, export_openapi
from detection.api.schemas import AlertOut, AlertUpdate
from detection.services.transitions import transition_allowed

__all__ = [
    "AlertOut",
    "AlertUpdate",
    "app",
    "create_app",
    "export_openapi",
    "transition_allowed",
]
