"""Rule catalog for dashboard filters and labels."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from detection.api.deps import provide_alert_service
from detection.api.mappers import rule_out
from detection.api.schemas import RuleOut
from detection.services.alerts import AlertService

router = APIRouter(prefix="/v1", tags=["rules"])


@router.get("/rules", response_model=list[RuleOut])
async def list_rules(
    service: Annotated[AlertService, Depends(provide_alert_service)],
) -> list[RuleOut]:
    rules = await service.list_rules()
    return [rule_out(rule) for rule in rules]
