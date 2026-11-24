"""Rule catalog for dashboard filters and labels."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from detection.api.deps import provide_alert_service
from detection.api.examples import RULE_CARD, RULE_MERCHANT, RULES_DESCRIPTION
from detection.api.mappers import rule_out
from detection.api.schemas import RuleOut
from detection.services.alerts import AlertService

router = APIRouter(prefix="/v1", tags=["rules"])


@router.get(
    "/rules",
    response_model=list[RuleOut],
    summary="List rules",
    description=RULES_DESCRIPTION,
    openapi_extra={"security": []},
    responses={
        200: {
            "description": "Seeded rules.",
            "content": {"application/json": {"example": [RULE_CARD, RULE_MERCHANT]}},
        }
    },
)
async def list_rules(
    service: Annotated[AlertService, Depends(provide_alert_service)],
) -> list[RuleOut]:
    rules = await service.list_rules()
    return [rule_out(rule) for rule in rules]
