"""Alert list, detail, and status transition routes."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Query

from detection.api.deps import (
    OperatorRole,
    provide_alert_service,
    require_operator_id,
    require_operator_role,
)
from detection.api.examples import (
    ALERT_ACKNOWLEDGED,
    ALERT_DETAIL_EXAMPLE,
    ALERT_LIST_EXAMPLE,
    GET_ALERT_DESCRIPTION,
    LIST_ALERTS_DESCRIPTION,
    PATCH_DESCRIPTION,
    PATCH_EXAMPLES,
)
from detection.api.mappers import alert_out, detail_out
from detection.api.schemas import (
    AlertDetailOut,
    AlertListOut,
    AlertOut,
    AlertUpdate,
    Entity,
    Sort,
    Status,
)
from detection.services.alerts import (
    AlertNotFoundError,
    AlertService,
    ForbiddenTransitionError,
    InvalidTransitionError,
    NoteRequiredError,
)
from detection.services.cursor import CursorError
from schemas import AppProblem

router = APIRouter(prefix="/v1", tags=["alerts"])

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000


@router.get(
    "/alerts",
    response_model=AlertListOut,
    summary="List alerts",
    description=LIST_ALERTS_DESCRIPTION,
    openapi_extra={"security": []},
    responses={
        200: {
            "description": "One page of alerts.",
            "content": {"application/json": {"example": ALERT_LIST_EXAMPLE}},
        }
    },
)
async def list_alerts(
    service: Annotated[AlertService, Depends(provide_alert_service)],
    status: Annotated[list[Status] | None, Query()] = None,
    entity: Annotated[str | None, Query()] = None,
    entity_type: Annotated[Entity | None, Query()] = None,
    rule_id: Annotated[UUID | None, Query()] = None,
    created_from: Annotated[datetime | None, Query()] = None,
    created_to: Annotated[datetime | None, Query()] = None,
    sort: Annotated[Sort, Query()] = "-created_at",
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = _DEFAULT_LIMIT,
) -> AlertListOut:
    try:
        page = await service.list_alerts(
            statuses=status,
            entity=entity,
            entity_type=entity_type,
            rule_id=rule_id,
            created_from=created_from,
            created_to=created_to,
            sort=sort,
            cursor=cursor,
            limit=limit,
        )
    except CursorError as exc:
        raise AppProblem("validation_error", "invalid cursor", 422) from exc
    return AlertListOut(
        items=[alert_out(item) for item in page.items],
        next_cursor=page.next_cursor,
    )


@router.get(
    "/alerts/{alert_id}",
    response_model=AlertDetailOut,
    summary="Get alert",
    description=GET_ALERT_DESCRIPTION,
    openapi_extra={"security": []},
    responses={
        200: {
            "description": "Alert, rule, and audit history.",
            "content": {"application/json": {"example": ALERT_DETAIL_EXAMPLE}},
        }
    },
)
async def get_alert(
    alert_id: UUID,
    service: Annotated[AlertService, Depends(provide_alert_service)],
) -> AlertDetailOut:
    try:
        detail = await service.get_detail(alert_id)
    except AlertNotFoundError as exc:
        raise AppProblem("not_found", "alert not found", 404) from exc
    return detail_out(detail)


@router.patch(
    "/alerts/{alert_id}",
    response_model=AlertOut,
    summary="Update alert status",
    description=PATCH_DESCRIPTION,
    openapi_extra={"security": []},
    responses={
        200: {
            "description": "Alert after the transition.",
            "content": {"application/json": {"example": ALERT_ACKNOWLEDGED}},
        }
    },
)
async def patch_alert(
    alert_id: UUID,
    body: Annotated[AlertUpdate, Body(openapi_examples=PATCH_EXAMPLES)],
    service: Annotated[AlertService, Depends(provide_alert_service)],
    operator_id: Annotated[str, Depends(require_operator_id)],
    role: Annotated[OperatorRole, Depends(require_operator_role)],
) -> AlertOut:
    try:
        alert = await service.transition(
            alert_id,
            new_status=body.status,
            note=body.note,
            operator_id=operator_id,
            role=role,
        )
    except ForbiddenTransitionError as exc:
        raise AppProblem(
            "forbidden",
            "viewer cannot change alert status",
            403,
        ) from exc
    except AlertNotFoundError as exc:
        raise AppProblem("not_found", "alert not found", 404) from exc
    except InvalidTransitionError as exc:
        raise AppProblem("invalid_transition", "invalid transition", 409) from exc
    except NoteRequiredError as exc:
        raise AppProblem(
            "validation_error",
            "note is required when resolving (3–500 characters)",
            422,
        ) from exc
    return alert_out(alert)
