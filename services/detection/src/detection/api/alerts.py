"""Alert list, detail, and status transition routes."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from detection.api.deps import (
    OperatorRole,
    provide_alert_service,
    require_operator_id,
    require_operator_role,
)
from detection.api.mappers import alert_out, detail_out
from detection.api.schemas import (
    AlertDetailOut,
    AlertListOut,
    AlertOut,
    AlertUpdate,
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

router = APIRouter(prefix="/v1", tags=["alerts"])

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000


@router.get(
    "/alerts",
    response_model=AlertListOut,
    responses={422: {"description": "Invalid cursor or query"}},
)
async def list_alerts(
    service: Annotated[AlertService, Depends(provide_alert_service)],
    status: Annotated[list[Status] | None, Query()] = None,
    entity: Annotated[str | None, Query()] = None,
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
            rule_id=rule_id,
            created_from=created_from,
            created_to=created_to,
            sort=sort,
            cursor=cursor,
            limit=limit,
        )
    except CursorError as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    return AlertListOut(
        items=[alert_out(item) for item in page.items],
        next_cursor=page.next_cursor,
    )


@router.get(
    "/alerts/{alert_id}",
    response_model=AlertDetailOut,
    responses={404: {"description": "Unknown alert"}},
)
async def get_alert(
    alert_id: UUID,
    service: Annotated[AlertService, Depends(provide_alert_service)],
) -> AlertDetailOut:
    try:
        detail = await service.get_detail(alert_id)
    except AlertNotFoundError as exc:
        raise HTTPException(status_code=404, detail="alert not found") from exc
    return detail_out(detail)


@router.patch(
    "/alerts/{alert_id}",
    response_model=AlertOut,
    responses={
        403: {"description": "Viewer cannot change status"},
        404: {"description": "Unknown alert"},
        409: {"description": "Invalid transition"},
        422: {"description": "Missing or invalid note / headers"},
    },
)
async def patch_alert(
    alert_id: UUID,
    body: AlertUpdate,
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
        raise HTTPException(status_code=403, detail="viewer cannot change alert status") from exc
    except AlertNotFoundError as exc:
        raise HTTPException(status_code=404, detail="alert not found") from exc
    except InvalidTransitionError as exc:
        raise HTTPException(status_code=409, detail="invalid transition") from exc
    except NoteRequiredError as exc:
        raise HTTPException(
            status_code=422,
            detail="note is required when resolving (3–500 characters)",
        ) from exc
    return alert_out(alert)
