"""FastAPI dependencies. Routers receive services, not sessions."""

from collections.abc import AsyncIterator
from typing import Annotated, Literal

from fastapi import Header, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from detection.services.alerts import AlertService
from schemas import AppProblem

OperatorRole = Literal["viewer", "analyst", "admin"]


async def provide_alert_service(request: Request) -> AsyncIterator[AlertService]:
    """Open a session, yield ``AlertService``, close the session after the request."""
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    async with factory() as session:
        yield AlertService(session)


def require_operator_id(
    x_operator_id: Annotated[str, Header(alias="X-Operator-Id")],
) -> str:
    """Operator id set by the gateway after JWT verification (T7.3)."""
    value = x_operator_id.strip()
    if not value:
        raise AppProblem("validation_error", "X-Operator-Id must not be empty", 422)
    return value


def require_operator_role(
    x_operator_role: Annotated[OperatorRole, Header(alias="X-Operator-Role")],
) -> OperatorRole:
    """Operator role set by the gateway after JWT verification (T7.3)."""
    return x_operator_role
