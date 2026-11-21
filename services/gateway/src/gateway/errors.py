"""Global error handlers for gateway-owned responses (T7.2)."""

from __future__ import annotations

import logging
import traceback

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from schemas import AppProblem, FieldError, mask_pans
from schemas.errors import problem_body, type_for_http

_LOGGER = logging.getLogger("gateway")


def request_id_of(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    if isinstance(value, str) and value:
        return value
    return ""


def unhandled_response(request: Request, exc: BaseException) -> JSONResponse:
    """500 ProblemDetails for unexpected errors (also used by middleware)."""
    request_id = request_id_of(request)
    stack = mask_pans("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    _LOGGER.error("unhandled error\n%s", stack, extra={"request_id": request_id})
    return problem_response(
        request,
        type="internal_error",
        title="Internal server error",
        status=500,
    )


def problem_response(
    request: Request,
    *,
    type: str,
    title: str,
    status: int,
    errors: list[FieldError] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build a gateway-owned ProblemDetails JSONResponse."""
    body = problem_body(
        type=type,
        title=mask_pans(title),
        status=status,
        request_id=request_id_of(request),
        errors=errors,
    )
    response = JSONResponse(status_code=status, content=body, headers=headers or {})
    request_id = request_id_of(request)
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppProblem)
    async def app_problem(request: Request, exc: AppProblem) -> JSONResponse:
        return problem_response(
            request,
            type=exc.type,
            title=exc.title,
            status=exc.status,
            errors=exc.errors,
            headers=exc.headers,
        )

    @app.exception_handler(HTTPException)
    async def http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        title = mask_pans(str(exc.detail))
        return problem_response(
            request,
            type=type_for_http(exc.status_code, title),
            title=title,
            status=exc.status_code,
            headers=dict(exc.headers or {}),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            FieldError(
                field=_field_name(err.get("loc", ())),
                message=mask_pans(str(err.get("msg", ""))),
            )
            for err in exc.errors()
        ]
        return problem_response(
            request,
            type="validation_error",
            title="Request validation failed",
            status=422,
            errors=errors,
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        return unhandled_response(request, exc)


def _field_name(loc: object) -> str:
    if not isinstance(loc, (list, tuple)) or not loc:
        return "body"
    parts = [str(item) for item in loc if item not in {"body", "query", "path", "header"}]
    return ".".join(parts) if parts else str(loc[-1])
