"""Global error handlers and X-Request-ID middleware for features-api."""

from __future__ import annotations

import logging
import traceback
from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.responses import Response

from schemas import AppProblem, FieldError, mask_pans
from schemas.errors import problem_body, type_for_http

_LOGGER = logging.getLogger("features")


def request_id_of(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    if isinstance(value, str) and value:
        return value
    return ""


def _unhandled_response(request: Request, exc: BaseException) -> JSONResponse:
    request_id = request_id_of(request)
    stack = mask_pans("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    _LOGGER.error(
        "unhandled error\n%s",
        stack,
        extra={"request_id": request_id},
    )
    return JSONResponse(
        status_code=500,
        content=problem_body(
            type="internal_error",
            title="Internal server error",
            status=500,
            request_id=request_id,
        ),
    )


def install_request_id_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        incoming = request.headers.get("x-request-id", "").strip()
        request_id = incoming or str(uuid4())
        request.state.request_id = request_id
        try:
            response = await call_next(request)
        except Exception as exc:
            response = _unhandled_response(request, exc)
        response.headers["X-Request-ID"] = request_id
        return response


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppProblem)
    async def app_problem(request: Request, exc: AppProblem) -> JSONResponse:
        body = problem_body(
            type=exc.type,
            title=mask_pans(exc.title),
            status=exc.status,
            request_id=request_id_of(request),
            errors=exc.errors,
        )
        return JSONResponse(status_code=exc.status, content=body, headers=exc.headers)

    @app.exception_handler(HTTPException)
    async def http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        title = mask_pans(str(exc.detail))
        body = problem_body(
            type=type_for_http(exc.status_code, title),
            title=title,
            status=exc.status_code,
            request_id=request_id_of(request),
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=body,
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
        body = problem_body(
            type="validation_error",
            title="Request validation failed",
            status=422,
            request_id=request_id_of(request),
            errors=errors,
        )
        return JSONResponse(status_code=422, content=body)

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        return _unhandled_response(request, exc)


def _field_name(loc: object) -> str:
    if not isinstance(loc, (list, tuple)) or not loc:
        return "body"
    parts = [str(item) for item in loc if item not in {"body", "query", "path", "header"}]
    return ".".join(parts) if parts else str(loc[-1])
