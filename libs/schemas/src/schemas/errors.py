"""Shared Problem Details contract (T7.2 / RFC 9457-shaped)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FieldError(BaseModel):
    """One validation field failure (422 only)."""

    model_config = ConfigDict(extra="forbid")

    field: str
    message: str


class ProblemDetails(BaseModel):
    """Uniform error body for all HTTP services."""

    model_config = ConfigDict(extra="forbid")

    type: str
    title: str
    status: int
    request_id: str
    errors: list[FieldError] = Field(default_factory=list)


class AppProblem(Exception):
    """Raise to return a typed ProblemDetails response."""

    def __init__(
        self,
        type: str,
        title: str,
        status: int,
        *,
        errors: list[FieldError] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.type = type
        self.title = title
        self.status = status
        self.errors = list(errors or [])
        self.headers = dict(headers or {})
        super().__init__(title)


_STATUS_TYPES: dict[int, str] = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    422: "validation_error",
    429: "rate_limited",
    503: "upstream_unavailable",
    504: "upstream_timeout",
}

_DETAIL_TYPES: dict[str, str] = {
    "invalid transition": "invalid_transition",
    "alert not found": "not_found",
    "viewer cannot change alert status": "forbidden",
    "missing api key": "unauthorized",
    "unknown api key": "forbidden",
    "rate limit exceeded": "rate_limited",
    "catalog unavailable": "upstream_unavailable",
    "redis unavailable": "upstream_unavailable",
    "event bus unavailable": "upstream_unavailable",
    "note is required when resolving (3–500 characters)": "validation_error",
    "X-Operator-Id must not be empty": "validation_error",
    "invalid cursor": "validation_error",
}


def type_for_http(status: int, detail: str | None = None) -> str:
    """Stable ``type`` from status and optional detail string."""
    if detail is not None:
        mapped = _DETAIL_TYPES.get(detail)
        if mapped is not None:
            return mapped
        if detail.endswith(" unavailable"):
            return "upstream_unavailable"
    return _STATUS_TYPES.get(status, "http_error")


def problem_body(
    *,
    type: str,
    title: str,
    status: int,
    request_id: str,
    errors: list[FieldError] | None = None,
) -> dict[str, Any]:
    """Serialize ProblemDetails for a JSONResponse."""
    return ProblemDetails(
        type=type,
        title=title,
        status=status,
        request_id=request_id,
        errors=list(errors or []),
    ).model_dump()
