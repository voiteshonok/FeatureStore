"""Shared OpenAPI fragments for Problem Details responses.

Services call :func:`finalize_documented_openapi` after FastAPI builds a document.
The response components are the taxonomy in ``docs/api-errors.md``.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any

from schemas.errors import FieldError, ProblemDetails

HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})
REQUEST_ID_EXAMPLE = "6f1c2a40-9b17-4e5a-8c31-0a6d4e8b7f22"

REQUEST_ID_HEADER: dict[str, Any] = {
    "description": "Correlation id. Echoed from the request or generated when the header is empty.",
    "schema": {"type": "string"},
    "example": REQUEST_ID_EXAMPLE,
}

REQUEST_ID_PARAMETER: dict[str, Any] = {
    "name": "X-Request-ID",
    "in": "header",
    "required": False,
    "schema": {"type": "string"},
    "description": "Correlation id. Generated when the header is empty.",
}

RETRY_AFTER_HEADER: dict[str, Any] = {
    "description": "Seconds to wait before retrying.",
    "schema": {"type": "string"},
    "example": "1",
}

_STATUS_COMPONENT: dict[str, str] = {
    "401": "Unauthorized",
    "403": "Forbidden",
    "404": "NotFound",
    "409": "Conflict",
    "422": "ValidationError",
    "429": "RateLimited",
    "500": "InternalError",
    "503": "UpstreamUnavailable",
    "504": "UpstreamTimeout",
}


def _problem(
    *,
    type_: str,
    title: str,
    status: int,
    errors: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "type": type_,
        "title": title,
        "status": status,
        "request_id": REQUEST_ID_EXAMPLE,
        "errors": [] if errors is None else errors,
    }


def _response(
    description: str,
    examples: dict[str, tuple[str, dict[str, Any]]],
    *,
    extra_headers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"X-Request-ID": copy.deepcopy(REQUEST_ID_HEADER)}
    if extra_headers is not None:
        headers.update(extra_headers)
    return {
        "description": description,
        "headers": headers,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ProblemDetails"},
                "examples": {
                    name: {"summary": summary, "value": value}
                    for name, (summary, value) in examples.items()
                },
            }
        },
    }


PROBLEM_RESPONSES: dict[str, dict[str, Any]] = {
    "Unauthorized": _response(
        "Missing credentials, invalid session, or bad login.",
        {
            "unauthorized": (
                "Missing credentials",
                _problem(type_="unauthorized", title="missing credentials", status=401),
            ),
        },
    ),
    "Forbidden": _response(
        "Unknown API key, insufficient role, or missing CSRF header.",
        {
            "forbidden": (
                "Not allowed",
                _problem(type_="forbidden", title="forbidden", status=403),
            ),
        },
    ),
    "NotFound": _response(
        "The resource id is unknown. Do not retry it.",
        {
            "not_found": (
                "Unknown alert",
                _problem(type_="not_found", title="alert not found", status=404),
            ),
        },
    ),
    "Conflict": _response(
        "The state change conflicts with the current resource.",
        {
            "invalid_transition": (
                "Alert status change is not allowed",
                _problem(type_="invalid_transition", title="invalid transition", status=409),
            ),
            "conflict": (
                "Generic conflict",
                _problem(type_="conflict", title="conflict", status=409),
            ),
        },
    ),
    "ValidationError": _response(
        "Body, query, or header failed schema or business validation.",
        {
            "validation_error": (
                "Field validation failed",
                _problem(
                    type_="validation_error",
                    title="Request validation failed",
                    status=422,
                    errors=[
                        {
                            "field": "note",
                            "message": "note is required when resolving (3–500 characters)",
                        }
                    ],
                ),
            ),
            "bad_request": (
                "Malformed request outside the schema",
                _problem(type_="bad_request", title="malformed request", status=400),
            ),
            "http_error": (
                "Unmapped HTTP status",
                _problem(type_="http_error", title="http error", status=400),
            ),
        },
    ),
    "RateLimited": _response(
        "The client exceeded a gateway rate limit. Honour Retry-After.",
        {
            "rate_limited": (
                "Too many requests",
                _problem(type_="rate_limited", title="rate limit exceeded", status=429),
            ),
        },
        extra_headers={"Retry-After": copy.deepcopy(RETRY_AFTER_HEADER)},
    ),
    "InternalError": _response(
        "Unexpected exception. Retry once and quote request_id.",
        {
            "internal_error": (
                "Unhandled exception",
                _problem(type_="internal_error", title="Internal server error", status=500),
            ),
        },
    ),
    "UpstreamUnavailable": _response(
        "A dependency is down. Retry with backoff.",
        {
            "upstream_unavailable": (
                "Dependency unavailable",
                _problem(
                    type_="upstream_unavailable",
                    title="upstream unavailable",
                    status=503,
                ),
            ),
        },
    ),
    "UpstreamTimeout": _response(
        "The gateway timed out waiting for an upstream. Retry with backoff.",
        {
            "upstream_timeout": (
                "Upstream timed out",
                _problem(type_="upstream_timeout", title="upstream timeout", status=504),
            ),
        },
    ),
}


def set_json_example(
    schema: dict[str, Any],
    method: str,
    path: str,
    status: str,
    example: Any,
    *,
    media_type: str = "application/json",
) -> None:
    """Put an example on a response after FastAPI encoding.

    ``get_openapi`` drops ``None`` (``exclude_none``). Assigning the example
    afterwards keeps JSON nulls that the schema marks as required.
    """
    response = schema["paths"][path][method]["responses"][status]
    content = response.setdefault("content", {})
    media = content.setdefault(media_type, {})
    media["example"] = example


def set_json_examples(
    schema: dict[str, Any],
    method: str,
    path: str,
    status: str,
    examples: dict[str, Any],
    *,
    media_type: str = "application/json",
) -> None:
    response = schema["paths"][path][method]["responses"][status]
    content = response.setdefault("content", {})
    media = content.setdefault(media_type, {})
    media["examples"] = examples


def ensure_problem_schema(schema: dict[str, Any]) -> None:
    """Insert ProblemDetails when the service does not already publish it."""
    components = schema.setdefault("components", {})
    models = components.setdefault("schemas", {})
    if "ProblemDetails" in models:
        return
    field_name = "FieldErrorOut" if "FieldErrorOut" in models else "FieldError"
    if field_name not in models:
        field_schema = FieldError.model_json_schema()
        field_schema.pop("$defs", None)
        models[field_name] = field_schema
    problem = ProblemDetails.model_json_schema()
    problem.pop("$defs", None)
    errors = problem.get("properties", {}).get("errors")
    if isinstance(errors, dict):
        errors["items"] = {"$ref": f"#/components/schemas/{field_name}"}
    models["ProblemDetails"] = problem


def install_problem_components(schema: dict[str, Any], *, only: set[str] | None = None) -> None:
    responses = schema.setdefault("components", {}).setdefault("responses", {})
    for name, body in PROBLEM_RESPONSES.items():
        if only is not None and name not in only:
            continue
        responses[name] = copy.deepcopy(body)


def attach_error_responses(
    schema: dict[str, Any],
    errors: Mapping[tuple[str, str], tuple[str, ...]],
) -> None:
    """Point listed status codes at the shared components. Other responses stay."""
    paths = schema.get("paths", {})
    if not isinstance(paths, dict):
        return
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            codes = errors.get((method, path))
            if not codes:
                continue
            responses = operation.setdefault("responses", {})
            for code in codes:
                component = _STATUS_COMPONENT[code]
                responses[code] = {"$ref": f"#/components/responses/{component}"}


def attach_request_id_parameters(schema: dict[str, Any]) -> None:
    paths = schema.get("paths", {})
    if not isinstance(paths, dict):
        return
    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            parameters = operation.setdefault("parameters", [])
            if not isinstance(parameters, list):
                continue
            exists = any(
                isinstance(item, dict)
                and item.get("in") == "header"
                and item.get("name") == "X-Request-ID"
                for item in parameters
            )
            if not exists:
                parameters.append(copy.deepcopy(REQUEST_ID_PARAMETER))


def attach_request_id_headers(schema: dict[str, Any]) -> None:
    """Add the response header on inline responses. Component responses already have it."""
    paths = schema.get("paths", {})
    if not isinstance(paths, dict):
        return
    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            responses = operation.get("responses", {})
            if not isinstance(responses, dict):
                continue
            for response in responses.values():
                if not isinstance(response, dict) or "$ref" in response:
                    continue
                headers = response.setdefault("headers", {})
                if isinstance(headers, dict):
                    headers.setdefault("X-Request-ID", copy.deepcopy(REQUEST_ID_HEADER))


def ensure_explicit_security(schema: dict[str, Any]) -> None:
    """Set ``security: []`` when an operation does not name a scheme."""
    paths = schema.get("paths", {})
    if not isinstance(paths, dict):
        return
    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            operation.setdefault("security", [])


def drop_empty_json_when_stream(schema: dict[str, Any]) -> None:
    """Drop FastAPI's empty JSON 200 when the operation is an SSE stream."""
    paths = schema.get("paths", {})
    if not isinstance(paths, dict):
        return
    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            response = operation.get("responses", {}).get("200")
            if not isinstance(response, dict):
                continue
            content = response.get("content")
            if not isinstance(content, dict) or "text/event-stream" not in content:
                continue
            json_body = content.get("application/json")
            if isinstance(json_body, dict) and json_body.get("schema") in ({}, None):
                del content["application/json"]


def drop_unused_default_schemas(schema: dict[str, Any]) -> None:
    """Remove FastAPI's default validation schemas once 422 points at ProblemDetails."""
    components = schema.get("components", {})
    models = components.get("schemas", {})
    if not isinstance(models, dict):
        return
    blob = json.dumps(
        {"paths": schema.get("paths", {}), "responses": components.get("responses", {})}
    )
    for name in ("HTTPValidationError", "ValidationError"):
        needle = f"#/components/schemas/{name}"
        if name in models and needle not in blob:
            del models[name]


def finalize_documented_openapi(
    schema: dict[str, Any],
    *,
    tags: list[dict[str, str]] | None = None,
    errors: Mapping[tuple[str, str], tuple[str, ...]] | None = None,
    public: bool = False,
) -> None:
    """Attach the shared error components, correlation header, and tag list."""
    ensure_problem_schema(schema)
    used = {
        _STATUS_COMPONENT[code]
        for codes in (errors or {}).values()
        for code in codes
        if code in _STATUS_COMPONENT
    }
    install_problem_components(schema, only=used or None)
    if tags is not None:
        schema["tags"] = tags
    if errors:
        attach_error_responses(schema, errors)
    attach_request_id_parameters(schema)
    attach_request_id_headers(schema)
    if public:
        ensure_explicit_security(schema)
    drop_empty_json_when_stream(schema)
    drop_unused_default_schemas(schema)
