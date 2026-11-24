"""Replay gateway OpenAPI examples against the spec and, in e2e, the stack.

The schema check runs with the unit suite. The HTTP replay is ``e2e`` and
does not publish to Kafka: stage 2 still needs an empty ``transactions.raw``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import jsonschema
import pytest

from gateway.examples import (
    ALERT_ID,
    FEATURE_EDGE,
    FEATURE_TYPICAL,
    LOGIN_EDGE,
    LOGIN_TYPICAL,
    PATCH_ACKNOWLEDGE,
    PATCH_RESOLVE,
)

_GATEWAY = "http://localhost:8080"
_API_KEY = "dev-scoring-key"
_ROOT = Path(__file__).resolve().parents[2]
_SPEC = _ROOT / "docs" / "openapi" / "gateway.json"
_METHODS = {"get", "post", "put", "patch", "delete"}
_SUCCESS_TYPICAL = {"/v1/auth/login", "/v1/features"}
_READS = ("/v1/auth/me", "/v1/alerts", "/v1/rules", "/v1/catalog")


def _document() -> dict[str, Any]:
    loaded = json.loads(_SPEC.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        pytest.fail("gateway spec is not an object")
    return loaded


def _pointer(document: dict[str, Any], ref: str) -> Any:
    node: Any = document
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            pytest.fail(f"dangling ref {ref}")
        node = node[part]
    return node


def _deref(node: Any, document: dict[str, Any], seen: set[str] | None = None) -> Any:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/"):
            stack = seen or set()
            if ref in stack:
                return {}
            stack = set(stack)
            stack.add(ref)
            resolved = _deref(_pointer(document, ref), document, stack)
            extras = {key: value for key, value in node.items() if key != "$ref"}
            if not extras:
                return resolved
            if isinstance(resolved, dict):
                merged = dict(resolved)
                merged.update(
                    {key: _deref(value, document, stack) for key, value in extras.items()}
                )
                return merged
            return resolved
        return {key: _deref(value, document, seen) for key, value in node.items()}
    if isinstance(node, list):
        return [_deref(item, document, seen) for item in node]
    return node


def _validate(instance: Any, schema: Any, document: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(_deref(schema, document)).validate(instance)


def _operations(document: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        return []
    found: list[tuple[str, str, dict[str, Any]]] = []
    for path, path_item in paths.items():
        if not isinstance(path, str) or not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method in _METHODS and isinstance(operation, dict):
                found.append((method, path, operation))
    return found


def _request_examples(operation: dict[str, Any]) -> dict[str, Any]:
    body = operation.get("requestBody")
    if not isinstance(body, dict):
        return {}
    content = body.get("content")
    if not isinstance(content, dict):
        return {}
    media = content.get("application/json")
    if not isinstance(media, dict):
        return {}
    examples = media.get("examples")
    if not isinstance(examples, dict):
        return {}
    values: dict[str, Any] = {}
    for name, wrapped in examples.items():
        if isinstance(wrapped, dict) and "value" in wrapped:
            values[str(name)] = wrapped["value"]
    return values


def test_gateway_request_examples_match_the_models_and_spec() -> None:
    document = _document()
    expected = {
        ("post", "/v1/auth/login"): {"typical": LOGIN_TYPICAL, "edge": LOGIN_EDGE},
        ("post", "/v1/features"): {"typical": FEATURE_TYPICAL, "edge": FEATURE_EDGE},
        ("patch", "/v1/alerts/{alert_id}"): {
            "typical": PATCH_ACKNOWLEDGE,
            "edge": PATCH_RESOLVE,
        },
    }
    seen: set[tuple[str, str]] = set()
    for method, path, operation in _operations(document):
        examples = _request_examples(operation)
        if not examples:
            continue
        seen.add((method, path))
        assert examples == expected[(method, path)]
        media = operation["requestBody"]["content"]["application/json"]
        for value in examples.values():
            _validate(value, media["schema"], document)
    assert seen == set(expected)


def _session_token(response: httpx.Response) -> str:
    token = response.cookies.get("fs_session")
    if isinstance(token, str) and token:
        return token
    header = response.headers.get("set-cookie", "")
    marker = "fs_session="
    if marker not in header:
        pytest.fail(f"login did not set fs_session: {header}")
    return header.split(marker, 1)[1].split(";", 1)[0]


def _headers(
    operation: dict[str, Any],
    method: str,
    token: str | None,
) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    schemes = {
        name for item in operation.get("security", []) if isinstance(item, dict) for name in item
    }
    if "ApiKey" in schemes:
        headers["X-API-Key"] = _API_KEY
        return headers
    if "SessionCookie" in schemes:
        if not token:
            pytest.fail("session example without a login")
        headers["Cookie"] = f"fs_session={token}"
        if method in {"post", "patch", "put", "delete"}:
            headers["X-Requested-With"] = "fetch"
    return headers


def _url(path: str) -> str:
    return _GATEWAY + path.replace("{alert_id}", ALERT_ID)


def _check_response(
    document: dict[str, Any],
    operation: dict[str, Any],
    response: httpx.Response,
    *,
    typical_success: bool,
) -> None:
    status = str(response.status_code)
    assert status in operation["responses"], response.text
    assert response.status_code != 422, response.text
    if typical_success:
        assert 200 <= response.status_code < 300, response.text
    if response.status_code == 204 or not response.content:
        return
    spec = operation["responses"][status]
    if isinstance(spec, dict) and "$ref" in spec:
        spec = _pointer(document, spec["$ref"])
    if not isinstance(spec, dict):
        return
    content = spec.get("content")
    if not isinstance(content, dict):
        return
    media = content.get("application/json")
    if not isinstance(media, dict) or "schema" not in media:
        return
    _validate(response.json(), media["schema"], document)


@pytest.mark.e2e
def test_gateway_examples_round_trip() -> None:
    document = _document()
    with httpx.Client(timeout=15.0) as client:
        token: str | None = None
        for method, path, operation in _operations(document):
            examples = _request_examples(operation)
            if not examples:
                continue
            for name, value in examples.items():
                if path == "/v1/auth/login" and name == "typical":
                    response = client.post(_url(path), json=value)
                    token = _session_token(response)
                else:
                    response = client.request(
                        method.upper(),
                        _url(path),
                        json=value,
                        headers=_headers(operation, method, token),
                    )
                _check_response(
                    document,
                    operation,
                    response,
                    typical_success=name == "typical" and path in _SUCCESS_TYPICAL,
                )
        if token is None:
            pytest.fail("typical login did not run")
        cookie = {"Cookie": f"fs_session={token}"}
        for path in _READS:
            response = client.get(_url(path), headers=cookie, params={"limit": 2})
            assert response.status_code == 200, response.text
            operation = document["paths"][path.split("?", 1)[0]]["get"]
            _check_response(document, operation, response, typical_success=True)
        for path in ("/v1/stream/alerts", "/v1/stream/system"):
            with client.stream("GET", _url(path), headers=cookie, timeout=5.0) as stream:
                assert stream.status_code == 200
                assert stream.headers["content-type"].startswith("text/event-stream")
