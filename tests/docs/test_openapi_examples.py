"""Every exported operation carries the examples AD-2 requires."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
OPENAPI = ROOT / "docs" / "openapi"
METHODS = {"get", "post", "put", "patch", "delete"}
# Handlers that return only 2xx/5xx. Redocly ignores operation-4xx-response
# for the same paths (see redocly.yaml).
NO_4XX = {
    "gateway.json": {("get", "/health"), ("get", "/ready")},
    "features.json": {("get", "/health"), ("get", "/ready")},
    "ingestion.json": {("get", "/health"), ("get", "/ready")},
    "detection.json": {
        ("get", "/health"),
        ("get", "/ready"),
        ("get", "/v1/rules"),
    },
}
PROBLEM_TYPES = {
    "validation_error",
    "unauthorized",
    "forbidden",
    "not_found",
    "invalid_transition",
    "conflict",
    "rate_limited",
    "upstream_unavailable",
    "upstream_timeout",
    "internal_error",
    "http_error",
    "bad_request",
}


def _documents() -> list[tuple[str, dict[str, object]]]:
    found = sorted(OPENAPI.glob("*.json"))
    assert found, "docs/openapi is empty; run make openapi"
    loaded: list[tuple[str, dict[str, object]]] = []
    for path in found:
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            pytest.fail(f"{path.name} is not an object")
        loaded.append((path.name, document))
    return loaded


def _pointer(document: dict[str, object], ref: str) -> object:
    if not ref.startswith("#/"):
        pytest.fail(f"unsupported ref {ref}")
    node: object = document
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            pytest.fail(f"dangling ref {ref}")
        node = node[part]
    return node


def _has_example(node: object) -> bool:
    if not isinstance(node, dict):
        return False
    if "example" in node or "examples" in node:
        return True
    headers = node.get("headers")
    if isinstance(headers, dict) and any(_has_example(header) for header in headers.values()):
        return True
    content = node.get("content")
    if isinstance(content, dict) and any(_has_example(media) for media in content.values()):
        return True
    return False


def _response_has_example(document: dict[str, object], response: object) -> bool:
    if isinstance(response, dict) and "$ref" in response:
        ref = response["$ref"]
        if isinstance(ref, str):
            response = _pointer(document, ref)
    return _has_example(response)


def _operations(document: dict[str, object]) -> list[tuple[str, str, dict[str, object]]]:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        return []
    found: list[tuple[str, str, dict[str, object]]] = []
    for path, path_item in paths.items():
        if not isinstance(path, str) or not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in METHODS or not isinstance(operation, dict):
                continue
            found.append((method, path, operation))
    return found


_DOCS = _documents()


@pytest.mark.parametrize("name,document", _DOCS, ids=[name for name, _document in _DOCS])
def test_operations_have_examples(name: str, document: dict[str, object]) -> None:
    _ = name
    for method, path, operation in _operations(document):
        body = operation.get("requestBody")
        if isinstance(body, dict):
            content = body.get("content")
            assert isinstance(content, dict), f"{method} {path} request body has no content"
            media = content.get("application/json")
            assert isinstance(media, dict), f"{method} {path} request is not JSON"
            examples = media.get("examples")
            assert isinstance(examples, dict), f"{method} {path} needs named request examples"
            assert "typical" in examples and "edge" in examples

        responses = operation.get("responses")
        assert isinstance(responses, dict)
        success = [
            response
            for code, response in responses.items()
            if isinstance(code, str) and code.startswith("2")
        ]
        assert success, f"{method} {path} has no 2xx"
        assert any(_response_has_example(document, response) for response in success), (
            f"{method} {path} 2xx has no example"
        )
        if (method, path) in NO_4XX.get(name, set()):
            continue
        failures = [
            response
            for code, response in responses.items()
            if isinstance(code, str) and code.startswith("4")
        ]
        assert failures, f"{method} {path} has no 4xx"
        assert any(_response_has_example(document, response) for response in failures), (
            f"{method} {path} 4xx has no example"
        )


def test_gateway_problem_examples_cover_the_taxonomy() -> None:
    document = json.loads((OPENAPI / "gateway.json").read_text(encoding="utf-8"))
    responses = document["components"]["responses"]
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            type_ = node.get("type")
            status = node.get("status")
            if isinstance(type_, str) and isinstance(status, int) and "title" in node:
                found.add(type_)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(responses)
    assert PROBLEM_TYPES <= found


def test_request_examples_match_the_models() -> None:
    from gateway.examples import (
        FEATURE_EDGE,
        FEATURE_TYPICAL,
        LOGIN_EDGE,
        LOGIN_TYPICAL,
        PATCH_ACKNOWLEDGE,
        PATCH_RESOLVE,
    )
    from gateway.models import AlertUpdate, LoginBody
    from ingestion.examples import PAYMENT_EDGE, PAYMENT_TYPICAL
    from schemas import FeatureRequest, PaymentIn

    for payload in (FEATURE_TYPICAL, FEATURE_EDGE):
        FeatureRequest.model_validate(payload)
    for payload in (LOGIN_TYPICAL, LOGIN_EDGE):
        LoginBody.model_validate(payload)
    for payload in (PATCH_ACKNOWLEDGE, PATCH_RESOLVE):
        AlertUpdate.model_validate(payload)
    for payload in (PAYMENT_TYPICAL, PAYMENT_EDGE):
        PaymentIn.model_validate(payload)


def test_feature_examples_agree_across_services() -> None:
    from features.examples import FEATURE_EDGE as features_edge
    from features.examples import FEATURE_RESPONSE_EXAMPLE as features_response
    from features.examples import FEATURE_TYPICAL as features_typical
    from gateway.examples import FEATURE_EDGE as gateway_edge
    from gateway.examples import FEATURE_RESPONSE_EXAMPLE as gateway_response
    from gateway.examples import FEATURE_TYPICAL as gateway_typical

    assert features_typical == gateway_typical
    assert features_edge == gateway_edge
    assert features_response == gateway_response
