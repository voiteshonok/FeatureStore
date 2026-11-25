"""Outbound timeouts and retries.

GET is tried three times (the call, then two retries) with 0.1 s and 0.4 s
between attempts. POST /v1/features retries once after 0.05 s, and only when
the first attempt raises ConnectError: the request never left the gateway.
Other POST and PATCH calls are a single attempt. Read timeouts are not
retried on that POST. A timeout becomes 504; any other transport failure
becomes 503. Any HTTP response, including 4xx and 5xx, is returned unchanged.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from types import TracebackType

import httpx
from starlette.requests import Request
from starlette.responses import Response

from gateway.metrics import GatewayMetrics

TIMEOUT = httpx.Timeout(connect=0.5, read=2.0, write=2.0, pool=0.5)
# SSE (ADR-021): keep the connect bound, never time out the open stream.
STREAM_TIMEOUT = httpx.Timeout(connect=0.5, read=None, write=2.0, pool=0.5)
_GET_BACKOFF_S = (0.1, 0.4)
_CONNECT_RETRY_S = 0.05
_DROP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-length",
        "content-encoding",
        "x-request-id",
    }
)
_STREAM_DROP_HEADERS = _DROP_HEADERS | frozenset({"content-length", "content-encoding"})


def new_client() -> httpx.AsyncClient:
    """Client used for upstream calls and readiness probes."""
    return httpx.AsyncClient(timeout=TIMEOUT)


async def pause(seconds: float) -> None:
    """Wait between idempotent retries. Tests replace this."""
    await asyncio.sleep(seconds)


def passthrough(upstream: httpx.Response, request_id: str) -> Response:
    """Copy status and body. The gateway request id replaces the upstream one."""
    headers: dict[str, str] = {}
    for key, value in upstream.headers.items():
        if key.lower() in _DROP_HEADERS:
            continue
        headers[key] = value
    headers["X-Request-ID"] = request_id
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=headers,
    )


def _target(base_url: str, request: Request) -> str:
    url = base_url.rstrip("/") + request.url.path
    if request.url.query:
        return f"{url}?{request.url.query}"
    return url


def _forward_headers(request: Request, request_id: str) -> dict[str, str]:
    headers = {"X-Request-ID": request_id}
    content_type = request.headers.get("content-type")
    if content_type is not None:
        headers["content-type"] = content_type
    last_event_id = request.headers.get("last-event-id")
    if last_event_id is not None and last_event_id.strip() != "":
        headers["Last-Event-ID"] = last_event_id.strip()
    # Operator identity comes only from a verified session (ADR-020). Never copy
    # client-supplied X-Operator-* headers.
    operator_id = getattr(request.state, "operator_id", None)
    operator_role = getattr(request.state, "operator_role", None)
    if isinstance(operator_id, str) and operator_id:
        headers["X-Operator-Id"] = operator_id
    if isinstance(operator_role, str) and operator_role:
        headers["X-Operator-Role"] = operator_role
    return headers


def _request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    if isinstance(value, str) and value:
        return value
    return ""


async def forward(
    client: httpx.AsyncClient,
    request: Request,
    *,
    base_url: str,
    service: str,
    retry_on_connect: bool = False,
) -> Response:
    """Send the request upstream; timeouts → 504, other transport errors → 503."""
    from gateway.errors import problem_response

    request_id = _request_id(request)
    get_retries = request.method == "GET"
    if get_retries:
        attempts = 1 + len(_GET_BACKOFF_S)
    elif retry_on_connect:
        attempts = 2
    else:
        attempts = 1
    body = await request.body()
    url = _target(base_url, request)
    headers = _forward_headers(request, request_id)
    last_error: httpx.TransportError | None = None
    for attempt in range(attempts):
        if attempt:
            delay = _GET_BACKOFF_S[attempt - 1] if get_retries else _CONNECT_RETRY_S
            await pause(delay)
        try:
            upstream = await client.request(
                request.method,
                url,
                content=body if body else None,
                headers=headers,
            )
        except httpx.TransportError as exc:
            last_error = exc
            _count_upstream_error(request, service, exc)
            if not get_retries and not isinstance(exc, httpx.ConnectError):
                break
        else:
            return passthrough(upstream, request_id)
    _log_failure(service, request, request_id, last_error)
    timed_out = isinstance(last_error, httpx.TimeoutException)
    if timed_out:
        return problem_response(
            request,
            type="upstream_timeout",
            title=f"{service} timeout",
            status=504,
        )
    return problem_response(
        request,
        type="upstream_unavailable",
        title=f"{service} unavailable",
        status=503,
    )


async def forward_stream(
    client: httpx.AsyncClient,
    request: Request,
    *,
    base_url: str,
    service: str,
    close_after_s: float | None = None,
) -> Response:
    """Proxy an SSE stream: no read timeout, no retries (ADR-021).

    ``close_after_s`` ends the stream when the operator JWT expires so the
    client reconnects and receives 401 if the session is gone.
    """
    from starlette.responses import StreamingResponse

    from gateway.errors import problem_response

    request_id = _request_id(request)
    url = _target(base_url, request)
    headers = _forward_headers(request, request_id)
    try:
        upstream = await client.send(
            client.build_request("GET", url, headers=headers, timeout=STREAM_TIMEOUT),
            stream=True,
        )
    except httpx.TimeoutException as exc:
        _count_upstream_error(request, service, exc)
        _log_failure(service, request, request_id, exc)
        return problem_response(
            request,
            type="upstream_timeout",
            title=f"{service} timeout",
            status=504,
        )
    except httpx.TransportError as exc:
        _count_upstream_error(request, service, exc)
        _log_failure(service, request, request_id, exc)
        return problem_response(
            request,
            type="upstream_unavailable",
            title=f"{service} unavailable",
            status=503,
        )

    if upstream.status_code >= 400:
        body = await upstream.aread()
        await upstream.aclose()
        out_headers: dict[str, str] = {"X-Request-ID": request_id}
        content_type = upstream.headers.get("content-type")
        if content_type is not None:
            out_headers["content-type"] = content_type
        return Response(content=body, status_code=upstream.status_code, headers=out_headers)

    response_headers: dict[str, str] = {"X-Request-ID": request_id}
    for key, value in upstream.headers.items():
        if key.lower() in _STREAM_DROP_HEADERS:
            continue
        response_headers[key] = value

    async def body_iter() -> AsyncIterator[bytes]:
        loop = asyncio.get_running_loop()
        deadline = None if close_after_s is None else loop.time() + close_after_s
        try:
            try:
                stream = upstream.aiter_bytes()
            except httpx.StreamConsumed:
                yield upstream.content
                return
            async for chunk in stream:
                if deadline is not None and loop.time() >= deadline:
                    break
                if await request.is_disconnected():
                    break
                yield chunk
        except httpx.StreamConsumed:
            if upstream.content:
                yield upstream.content
        finally:
            await upstream.aclose()

    return StreamingResponse(
        body_iter(),
        status_code=upstream.status_code,
        headers=response_headers,
        media_type=upstream.headers.get("content-type", "text/event-stream"),
    )


def _count_upstream_error(request: Request, service: str, exc: httpx.TransportError) -> None:
    metrics: GatewayMetrics = request.app.state.metrics
    kind = "timeout" if isinstance(exc, httpx.TimeoutException) else "connect"
    metrics.upstream_errors.labels(service=service, kind=kind).inc()


def _log_failure(
    service: str,
    request: Request,
    request_id: str,
    error: BaseException | None,
) -> None:
    logging.getLogger("gateway").warning(
        "%s unavailable",
        service,
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
        },
        exc_info=_exc_info(error),
    )


def _exc_info(
    error: BaseException | None,
) -> tuple[type[BaseException], BaseException, TracebackType | None] | None:
    if error is None:
        return None
    return (type(error), error, error.__traceback__)
