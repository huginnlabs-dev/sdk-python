"""ASGI middleware for FastAPI / Starlette: wraps every request into an
HTTP_SERVER span with redacted headers, a body excerpt and the trace id
response header. Works for sync and async endpoints, streaming bodies
included — the original receive stream is siphoned, never consumed."""

from __future__ import annotations

import time
from typing import Iterable, Sequence, Tuple

from .agent import agent_attrs
from .config import enabled, settings
from .spans import start_span

# Headers whose values never leave the host process (mirrors the Go SDK).
REDACTED_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
}

TRACE_HEADER = "x-dataflow-trace-id"


class ASGIMiddleware:
    """Pure-ASGI middleware.

        app.add_middleware(dataflow.ASGIMiddleware)
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or not enabled():
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")
        span = start_span(f"{method} {path}", "HTTP_SERVER")
        for k, v in agent_attrs():
            span.set_attr(k, v)

        headers: Sequence[Tuple[bytes, bytes]] = scope.get("headers", [])
        self._capture_headers(span, headers)
        incoming_trace = self._header_value(headers, TRACE_HEADER.encode())
        if incoming_trace:
            span.ev.trace_id = incoming_trace

        # Siphon the request body as it flows through, capped at
        # max_body_bytes, without disturbing the app's view of the stream.
        captured = {"data": bytearray()}
        limit = settings().max_body_bytes

        async def receive_wrapper():
            message = await receive()
            if message["type"] == "http.request" and len(captured["data"]) < limit:
                captured["data"] += message.get("body", b"")[: max(0, limit - len(captured["data"]))]
            return message

        status_holder = {"status": 500}

        async def send_wrapper(message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                extended = list(message.get("headers", []))
                extended.append((TRACE_HEADER.encode(), span.trace_id.encode()))
                message = {**message, "headers": extended}
            await send(message)

        started = time.time()
        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        except Exception as exc:
            span.record_error(exc)
            span.set_attr("http.duration_ms", str(int((time.time() - started) * 1000)))
            span.end()
            raise

        duration_ms = int((time.time() - started) * 1000)
        status = status_holder["status"]
        span.set_status(status)
        span.set_attr("http.status_code", str(status))
        span.set_attr("http.method", method)
        span.set_attr("http.path", path)
        span.set_attr("http.duration_ms", str(duration_ms))
        if captured["data"]:
            try:
                excerpt = bytes(captured["data"]).decode("utf-8")
            except UnicodeDecodeError:
                excerpt = repr(bytes(captured["data"]))
            span.set_data("request", {"body_excerpt": excerpt, "truncated": len(captured["data"]) >= limit})
        if status >= 500:
            span.record_error(f"http {status}")
        span.end()

    # -- helpers ----------------------------------------------------------
    @staticmethod
    def _header_value(headers: Iterable[Tuple[bytes, bytes]], name: bytes) -> str:
        for key, value in headers:
            if key.lower() == name:
                return value.decode("latin-1", "replace")
        return ""

    @staticmethod
    def _capture_headers(span, headers: Sequence[Tuple[bytes, bytes]]) -> None:
        for key, value in headers:
            lower = key.decode("latin-1").lower()
            if lower in REDACTED_HEADERS:
                span.set_attr(f"http.header.{lower}", "[REDACTED]")
            else:
                span.set_attr(f"http.header.{lower}", value.decode("latin-1", "replace"))
