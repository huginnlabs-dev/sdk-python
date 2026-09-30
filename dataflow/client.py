"""gRPC streaming delivery: a background sender thread keeps one bidi
stream alive, replays everything above the server's ack watermark after a
reconnect, and drops the oldest buffered events on overflow."""

from __future__ import annotations

import logging
import threading
import time
from typing import Iterator, Optional

import grpc

from .buffer import EventBuffer
from .config import enabled, settings
from .manifest import send_manifest
from .spans import encryption_envelope

try:
    from .proto_gen import dataflow_pb2 as pb
    from .proto_gen import dataflow_pb2_grpc as pb_grpc
except ImportError:  # pragma: no cover
    pb = None  # type: ignore
    pb_grpc = None  # type: ignore

log = logging.getLogger("dataflow")

SEND_WINDOW = 64

_state_lock = threading.Lock()
_started = False
_buf: Optional[EventBuffer] = None
_acked = 0
_wake = threading.Event()


def _acked_value() -> int:
    return _acked  # single-writer (sender thread) updates; int reads are atomic


def ensure_started() -> None:
    """Idempotently start the background sender when configured."""
    global _started, _buf
    if _started or not enabled():
        if not _started and not enabled():
            s = settings()
            if not s.disabled and (not s.api_key or not s.endpoint):
                log.warning(
                    "dataflow: DATAFLOW_API_KEY/DATAFLOW_ENDPOINT not set; SDK stays passive"
                )
        return
    with _state_lock:
        if _started:
            return
        s = settings()
        _buf = EventBuffer(s.buffer_size)
        if not s.encryption_key:
            log.warning("dataflow: warning: no encryption key set; payloads are sent as plaintext")
        encryption_envelope()  # derive once up front
        thread = threading.Thread(target=_run, name="dataflow-sender", daemon=True)
        thread.start()
        _started = True
        # Report the service manifest (framework + dependency inventory)
        # once; best-effort, independent of the tracing pipeline.
        send_manifest()


def enqueue(ev) -> None:
    """Entry point from Span.end into the delivery path."""
    ensure_started()
    if _buf is None:
        return
    _buf.add(ev)
    _wake.set()


def _run() -> None:
    backoff = 1.0
    while True:
        try:
            _stream_once()
            backoff = 1.0
        except Exception as exc:  # noqa: BLE001 - keep the sender alive forever
            log.warning("dataflow: ingest stream error: %s; retrying in %.1fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def _stream_once() -> None:
    s = settings()
    channel = grpc.insecure_channel(s.endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        stub = pb_grpc.DataflowServiceStub(channel)

        def outgoing() -> Iterator:
            while True:
                pending = _buf.after(_acked_value())
                if not pending:
                    _wake.wait(0.5)
                    _wake.clear()
                    continue
                last = pending[0].seq
                for ev in pending[:SEND_WINDOW]:
                    yield ev
                    last = ev.seq
                deadline = time.time() + 10
                while _acked_value() < last and time.time() < deadline:
                    time.sleep(0.02)

        responses = stub.StreamEvents(
            outgoing(), metadata=(("x-api-key", s.api_key),)
        )
        for ack in responses:
            global _acked
            if ack.last_seq > _acked:
                _acked = ack.last_seq
                _buf.acked(_acked)
    finally:
        channel.close()


def http_client(**kwargs):
    """An httpx.Client whose requests become HTTP_CLIENT spans and carry
    the trace id, so service-to-service calls join one trace.

        with dataflow.http_client() as client:
            client.post("http://other/api", json={...})
    """
    import httpx

    client = httpx.Client(**kwargs)
    original = client.build_request

    def build_request(*args, **kwargs):
        request = original(*args, **kwargs)
        from .spans import current_span

        active = current_span()
        if active is not None:
            request.headers["X-Dataflow-Trace-Id"] = active.trace_id
        return request

    client.build_request = build_request  # type: ignore[method-assign]

    def _span_request(request: httpx.Request):
        from .spans import start_span

        span = start_span(f"{request.method} {request.url.host}{request.url.path}", "HTTP_CLIENT")
        span.set_attr("http.method", request.method)
        span.set_attr("http.url", str(request.url))
        span.ev.callee_package = request.url.host
        request.extensions["dataflow_span"] = span
        request.extensions["dataflow_t0"] = time.time()

    def _span_response(response: httpx.Response):
        request = response.request
        span: Optional[object] = request.extensions.get("dataflow_span")
        if span is None:
            return
        span.set_status(response.status_code)
        span.end()

    def _span_error(exc: Exception):
        request = getattr(exc, "request", None)
        if request is None:
            return
        span = request.extensions.get("dataflow_span")
        if span is not None:
            span.record_error(exc)
            span.end()

    client.event_hooks["request"] = [_span_request]
    client.event_hooks["response"] = [_span_response]
    return client
