"""Spans: the core tracing engine.

Parent links ride on contextvars, so nested ``dataflow.trace`` blocks and
decorated functions join the enclosing HTTP trace automatically, including
across ``await`` points.
"""

from __future__ import annotations

import contextvars
import inspect
import json
import random
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional, TypeVar

from . import crypto
from .config import enabled, settings
from .pii import classify_pii

try:  # generated at build time (see sdk-python/Dockerfile)
    from .proto_gen import dataflow_pb2 as pb
except ImportError:  # pragma: no cover - allows importing helpers without a build
    pb = None  # type: ignore

_current_span: contextvars.ContextVar = contextvars.ContextVar("dataflow_span", default=None)
_envelope_lock = threading.Lock()
_envelope = {"key": b"", "salt": b"", "salt_hex": ""}

T = TypeVar("T")

EVENT_HTTP_SERVER = "HTTP_SERVER"
EVENT_HTTP_CLIENT = "HTTP_CLIENT"
EVENT_FUNC_CALL = "FUNCTION_CALL"
EVENT_GRPC = "GRPC"


def new_id() -> str:
    return uuid.uuid4().hex[:16]


def should_sample() -> bool:
    ratio = settings().sample_ratio
    return ratio >= 1 or random.random() < ratio


def encryption_envelope() -> Dict[str, Any]:
    """Derive the payload key once per process; returns {} when unset."""
    with _envelope_lock:
        if not _envelope["salt_hex"] and settings().encryption_key:
            salt = crypto.new_salt()
            _envelope["key"] = crypto.derive_key(settings().encryption_key, salt)
            _envelope["salt"] = salt
            _envelope["salt_hex"] = salt.hex()
        return dict(_envelope)


def _package_of(frame) -> str:
    mod = frame.f_globals.get("__name__", "")
    if not mod:
        return ""
    # warehouse.inventory.check -> "warehouse" (root package)
    return mod.split(".")[0]


def _is_sdk_package(pkg: str) -> bool:
    return pkg in ("dataflow", "")


def _label_package(name: str) -> str:
    """'warehouse.Reserve' -> 'warehouse'; route labels have no package."""
    if name and " " not in name and "/" not in name and "." in name:
        return name.rsplit(".", 1)[0]
    return ""


class Span:
    """One measured unit of work. Call end() exactly once (the trace()
    helpers do it for you)."""

    def __init__(self, name: str, event_type: str = EVENT_FUNC_CALL, parent: Optional["Span"] = None):
        s = settings()
        self._lock = threading.Lock()
        self._start = time.time()
        self._ended = False
        self._sampled = should_sample()
        self._payload: Dict[str, Any] = {}
        trace_id = parent.trace_id if parent else new_id()
        self.ev = pb.TraceEvent(  # type: ignore[union-attr]
            event_id=new_id(),
            timestamp=int(self._start * 1000),
            type=pb.EVENT_TYPE_FUNCTION_CALL,  # type: ignore[union-attr]
            name=name,
            service_name=s.service_name,
            trace_id=trace_id,
            span_id=new_id(),
            parent_span_id=parent.span_id if parent else "",
        )
        self.set_type(event_type)
        # Package-boundary attribution: a "pkg.Func"-style label pins the
        # callee directly (like the Go SDK parses its qualified names); the
        # caller is the nearest foreign root package on the stack.
        callee = _label_package(name)
        caller = ""
        frame = sys._getframe(2)  # caller of start_span (trace() or user code)
        while frame is not None:
            pkg = _package_of(frame)
            if pkg and not _is_sdk_package(pkg) and pkg != callee:
                caller = pkg
                break
            if not callee and pkg and not _is_sdk_package(pkg):
                callee = pkg
            frame = frame.f_back
        self.ev.callee_package = callee
        # Caller stays empty on roots: caller==callee self-edges would
        # corrupt the flow graph layering.
        self.ev.caller_package = caller

    # -- attributes -------------------------------------------------------
    def set_type(self, event_type: str) -> "Span":
        with self._lock:
            mapping = {
                EVENT_HTTP_SERVER: pb.EVENT_TYPE_HTTP_SERVER,
                EVENT_HTTP_CLIENT: pb.EVENT_TYPE_HTTP_CLIENT,
                EVENT_GRPC: pb.EVENT_TYPE_GRPC,
                EVENT_FUNC_CALL: pb.EVENT_TYPE_FUNCTION_CALL,
            }
            self.ev.type = mapping.get(event_type, pb.EVENT_TYPE_FUNCTION_CALL)
        return self

    def set_attr(self, key: str, value: str) -> "Span":
        with self._lock:
            self.ev.metadata[key] = str(value)
        return self

    def set_data(self, key: str, value: Any) -> "Span":
        with self._lock:
            self._payload[key] = _jsonable(value)
        return self

    def record_error(self, err: BaseException | str) -> "Span":
        with self._lock:
            self.ev.error_message = str(err)
        return self

    def set_status(self, code: int) -> "Span":
        with self._lock:
            self.ev.status_code = code
        return self

    @property
    def trace_id(self) -> str:
        return self.ev.trace_id

    @property
    def span_id(self) -> str:
        return self.ev.span_id

    # -- lifecycle --------------------------------------------------------
    def end(self) -> None:
        if self._ended or not self._sampled:
            self._ended = True
            return
        self._ended = True
        with self._lock:
            ev = self.ev
            ev.duration_ms = int((time.time() - self._start) * 1000)
            payload = self._payload
        if payload:
            _attach_payload(ev, payload)
        from .client import enqueue

        enqueue(ev)

    def __enter__(self) -> "Span":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            self.record_error(exc)
        self.end()
        return False


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def _attach_payload(ev, payload: Dict[str, Any]) -> None:
    # Field-name lineage: key names (never values) travel as plaintext
    # metadata even when payload values are encrypted. PII categories are
    # classified client-side the same way.
    if payload:
        keys = sorted(payload.keys())
        ev.metadata["data.fields"] = ",".join(keys)
        pii = classify_pii(keys)
        if pii:
            ev.metadata["data.pii"] = pii
    raw = json.dumps(payload, default=repr).encode("utf-8")
    envelope = encryption_envelope()
    if not envelope.get("salt_hex"):
        ev.payload.encrypted = False
        ev.payload.data = raw
        return
    ct, iv = crypto.encrypt(envelope["key"], raw)
    ev.payload.encrypted = True
    ev.payload.data = ct
    ev.payload.iv = iv
    ev.payload.key_salt = envelope["salt_hex"]


def start_span(name: str, event_type: str = EVENT_FUNC_CALL) -> Span:
    """Open a child of the currently active span (if any)."""
    parent = _current_span.get()
    span = Span(name, event_type=event_type, parent=parent)
    return span


def span_from_context(_ignored: Any = None) -> Optional[Span]:
    """Compatibility shim mirroring the Go API: the active span *is* the
    context in Python (contextvars)."""
    return _current_span.get()


def current_span() -> Optional[Span]:
    return _current_span.get()


def _run_child(span: Span) -> None:
    _current_span.set(span)


def trace(name: str) -> Span:
    """Context manager tracing a block:

        with dataflow.trace("warehouse.Reserve") as span:
            span.set_data("sku", sku)
            ...
    """
    span = start_span(name)

    class _Ctx:
        def __enter__(self_inner) -> Span:
            self_inner._token = _current_span.set(span)
            return span

        def __exit__(self_inner, exc_type, exc, tb) -> bool:
            _current_span.reset(self_inner._token)
            if exc is not None:
                span.record_error(exc)
            span.end()
            return False

    return _Ctx()


F = TypeVar("F", bound=Callable[..., Any])


def traced(name: Optional[str] = None) -> Callable[[F], F]:
    """Decorator flavour of trace(); sync and async functions both work."""

    def decorate(fn: F) -> F:
        label = name or f"{fn.__module__.split('.')[0]}.{fn.__qualname__}"

        if inspect.iscoroutinefunction(fn):

            async def async_wrapper(*args, **kwargs):
                with trace(label) as span:
                    return await fn(*args, **kwargs)

            return async_wrapper  # type: ignore[return-value]

        def sync_wrapper(*args, **kwargs):
            with trace(label) as span:
                return fn(*args, **kwargs)

        return sync_wrapper  # type: ignore[return-value]

    return decorate
