"""Transport tracing: outgoing HTTP (requests) and database query spans.

Mirrors the Go SDK's sqltrace.go and outgoing-HTTP tracing wire-for-wire:

- ``instrument_requests`` wraps ``requests.Session.request`` (one session or
  the class) so every outgoing call emits an HTTP_CLIENT span named
  ``"GET api.example.com/orders"`` with ``http.method``/``http.url``
  metadata and an ``X-Dataflow-Trace-Id`` request header, so a downstream
  Dataflow-instrumented service joins the same trace.
- ``db_span`` wraps a block in a DB_QUERY span named ``"SELECT orders"``;
  the statement travels single-spaced and truncated to 200 characters under
  ``db.statement``. Bind parameter values are never captured.

Everything here is best-effort: with tracing disabled (no API key/endpoint,
or DATAFLOW_DISABLED) or when the span engine fails, no spans are produced
and instrumented calls behave exactly as they would without Dataflow.
"""

from __future__ import annotations

import functools
import re
import threading
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from .config import enabled
from .spans import (
    EVENT_DB_QUERY,
    EVENT_HTTP_CLIENT,
    Span,
    _current_span,
    start_span,
)

__all__ = ["db_span", "instrument_requests", "stmt_summary"]

TRACE_HEADER = "X-Dataflow-Trace-Id"
_TRACE_HEADER_LOWER = TRACE_HEADER.lower()

# requests.Session.request(method, url, params, data, headers, ...) — the
# positional slot of the headers argument.
_HEADERS_ARG = 4

_patch_lock = threading.Lock()


# -- outgoing HTTP (requests) ---------------------------------------------

def instrument_requests(session: Optional[Any] = None) -> Any:
    """Emit an HTTP_CLIENT span around every ``requests`` call.

        dataflow.instrument_requests()               # all Session instances
        dataflow.instrument_requests(session=my_session)  # one session

    Spans are named ``"GET api.example.com/orders"``, carry ``http.method``
    and ``http.url``, and the request gains an ``X-Dataflow-Trace-Id``
    header so downstream Dataflow services join the same trace. ``requests``
    is imported lazily; a clear ImportError is raised here only when it is
    not installed. Idempotent: patching twice wraps once. Returns the
    patched target (the session, or the ``requests.Session`` class).
    """
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_requests requires the 'requests' package; "
            "install it with: pip install requests"
        ) from exc

    if session is not None:
        original = session.request
        if getattr(original, "_dataflow_instrumented", False):
            return session

        @functools.wraps(original)
        def request_bound(*args, **kwargs):
            args = list(args)
            span = _trace_call(args, kwargs)
            try:
                response = original(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - the error itself propagates
                _finish_client_error(span, exc)
                raise
            _finish_client_ok(span, response)
            return response

        request_bound._dataflow_instrumented = True
        session.request = request_bound  # type: ignore[method-assign]
        return session

    with _patch_lock:
        original = requests.Session.request
        if getattr(original, "_dataflow_instrumented", False):
            return requests.Session

        @functools.wraps(original)
        def request_class(self, *args, **kwargs):
            args = list(args)
            span = _trace_call(args, kwargs)
            try:
                response = original(self, *args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - the error itself propagates
                _finish_client_error(span, exc)
                raise
            _finish_client_ok(span, response)
            return response

        request_class._dataflow_instrumented = True
        requests.Session.request = request_class  # type: ignore[method-assign]
        return requests.Session


def _trace_call(args: List[Any], kwargs: Dict[str, Any]) -> Optional[Span]:
    """Open the HTTP_CLIENT span for a Session.request call and inject the
    trace id header. Returns the span, or None when disabled or the span
    engine failed (tracing is silent and never blocks the caller)."""
    if not enabled():
        return None
    try:
        method = str(kwargs.get("method") or (args[0] if args else "") or "GET").upper()
        url = str(kwargs.get("url") or (args[1] if len(args) > 1 else "") or "")
        span = _start_client_span(method, url)
        _inject_trace_header(args, kwargs, span.trace_id)
        return span
    except Exception:  # noqa: BLE001 - best-effort
        return None


def _start_client_span(method: str, url: str) -> Span:
    parts = urlsplit(url)
    host = parts.hostname or ""
    span = start_span(f"{method} {host}{parts.path or '/'}", EVENT_HTTP_CLIENT)
    span.set_attr("http.method", method)
    span.set_attr("http.url", url)
    span.ev.callee_package = host
    return span


def _inject_trace_header(args: List[Any], kwargs: Dict[str, Any], trace_id: str) -> None:
    """Attach X-Dataflow-Trace-Id without mutating caller-owned mappings."""
    if len(args) > _HEADERS_ARG:
        headers = args[_HEADERS_ARG]
    else:
        headers = kwargs.get("headers")
    if headers is None:
        merged: Any = {TRACE_HEADER: trace_id}
    elif hasattr(headers, "items"):
        merged = dict(headers)
        for key in [k for k in merged if str(k).lower() == _TRACE_HEADER_LOWER]:
            del merged[key]
        merged[TRACE_HEADER] = trace_id
    else:
        return  # exotic header container: skip injection, still traced
    if len(args) > _HEADERS_ARG:
        args[_HEADERS_ARG] = merged
    else:
        kwargs["headers"] = merged


def _finish_client_ok(span: Optional[Span], response: Any) -> None:
    if span is None:
        return
    try:
        span.set_status(int(getattr(response, "status_code", 0) or 0))
        span.end()
    except Exception:  # noqa: BLE001
        pass


def _finish_client_error(span: Optional[Span], exc: Exception) -> None:
    if span is None:
        return
    try:
        span.record_error(exc)
        span.end()
    except Exception:  # noqa: BLE001
        pass


# -- database queries ------------------------------------------------------

def db_span(system: str, statement: str, params: Any = None) -> "_DbSpanCtx":
    """Context manager emitting one DB_QUERY span around the block:

        with dataflow.db_span("postgres", "SELECT * FROM orders WHERE id = %s", params=[order_id]):
            cursor.execute(sql, params)

    The span is named ``"SELECT orders"`` (verb + first table reference),
    ``callee_package`` is the db system ("postgres", "mysql", "sqlite",
    "redis", "mongo"), and metadata carries ``db.system`` plus the
    single-spaced statement truncated to 200 characters in ``db.statement``.
    ``params`` is accepted for call-site convenience; parameter VALUES are
    never read, sent or logged. Errors set status 500 and are recorded.
    """
    return _DbSpanCtx(system, statement)


class _DbSpanCtx:
    """trace()-style context manager: installs the span as the current span
    (nested spans parent to it) and ends it exactly once."""

    def __init__(self, system: str, statement: str):
        self._system = system
        self._statement = statement
        self._span: Optional[Span] = None
        self._token: Any = None
        if enabled():
            try:
                span = start_span(stmt_summary(statement), EVENT_DB_QUERY)
                span.ev.callee_package = system
                span.set_attr("db.system", system)
                clipped = _clip_statement(statement)
                if clipped:
                    span.set_attr("db.statement", clipped)
                self._span = span
            except Exception:  # noqa: BLE001 - best-effort
                self._span = None

    def __enter__(self) -> Any:
        span = self._span
        if span is None:
            return _INACTIVE_SPAN
        self._token = _current_span.set(span)
        return span

    def __exit__(self, exc_type, exc, tb) -> bool:
        span = self._span
        if span is None:
            return False
        _current_span.reset(self._token)
        try:
            if exc is not None:
                span.record_error(exc)
                span.set_status(500)
            else:
                span.set_status(200)
        except Exception:  # noqa: BLE001
            pass
        try:
            span.end()
        except Exception:  # noqa: BLE001
            pass
        return False


class _InactiveSpan:
    """Stand-in yielded when tracing is disabled or the span engine failed;
    every mutator is a no-op so caller code paths stay identical."""

    trace_id = ""
    span_id = ""
    ev = None

    def set_attr(self, key: str, value: str) -> "_InactiveSpan":
        return self

    def set_data(self, key: str, value: Any) -> "_InactiveSpan":
        return self

    def set_status(self, code: int) -> "_InactiveSpan":
        return self

    def record_error(self, err: Any) -> "_InactiveSpan":
        return self

    def end(self) -> None:
        return None


_INACTIVE_SPAN = _InactiveSpan()


def _clip_statement(statement: str) -> str:
    """Single-spaced statement text, truncated to 200 chars."""
    return " ".join((statement or "").split())[:200]


# Statement summary — mirrors Go sqltrace.go stmtSummary wire-for-wire.
_STMT_VERB_RE = re.compile(
    r"^\s*\(?\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH"
    r"|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN)\b",
    re.IGNORECASE | re.DOTALL,
)
_STMT_TABLE_RE = re.compile(
    r"\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?[`\"'\[]?"
    r"([A-Za-z_][\w$.]*)",
    re.IGNORECASE | re.DOTALL,
)


def stmt_summary(statement: str) -> str:
    """Short human name for a statement: the verb plus the first table
    reference when one exists ("SELECT orders", "INSERT users"); bare verbs
    and non-SQL fall back to the first word uppercased ("QUERY" when there
    is none). Schema-qualified names ("public.items") report the bare table
    and "IF [NOT] EXISTS" is skipped."""
    one = " ".join((statement or "").split())
    m = _STMT_VERB_RE.match(one)
    if m is None:
        i = _first_break(one)
        if i > 0:
            return one[:i].upper()
        return "QUERY"
    verb = m.group(1).upper()
    t = _STMT_TABLE_RE.search(one)
    if t is None:
        return verb
    table = t.group(1)
    cut = max(table.rfind("."), table.rfind("$"))
    if cut >= 0:
        table = table[cut + 1:]
    return f"{verb} {table}"


def _first_break(one: str) -> int:
    breaks = [i for i in (one.find(" "), one.find("(")) if i >= 0]
    return min(breaks) if breaks else -1
