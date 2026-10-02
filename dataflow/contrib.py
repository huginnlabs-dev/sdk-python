"""Optional library integrations: SQLAlchemy, psycopg3, asyncpg, Django,
httpx, celery.

All of these ride the existing span pipeline: database calls emit DB_QUERY
spans through :func:`dataflow.transport.db_span` semantics (name from
:func:`dataflow.transport.stmt_summary` — ``"SELECT orders"`` —, ``db.system``
metadata, single-spaced statement truncated to 200 characters in
``db.statement``, bind parameter values never captured), HTTP requests emit
HTTP_SERVER / HTTP_CLIENT spans like ``ASGIMiddleware`` /
``instrument_requests`` do, and tasks emit FUNCTION_CALL spans. Nested spans
parent to them automatically via the usual contextvars.

- :func:`instrument_sqlalchemy` listens on the Engine's
  ``before_cursor_execute`` / ``after_cursor_execute`` / ``handle_error``
  events, so every statement executed through the engine (Core or ORM,
  sync or async) is traced. Idempotent per engine; undo with
  :func:`uninstrument_sqlalchemy`.
- :func:`instrument_psycopg` wraps ``Connection.execute`` (psycopg 3), or a
  pool's ``getconn`` so every checked-out connection is instrumented.
- :func:`instrument_asyncpg` wraps ``execute`` / ``fetch`` / ``fetchrow`` /
  ``fetchval`` on a connection, or the pool's ``acquire`` context manager.
- :func:`instrument_httpx` attaches event hooks to a given ``httpx.Client``
  / ``AsyncClient`` (or patches the classes globally) so every outgoing call
  emits an HTTP_CLIENT span and carries the ``X-Dataflow-Trace-Id`` header;
  the SDK's own ingest endpoints are never traced. Undo with
  :func:`restore_httpx`.
- :func:`instrument_celery` connects celery's ``task_prerun`` /
  ``task_postrun`` / ``task_failure`` signals so every task becomes a
  FUNCTION_CALL span; undo with :func:`uninstrument_celery`.
- :class:`DataflowMiddleware` is a Django new-style request middleware
  emitting one HTTP_SERVER span per request, named after
  ``request.resolver_match.route`` when URL resolution has produced one.

Third-party packages are imported lazily — a missing library raises a clear
ImportError only when its instrument function is called. Everything is
best-effort: with tracing disabled (no API key/endpoint, or
DATAFLOW_DISABLED) the hooks install but produce no spans and the
instrumented calls behave exactly as they would without Dataflow. Signal
receivers and wrappers never raise, so the remaining handlers in a chain
(other celery receivers, user event hooks) always still run.
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
import traceback
import weakref
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlsplit

from .agent import agent_attrs
from .config import enabled
from .crash import MESSAGE_MAX_CHARS, STACK_ATTR, _clip_stack
from .spans import (
    EVENT_FUNC_CALL,
    EVENT_HTTP_CLIENT,
    EVENT_HTTP_SERVER,
    _current_span,
    start_span,
)
from .transport import TRACE_HEADER, db_span

__all__ = [
    "instrument_sqlalchemy",
    "uninstrument_sqlalchemy",
    "instrument_psycopg",
    "instrument_asyncpg",
    "instrument_httpx",
    "restore_httpx",
    "instrument_celery",
    "uninstrument_celery",
    "DataflowMiddleware",
]

DB_SYSTEM_SQLALCHEMY = "sqlalchemy"
DB_SYSTEM_POSTGRES = "postgres"


def _sqlalchemy_import_error() -> ImportError:
    return ImportError(
        "dataflow.instrument_sqlalchemy requires the 'sqlalchemy' package; "
        "install it with: pip install sqlalchemy"
    )


# -- SQLAlchemy (Engine events) ----------------------------------------------

# id(engine) -> (weakref.ref(engine), before, after, on_error). Keyed by id
# per the instrumentation contract; the weakref detects id reuse after an
# uninstrumented engine was garbage collected.
_sqla_lock = threading.Lock()
_sqla_registry: Dict[int, Tuple[Any, ...]] = {}


def instrument_sqlalchemy(target: Any) -> Any:
    """Trace every statement executed through a SQLAlchemy Engine.

        dataflow.instrument_sqlalchemy(engine)              # an Engine
        dataflow.instrument_sqlalchemy(sessionmaker)        # or its factory
        dataflow.uninstrument_sqlalchemy(engine)            # undo

    Listens on ``before_cursor_execute`` / ``after_cursor_execute`` /
    ``handle_error``: one DB_QUERY span per statement, named via the SQL
    summary ("SELECT users"), ``db.system="sqlalchemy"``,
    ``db.dialect=<engine.dialect.name>``, the single-spaced statement
    truncated to 200 characters in ``db.statement`` — bind parameter values
    are never captured. Errors record status 500 + the exception and
    re-raise. Idempotent per engine (a sessionmaker resolves to its bound
    engine). Returns the passed-in target.
    """
    try:
        import sqlalchemy
    except ImportError as exc:
        raise _sqlalchemy_import_error() from exc

    engine = _resolve_sqlalchemy_engine(target)
    key = id(engine)
    with _sqla_lock:
        entry = _sqla_registry.get(key)
        if entry is not None:
            if entry[0]() is engine:
                return target  # already instrumented: a no-op
            del _sqla_registry[key]  # stale id: the old engine was GC'd
        before, after, on_error = _make_sqlalchemy_hooks(engine)
        sqlalchemy.event.listen(engine, "before_cursor_execute", before)
        sqlalchemy.event.listen(engine, "after_cursor_execute", after)
        sqlalchemy.event.listen(engine, "handle_error", on_error)
        _sqla_registry[key] = (weakref.ref(engine), before, after, on_error)
    return target


def uninstrument_sqlalchemy(target: Any) -> Any:
    """Remove the Dataflow listeners previously installed on an engine (or a
    sessionmaker bound to it). No-op when nothing is installed."""
    try:
        import sqlalchemy
    except ImportError as exc:
        raise _sqlalchemy_import_error() from exc

    engine = _resolve_sqlalchemy_engine(target)
    with _sqla_lock:
        entry = _sqla_registry.pop(id(engine), None)
    if entry is None or entry[0]() is not engine:
        return target
    _, before, after, on_error = entry
    for event_name, handler in (
        ("before_cursor_execute", before),
        ("after_cursor_execute", after),
        ("handle_error", on_error),
    ):
        try:
            sqlalchemy.event.remove(engine, event_name, handler)
        except Exception:  # noqa: BLE001 - uninstrument is best-effort
            pass
    return target


def _resolve_sqlalchemy_engine(target: Any) -> Any:
    """Engine directly, or the engine bound to a sessionmaker /
    scoped_session factory. Raises ValueError for anything else."""
    factory = getattr(target, "session_factory", None)  # scoped_session
    if factory is not None and callable(factory):
        target = factory
    kw = getattr(target, "kw", None)  # sessionmaker instance
    engine = kw.get("bind") if isinstance(kw, dict) else None
    if engine is None and hasattr(target, "dialect"):
        engine = target
    if engine is None or not hasattr(engine, "dialect"):
        raise ValueError(
            "dataflow.instrument_sqlalchemy expects a SQLAlchemy Engine or a "
            f"sessionmaker bound to one, got: {type(target).__name__}"
        )
    return engine


def _make_sqlalchemy_hooks(engine: Any) -> Tuple[Callable[..., Any], ...]:
    def before(conn, cursor, statement, parameters, context, executemany) -> None:
        if not enabled() or context is None:
            return
        try:
            ctx = db_span(DB_SYSTEM_SQLALCHEMY, statement)
            span = ctx.__enter__()
            if getattr(span, "ev", None) is not None:
                span.set_attr("db.dialect", engine.dialect.name)
            context._dataflow_ctx = ctx
        except Exception:  # noqa: BLE001 - tracing must never disturb the app
            pass

    def after(conn, cursor, statement, parameters, context, executemany) -> None:
        if context is None:
            return
        ctx = getattr(context, "_dataflow_ctx", None)
        try:
            context._dataflow_ctx = None
        except Exception:  # noqa: BLE001
            pass
        if ctx is not None:
            try:
                ctx.__exit__(None, None, None)  # status 200 + end
            except Exception:  # noqa: BLE001
                pass

    def on_error(exception_context) -> None:
        if exception_context is None:
            return
        context = getattr(exception_context, "execution_context", None)
        ctx = getattr(context, "_dataflow_ctx", None) if context is not None else None
        if ctx is not None:
            try:
                context._dataflow_ctx = None
            except Exception:  # noqa: BLE001
                pass
            # The ExceptionContext protocol names the caught exception
            # ".exception", but concrete SQLAlchemy versions expose it as
            # ".original_exception" / ".sqlalchemy_exception" — try all three.
            exc = getattr(exception_context, "exception", None)
            if exc is None:
                exc = getattr(exception_context, "original_exception", None)
            if exc is None:
                exc = getattr(exception_context, "sqlalchemy_exception", None)
            try:
                ctx.__exit__(type(exc), exc, exc.__traceback__ if exc else None)
            except Exception:  # noqa: BLE001
                pass

    return before, after, on_error


# -- psycopg 3 -----------------------------------------------------------------

def instrument_psycopg(conn_or_pool: Any) -> Any:
    """Trace queries on a psycopg 3 ``Connection`` or connection pool.

        dataflow.instrument_psycopg(conn)     # one Connection (sync or async)
        dataflow.instrument_psycopg(pool)     # every getconn()-ed connection

    ``Connection.execute`` is wrapped in a DB_QUERY span with
    ``db.system="postgres"``; the query text travels single-spaced, truncated
    to 200 characters — parameter values are never captured. For a pool, the
    wrapping is applied to each connection handed out by ``getconn`` (the
    ``pool.connection()`` context manager goes through it too). ``psycopg``
    is imported lazily; a clear ImportError is raised here only when it is
    not installed. Idempotent: wrapping twice wraps once.
    """
    try:
        import psycopg  # noqa: F401 - availability check only

    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_psycopg requires the 'psycopg' package "
            "(psycopg 3); install it with: pip install psycopg"
        ) from exc

    getconn = getattr(conn_or_pool, "getconn", None)
    if callable(getconn):
        return _instrument_psycopg_pool(conn_or_pool)
    return _instrument_psycopg_conn(conn_or_pool)


def _instrument_psycopg_pool(pool: Any) -> Any:
    original = pool.getconn
    if getattr(original, "_dataflow_instrumented", False):
        return pool

    if inspect.iscoroutinefunction(original):

        @functools.wraps(original)
        async def getconn_async(*args, **kwargs):
            conn = await original(*args, **kwargs)
            try:
                _instrument_psycopg_conn(conn)
            except Exception:  # noqa: BLE001 - best-effort
                pass
            return conn

        getconn_async._dataflow_instrumented = True
        pool.getconn = getconn_async  # type: ignore[method-assign]
        return pool

    @functools.wraps(original)
    def getconn_sync(*args, **kwargs):
        conn = original(*args, **kwargs)
        try:
            _instrument_psycopg_conn(conn)
        except Exception:  # noqa: BLE001 - best-effort
            pass
        return conn

    getconn_sync._dataflow_instrumented = True
    pool.getconn = getconn_sync  # type: ignore[method-assign]
    return pool


def _instrument_psycopg_conn(conn: Any) -> Any:
    original = getattr(conn, "execute", None)
    if original is None or getattr(original, "_dataflow_instrumented", False):
        return conn

    if inspect.iscoroutinefunction(original):

        @functools.wraps(original)
        async def execute_async(query: Any, *args: Any, **kwargs: Any):
            with db_span(DB_SYSTEM_POSTGRES, str(query)):
                return await original(query, *args, **kwargs)

        execute_async._dataflow_instrumented = True
        conn.execute = execute_async  # type: ignore[method-assign]
        return conn

    @functools.wraps(original)
    def execute_sync(query: Any, *args: Any, **kwargs: Any):
        with db_span(DB_SYSTEM_POSTGRES, str(query)):
            return original(query, *args, **kwargs)

    execute_sync._dataflow_instrumented = True
    conn.execute = execute_sync  # type: ignore[method-assign]
    return conn


# -- asyncpg -------------------------------------------------------------------

_ASYNCPG_METHODS = ("execute", "fetch", "fetchrow", "fetchval")


def instrument_asyncpg(pool_or_conn: Any) -> Any:
    """Trace queries on an asyncpg ``Connection`` or ``Pool``.

        dataflow.instrument_asyncpg(conn)    # one Connection
        dataflow.instrument_asyncpg(pool)    # every acquired connection

    ``execute`` / ``fetch`` / ``fetchrow`` / ``fetchval`` are wrapped in
    DB_QUERY spans with ``db.system="postgres"``; the query text travels
    single-spaced, truncated to 200 characters — argument values are never
    captured. For a pool, connections are instrumented as they come out of
    ``async with pool.acquire()``. ``asyncpg`` is imported lazily; a clear
    ImportError is raised here only when it is not installed. Idempotent.
    """
    try:
        import asyncpg  # noqa: F401 - availability check only

    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_asyncpg requires the 'asyncpg' package; "
            "install it with: pip install asyncpg"
        ) from exc

    if callable(getattr(pool_or_conn, "acquire", None)):
        return _instrument_asyncpg_pool(pool_or_conn)
    return _instrument_asyncpg_conn(pool_or_conn)


def _instrument_asyncpg_pool(pool: Any) -> Any:
    original = pool.acquire
    if getattr(original, "_dataflow_instrumented", False):
        return pool

    @functools.wraps(original)
    def acquire(*args, **kwargs):
        return _AcquireProxy(original(*args, **kwargs))

    acquire._dataflow_instrumented = True
    pool.acquire = acquire  # type: ignore[method-assign]
    return pool


class _AcquireProxy:
    """``async with pool.acquire() as conn`` proxy that instruments each
    connection on the way out and otherwise behaves like the original
    acquire context."""

    def __init__(self, ctx: Any):
        self._ctx = ctx

    async def __aenter__(self) -> Any:
        conn = await self._ctx.__aenter__()
        try:
            _instrument_asyncpg_conn(conn)
        except Exception:  # noqa: BLE001 - best-effort
            pass
        return conn

    async def __aexit__(self, *exc_info: Any) -> Any:
        return await self._ctx.__aexit__(*exc_info)


def _instrument_asyncpg_conn(conn: Any) -> Any:
    for method in _ASYNCPG_METHODS:
        original = getattr(conn, method, None)
        if original is None or getattr(original, "_dataflow_instrumented", False):
            continue

        @functools.wraps(original)
        async def wrapped(query: Any, *args: Any, _orig=original, **kwargs: Any):
            with db_span(DB_SYSTEM_POSTGRES, str(query)):
                return await _orig(query, *args, **kwargs)

        wrapped._dataflow_instrumented = True
        try:
            setattr(conn, method, wrapped)
        except Exception:  # noqa: BLE001 - e.g. __slots__ types: skip silently
            continue
    return conn


# -- httpx -------------------------------------------------------------------

# The SDK's own ingest endpoints (dataflow.logs POSTs /api/v1/logs,
# dataflow.manifest POSTs /api/v1/manifest): an instrumented client must
# never trace the SDK shipping its own telemetry.
_INGEST_PATHS = frozenset({"/api/v1/logs", "/api/v1/manifest"})

_HTTPX_SPAN_KEY = "dataflow_span"  # request.extensions slot (dataflow.http_client shares it)

_httpx_lock = threading.Lock()
_httpx_originals: Dict[str, Any] = {}  # "sync" / "async" -> original class send


def instrument_httpx(client: Optional[Any] = None) -> Any:
    """Emit an HTTP_CLIENT span around every ``httpx`` call.

        dataflow.instrument_httpx(client)   # one Client / AsyncClient
        dataflow.instrument_httpx()         # patch the classes globally
        dataflow.restore_httpx(client)      # undo (per client / globally)

    Spans are named ``"GET api.example.com/orders"``, carry ``http.method``
    and ``http.url``, end with the response status, and failures (connect
    errors, timeouts) record the error with status 500 and an ``error.stack``
    metadata. The request gains an ``X-Dataflow-Trace-Id`` header so
    downstream Dataflow services join the same trace, and the span joins the
    currently active trace like every other span. The SDK's own ingest
    endpoints (``/api/v1/logs``, ``/api/v1/manifest``) are skipped — no
    self-tracing.

    For a single client the tracing rides httpx event hooks (request /
    response), with ``send`` wrapped solely to catch failures — httpx has no
    error hook; user-registered hooks are preserved and run as before.
    Globally the ``Client.send`` / ``AsyncClient.send`` methods are patched,
    covering instances created before the call too. ``httpx`` is imported
    lazily; a clear ImportError is raised here only when it is not
    installed. Idempotent per client and at class level. Returns the
    instrumented client, or ``httpx.Client`` for the global form.
    """
    try:
        import httpx
    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_httpx requires the 'httpx' package; "
            "install it with: pip install httpx"
        ) from exc

    if client is not None:
        hooks = client.event_hooks
        # AsyncClient awaits its hooks, so the installed pair must match the
        # client flavour (sync functions would die with
        # "TypeError: object NoneType can't be awaited").
        async_client = inspect.iscoroutinefunction(client.send)
        req_hook = _httpx_request_hook_async if async_client else _httpx_request_hook
        resp_hook = _httpx_response_hook_async if async_client else _httpx_response_hook
        request_hooks = hooks.setdefault("request", [])
        if req_hook in request_hooks or getattr(client.send, "_dataflow_instrumented", False):
            return client  # already instrumented: a no-op
        request_hooks.append(req_hook)
        hooks.setdefault("response", []).append(resp_hook)
        # httpx has no error hook: wrap send so failures still complete the
        # span the request hook opened.
        client.send = _httpx_error_send(client.send)  # type: ignore[method-assign]
        return client

    with _httpx_lock:
        if _httpx_originals:
            return httpx.Client  # already patched: a no-op
        _httpx_originals["sync"] = httpx.Client.send
        _httpx_originals["async"] = httpx.AsyncClient.send
        httpx.Client.send = _httpx_class_send(_httpx_originals["sync"])
        httpx.AsyncClient.send = _httpx_class_send(_httpx_originals["async"])
    return httpx.Client


def restore_httpx(client: Optional[Any] = None) -> Any:
    """Undo :func:`instrument_httpx`: with ``client``, detach the Dataflow
    hooks and send wrapper from that one client; without, restore the
    original ``Client.send`` / ``AsyncClient.send``. No-op when nothing is
    installed (per-client instrumentation survives a global restore)."""
    try:
        import httpx
    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_httpx requires the 'httpx' package; "
            "install it with: pip install httpx"
        ) from exc

    if client is not None:
        hooks = client.event_hooks
        for event in ("request", "response"):
            installed = hooks.get(event, [])
            for hook in (
                _httpx_request_hook,
                _httpx_response_hook,
                _httpx_request_hook_async,
                _httpx_response_hook_async,
            ):
                try:
                    installed.remove(hook)
                except ValueError:
                    pass
        if "send" in getattr(client, "__dict__", {}):
            original = getattr(client.send, "_dataflow_original", None)
            if original is not None:
                client.send = original  # type: ignore[method-assign]
        return client

    with _httpx_lock:
        targets = {"sync": httpx.Client, "async": httpx.AsyncClient}
        for flavor, original in _httpx_originals.items():
            targets[flavor].send = original
        _httpx_originals.clear()
    return None


def _httpx_request_hook(request: Any) -> None:
    """Event hook (sync clients): open the HTTP_CLIENT span."""
    _httpx_start(request)


def _httpx_response_hook(response: Any) -> None:
    """Event hook (sync clients): end the span with the response status."""
    _httpx_finish_ok(response)


async def _httpx_request_hook_async(request: Any) -> None:
    """Event hook (AsyncClient awaits its hooks)."""
    _httpx_start(request)


async def _httpx_response_hook_async(response: Any) -> None:
    """Event hook (AsyncClient awaits its hooks)."""
    _httpx_finish_ok(response)


def _httpx_start(request: Any) -> Any:
    """Open the HTTP_CLIENT span for an outgoing request (or return None
    when disabled, the target is an ingest endpoint, or the span engine
    failed). Tracing is silent and never blocks the caller."""
    if not enabled():
        return None
    try:
        url = str(request.url)
        parts = urlsplit(url)
        if parts.path in _INGEST_PATHS:
            return None  # no self-tracing
        method = str(request.method or "GET").upper()
        host = parts.hostname or ""
        span = start_span(f"{method} {host}{parts.path or '/'}", EVENT_HTTP_CLIENT)
        span.set_attr("http.method", method)
        span.set_attr("http.url", url)
        span.ev.callee_package = host
        request.extensions[_HTTPX_SPAN_KEY] = span
        # The downstream service joins the same trace via the request header.
        request.headers[TRACE_HEADER] = span.trace_id
        return span
    except Exception:  # noqa: BLE001 - best-effort
        return None


def _httpx_finish_ok(response: Any) -> None:
    request = getattr(response, "request", None)
    span = request.extensions.get(_HTTPX_SPAN_KEY) if request is not None else None
    if span is None:
        return
    try:
        request.extensions[_HTTPX_SPAN_KEY] = None
        span.set_status(int(getattr(response, "status_code", 0) or 0))
        span.end()
    except Exception:  # noqa: BLE001
        pass


def _httpx_finish_error(exc: BaseException) -> None:
    request = getattr(exc, "request", None)
    span = request.extensions.get(_HTTPX_SPAN_KEY) if request is not None else None
    if span is None:
        return
    try:
        request.extensions[_HTTPX_SPAN_KEY] = None
        _record_exception(span, exc)
        span.end()
    except Exception:  # noqa: BLE001
        pass


def _record_exception(span: Any, exc: BaseException, tb: Any = None) -> None:
    """crash-capture conventions on a span: ``"Type: message"`` (clipped to
    500 chars), status 500, and the formatted traceback under ``error.stack``
    capped at 8192 bytes with the top kept. ``tb`` overrides the traceback
    (celery hands the caught traceback through its signal)."""
    try:
        span.record_error(f"{type(exc).__name__}: {exc}"[:MESSAGE_MAX_CHARS])
    except Exception:  # noqa: BLE001
        pass
    try:
        stack = "".join(
            traceback.format_exception(type(exc), exc, tb if tb is not None else exc.__traceback__)
        )
        if stack:
            span.set_attr(STACK_ATTR, _clip_stack(stack))
    except Exception:  # noqa: BLE001
        pass
    try:
        span.set_status(500)
    except Exception:  # noqa: BLE001
        pass


def _httpx_error_send(original: Any) -> Any:
    """Per-client send wrapper: hooks own the span lifecycle, this only
    completes it when the call fails. Works bound (instance) — signature
    agnostic so sync and async clients share it."""

    if inspect.iscoroutinefunction(original):

        @functools.wraps(original)
        async def send_async(*args: Any, **kwargs: Any):
            try:
                return await original(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - the error itself propagates
                _httpx_finish_error(exc)
                raise

        send_async._dataflow_instrumented = True
        send_async._dataflow_original = original
        return send_async

    @functools.wraps(original)
    def send_sync(*args: Any, **kwargs: Any):
        try:
            return original(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - the error itself propagates
            _httpx_finish_error(exc)
            raise

    send_sync._dataflow_instrumented = True
    send_sync._dataflow_original = original
    return send_sync


def _httpx_class_send(original: Any) -> Any:
    """Class-level send patch (global mode): the wrapper owns the whole span
    lifecycle — hooks stay untouched on every instance."""

    if inspect.iscoroutinefunction(original):

        @functools.wraps(original)
        async def send_async(self: Any, request: Any, *args: Any, **kwargs: Any):
            _httpx_start(request)
            try:
                response = await original(self, request, *args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - the error itself propagates
                _httpx_finish_error(exc)
                raise
            _httpx_finish_ok(response)
            return response

        send_async._dataflow_instrumented = True
        send_async._dataflow_original = original
        return send_async

    @functools.wraps(original)
    def send_sync(self: Any, request: Any, *args: Any, **kwargs: Any):
        _httpx_start(request)
        try:
            response = original(self, request, *args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - the error itself propagates
            _httpx_finish_error(exc)
            raise
        _httpx_finish_ok(response)
        return response

    send_sync._dataflow_instrumented = True
    send_sync._dataflow_original = original
    return send_sync


# -- celery --------------------------------------------------------------------

_CELERY_DISPATCH_UID = "dataflow.celery"

# task_id -> (span, contextvar token). Tasks run to completion in one worker
# context, so entries live for the span of a single task execution.
_celery_lock = threading.Lock()
_celery_spans: Dict[str, Tuple[Any, Any]] = {}


def instrument_celery() -> None:
    """Trace every celery task as a FUNCTION_CALL span.

        dataflow.instrument_celery()
        dataflow.uninstrument_celery()      # undo

    Connects celery's ``task_prerun`` / ``task_postrun`` / ``task_failure``
    signals: the span is named after the task (``"tasks.add"``), stays the
    current span while the task body runs (so database and HTTP spans nest
    under it), ends with the task state on ``task_postrun``, and failures
    are recorded on ``task_failure`` with status 500, the
    ``"Type: message"`` error and an ``error.stack``. ``celery`` is imported
    lazily; a clear ImportError is raised here only when it is not
    installed. Idempotent (registered under one dispatch_uid). The receivers
    never raise, so the remaining handlers in the signal chain always run,
    and uninstrumenting touches only the Dataflow receivers.
    """
    try:
        import celery  # noqa: F401 - availability check only
    except ImportError as exc:
        raise ImportError(
            "dataflow.instrument_celery requires the 'celery' package; "
            "install it with: pip install celery"
        ) from exc
    from celery.signals import task_failure, task_postrun, task_prerun

    # dispatch_uid keeps the registration idempotent (reconnecting replaces
    # the receiver instead of stacking a second one).
    task_prerun.connect(_celery_prerun, dispatch_uid=_CELERY_DISPATCH_UID)
    task_postrun.connect(_celery_postrun, dispatch_uid=_CELERY_DISPATCH_UID)
    task_failure.connect(_celery_failure, dispatch_uid=_CELERY_DISPATCH_UID)


def uninstrument_celery() -> None:
    """Disconnect the Dataflow celery receivers. No-op when celery is not
    installed or nothing is connected; unrelated receivers stay untouched."""
    try:
        from celery.signals import task_failure, task_postrun, task_prerun
    except ImportError:  # noqa: BLE001 - nothing to undo without celery
        return
    for signal in (task_prerun, task_postrun, task_failure):
        try:
            signal.disconnect(dispatch_uid=_CELERY_DISPATCH_UID)
        except Exception:  # noqa: BLE001 - uninstrument is best-effort
            pass
    with _celery_lock:
        _celery_spans.clear()


def _celery_prerun(sender: Any = None, **kwargs: Any) -> None:
    if not enabled():
        return
    try:
        task = kwargs.get("task", sender)
        name = _celery_task_name(task, kwargs.get("task_id"))
        span = start_span(name, EVENT_FUNC_CALL)
        token = _current_span.set(span)
        with _celery_lock:
            _celery_spans[_celery_key(kwargs.get("task_id"))] = (span, token)
    except Exception:  # noqa: BLE001 - never break the signal chain
        pass


def _celery_postrun(sender: Any = None, **kwargs: Any) -> None:
    with _celery_lock:
        entry = _celery_spans.pop(_celery_key(kwargs.get("task_id")), None)
    if entry is None:
        return
    span, token = entry
    try:
        _current_span.reset(token)
    except Exception:  # noqa: BLE001
        pass
    try:
        state = str(kwargs.get("state") or "")
        span.set_status(500 if state == "FAILURE" else 200)
        span.end()
    except Exception:  # noqa: BLE001
        pass


def _celery_failure(sender: Any = None, **kwargs: Any) -> None:
    with _celery_lock:
        entry = _celery_spans.get(_celery_key(kwargs.get("task_id")))
    if entry is None:  # postrun already ran, or prerun never did
        return
    span = entry[0]
    exc = kwargs.get("exception")
    try:
        if exc is not None:
            _record_exception(span, exc, kwargs.get("traceback"))
        else:
            span.record_error("task failed")
            span.set_status(500)
    except Exception:  # noqa: BLE001 - never break the signal chain
        pass


def _celery_task_name(task: Any, task_id: Any) -> str:
    for attr in ("name", "__name__"):
        name = getattr(task, attr, None)
        if isinstance(name, str) and name:
            return name
    return str(task_id or "task")


def _celery_key(task_id: Any) -> str:
    return str(task_id or "")


# -- Django ----------------------------------------------------------------------

class DataflowMiddleware:
    """Django new-style request middleware: one HTTP_SERVER span per request.

        MIDDLEWARE = [
            ...
            "dataflow.django_middleware.DataflowMiddleware",
        ]

    The span opens as ``"METHOD path"`` and, once URL resolution has filled
    ``request.resolver_match``, is renamed to ``"METHOD route"`` (with
    ``http.route`` metadata) so dynamic paths collapse into route templates.
    Response status becomes the span status (errors at >= 500 are recorded);
    exceptions are recorded (status 500) and re-raised. Database queries are
    traced by instrumenting the engine(s) with
    :func:`instrument_sqlalchemy` — spans nest under the request span
    automatically. Minimal by design: no body or header capture. Django
    itself is not imported — the middleware is duck-typed.
    """

    def __init__(self, get_response: Callable[[Any], Any]) -> None:
        self.get_response = get_response

    def __call__(self, request: Any) -> Any:
        if not enabled():
            return self.get_response(request)
        return self._traced(request)

    def _traced(self, request: Any) -> Any:
        method = str(getattr(request, "method", "") or "GET").upper()
        path = str(getattr(request, "path", "") or "/")
        try:
            span = start_span(f"{method} {path}", EVENT_HTTP_SERVER)
            for key, value in agent_attrs():
                span.set_attr(key, value)
        except Exception:  # noqa: BLE001 - fall back to pure passthrough
            return self.get_response(request)

        started = time.time()
        try:
            response = self.get_response(request)
        except Exception as exc:
            try:
                span.record_error(exc)
                span.set_status(500)
                span.set_attr("http.duration_ms", str(int((time.time() - started) * 1000)))
                span.end()
            except Exception:  # noqa: BLE001
                pass
            raise
        try:
            status = int(getattr(response, "status_code", 0) or 0)
            route = str(
                getattr(getattr(request, "resolver_match", None), "route", "") or ""
            )
            if route:
                span.ev.name = f"{method} {route}"
                span.set_attr("http.route", route)
            span.set_status(status)
            span.set_attr("http.status_code", str(status))
            span.set_attr("http.method", method)
            span.set_attr("http.path", path)
            span.set_attr("http.duration_ms", str(int((time.time() - started) * 1000)))
            if status >= 500:
                span.record_error(f"http {status}")
            span.end()
        except Exception:  # noqa: BLE001 - never disturb the response
            pass
        return response
