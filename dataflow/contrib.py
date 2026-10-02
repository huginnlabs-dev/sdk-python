"""Optional library integrations: SQLAlchemy, psycopg3, asyncpg, Django.

All of these ride the existing span pipeline: database calls emit DB_QUERY
spans through :func:`dataflow.transport.db_span` semantics (name from
:func:`dataflow.transport.stmt_summary` — ``"SELECT orders"`` —, ``db.system``
metadata, single-spaced statement truncated to 200 characters in
``db.statement``, bind parameter values never captured) and HTTP requests
emit HTTP_SERVER spans like ``ASGIMiddleware`` does. Nested spans parent to
them automatically via the usual contextvars.

- :func:`instrument_sqlalchemy` listens on the Engine's
  ``before_cursor_execute`` / ``after_cursor_execute`` / ``handle_error``
  events, so every statement executed through the engine (Core or ORM,
  sync or async) is traced. Idempotent per engine; undo with
  :func:`uninstrument_sqlalchemy`.
- :func:`instrument_psycopg` wraps ``Connection.execute`` (psycopg 3), or a
  pool's ``getconn`` so every checked-out connection is instrumented.
- :func:`instrument_asyncpg` wraps ``execute`` / ``fetch`` / ``fetchrow`` /
  ``fetchval`` on a connection, or the pool's ``acquire`` context manager.
- :class:`DataflowMiddleware` is a Django new-style request middleware
  emitting one HTTP_SERVER span per request, named after
  ``request.resolver_match.route`` when URL resolution has produced one.

Third-party packages are imported lazily — a missing library raises a clear
ImportError only when its instrument function is called. Everything is
best-effort: with tracing disabled (no API key/endpoint, or
DATAFLOW_DISABLED) the hooks install but produce no spans and the
instrumented calls behave exactly as they would without Dataflow.
"""

from __future__ import annotations

import functools
import inspect
import threading
import time
import weakref
from typing import Any, Callable, Dict, Optional, Tuple

from .agent import agent_attrs
from .config import enabled
from .spans import EVENT_HTTP_SERVER, start_span
from .transport import db_span

__all__ = [
    "instrument_sqlalchemy",
    "uninstrument_sqlalchemy",
    "instrument_psycopg",
    "instrument_asyncpg",
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
