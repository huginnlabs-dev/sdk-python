"""Tests for dataflow.contrib: SQLAlchemy / psycopg / asyncpg instrumentation,
the httpx + celery integrations, the Django middleware and the
@dataflow.traced decorator.

Hermeticity rules:
- sqlalchemy is a dev extra and installed for the test run; if missing, its
  tests skip (pytest.importorskip).
- psycopg / asyncpg are NOT required: a stub module is injected into
  sys.modules and the wrapped objects are duck-typed fakes, so the wrapper
  logic is tested without the real drivers. ImportError plumbing is tested
  by shadowing the module with None.
- httpx is a core SDK dependency; its tests run against stub transports
  (no network).
- celery is a dev extra; its tests skip without it. The signals are fired
  manually through the celery signal API — no app, worker or broker.
- The Django middleware is duck-typed (dataflow never imports django), so
  the fake-request tests always run; one extra test exercises a real
  django.test request and skips when django is not installed.
"""

import asyncio
import importlib
import sys
import types

import pytest

import dataflow
from dataflow.contrib import DataflowMiddleware

# Wire enum values (proto/dataflow.proto).
FUNCTION_CALL = 1
HTTP_SERVER = 2
HTTP_CLIENT = 3
DB_QUERY = 6


def _stub_module(monkeypatch, name: str) -> types.ModuleType:
    """Inject an empty stand-in so `import name` succeeds."""
    mod = types.ModuleType(name)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


# -- @dataflow.traced -------------------------------------------------------


def test_traced_sync_returns_value_and_emits_span(events):
    @dataflow.traced("worker.Encode")
    def encode(value):
        return value * 2

    assert encode(21) == 42
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "worker.Encode"
    assert ev.type == FUNCTION_CALL
    assert ev.parent_span_id == ""  # root span


def test_traced_async(events):
    @dataflow.traced("worker.Upload")
    async def upload(payload):
        return len(payload)

    assert asyncio.run(upload(b"abc")) == 3
    assert len(events) == 1
    assert events[0].name == "worker.Upload"
    assert events[0].type == FUNCTION_CALL


def test_traced_error_sets_500_and_reraises(events):
    @dataflow.traced("worker.Explode")
    def explode():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        explode()
    assert len(events) == 1
    ev = events[0]
    assert ev.status_code == 500
    assert "boom" in ev.error_message


def test_traced_async_error_sets_500_and_reraises(events):
    @dataflow.traced("worker.ExplodeAsync")
    async def explode():
        raise RuntimeError("async boom")

    with pytest.raises(RuntimeError):
        asyncio.run(explode())
    ev = events[0]
    assert ev.status_code == 500
    assert "async boom" in ev.error_message


def test_traced_method_self_passthrough(events):
    class Pricer:
        def __init__(self):
            self.margin = 2

        @dataflow.traced("shop.Price")
        def price(self, base):
            return base + self.margin

    assert Pricer().price(40) == 42
    assert events[0].name == "shop.Price"


def test_traced_default_label_from_qualname(events):
    def stash():
        return 1

    decorated = dataflow.traced()(stash)
    decorated()
    root = __name__.split(".")[0]
    assert events[0].name == f"{root}.{stash.__qualname__}"


def test_traced_preserves_function_metadata(events):
    @dataflow.traced("x.Y")
    def documented():
        """docs"""

    assert documented.__name__ == "documented"
    assert documented.__doc__ == "docs"


def test_traced_nests_under_enclosing_trace(events):
    @dataflow.traced("worker.Inner")
    def inner():
        return 1

    with dataflow.trace("shop.Checkout") as outer:
        inner()
    assert len(events) == 2
    child = next(e for e in events if e.name == "worker.Inner")
    assert child.parent_span_id == outer.span_id
    assert child.trace_id == outer.trace_id


def test_traced_disabled_is_inert(monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))

    @dataflow.traced("worker.Hidden")
    def work():
        return 5

    assert work() == 5


# -- instrument_sqlalchemy ---------------------------------------------------

sqlalchemy = pytest.importorskip("sqlalchemy")


@pytest.fixture
def sqla_engine(events):
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite://",  # in-memory
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("CREATE TABLE carts (id INTEGER PRIMARY KEY)"))
    yield engine
    engine.dispose()


@pytest.fixture
def instrumented(sqla_engine):
    dataflow.instrument_sqlalchemy(sqla_engine)
    yield sqla_engine
    dataflow.uninstrument_sqlalchemy(sqla_engine)


def _query(engine, statement):
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(text(statement))
        try:
            return result.fetchall()  # SELECTs
        except sqlalchemy.exc.ResourceClosedError:  # DML returns no rows
            return []


def test_sqlalchemy_select_span(instrumented, events):
    assert _query(instrumented, "SELECT * FROM users WHERE id = 1") == []
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "SELECT users"
    assert ev.type == DB_QUERY
    assert ev.metadata["db.system"] == "sqlalchemy"
    assert ev.metadata["db.dialect"] == "sqlite"
    assert ev.metadata["db.statement"] == "SELECT * FROM users WHERE id = 1"
    assert ev.status_code == 200


def test_sqlalchemy_statement_collapsed_and_clipped(instrumented, events):
    stmt = "SELECT   *\n\tFROM users\n WHERE name = '" + "x" * 500 + "'"
    _query(instrumented, stmt)
    sent = events[0].metadata["db.statement"]
    assert len(sent) == 200
    assert sent == " ".join(stmt.split())[:200]


def test_sqlalchemy_never_captures_bind_values(instrumented, events):
    secret = "hunter2-super-secret"
    from sqlalchemy import text

    with instrumented.connect() as conn:
        conn.execute(
            text("SELECT * FROM users WHERE name = :name"), {"name": secret}
        ).fetchall()
    sent = repr(events[0].metadata) + repr(events[0].payload.data)
    assert secret not in sent


def test_sqlalchemy_insert_span(instrumented, events):
    _query(instrumented, "INSERT INTO carts (id) VALUES (9)")
    assert events[0].name == "INSERT carts"
    assert events[0].status_code == 200


def test_sqlalchemy_error_sets_500(instrumented, events):
    with pytest.raises(sqlalchemy.exc.OperationalError):
        _query(instrumented, "SELECT * FROM missing_table")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "SELECT missing_table"
    assert ev.status_code == 500
    assert "no such table" in ev.error_message


def test_sqlalchemy_nests_under_trace(instrumented, events):
    with dataflow.trace("shop.Checkout") as outer:
        _query(instrumented, "SELECT * FROM users")
    assert len(events) == 2
    child = next(e for e in events if e.name == "SELECT users")
    assert child.parent_span_id == outer.span_id
    assert child.trace_id == outer.trace_id


def test_sqlalchemy_idempotent_per_engine(sqla_engine, events):
    assert dataflow.instrument_sqlalchemy(sqla_engine) is sqla_engine
    assert dataflow.instrument_sqlalchemy(sqla_engine) is sqla_engine
    _query(sqla_engine, "SELECT * FROM users")
    assert len(events) == 1


def test_sqlalchemy_sessionmaker_target(sqla_engine, events):
    from sqlalchemy.orm import sessionmaker

    sm = sessionmaker(bind=sqla_engine)
    assert dataflow.instrument_sqlalchemy(sm) is sm
    with sm() as session:
        session.execute(sqlalchemy.text("SELECT * FROM users")).fetchall()
    assert len(events) == 1
    assert events[0].name == "SELECT users"
    dataflow.uninstrument_sqlalchemy(sm)
    with sm() as session:
        session.execute(sqlalchemy.text("SELECT * FROM users")).fetchall()
    assert len(events) == 1


def test_sqlalchemy_uninstrument_stops_spans(sqla_engine, events):
    dataflow.instrument_sqlalchemy(sqla_engine)
    _query(sqla_engine, "SELECT * FROM users")
    assert len(events) == 1
    dataflow.uninstrument_sqlalchemy(sqla_engine)
    _query(sqla_engine, "SELECT * FROM users")
    assert len(events) == 1


def test_sqlalchemy_disabled_inert(sqla_engine, events, monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    dataflow.instrument_sqlalchemy(sqla_engine)
    assert _query(sqla_engine, "SELECT * FROM users") == []
    assert events == []


def test_sqlalchemy_rejects_non_engine(events):
    with pytest.raises(ValueError, match="sessionmaker"):
        dataflow.instrument_sqlalchemy(object())


def test_sqlalchemy_missing_sqlalchemy(monkeypatch, events):
    monkeypatch.setitem(sys.modules, "sqlalchemy", None)
    with pytest.raises(ImportError, match="sqlalchemy"):
        dataflow.instrument_sqlalchemy("engine")


# -- instrument_psycopg --------------------------------------------------------


class FakePsycopgConn:
    def __init__(self):
        self.calls = []

    def execute(self, query, *args, **kwargs):
        self.calls.append((query, args, kwargs))
        return "CURSOR"


class FakePsycopgAsyncConn:
    def __init__(self):
        self.calls = []

    async def execute(self, query, *args, **kwargs):
        self.calls.append((query, args, kwargs))
        return "CURSOR"


def test_psycopg_conn_sync(events, monkeypatch):
    _stub_module(monkeypatch, "psycopg")
    conn = FakePsycopgConn()
    assert dataflow.instrument_psycopg(conn) is conn
    assert conn.execute("SELECT * FROM orders WHERE id = %s", [7]) == "CURSOR"
    assert len(conn.calls) == 1  # arguments pass through untouched
    ev = events[0]
    assert ev.name == "SELECT orders"
    assert ev.type == DB_QUERY
    assert ev.metadata["db.system"] == "postgres"
    assert ev.metadata["db.statement"] == "SELECT * FROM orders WHERE id = %s"
    assert ev.status_code == 200
    assert "7" not in repr(ev.metadata)  # bind values never captured


def test_psycopg_conn_async(events, monkeypatch):
    _stub_module(monkeypatch, "psycopg")
    conn = FakePsycopgAsyncConn()
    assert dataflow.instrument_psycopg(conn) is conn
    assert asyncio.run(conn.execute("INSERT INTO carts (id) VALUES ($1)", 3)) == "CURSOR"
    ev = events[0]
    assert ev.name == "INSERT carts"
    assert ev.metadata["db.system"] == "postgres"
    assert ev.status_code == 200


def test_psycopg_conn_error_sets_500(events, monkeypatch):
    _stub_module(monkeypatch, "psycopg")

    class Boom:
        def execute(self, query, *args, **kwargs):
            raise RuntimeError("connection refused")

    conn = Boom()
    dataflow.instrument_psycopg(conn)
    with pytest.raises(RuntimeError, match="connection refused"):
        conn.execute("DELETE FROM carts")
    ev = events[0]
    assert ev.name == "DELETE carts"
    assert ev.status_code == 500
    assert "connection refused" in ev.error_message


def test_psycopg_pool_instruments_each_connection(events, monkeypatch):
    _stub_module(monkeypatch, "psycopg")

    class FakePool:
        def getconn(self):
            return FakePsycopgConn()

    pool = FakePool()
    assert dataflow.instrument_psycopg(pool) is pool
    conn1 = pool.getconn()
    conn2 = pool.getconn()
    conn1.execute("SELECT * FROM users")
    conn2.execute("SELECT * FROM carts")
    assert [e.name for e in events] == ["SELECT users", "SELECT carts"]


def test_psycopg_idempotent(events, monkeypatch):
    _stub_module(monkeypatch, "psycopg")
    conn = FakePsycopgConn()
    dataflow.instrument_psycopg(conn)
    dataflow.instrument_psycopg(conn)
    conn.execute("SELECT 1")
    assert len(events) == 1


def test_psycopg_missing_psycopg(monkeypatch, events):
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(ImportError, match="psycopg"):
        dataflow.instrument_psycopg(object())


# -- instrument_asyncpg --------------------------------------------------------


class FakeAsyncpgConn:
    async def execute(self, query, *args, **kwargs):
        return "DONE"

    async def fetch(self, query, *args, **kwargs):
        return [1, 2]

    async def fetchrow(self, query, *args, **kwargs):
        return {"id": 1}

    async def fetchval(self, query, *args, **kwargs):
        return 1


def test_asyncpg_conn_methods(events, monkeypatch):
    _stub_module(monkeypatch, "asyncpg")
    conn = FakeAsyncpgConn()
    assert dataflow.instrument_asyncpg(conn) is conn
    assert asyncio.run(conn.fetch("SELECT * FROM orders")) == [1, 2]
    assert asyncio.run(conn.execute("DELETE FROM carts WHERE id = 1")) == "DONE"
    assert asyncio.run(conn.fetchval("SELECT count(*) FROM users")) == 1
    assert [e.name for e in events] == ["SELECT orders", "DELETE carts", "SELECT users"]
    assert all(e.metadata["db.system"] == "postgres" for e in events)
    assert all(e.status_code == 200 for e in events)


def test_asyncpg_error_sets_500(events, monkeypatch):
    _stub_module(monkeypatch, "asyncpg")

    class Boom:
        async def fetch(self, query, *args, **kwargs):
            raise RuntimeError("deadpool")

    conn = Boom()
    dataflow.instrument_asyncpg(conn)
    with pytest.raises(RuntimeError, match="deadpool"):
        asyncio.run(conn.fetch("SELECT * FROM users"))
    ev = events[0]
    assert ev.name == "SELECT users"
    assert ev.status_code == 500
    assert "deadpool" in ev.error_message


def test_asyncpg_pool_acquire(events, monkeypatch):
    _stub_module(monkeypatch, "asyncpg")

    class FakeAcquireCtx:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *exc):
            return False

    class FakePool:
        def __init__(self):
            self.conn = FakeAsyncpgConn()

        def acquire(self):
            return FakeAcquireCtx(self.conn)

    pool = FakePool()
    assert dataflow.instrument_asyncpg(pool) is pool

    async def run():
        async with pool.acquire() as conn:
            assert await conn.fetch("SELECT * FROM users") == [1, 2]

    asyncio.run(run())
    assert len(events) == 1
    assert events[0].name == "SELECT users"


def test_asyncpg_idempotent(events, monkeypatch):
    _stub_module(monkeypatch, "asyncpg")
    conn = FakeAsyncpgConn()
    dataflow.instrument_asyncpg(conn)
    dataflow.instrument_asyncpg(conn)
    asyncio.run(conn.fetch("SELECT 1"))
    assert len(events) == 1


def test_asyncpg_missing_asyncpg(monkeypatch, events):
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    with pytest.raises(ImportError, match="asyncpg"):
        dataflow.instrument_asyncpg(object())


# -- instrument_httpx -----------------------------------------------------------

import httpx


class _StubTransport(httpx.BaseTransport):
    """Records the outgoing request, answers 201 — no network."""

    def __init__(self):
        self.requests = []

    def handle_request(self, request):
        self.requests.append(request)
        return httpx.Response(201, json={"ok": True})


class _StubAsyncTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.requests = []

    async def handle_async_request(self, request):
        self.requests.append(request)
        return httpx.Response(201, json={"ok": True})


class _BoomTransport(httpx.BaseTransport):
    def handle_request(self, request):
        raise httpx.ConnectError("connection refused", request=request)


class _BoomAsyncTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request):
        raise httpx.ConnectError("async refused", request=request)


def test_httpx_client_span(events):
    transport = _StubTransport()
    client = httpx.Client(transport=transport)
    assert dataflow.instrument_httpx(client) is client
    client.get("https://api.example.com/orders?limit=5")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "GET api.example.com/orders"
    assert ev.type == HTTP_CLIENT
    assert ev.callee_package == "api.example.com"
    assert ev.metadata["http.method"] == "GET"
    assert ev.metadata["http.url"] == "https://api.example.com/orders?limit=5"
    assert ev.status_code == 201
    # The downstream service joins the same trace via the request header.
    assert transport.requests[0].headers["X-Dataflow-Trace-Id"] == ev.trace_id


def test_httpx_joins_existing_trace(events):
    client = httpx.Client(transport=_StubTransport())
    dataflow.instrument_httpx(client)
    with dataflow.trace("shop.Checkout") as outer:
        client.post("https://pay.example.com/capture", json={"amount": 1})
    assert len(events) == 2
    ev = events[0]  # the HTTP span ends first (inside the with block)
    assert ev.name == "POST pay.example.com/capture"
    assert ev.parent_span_id == outer.span_id
    assert ev.trace_id == outer.trace_id


def test_httpx_error_sets_500_and_stack(events):
    client = httpx.Client(transport=_BoomTransport())
    dataflow.instrument_httpx(client)
    with pytest.raises(httpx.ConnectError):
        client.get("https://api.example.com/orders")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "GET api.example.com/orders"
    assert ev.status_code == 500
    assert "ConnectError: connection refused" in ev.error_message
    # error.stack carries the formatted traceback (raise site on down)
    assert "ConnectError: connection refused" in ev.metadata["error.stack"]


def test_httpx_async(events):
    transport = _StubAsyncTransport()
    client = httpx.AsyncClient(transport=transport)
    assert dataflow.instrument_httpx(client) is client

    async def run():
        return await client.get("https://api.example.com/async")

    assert asyncio.run(run()).status_code == 201
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "GET api.example.com/async"
    assert ev.type == HTTP_CLIENT
    assert ev.status_code == 201
    assert transport.requests[0].headers["X-Dataflow-Trace-Id"] == ev.trace_id


def test_httpx_async_error_sets_500_and_stack(events):
    client = httpx.AsyncClient(transport=_BoomAsyncTransport())
    dataflow.instrument_httpx(client)

    async def run():
        await client.get("https://api.example.com/async")

    with pytest.raises(httpx.ConnectError):
        asyncio.run(run())
    ev = events[0]
    assert ev.status_code == 500
    assert "async refused" in ev.error_message
    assert ev.metadata["error.stack"]


def test_httpx_skips_ingest_endpoints(events):
    transport = _StubTransport()
    client = httpx.Client(transport=transport)
    dataflow.instrument_httpx(client)
    client.post("https://collector.example.com/api/v1/logs")
    client.post("https://collector.example.com/api/v1/manifest")
    client.get("https://api.example.com/kept")  # not ingest: still traced
    assert len(events) == 1
    assert events[0].name == "GET api.example.com/kept"
    # the skipped calls carry no trace id header and no span was opened
    for request in transport.requests[:2]:
        assert "X-Dataflow-Trace-Id" not in request.headers


def test_httpx_idempotent_per_client(events):
    client = httpx.Client(transport=_StubTransport())
    assert dataflow.instrument_httpx(client) is client
    assert dataflow.instrument_httpx(client) is client
    client.get("https://api.example.com/once")
    assert len(events) == 1


def test_httpx_restore_client(events):
    client = httpx.Client(transport=_StubTransport())
    dataflow.instrument_httpx(client)
    client.get("https://api.example.com/before")
    assert len(events) == 1
    assert dataflow.restore_httpx(client) is client
    client.get("https://api.example.com/after")
    assert len(events) == 1
    # user-facing plumbing back to stock: no Dataflow hooks remain
    assert dataflow.contrib._httpx_request_hook not in client.event_hooks["request"]
    assert dataflow.contrib._httpx_response_hook not in client.event_hooks["response"]


def test_httpx_global_patch(events):
    import httpx as httpx_mod

    original_sync = httpx_mod.Client.send
    original_async = httpx_mod.AsyncClient.send
    try:
        assert dataflow.instrument_httpx() is httpx_mod.Client
        assert dataflow.instrument_httpx() is httpx_mod.Client  # idempotent

        client = httpx_mod.Client(transport=_StubTransport())
        client.get("https://api.example.com/global")
        assert len(events) == 1
        assert events[0].name == "GET api.example.com/global"

        async_client = httpx_mod.AsyncClient(transport=_StubAsyncTransport())
        asyncio.run(async_client.get("https://api.example.com/aglobal"))
        assert len(events) == 2
        assert events[1].name == "GET api.example.com/aglobal"
    finally:
        dataflow.restore_httpx()
    assert httpx_mod.Client.send is original_sync
    assert httpx_mod.AsyncClient.send is original_async
    # restored: no further spans
    httpx_mod.Client(transport=_StubTransport()).get("https://api.example.com/after")
    assert len(events) == 2


def test_httpx_global_patch_restores_even_on_error(events):
    import httpx as httpx_mod

    original_sync = httpx_mod.Client.send
    try:
        dataflow.instrument_httpx()
    finally:
        dataflow.restore_httpx()
    assert httpx_mod.Client.send is original_sync


def test_httpx_disabled_is_inert(events, monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    transport = _StubTransport()
    client = httpx.Client(transport=transport)
    dataflow.instrument_httpx(client)
    assert client.get("https://api.example.com/orders").status_code == 201
    assert events == []
    # disabled: no span, no trace id header, call untouched
    assert "X-Dataflow-Trace-Id" not in transport.requests[0].headers


def test_httpx_missing_httpx(monkeypatch, events):
    monkeypatch.setitem(sys.modules, "httpx", None)
    with pytest.raises(ImportError, match="httpx"):
        dataflow.instrument_httpx()


def test_httpx_user_hooks_survive(events):
    client = httpx.Client(transport=_StubTransport())
    seen = []
    client.event_hooks["request"].append(lambda request: seen.append(request))
    dataflow.instrument_httpx(client)
    client.get("https://api.example.com/hooked")
    assert len(seen) == 1  # the user's hook ran
    assert len(events) == 1  # and so did ours


# -- instrument_celery -----------------------------------------------------------

celery = pytest.importorskip("celery")
from celery.signals import task_failure, task_postrun, task_prerun  # noqa: E402


class _FakeTask:
    """Stand-in for a celery Task: only the name matters to the hooks."""

    name = "tasks.add"


@pytest.fixture
def instrumented_celery(events):
    assert dataflow.instrument_celery() is None
    yield
    dataflow.uninstrument_celery()


def _fire_prerun(task_id="t1", task=None):
    task = task if task is not None else _FakeTask()
    task_prerun.send(sender=task, task=task, task_id=task_id)
    return task


def _fire_postrun(task_id="t1", state="SUCCESS", retval=None, task=None):
    task = task if task is not None else _FakeTask()
    task_postrun.send(sender=task, task=task, task_id=task_id, retval=retval, state=state)


def _fire_failure(task_id="t1", exc=None):
    try:
        raise ValueError("boom")
    except ValueError as raised:
        exc = exc or raised
        task_failure.send(
            sender=_FakeTask(),
            task_id=task_id,
            exception=exc,
            args=(),
            kwargs={},
            traceback=raised.__traceback__,
            einfo=None,
        )
    return exc


def test_celery_task_span(instrumented_celery, events):
    _fire_prerun(task_id="t1")
    _fire_postrun(task_id="t1", state="SUCCESS", retval=42)
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "tasks.add"
    assert ev.type == FUNCTION_CALL
    assert ev.status_code == 200
    assert ev.parent_span_id == ""  # root span
    assert ev.duration_ms >= 0


def test_celery_nests_under_task_span(instrumented_celery, events):
    _fire_prerun(task_id="t1")
    with dataflow.trace("tasks.Inner") as inner:
        pass
    _fire_postrun(task_id="t1", state="SUCCESS")
    assert len(events) == 2
    task_span = next(e for e in events if e.name == "tasks.add")
    child = next(e for e in events if e.name == "tasks.Inner")
    assert child.parent_span_id == task_span.span_id
    assert child.trace_id == task_span.trace_id


def test_celery_failure_sets_500_and_stack(instrumented_celery, events):
    _fire_prerun(task_id="t1")
    _fire_failure(task_id="t1")
    _fire_postrun(task_id="t1", state="FAILURE")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "tasks.add"
    assert ev.status_code == 500
    assert "ValueError: boom" in ev.error_message
    assert "ValueError" in ev.metadata["error.stack"]


def test_celery_postrun_without_prerun_is_inert(instrumented_celery, events):
    _fire_postrun(task_id="ghost", state="SUCCESS")
    task_failure.send(
        sender=None,
        task_id="ghost",
        exception=RuntimeError("late"),
        args=(),
        kwargs={},
        traceback=None,
        einfo=None,
    )
    assert events == []


def test_celery_prerun_without_postrun_does_not_leak_events(instrumented_celery, events):
    # prerun alone opens nothing on the wire until the span ends; firing a
    # second task's cycle must not be confused by the abandoned entry.
    _fire_prerun(task_id="abandoned")
    _fire_prerun(task_id="t2")
    _fire_postrun(task_id="t2", state="SUCCESS")
    assert len(events) == 1
    assert events[0].name == "tasks.add"
    # test hygiene: drop the abandoned entry and its current-span context so
    # nothing leaks into the rest of the suite (a real worker loses the whole
    # context with the killed task, so the SDK has nothing to clean up).
    from dataflow.spans import _current_span as current

    current.set(None)
    with dataflow.contrib._celery_lock:
        dataflow.contrib._celery_spans.clear()


def test_celery_idempotent(instrumented_celery, events):
    dataflow.instrument_celery()  # second call: dispatch_uid dedupes
    _fire_prerun(task_id="t1")
    _fire_postrun(task_id="t1", state="SUCCESS")
    assert len(events) == 1


def test_celery_uninstrument_stops_spans(events):
    dataflow.instrument_celery()
    _fire_prerun(task_id="t1")
    _fire_postrun(task_id="t1", state="SUCCESS")
    assert len(events) == 1
    dataflow.uninstrument_celery()
    _fire_prerun(task_id="t2")
    _fire_postrun(task_id="t2", state="SUCCESS")
    assert len(events) == 1


def test_celery_chains_with_other_receivers(instrumented_celery, events):
    seen = []

    def before(sender=None, **kwargs):
        seen.append("before")

    task_prerun.connect(before, dispatch_uid="before")
    try:
        _fire_prerun(task_id="t1")
        # both the pre-existing receiver and the Dataflow one ran
        assert seen == ["before"]
        _fire_postrun(task_id="t1", state="SUCCESS")  # ends the span
        assert len(events) == 1
    finally:
        task_prerun.disconnect(dispatch_uid="before")

    # after uninstrument_celery the unrelated receiver is still connected
    dataflow.uninstrument_celery()
    seen.clear()
    baseline = len(events)

    def kept(sender=None, **kwargs):
        seen.append("kept")

    task_prerun.connect(kept, dispatch_uid="kept")
    try:
        _fire_prerun(task_id="t2")
        assert seen == ["kept"]
        assert len(events) == baseline  # no new spans
    finally:
        task_prerun.disconnect(dispatch_uid="kept")


def test_celery_weird_signal_args_do_not_raise(instrumented_celery, events):
    # receivers must never raise into celery's signal dispatch
    task_prerun.send(sender=None, task_id=None)
    task_postrun.send(sender=None, task_id=None, state="SUCCESS", retval=None)
    task_failure.send(
        sender=None, task_id=None, exception=None, args=(), kwargs={}, traceback=None, einfo=None
    )
    task_failure.send(sender=None, task_id="x", exception=None, args=(), kwargs={}, traceback=None, einfo=None)
    task_prerun.send(sender="just-a-string", task=None, task_id="anon")
    _fire_postrun(task_id="anon", state="SUCCESS")
    assert [e.name for e in events] == ["task", "anon"]  # id fallback for the label


def test_celery_disabled_is_inert(events, monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    dataflow.instrument_celery()
    try:
        _fire_prerun(task_id="t1")
        _fire_postrun(task_id="t1", state="SUCCESS")
        assert events == []
    finally:
        dataflow.uninstrument_celery()


def test_celery_missing_celery(monkeypatch, events):
    monkeypatch.setitem(sys.modules, "celery", None)
    with pytest.raises(ImportError, match="celery"):
        dataflow.instrument_celery()


# -- Django middleware ----------------------------------------------------------


class FakeDjangoResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code


def _fake_request(method="POST", path="/ship", route=None):
    request = types.SimpleNamespace(method=method, path=path, resolver_match=None)
    if route is not None:
        request.resolver_match = types.SimpleNamespace(route=route)
    return request


def test_django_middleware_basic(events):
    response = FakeDjangoResponse(201)
    middleware = DataflowMiddleware(lambda request: response)
    assert middleware(_fake_request(method="POST", path="/ship")) is response
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "POST /ship"
    assert ev.type == HTTP_SERVER
    assert ev.status_code == 201
    assert ev.metadata["http.status_code"] == "201"
    assert ev.metadata["http.method"] == "POST"
    assert ev.metadata["http.path"] == "/ship"
    assert "http.duration_ms" in ev.metadata


def test_django_middleware_route_renames_span(events):
    response = FakeDjangoResponse(200)
    middleware = DataflowMiddleware(lambda request: response)
    request = _fake_request(method="GET", path="/orders/42", route="orders/<int:pk>/")
    middleware(request)
    ev = events[0]
    assert ev.name == "GET orders/<int:pk>/"
    assert ev.metadata["http.route"] == "orders/<int:pk>/"
    assert ev.metadata["http.path"] == "/orders/42"


def test_django_middleware_500_records_error(events):
    middleware = DataflowMiddleware(lambda request: FakeDjangoResponse(500))
    middleware(_fake_request())
    ev = events[0]
    assert ev.status_code == 500
    assert "http 500" in ev.error_message


def test_django_middleware_exception_sets_500_and_reraises(events):
    def boom(request):
        raise RuntimeError("kaboom")

    middleware = DataflowMiddleware(boom)
    with pytest.raises(RuntimeError, match="kaboom"):
        middleware(_fake_request())
    ev = events[0]
    assert ev.status_code == 500
    assert "kaboom" in ev.error_message


def test_django_middleware_disabled_passthrough(events, monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    response = FakeDjangoResponse(200)
    middleware = DataflowMiddleware(lambda request: response)
    assert middleware(_fake_request()) is response
    assert events == []


def test_django_middleware_module_imports_without_django(monkeypatch):
    monkeypatch.setitem(sys.modules, "django", None)
    sys.modules.pop("dataflow.django_middleware", None)
    try:
        mod = importlib.import_module("dataflow.django_middleware")
        assert mod.DataflowMiddleware is DataflowMiddleware
    finally:
        sys.modules.pop("dataflow.django_middleware", None)


def test_django_middleware_real_request(events):
    pytest.importorskip("django")
    import django
    from django.conf import settings as dj_settings

    if not dj_settings.configured:
        dj_settings.configure(
            DEBUG=False,
            INSTALLED_APPS=[],
            DATABASES={},
            ALLOWED_HOSTS=["testserver"],
            USE_TZ=True,
        )
        django.setup()
    from django.http import HttpResponse
    from django.test import RequestFactory

    from dataflow.contrib import DataflowMiddleware as MW

    request = RequestFactory().get("/orders/42")
    request.resolver_match = types.SimpleNamespace(route="orders/<int:pk>/")
    middleware = MW(lambda req: HttpResponse("ok", status=201))
    response = middleware(request)
    assert response.status_code == 201
    ev = events[0]
    assert ev.name == "GET orders/<int:pk>/"
    assert ev.type == HTTP_SERVER
    assert ev.metadata["http.path"] == "/orders/42"


# -- smoke: exports ---------------------------------------------------------------


def test_contrib_exports():
    for name in (
        "instrument_sqlalchemy",
        "uninstrument_sqlalchemy",
        "instrument_psycopg",
        "instrument_asyncpg",
        "instrument_httpx",
        "restore_httpx",
        "instrument_celery",
        "uninstrument_celery",
        "DataflowMiddleware",
    ):
        assert hasattr(dataflow, name), name
        assert name in dataflow.__all__
    assert dataflow.__version__ == "0.8.0"
    assert dataflow.django_middleware.DataflowMiddleware is DataflowMiddleware
