"""Tests for dataflow.contrib: SQLAlchemy / psycopg / asyncpg instrumentation,
the Django middleware and the @dataflow.traced decorator.

Hermeticity rules:
- sqlalchemy is a dev extra and installed for the test run; if missing, its
  tests skip (pytest.importorskip).
- psycopg / asyncpg are NOT required: a stub module is injected into
  sys.modules and the wrapped objects are duck-typed fakes, so the wrapper
  logic is tested without the real drivers. ImportError plumbing is tested
  by shadowing the module with None.
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
        "DataflowMiddleware",
    ):
        assert hasattr(dataflow, name), name
        assert name in dataflow.__all__
    assert dataflow.__version__ == "0.7.0"
    assert dataflow.django_middleware.DataflowMiddleware is DataflowMiddleware
