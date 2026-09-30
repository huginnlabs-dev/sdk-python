"""Tests for transport tracing: outgoing HTTP (requests) + DB_QUERY spans."""

import sys
import types

import pytest

import dataflow
from dataflow.transport import stmt_summary

# Wire enum values (proto/dataflow.proto): HTTP_CLIENT=3, DB_QUERY=6.
HTTP_CLIENT = 3
DB_QUERY = 6


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code
        self.headers = {}


# -- stmt_summary: verb + table derivation ---------------------------------


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("SELECT * FROM orders WHERE id = %s", "SELECT orders"),
        ("select id, total from orders", "SELECT orders"),
        ("select * from\n\tpublic.orders\n where x = 1", "SELECT orders"),
        ("INSERT INTO orders (id, total) VALUES (1, 2)", "INSERT orders"),
        ("insert into public.orders (id) values (1)", "INSERT orders"),
        ("UPDATE public.items SET price = 5 WHERE id = 1", "UPDATE items"),
        ("DELETE FROM carts WHERE id = 3", "DELETE carts"),
        ("CREATE TABLE IF NOT EXISTS users (id int)", "CREATE users"),
        ("CREATE INDEX idx_orders ON orders (id)", "CREATE"),
        ("DROP TABLE IF EXISTS old_logs", "DROP old_logs"),
        ("ALTER TABLE items ADD COLUMN note text", "ALTER items"),
        ("TRUNCATE TABLE events", "TRUNCATE events"),
        ("WITH recent AS (SELECT 1) SELECT * FROM orders", "WITH orders"),
        ("explain select * from orders", "EXPLAIN orders"),
        ("begin", "BEGIN"),
        ("VACUUM ANALYZE items", "VACUUM"),
        ("VACUUM", "QUERY"),
        ("", "QUERY"),
    ],
)
def test_stmt_summary(statement, expected):
    assert stmt_summary(statement) == expected


# -- db_span ---------------------------------------------------------------


def test_db_span_metadata(events):
    with dataflow.db_span(
        "postgres", "SELECT   *\n\tFROM orders\n WHERE id = %s", params=[42]
    ) as span:
        span.set_attr("db.rows", "1")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "SELECT orders"
    assert ev.type == DB_QUERY
    assert ev.callee_package == "postgres"
    assert ev.metadata["db.system"] == "postgres"
    assert ev.metadata["db.statement"] == "SELECT * FROM orders WHERE id = %s"
    assert ev.status_code == 200
    assert ev.parent_span_id == ""  # root span
    assert ev.trace_id == span.trace_id


def test_db_span_statement_truncated_to_200(events):
    stmt = "SELECT * FROM orders WHERE note = '" + "x" * 500 + "'"
    with dataflow.db_span("sqlite", stmt):
        pass
    sent = events[0].metadata["db.statement"]
    assert len(sent) == 200
    assert sent == " ".join(stmt.split())[:200]


def test_db_span_never_captures_param_values(events):
    secret = "hunter2-super-secret-value"
    with dataflow.db_span(
        "postgres", "SELECT * FROM users WHERE password = %s", params=[secret]
    ):
        pass
    sent = repr(events[0].metadata) + repr(events[0].payload.data)
    assert secret not in sent


def test_db_span_records_error(events):
    with pytest.raises(RuntimeError):
        with dataflow.db_span("postgres", "DELETE FROM carts"):
            raise RuntimeError("boom")
    ev = events[0]
    assert ev.name == "DELETE carts"
    assert ev.status_code == 500
    assert "boom" in ev.error_message


def test_db_span_nests_under_trace(events):
    with dataflow.trace("shop.Checkout") as outer:
        with dataflow.db_span("postgres", "SELECT * FROM orders"):
            pass
    assert len(events) == 2
    child = next(e for e in events if e.name == "SELECT orders")
    assert child.parent_span_id == outer.span_id
    assert child.trace_id == outer.trace_id


def test_db_span_disabled_is_inert(monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    with dataflow.db_span("postgres", "SELECT 1") as span:
        span.set_attr("x", "y")  # must not raise
        span.set_status(200)


def test_db_span_best_effort_on_engine_failure(monkeypatch, events):
    import dataflow.transport as transport

    def explode(*args, **kwargs):
        raise RuntimeError("no proto stubs")

    monkeypatch.setattr(transport, "start_span", explode)
    with dataflow.db_span("postgres", "SELECT 1") as span:
        span.set_attr("x", "y")  # inactive span: must not raise
    assert events == []


# -- instrument_requests ---------------------------------------------------


class FakeSession:
    """Duck-typed session: proves the wrapper works without real requests
    plumbing and captures the kwargs it was called with."""

    def __init__(self):
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return FakeResponse(200)


@pytest.fixture
def sending_session():
    """A real requests.Session whose send() is short-circuited (no network),
    capturing the prepared request."""
    import requests

    captured = {}

    def fake_send(self, request, **kwargs):
        captured["request"] = request
        return FakeResponse(201)

    session = requests.Session()
    session.send = types.MethodType(fake_send, session)
    return session, captured


def test_instrument_requests_span(events, sending_session):
    session, captured = sending_session
    assert dataflow.instrument_requests(session) is session
    resp = session.request("GET", "https://api.example.com/orders?limit=5")
    assert resp.status_code == 201
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "GET api.example.com/orders"
    assert ev.type == HTTP_CLIENT
    assert ev.callee_package == "api.example.com"
    assert ev.metadata["http.method"] == "GET"
    assert ev.metadata["http.url"] == "https://api.example.com/orders?limit=5"
    assert ev.status_code == 201
    # The downstream service joins the same trace via the request header.
    assert captured["request"].headers["X-Dataflow-Trace-Id"] == ev.trace_id


def test_instrument_requests_convenience_method_and_headers(events, sending_session):
    session, captured = sending_session
    dataflow.instrument_requests(session)
    headers = {"Authorization": "Bearer token-1"}
    session.get("https://api.example.com/orders", headers=headers)
    req = captured["request"]
    assert req.headers["authorization"] == "Bearer token-1"
    assert req.headers["X-Dataflow-Trace-Id"]
    # The caller's own header mapping is never mutated.
    assert headers == {"Authorization": "Bearer token-1"}
    assert events[0].name == "GET api.example.com/orders"
    assert events[0].metadata["http.method"] == "GET"


def test_instrument_requests_kwargs_form(events, sending_session):
    session, _ = sending_session
    dataflow.instrument_requests(session)
    session.request(method="GET", url="https://api.example.com/kw")
    assert events[0].name == "GET api.example.com/kw"


def test_instrument_requests_joins_existing_trace(events, sending_session):
    session, captured = sending_session
    dataflow.instrument_requests(session)
    with dataflow.trace("shop.Checkout") as outer:
        session.post("https://pay.example.com/capture", json={"amount": 1})
    assert len(events) == 2
    # The HTTP span ends first (inside the with block), the trace span last.
    ev = events[0]
    assert ev.name == "POST pay.example.com/capture"
    assert captured["request"].headers["X-Dataflow-Trace-Id"] == outer.trace_id
    assert ev.trace_id == outer.trace_id
    assert ev.parent_span_id == outer.span_id


def test_instrument_requests_error(events, sending_session):
    session, _ = sending_session

    def boom(self, request, **kwargs):
        raise ConnectionError("connection refused")

    session.send = types.MethodType(boom, session)
    dataflow.instrument_requests(session)
    with pytest.raises(ConnectionError):
        session.request("GET", "https://api.example.com/orders")
    assert len(events) == 1
    assert "connection refused" in events[0].error_message


def test_instrument_requests_idempotent(events, sending_session):
    session, _ = sending_session
    dataflow.instrument_requests(session)
    dataflow.instrument_requests(session)
    session.get("https://api.example.com/once")
    assert len(events) == 1


def test_instrument_requests_fake_session(events):
    session = FakeSession()
    dataflow.instrument_requests(session)
    session.request("GET", "https://api.example.com/fake")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "GET api.example.com/fake"
    assert ev.callee_package == "api.example.com"
    assert session.calls[0][2]["headers"]["X-Dataflow-Trace-Id"] == ev.trace_id


def test_instrument_requests_global(events):
    import requests

    original = requests.Session.request
    try:
        dataflow.instrument_requests()
        dataflow.instrument_requests()  # idempotent at class level too
        session = requests.Session()

        def fake_send(self, request, **kwargs):
            return FakeResponse(200)

        session.send = types.MethodType(fake_send, session)
        session.get("https://api.example.com/global")
        assert len(events) == 1
        assert events[0].name == "GET api.example.com/global"
    finally:
        requests.Session.request = original


def test_instrument_requests_missing_requests(monkeypatch):
    monkeypatch.setitem(sys.modules, "requests", None)
    with pytest.raises(ImportError, match="requests"):
        dataflow.instrument_requests()


def test_instrument_requests_disabled(monkeypatch, sending_session):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    session, captured = sending_session
    dataflow.instrument_requests(session)
    resp = session.get("https://api.example.com/orders")
    assert resp.status_code == 201
    # Disabled: no span, no trace id header, call untouched.
    assert "X-Dataflow-Trace-Id" not in captured["request"].headers
