"""Tests for log shipping: helpers, batching, the logging.Handler tap and
the disabled no-op paths."""

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import dataflow
import dataflow.config as config
import dataflow.logs as logs


@pytest.fixture(autouse=True)
def _logs_env(monkeypatch):
    """Hermetic log tests: park the background flusher on a long interval so
    shipping only happens when a test calls flush_logs(), keep span delivery
    inert, and never leak flusher/handlers/buffer between tests."""
    import dataflow.client as client

    monkeypatch.setattr(logs, "FLUSH_INTERVAL", 3600.0)
    # Park the 50-record threshold too: without it, _wake fires mid-test and
    # the flusher thread races flush_logs() for the buffered records.
    monkeypatch.setattr(logs, "FLUSH_THRESHOLD", 10**9)
    monkeypatch.setattr(client, "enqueue", lambda ev: None)
    yield
    logs._reset_for_tests()


@pytest.fixture
def log_sdk(monkeypatch):
    """SDK enabled against a URL-form endpoint (never contacted; tests that
    care about delivery short-circuit _send or use the live server)."""
    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(
            api_key="test-key", endpoint="http://logs.local", service_name="test-svc"
        ),
    )


@pytest.fixture
def sent(monkeypatch):
    """Delivery short-circuited into a list of drained batches (the log
    counterpart of the conftest ``events`` fixture)."""
    captured: list = []

    def fake_send(batch):
        captured.append(batch)
        return True

    monkeypatch.setattr(logs, "_send", fake_send)
    return captured


# -- helpers: entry shape and trace correlation ------------------------------


def test_helpers_attach_current_trace_ids(log_sdk, sent):
    with dataflow.trace("shop.Checkout") as span:
        dataflow.info("reserving", order_id="o-42")
    assert dataflow.flush_logs() == 1
    entry = sent[0][0]
    assert entry["message"] == "reserving"
    assert entry["level"] == "info"
    assert entry["trace_id"] == span.trace_id
    assert entry["span_id"] == span.span_id
    assert entry["service_name"] == "test-svc"
    assert entry["fields"] == {"order_id": "o-42"}
    assert isinstance(entry["timestamp"], int)
    assert abs(entry["timestamp"] - time.time() * 1000) < 5000  # unix ms


def test_helpers_outside_a_span_have_empty_ids(log_sdk, sent):
    dataflow.debug("background work")
    assert dataflow.flush_logs() == 1
    entry = sent[0][0]
    assert entry["trace_id"] == ""
    assert entry["span_id"] == ""


@pytest.mark.parametrize(
    "emit, expected",
    [
        (lambda: dataflow.debug("m"), "debug"),
        (lambda: dataflow.info("m"), "info"),
        (lambda: dataflow.warn("m"), "warn"),
        (lambda: dataflow.error("m"), "error"),
    ],
)
def test_helper_levels(log_sdk, sent, emit, expected):
    emit()
    assert dataflow.flush_logs() == 1
    assert sent[0][0]["level"] == expected


def test_log_normalizes_warning_and_clamps_unknown(log_sdk, sent):
    dataflow.log("WARNING", "w")
    dataflow.log("Error", "e")
    dataflow.log("verbose", "v")
    assert dataflow.flush_logs() == 3
    assert [entry["level"] for entry in sent[0]] == ["warn", "error", "info"]


def test_fields_are_stringified_and_capped_at_50(log_sdk, sent):
    extra = {f"k{i}": i for i in range(60)}
    dataflow.error("boom", count=7, nested={"a": 1}, none=None, **extra)
    assert dataflow.flush_logs() == 1
    fields = sent[0][0]["fields"]
    assert len(fields) == 50  # server clamp mirrored client-side
    assert fields["count"] == "7"
    assert fields["nested"] == "{'a': 1}"
    assert fields["none"] == "None"


def test_message_clipped_to_8k(log_sdk, sent):
    dataflow.error("x" * 20_000)
    assert dataflow.flush_logs() == 1
    message = sent[0][0]["message"]
    assert len(message.encode("utf-8")) <= logs.MESSAGE_MAX_BYTES


# -- buffering ----------------------------------------------------------------


def test_buffer_drops_oldest_at_1024(log_sdk, sent):
    for i in range(1030):
        dataflow.info(f"m{i}")
    assert dataflow.flush_logs() == 1024  # m6 .. m1029 survive
    first, second = sent[0], sent[1]
    assert len(first) == 1000  # server batch cap
    assert len(second) == 24
    assert first[0]["message"] == "m6"
    assert second[-1]["message"] == "m1029"


# -- wire delivery against a local HTTP server --------------------------------


class _LogsHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = self.rfile.read(length)
        self.server.captured.append(
            {
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": json.loads(payload.decode("utf-8")) if payload else None,
            }
        )
        self.send_response(self.server.status)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def logs_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _LogsHandler)
    server.captured = []
    server.status = 200
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def live_sdk(monkeypatch, logs_server):
    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(
            api_key="k-123",
            endpoint=f"http://127.0.0.1:{logs_server.server_address[1]}",
            service_name="svc-logs",
        ),
    )
    return logs_server


def test_batch_posts_wire_shape(live_sdk):
    with dataflow.trace("shop.Checkout") as span:
        dataflow.info("reserving", order_id="o-42")
    dataflow.debug("d")
    dataflow.warn("w")
    dataflow.error("e")
    assert dataflow.flush_logs() == 4

    captured = live_sdk.captured
    assert len(captured) == 1
    request = captured[0]
    assert request["path"] == "/api/v1/logs"
    assert request["headers"]["x-api-key"] == "k-123"
    assert request["headers"]["content-type"] == "application/json"

    body = request["body"]
    assert list(body) == ["logs"]
    entries = body["logs"]
    assert len(entries) == 4
    assert [e["level"] for e in entries] == ["info", "debug", "warn", "error"]
    for entry in entries:
        assert set(entry) == {
            "timestamp",
            "level",
            "message",
            "trace_id",
            "span_id",
            "service_name",
            "fields",
        }
        assert isinstance(entry["timestamp"], int)
        assert entry["service_name"] == "svc-logs"
        assert isinstance(entry["fields"], dict)
    assert entries[0]["trace_id"] == span.trace_id
    assert entries[0]["span_id"] == span.span_id
    assert entries[0]["fields"] == {"order_id": "o-42"}


def test_batches_capped_at_1000_per_post(live_sdk):
    for i in range(1030):
        dataflow.info(f"m{i}")
    assert dataflow.flush_logs() == 1024
    sizes = [len(request["body"]["logs"]) for request in live_sdk.captured]
    assert sizes == [1000, 24]


def test_one_retry_then_drop(live_sdk):
    live_sdk.status = 500
    dataflow.error("boom")
    assert dataflow.flush_logs() == 0  # dropped, never raised
    assert len(live_sdk.captured) == 2  # original post + exactly one retry


# -- stdlib logging.Handler tap ------------------------------------------------


def test_handler_forwards_records_with_extras(log_sdk, sent):
    logger = logging.getLogger("dataflow.test.app")
    logger.setLevel(logging.DEBUG)
    dataflow.install_log_handler(logger)
    assert logger.propagate is True  # untouched: the handler only taps

    root_seen = []
    root_handler = logging.Handler()
    root_handler.emit = lambda record: root_seen.append(record)
    logging.getLogger().addHandler(root_handler)
    try:
        with dataflow.trace("shop.Pay"):
            logger.warning("disk %d%% full", 93, extra={"table": "orders"})
    finally:
        logging.getLogger().removeHandler(root_handler)

    assert root_seen  # the record still propagated to the root logger
    assert dataflow.flush_logs() == 1
    entry = sent[0][0]
    assert entry["level"] == "warn"  # WARNING normalizes to warn
    assert entry["message"] == "disk 93% full"
    assert entry["fields"] == {"table": "orders"}
    assert entry["trace_id"] != ""
    assert entry["span_id"] != ""


def test_handler_skips_standard_attrs_and_bad_extras(log_sdk, sent):
    class Bad:
        def __str__(self):
            raise RuntimeError("no")

    logger = logging.getLogger("dataflow.test.stdattrs")
    logger.setLevel(logging.DEBUG)
    dataflow.install_log_handler(logger)
    logger.info("plain")
    logger.error("mixed", extra={"good": 1, "bad": Bad()})
    assert dataflow.flush_logs() == 2
    assert sent[0][0]["fields"] == {}  # stdlib record attrs are not fields
    assert sent[0][1]["fields"] == {"good": "1"}  # unstringifiable extras skipped


def test_handler_maps_critical_to_error(log_sdk, sent):
    logger = logging.getLogger("dataflow.test.critical")
    dataflow.install_log_handler(logger)
    logger.critical("on fire")
    assert dataflow.flush_logs() == 1
    assert sent[0][0]["level"] == "error"


def test_install_is_idempotent_and_remove_restores(log_sdk, sent):
    logger = logging.getLogger("dataflow.test.idem")
    logger.setLevel(logging.DEBUG)
    dataflow.install_log_handler(logger)
    first = logger.handlers[-1]
    dataflow.install_log_handler(logger)  # second install must not duplicate
    assert logger.handlers == [first]

    logger.error("one")
    logger.error("two")
    assert dataflow.flush_logs() == 2

    dataflow.remove_log_handler(logger)
    assert logger.handlers == []
    dataflow.remove_log_handler(logger)  # restoring twice is a no-op
    logger.critical("gone")
    assert dataflow.flush_logs() == 0
    assert len(sent) == 1  # only the first two records ever shipped


def test_install_default_attaches_to_root(log_sdk, sent):
    dataflow.install_log_handler()
    root = logging.getLogger()
    assert any(isinstance(h, logs.DataflowLogHandler) for h in root.handlers)
    dataflow.info("via api")
    root.warning("via stdlib")  # root's default level is WARNING
    assert dataflow.flush_logs() == 2
    messages = {entry["message"] for batch in sent for entry in batch}
    assert messages == {"via api", "via stdlib"}


# -- disabled / logging-off configurations -------------------------------------


def test_disabled_config_noops_everything(monkeypatch):
    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(api_key="k", endpoint="http://x", disabled=True),
    )
    dataflow.error("nope", x=1)
    assert dataflow.flush_logs() == 0
    logger = logging.getLogger("dataflow.test.disabled")
    dataflow.install_log_handler(logger)  # handler never installed
    assert logger.handlers == []


def test_bare_host_port_endpoint_means_logging_off(monkeypatch):
    monkeypatch.delenv("DATAFLOW_HTTP_URL", raising=False)  # conftest clears it
    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(api_key="k", endpoint="localhost:9090"),
    )
    dataflow.warn("silently ignored")
    assert dataflow.flush_logs() == 0
    root = logging.getLogger()
    before = list(root.handlers)
    dataflow.install_log_handler()
    assert root.handlers == before


def test_http_url_override_enables_logging(monkeypatch, sent):
    monkeypatch.setenv("DATAFLOW_HTTP_URL", "http://override.local")
    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(api_key="k", endpoint="localhost:9090", service_name="svc"),
    )
    dataflow.info("x")
    assert dataflow.flush_logs() == 1
    assert sent[0][0]["message"] == "x"
