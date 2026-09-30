"""Hermetic test bootstrap.

The SDK's heavy runtime pieces are build artifacts: the proto stubs are
generated into ``dataflow/proto_gen`` during the Docker build and grpcio is
imported by the delivery client. Neither is needed to unit-test the tracing
logic, so when they are missing we install minimal stand-ins before any
test imports ``dataflow``.
"""

import sys
import types

import pytest


def _ensure_grpc_stub() -> None:
    if "grpc" in sys.modules:
        return
    try:
        import grpc  # noqa: F401

        return
    except ImportError:
        pass
    stub = types.ModuleType("grpc")
    stub.insecure_channel = lambda *a, **k: None
    stub.channel_ready_future = lambda *a, **k: types.SimpleNamespace(
        result=lambda timeout: None
    )
    sys.modules["grpc"] = stub


def _ensure_pb_stub() -> None:
    import dataflow.spans as spans

    if spans.pb is not None:
        return

    class _Payload:
        def __init__(self):
            self.encrypted = False
            self.data = b""
            self.iv = b""
            self.key_salt = ""

    class _TraceEvent:
        def __init__(
            self,
            event_id="",
            timestamp=0,
            type=0,
            name="",
            service_name="",
            trace_id="",
            span_id="",
            parent_span_id="",
        ):
            self.event_id = event_id
            self.timestamp = timestamp
            self.type = type
            self.name = name
            self.service_name = service_name
            self.trace_id = trace_id
            self.span_id = span_id
            self.parent_span_id = parent_span_id
            self.caller_package = ""
            self.callee_package = ""
            self.function_name = ""
            self.status_code = 0
            self.duration_ms = 0
            self.error_message = ""
            self.metadata = {}
            self.payload = _Payload()
            self.seq = 0

    spans.pb = types.SimpleNamespace(
        TraceEvent=_TraceEvent,
        EVENT_TYPE_FUNCTION_CALL=1,
        EVENT_TYPE_HTTP_SERVER=2,
        EVENT_TYPE_HTTP_CLIENT=3,
        EVENT_TYPE_GRPC=4,
        EVENT_TYPE_DB_QUERY=6,
    )


_ensure_grpc_stub()
_ensure_pb_stub()


@pytest.fixture(autouse=True)
def _default_settings(monkeypatch):
    """Every test starts from a fresh, disabled configuration regardless of
    the developer's shell environment."""
    import dataflow.config as config

    for name in (
        "DATAFLOW_API_KEY",
        "DATAFLOW_ENDPOINT",
        "DATAFLOW_DISABLED",
        "DATAFLOW_SAMPLE_RATIO",
        "DATAFLOW_ENCRYPTION_KEY",
        "DATAFLOW_HTTP_URL",
        "DATAFLOW_SERVICE_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "_settings", config.Settings())


@pytest.fixture
def events(monkeypatch):
    """Hermetic capture: SDK enabled, delivery short-circuited into a list."""
    import dataflow.client as client
    import dataflow.config as config

    monkeypatch.setattr(
        config,
        "_settings",
        config.Settings(
            api_key="test-key", endpoint="localhost:1", service_name="test-svc"
        ),
    )
    captured: list = []
    monkeypatch.setattr(client, "enqueue", captured.append)
    return captured
