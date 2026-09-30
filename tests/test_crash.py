"""Tests for crash capture: capture_exceptions + capture_uncaught hooks."""

import sys
import threading
import traceback

import pytest

import dataflow
import dataflow.crash as crash


@pytest.fixture(autouse=True)
def _uncaught_cleanup():
    """Guarantee the excepthook wrappers never leak between tests, even
    when a test fails before its own ignore_uncaught() runs."""
    yield
    crash.ignore_uncaught()


def _fake_exc(cls, message):
    """An exception instance carrying a real traceback object, like the
    interpreter hands to excepthook."""
    try:
        raise cls(message)
    except cls as e:
        return e


# -- capture_exceptions ----------------------------------------------------


def test_capture_exceptions_synthetic_span(events):
    with pytest.raises(ValueError, match="boom"):
        with dataflow.capture_exceptions():
            raise ValueError("boom")
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "exception"
    assert ev.status_code == 500
    assert ev.error_message == "ValueError: boom"
    stack = ev.metadata["error.stack"]
    assert stack.startswith("Traceback (most recent call last):")
    assert "ValueError: boom" in stack
    assert ev.parent_span_id == ""  # synthetic root span


def test_capture_exceptions_records_on_current_span(events):
    with pytest.raises(RuntimeError):
        with dataflow.trace("shop.Checkout") as outer:
            with dataflow.capture_exceptions():
                raise RuntimeError("boom")
    # Recorded on the enclosing trace span — nothing extra is enqueued.
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "shop.Checkout"
    assert ev.trace_id == outer.trace_id
    assert ev.status_code == 500
    assert "boom" in ev.error_message
    assert "error.stack" in ev.metadata


def test_capture_exceptions_re_raises_original(events):
    sentinel = ValueError("sentinel")
    with pytest.raises(ValueError) as info:
        with dataflow.capture_exceptions():
            raise sentinel
    assert info.value is sentinel


def test_capture_exceptions_stack_clipped_to_8192(events, monkeypatch):
    huge = "Traceback (most recent call last):\n" + "x" * 100_000
    monkeypatch.setattr(traceback, "format_exc", lambda *a, **k: huge)
    with pytest.raises(ValueError):
        with dataflow.capture_exceptions():
            raise ValueError("boom")
    stack = events[0].metadata["error.stack"]
    assert len(stack.encode("utf-8")) <= crash.STACK_MAX_BYTES
    assert stack.startswith("Traceback (most recent call last):")  # top kept
    assert len(stack) < len(huge)


def test_capture_exceptions_disabled_is_passthrough(monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    with pytest.raises(ValueError, match="boom"):
        with dataflow.capture_exceptions():
            raise ValueError("boom")


def test_capture_exceptions_best_effort_on_engine_failure(events, monkeypatch):
    def explode(*args, **kwargs):
        raise RuntimeError("no proto stubs")

    monkeypatch.setattr(crash, "current_span", lambda: None)
    monkeypatch.setattr(crash, "start_span", explode)
    with pytest.raises(ValueError, match="boom"):
        with dataflow.capture_exceptions():
            raise ValueError("boom")
    assert events == []


# -- capture_uncaught ------------------------------------------------------


def test_capture_uncaught_chains_and_records(events, monkeypatch):
    seen = []

    def prev_sys(exc_type, exc_value, exc_tb):
        seen.append(("sys", exc_value))

    def prev_thread(args):
        seen.append(("thread", args))

    monkeypatch.setattr(sys, "excepthook", prev_sys)
    monkeypatch.setattr(threading, "excepthook", prev_thread)

    dataflow.capture_uncaught()
    assert sys.excepthook is not prev_sys  # wrapped
    assert threading.excepthook is not prev_thread

    exc = _fake_exc(ValueError, "boom")
    sys.excepthook(ValueError, exc, exc.__traceback__)  # installed hook, fake exc tuple
    assert seen == [("sys", exc)]  # chained to the previous hook
    assert len(events) == 1
    ev = events[0]
    assert ev.name == "uncaught exception"
    assert ev.status_code == 500
    assert ev.error_message == "ValueError: boom"
    assert ev.metadata["error.stack"].startswith("Traceback")

    dataflow.ignore_uncaught()
    assert sys.excepthook is prev_sys
    assert threading.excepthook is prev_thread


def test_capture_uncaught_threading_hook(events, monkeypatch):
    seen = []
    monkeypatch.setattr(threading, "excepthook", lambda args: seen.append(args))

    dataflow.capture_uncaught()
    try:
        exc = _fake_exc(ValueError, "thread boom")
        args = threading.ExceptHookArgs((ValueError, exc, exc.__traceback__, None))
        threading.excepthook(args)
        assert seen == [args]
        assert len(events) == 1
        ev = events[0]
        assert ev.name == "uncaught exception"
        assert ev.error_message == "ValueError: thread boom"
        assert ev.metadata["error.stack"].startswith("Traceback")
    finally:
        dataflow.ignore_uncaught()


def test_capture_uncaught_idempotent(events, monkeypatch):
    monkeypatch.setattr(sys, "excepthook", lambda t, v, tb: None)

    dataflow.capture_uncaught()
    try:
        first_sys, first_thread = sys.excepthook, threading.excepthook
        dataflow.capture_uncaught()  # second install must not wrap again
        assert sys.excepthook is first_sys
        assert threading.excepthook is first_thread
    finally:
        dataflow.ignore_uncaught()
    dataflow.ignore_uncaught()  # restoring twice is a no-op


def test_capture_uncaught_disabled(monkeypatch):
    import dataflow.config as config

    monkeypatch.setattr(config, "_settings", config.Settings(disabled=True))
    before_sys, before_thread = sys.excepthook, threading.excepthook
    dataflow.capture_uncaught()  # must not install anything
    assert sys.excepthook is before_sys
    assert threading.excepthook is before_thread
    dataflow.ignore_uncaught()  # no-op, must not raise


def test_capture_uncaught_chains_when_recording_fails(events, monkeypatch):
    def explode(*args, **kwargs):
        raise RuntimeError("no proto stubs")

    seen = []
    monkeypatch.setattr(sys, "excepthook", lambda t, v, tb: seen.append(v))
    monkeypatch.setattr(crash, "start_span", explode)

    dataflow.capture_uncaught()
    try:
        exc = ValueError("boom")
        sys.excepthook(ValueError, exc, None)
        assert seen == [exc]  # the previous hook still ran, nothing escaped
        assert events == []
    finally:
        dataflow.ignore_uncaught()
