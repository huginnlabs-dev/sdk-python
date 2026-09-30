"""Crash capture: exception stack traces onto the current span.

Wire contract (the server renders it): status 500, ``error_message`` is the
``"Type: message"`` repr truncated to 500 characters, and the ``error.stack``
metadata carries the formatted traceback capped at 8192 bytes with the TOP
kept (header plus the outermost frames).

- ``capture_exceptions`` wraps a block and never swallows: the exception is
  recorded, then re-raised.
- ``capture_uncaught`` installs ``sys.excepthook`` and
  ``threading.excepthook`` wrappers that record the crash on a synthetic
  ``"uncaught exception"`` span, then chain to the previous hook so existing
  behaviour is preserved; ``ignore_uncaught`` restores the originals.

Everything here is best-effort: with tracing disabled (no API key/endpoint,
or DATAFLOW_DISABLED) nothing is recorded and both helpers are pure
passthrough, and every span mutation is guarded so recording can never mask
the original exception.
"""

from __future__ import annotations

import sys
import threading
import traceback
from typing import Any, Optional

from .config import enabled
from .spans import Span, current_span, start_span

__all__ = ["capture_exceptions", "capture_uncaught", "ignore_uncaught"]

STACK_ATTR = "error.stack"
STACK_MAX_BYTES = 8192
MESSAGE_MAX_CHARS = 500

_SYNTHETIC_SPAN = "exception"
_UNCAUGHT_SPAN = "uncaught exception"

_install_lock = threading.Lock()
_installed = False
_prev_sys_hook: Any = None
_prev_threading_hook: Any = None


# -- capture_exceptions ----------------------------------------------------

def capture_exceptions() -> "_CaptureCtx":
    """Context manager recording any exception raised in the block, then
    re-raising it:

        with dataflow.capture_exceptions():
            do_work()

    The crash lands on the enclosing span (a ``dataflow.trace`` block, an
    HTTP server span, ...) or, when none is active, on a short-lived
    synthetic ``"exception"`` span. Status becomes 500, ``error_message``
    carries the ``"Type: message"`` repr truncated to 500 characters and
    ``error.stack`` the formatted traceback capped at 8192 bytes (top kept).
    Best-effort: with tracing disabled this is a pure passthrough, and a
    recording failure never masks the original exception.
    """
    return _CaptureCtx()


class _CaptureCtx:
    """trace()-style context manager: record on __exit__, always re-raise."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None and enabled():
            _record(exc, traceback.format_exc(), _SYNTHETIC_SPAN)
        return False


def _record(exc: BaseException, stack: str, synthetic_name: str) -> None:
    """Best-effort recording: current span when one is active, a short-lived
    synthetic span otherwise. Every step is guarded — recording must never
    raise into the crash path."""
    if not enabled():
        return
    try:
        span = current_span()
        synthetic: Optional[Span] = None
        if span is None:
            synthetic = start_span(synthetic_name)
            span = synthetic
        try:
            span.set_attr(STACK_ATTR, _clip_stack(stack))
            span.record_error(f"{type(exc).__name__}: {exc}"[:MESSAGE_MAX_CHARS])
            span.set_status(500)
        finally:
            if synthetic is not None:
                synthetic.end()
    except Exception:  # noqa: BLE001 - best-effort
        pass


def _clip_stack(stack: str) -> str:
    """UTF-8 clip to STACK_MAX_BYTES, keeping the top of the traceback."""
    return stack.encode("utf-8", "replace")[:STACK_MAX_BYTES].decode("utf-8", "ignore")


# -- capture_uncaught ------------------------------------------------------

def capture_uncaught() -> None:
    """Install ``sys.excepthook`` / ``threading.excepthook`` wrappers so
    exceptions that escape to the interpreter still land on a synthetic
    ``"uncaught exception"`` span. There is no re-raising here: after
    recording, the PREVIOUS hooks are chained so existing behaviour is
    preserved. Idempotent (flag-guarded); a no-op when tracing is disabled.
    Pair with ``ignore_uncaught`` to restore.
    """
    global _installed, _prev_sys_hook, _prev_threading_hook
    if not enabled():
        return
    with _install_lock:
        if _installed:
            return
        _prev_sys_hook = sys.excepthook
        _prev_threading_hook = threading.excepthook
        sys.excepthook = _sys_hook
        threading.excepthook = _thread_hook  # type: ignore[assignment]
        _installed = True


def ignore_uncaught() -> None:
    """Restore the hooks that were active before ``capture_uncaught()``.
    No-op when nothing is installed."""
    global _installed, _prev_sys_hook, _prev_threading_hook
    with _install_lock:
        if not _installed:
            return
        sys.excepthook = _prev_sys_hook
        threading.excepthook = _prev_threading_hook
        _prev_sys_hook = None
        _prev_threading_hook = None
        _installed = False


def _sys_hook(exc_type, exc_value, exc_tb) -> None:
    try:
        stack = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        _record(exc_value if exc_value is not None else exc_type(), stack, _UNCAUGHT_SPAN)
    except Exception:  # noqa: BLE001 - the hook chain must still run
        pass
    finally:
        prev = _prev_sys_hook
        if prev is not None:
            prev(exc_type, exc_value, exc_tb)


def _thread_hook(args: "threading.ExceptHookArgs") -> None:
    try:
        exc_type = getattr(args, "exc_type", None)
        exc_value = getattr(args, "exc_value", None)
        exc_tb = getattr(args, "exc_traceback", None)
        stack = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        _record(exc_value if exc_value is not None else exc_type(), stack, _UNCAUGHT_SPAN)
    except Exception:  # noqa: BLE001 - the hook chain must still run
        pass
    finally:
        prev = _prev_threading_hook
        if prev is not None:
            prev(args)
