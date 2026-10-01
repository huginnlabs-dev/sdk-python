"""Application log shipping with trace correlation.

Wire contract (the server is live): ``POST {base}/api/v1/logs`` with the
``X-Api-Key`` header and a JSON body::

    {"logs": [{"timestamp": <unix ms>, "level": "debug|info|warn|error",
               "message": str, "trace_id": str, "span_id": str,
               "service_name": str, "fields": {k: str}}]}

Batches hold at most 1000 records; the server clamps messages to 8 KiB and
fields to 50 x 512 bytes (all mirrored client-side). ``base`` resolves like
the manifest reporter: DATAFLOW_HTTP_URL wins, URL-form endpoints map
directly, and a bare host:port gRPC endpoint has no derivable HTTP base —
logging stays off there.

Everything here is best-effort: the helpers never block or raise, records
buffer in a bounded deque (drop-oldest), and the background flusher posts
with a short timeout, retries once, then drops the batch.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from collections import deque
from typing import Any, Dict, List, Optional, Union
from weakref import WeakKeyDictionary

from .config import settings
from .manifest import resolve_http_base
from .spans import current_span

__all__ = [
    "debug",
    "info",
    "warn",
    "error",
    "log",
    "install_log_handler",
    "remove_log_handler",
    "flush_logs",
    "DataflowLogHandler",
]

LOGS_PATH = "/api/v1/logs"
MAX_BATCH = 1000  # server-enforced batch limit, mirrored client-side
BUFFER_MAX = 1024  # bounded buffer; the oldest records drop first
FLUSH_INTERVAL = 0.5  # seconds between background flush passes
FLUSH_THRESHOLD = 50  # buffered records that trigger an immediate pass
HTTP_TIMEOUT = 5.0  # seconds per post attempt
MESSAGE_MAX_BYTES = 8192  # server clamp, mirrored client-side
MAX_FIELDS = 50  # server clamp, mirrored client-side
FIELD_VALUE_MAX_BYTES = 512

LEVELS = ("debug", "info", "warn", "error")

_buf_lock = threading.Lock()
_buffer = deque(maxlen=BUFFER_MAX)
_install_lock = threading.Lock()
_handlers: WeakKeyDictionary = WeakKeyDictionary()  # logger -> handler
_wake = threading.Event()
_stop = threading.Event()
_flusher: Optional[threading.Thread] = None


# -- public helpers ---------------------------------------------------------

def debug(message: Any, **fields: Any) -> None:
    """Buffer a ``debug`` record (see :func:`log`)."""
    log("debug", message, **fields)


def info(message: Any, **fields: Any) -> None:
    """Buffer an ``info`` record (see :func:`log`)."""
    log("info", message, **fields)


def warn(message: Any, **fields: Any) -> None:
    """Buffer a ``warn`` record (see :func:`log`)."""
    log("warn", message, **fields)


def error(message: Any, **fields: Any) -> None:
    """Buffer an ``error`` record (see :func:`log`)."""
    log("error", message, **fields)


def log(level: str, message: Any, **fields: Any) -> None:
    """Buffer one application log record, correlated with the current span's
    trace/span ids when one is active. Never blocks or raises; with logging
    disabled (or no resolvable HTTP base) this is a no-op. ``level`` is one
    of ``debug|info|warn|error`` — ``warning`` normalizes to ``warn`` and
    unknown levels clamp to ``info``."""
    if not _log_enabled():
        return
    try:
        entry = _build_entry(level, message, fields)
    except Exception:  # noqa: BLE001 - logging must never disturb the app
        return
    with _buf_lock:
        _buffer.append(entry)  # deque(maxlen=...) drops the oldest silently
        buffered = len(_buffer)
    if buffered >= FLUSH_THRESHOLD:
        _wake.set()
    _start_flusher()


def flush_logs() -> int:
    """Synchronously drain and ship buffered records; returns how many
    reached the wire. Best-effort: a batch that fails after its single retry
    is dropped, and a disabled configuration flushes nothing."""
    sent = 0
    while _log_enabled():
        batch = _drain()
        if not batch or not _send(batch):
            break
        sent += len(batch)
    return sent


# -- stdlib logging tap -----------------------------------------------------

# Standard LogRecord attributes (never forwarded as fields; extras are the
# keys a caller passes via logging's ``extra={...}``).
_STD_RECORD_ATTRS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


class DataflowLogHandler(logging.Handler):
    """``logging.Handler`` that taps records into the Dataflow log buffer.

    Pure tap: it only emits — ``propagate`` and every other handler stay
    untouched, so normal logging behaviour is fully preserved. Attach it
    with :func:`install_log_handler`, detach with
    :func:`remove_log_handler`.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            fields: Dict[str, Any] = {}
            for key, value in record.__dict__.items():
                if key in _STD_RECORD_ATTRS or key.startswith("_"):
                    continue
                if len(fields) >= MAX_FIELDS:
                    break
                try:
                    fields[key] = str(value)
                except Exception:  # noqa: BLE001 - skip unstringifiable extras
                    continue
            log(_level_of(record), record.getMessage(), **fields)
        except Exception:  # noqa: BLE001 - a tap must never disturb the app
            pass


def _level_of(record: logging.LogRecord) -> str:
    name = (record.levelname or "").lower()
    if name == "warning":
        return "warn"
    if name in LEVELS:
        return name
    if record.levelno >= logging.ERROR:
        return "error"
    if record.levelno >= logging.WARNING:
        return "warn"
    if record.levelno >= logging.INFO:
        return "info"
    return "debug"


def install_log_handler(logger: Union[logging.Logger, str, None] = None) -> None:
    """Attach a :class:`DataflowLogHandler` to the given logger (default:
    the root logger; a name string is also accepted) so standard-library
    log records ship alongside ``dataflow.info`` calls with the same trace
    correlation: WARNING maps to ``warn``, the message is the formatted
    record message, and ``extra`` fields that stringify travel along.
    Records keep propagating — the handler only taps, it never swallows.
    Idempotent per logger; a no-op when logging is disabled."""
    target = _as_logger(logger)
    if not _log_enabled():
        return
    with _install_lock:
        if target in _handlers:
            return
        handler = DataflowLogHandler()
        target.addHandler(handler)
        _handlers[target] = handler
    _start_flusher()


def remove_log_handler(logger: Union[logging.Logger, str, None] = None) -> None:
    """Detach the handler installed on the given logger (default: root).
    No-op when nothing is installed."""
    target = _as_logger(logger)
    with _install_lock:
        handler = _handlers.pop(target, None)
    if handler is not None:
        target.removeHandler(handler)


def _as_logger(logger: Union[logging.Logger, str, None]) -> logging.Logger:
    if logger is None:
        return logging.getLogger()
    if isinstance(logger, str):
        return logging.getLogger(logger)
    return logger


# -- internals ----------------------------------------------------------------

def _log_enabled() -> bool:
    """Logging is on only when the SDK has an API key, is not disabled, and
    has a resolvable HTTP base (a bare host:port gRPC endpoint means off)."""
    s = settings()
    if not s.api_key or s.disabled:
        return False
    return resolve_http_base(s.endpoint) is not None


def _build_entry(level: str, message: Any, fields: Dict[str, Any]) -> Dict[str, Any]:
    span = current_span()
    return {
        "timestamp": int(time.time() * 1000),
        "level": _normalize_level(level),
        "message": _clip(message if isinstance(message, str) else str(message), MESSAGE_MAX_BYTES),
        "trace_id": span.trace_id if span is not None else "",
        "span_id": span.span_id if span is not None else "",
        "service_name": settings().service_name,
        "fields": _stringify_fields(fields),
    }


def _normalize_level(level: str) -> str:
    name = (level or "").strip().lower()
    if name == "warning":
        return "warn"
    if name in LEVELS:
        return name
    return "info"  # clamp unknown levels rather than dropping the record


def _stringify_fields(fields: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in fields.items():
        if len(out) >= MAX_FIELDS:
            break
        try:
            out[str(key)] = _clip(str(value), FIELD_VALUE_MAX_BYTES)
        except Exception:  # noqa: BLE001 - skip unstringifiable values
            continue
    return out


def _clip(text: str, max_bytes: int) -> str:
    return text.encode("utf-8", "replace")[:max_bytes].decode("utf-8", "ignore")


def _drain(limit: int = MAX_BATCH) -> List[Dict[str, Any]]:
    batch: List[Dict[str, Any]] = []
    with _buf_lock:
        while _buffer and len(batch) < limit:
            batch.append(_buffer.popleft())
    return batch


def _send(entries: List[Dict[str, Any]]) -> bool:
    """One post with a single retry; False means the batch was dropped."""
    s = settings()
    base = resolve_http_base(s.endpoint)
    if base is None or not s.api_key:
        return False
    body = json.dumps({"logs": entries}).encode("utf-8")
    for _ in range(2):
        try:
            req = urllib.request.Request(
                base + LOGS_PATH,
                data=body,
                headers={"Content-Type": "application/json", "X-Api-Key": s.api_key},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                resp.read()
            return True
        except Exception:  # noqa: BLE001 - log shipping is best-effort
            continue
    return False


# -- background flusher -------------------------------------------------------

def _start_flusher() -> None:
    """Idempotently spawn the background flusher daemon."""
    global _flusher
    flusher = _flusher
    if flusher is not None and flusher.is_alive():
        return
    with _buf_lock:
        if _flusher is not None and _flusher.is_alive():
            return
        _stop.clear()
        _flusher = threading.Thread(target=_flusher_loop, name="dataflow-logs", daemon=True)
        _flusher.start()


def _flusher_loop() -> None:
    while not _stop.is_set():
        _wake.wait(FLUSH_INTERVAL)  # read per pass; tests shrink it
        _wake.clear()
        if _stop.is_set():
            return
        try:
            _flush_once()
        except Exception:  # noqa: BLE001 - the flusher must never die
            continue


def _flush_once() -> None:
    while True:
        batch = _drain()
        if not batch or not _send(batch):
            return


def _reset_for_tests() -> None:
    """Stop the flusher, detach installed handlers and clear the buffer.
    Test-only: unit tests must never leak threads or state between cases."""
    global _flusher
    _stop.set()
    _wake.set()
    flusher = _flusher
    if flusher is not None:
        flusher.join(timeout=2.0)
    with _install_lock:
        installed = list(_handlers.items())
        _handlers.clear()
    for target, handler in installed:
        target.removeHandler(handler)
    with _buf_lock:
        _buffer.clear()
    _flusher = None
    _wake.clear()
    _stop.clear()
