"""HuginnLabs Dataflow SDK for Python.

One import auto-configures the SDK from DATAFLOW_* environment variables and
starts a background gRPC streaming client that ships trace events to the
SaaS ingestion endpoint, mirroring the Go SDK wire-for-wire:

    import dataflow          # auto-configures from the environment

    from fastapi import FastAPI
    import dataflow

    app = FastAPI()
    app.add_middleware(dataflow.ASGIMiddleware)

    @app.post("/ship")
    async def ship(req: ShipRequest):
        with dataflow.trace("warehouse.Reserve") as span:
            span.set_data("request", req.model_dump())
            ...
"""

from .config import configure, settings, enabled
from .spans import (
    Span,
    start_span,
    trace,
    traced,
    span_from_context,
    current_span,
)
from .middleware import ASGIMiddleware
from .client import http_client
from .transport import db_span, instrument_requests
from . import django_middleware
from .contrib import (
    DataflowMiddleware,
    instrument_asyncpg,
    instrument_celery,
    instrument_httpx,
    instrument_psycopg,
    instrument_sqlalchemy,
    restore_httpx,
    uninstrument_celery,
    uninstrument_sqlalchemy,
)
from .crash import capture_exceptions, capture_uncaught, ignore_uncaught
from .logs import (
    DataflowLogHandler,
    debug,
    error,
    flush_logs,
    info,
    install_log_handler,
    log,
    remove_log_handler,
    warn,
)

__all__ = [
    "configure",
    "settings",
    "enabled",
    "Span",
    "start_span",
    "trace",
    "traced",
    "span_from_context",
    "current_span",
    "ASGIMiddleware",
    "http_client",
    "db_span",
    "instrument_requests",
    "instrument_sqlalchemy",
    "uninstrument_sqlalchemy",
    "instrument_psycopg",
    "instrument_asyncpg",
    "instrument_httpx",
    "restore_httpx",
    "instrument_celery",
    "uninstrument_celery",
    "DataflowMiddleware",
    "capture_exceptions",
    "capture_uncaught",
    "ignore_uncaught",
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
__version__ = "0.8.0"
