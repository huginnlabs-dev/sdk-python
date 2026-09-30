# HuginnLabs Dataflow — Python SDK

Runtime tracing for Python services, wire-compatible with the Go SDK: spans
stream to the SaaS ingestion API over gRPC, payloads are sealed client-side
with AES-256-GCM, and delivery uses a bounded replay buffer with
ack-watermark redelivery.

```python
import dataflow  # auto-configures from DATAFLOW_* env vars

app.add_middleware(dataflow.ASGIMiddleware)   # FastAPI / Starlette

@app.post("/ship")
async def ship(req: ShipRequest):
    with dataflow.trace("warehouse.Reserve") as span:
        span.set_data("request", req.model_dump())
        ...

@dataflow.traced("couriers.Quote")            # decorator flavour
async def quote(weight_kg: float) -> float: ...
```

Environment: `DATAFLOW_API_KEY`, `DATAFLOW_ENDPOINT`, `DATAFLOW_SERVICE_NAME`,
`DATAFLOW_ENCRYPTION_KEY`, `DATAFLOW_SAMPLE_RATIO`, `DATAFLOW_BUFFER_SIZE`,
`DATAFLOW_MAX_BODY_BYTES`, `DATAFLOW_DISABLED` — identical to the Go SDK.

## Outgoing HTTP and database tracing

```python
import dataflow

# requests: one HTTP_CLIENT span per call, named "GET api.example.com/orders",
# with http.method / http.url metadata and an X-Dataflow-Trace-Id request
# header so downstream Dataflow-instrumented services join the same trace.
dataflow.instrument_requests()                     # every requests.Session
dataflow.instrument_requests(session=my_session)   # or a single session

# database blocks: one DB_QUERY span named "SELECT orders". db.system carries
# the engine, db.statement the single-spaced statement truncated to 200 chars.
# Bind parameter values are never captured.
with dataflow.db_span("postgres", "SELECT * FROM orders WHERE id = %s", params=[order_id]):
    cursor.execute(sql, params)
```

`instrument_requests` needs the optional `requests` package and raises a
clear `ImportError` only when it is missing. Both helpers are best-effort:
with tracing disabled (no API key/endpoint, or `DATAFLOW_DISABLED`) they
produce no spans and never disturb the calls they wrap.

## Exception capture

```python
import dataflow

with dataflow.capture_exceptions():
    do_work()   # any exception raised here is recorded, then re-raised
```

A raised exception is recorded on the enclosing span (a `dataflow.trace`
block, an HTTP server span, ...) or, when none is active, on a short-lived
synthetic `exception` span: status 500, `error_message` carrying the
`"Type: message"` repr truncated to 500 characters, and `error.stack` with
the formatted traceback capped at 8192 bytes (the top of the traceback is
kept). The exception is never swallowed — it re-raises after recording, and
a recording failure can never mask the original exception.

`dataflow.capture_uncaught()` installs `sys.excepthook` /
`threading.excepthook` wrappers so crashes that escape to the interpreter
still land on a synthetic `uncaught exception` span; the previous hooks are
chained afterwards, preserving existing behaviour. `dataflow.ignore_uncaught()`
restores the originals.

WSGI/ASGI: most frameworks intercept handler exceptions and render a 500
response before Dataflow would see them, so wrap the inner app call — e.g.
`with dataflow.capture_exceptions(): await self.app(scope, receive, send)`
in a thin ASGI middleware, or the WSGI callable invocation on the WSGI side.
`ASGIMiddleware` already records the exception message on its HTTP_SERVER
span; nesting `capture_exceptions()` adds `error.stack`.

## Route scanning

`python -m dataflow.scan` statically extracts the HTTP endpoints a service
declares and posts them to the Dataflow service catalog. It parses source
with the stdlib `ast` module only — the scanned code is never imported or
executed, and no web framework needs to be installed where the scan runs.

```
python -m dataflow.scan --dir . --service orders-api \
    --url https://dataflow.example.com --api-key "$DATAFLOW_API_KEY"
```

Recognized declarations:

- **Flask** — `@app.get/post/put/delete/patch` and
  `@app.route("/p", methods=[...])` on any decorator-object name,
  including Blueprints (`@bp.route`). `methods` defaults to `GET`.
- **FastAPI** — `@app.get(...)`, `@router.get(...)`, `@app.api_route(...)`;
  `{id}` path parameters are kept exactly as written.
- **Starlette** — `Route("/p", endpoint, methods=[...])` constructor calls;
  `methods` defaults to `GET`, `handler` is the endpoint argument's name.
- **aiohttp** — `router.add_get/post/...` and `@routes.get` decorators.

Routes whose path or method list is not a literal (a variable, an f-string)
are skipped rather than guessed. Files named `test_*.py` / `*_test.py` and
the directories `venv/`, `node_modules/`, `__pycache__/` and every dotted
directory are not scanned; files that fail to parse are counted and
skipped, never fatal. At most 1000 routes are posted (the server's limit).
Django is not covered — `urls.py` routing is its own mini-language.

Flags: `--dir` (default `.`), `--service` (default `DATAFLOW_SERVICE_NAME`
or the directory basename), `--url`, `--api-key`, and `--print` to dump the
catalog JSON to stdout instead of posting. The base URL resolves as
`--url` > `DATAFLOW_HTTP_URL` > URL-form `DATAFLOW_ENDPOINT`; a bare
`host:port` `DATAFLOW_ENDPOINT` with no override ends the scan with a clear
message and nothing is posted. Exit codes: 0 ok (posted, printed, or
nothing to post), 1 scan error (bad `--dir`), 2 post failure (missing API
key, network error or non-2xx response). A friendly summary
(`N routes across M files`) always goes to stderr.

The protobuf stubs under `dataflow/proto_gen/` are generated from
`proto/dataflow.proto` during the Docker build (grpcio-tools) and kept in
sync with the wire contract automatically.
