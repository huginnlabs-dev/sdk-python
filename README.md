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

The protobuf stubs under `dataflow/proto_gen/` are generated from
`proto/dataflow.proto` during the Docker build (grpcio-tools) and kept in
sync with the wire contract automatically.
