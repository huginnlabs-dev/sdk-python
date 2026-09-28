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

The protobuf stubs under `dataflow/proto_gen/` are generated from
`proto/dataflow.proto` during the Docker build (grpcio-tools) and kept in
sync with the wire contract automatically.
