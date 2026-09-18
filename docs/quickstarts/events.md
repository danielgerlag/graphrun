# Wait for an external event

The run parks on `wait_signal` until the host calls `Engine::signal` with the same signal name and correlation key.

## Definition

[`samples/03-events/workflow.yaml`](../../samples/03-events/workflow.yaml)

```yaml
dsl: graphrun/v1
id: external_approval
version: 1
input_schema: event_request/v1
output_schema: approval/v1
signals:
  approval: {schema: approval/v1}
start: approval
nodes:
  approval:
    kind: wait_signal
    signal: approval
    key: {literal: order-1}
    consume_from: buffered
    timeout: null
    next: finish
  finish:
    kind: complete
    output: {from: nodes.approval.output}
```

Catalog: [`samples/03-events/catalog.json`](../../samples/03-events/catalog.json). Builder: [`builder.rs`](../../samples/03-events/builder.rs).

## Drive it

```rust
let run = engine.start_yaml(include_str!("workflow.yaml"), &catalog, input).await?;
engine
	.signal(run, event_id, "approval", "order-1", payload)
	.await?;
let output = engine.wait_terminal(run, Duration::from_secs(15)).await?;
```

`EventId` is 32 hex characters. Duplicate ids are idempotent. Output is `{"approved":true}`.

```sh
cargo run -p graphrun-samples --bin 03-events
```

Optional CLI, against a directory your app already uses:

```sh
graphrun signal \
	--run <run-id> \
	--name approval \
	--key order-1 \
	--event-id aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
	--payload approval.json \
	--local-dir ./graphrun-data
```

`--key` is the wait correlation key. Certificate private keys use `--tls-key`.
