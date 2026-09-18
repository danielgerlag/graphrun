# Inspect a stalled run

A wait that never gets a signal stays `active` with a pending wait. This is that graph:

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

Until `Engine::signal` / `graphrun signal` delivers `approval` with key `order-1`, inspect shows `status: active` and `pending_waits` with `"signal":"approval"`.

```sh
graphrun inspect --run <run-id> --local-dir ./graphrun-data
```

Read these fields in order:

1. `blocked_reason`. Why the run is not advancing, if it is still active.
2. `status`. `active` means the run is not terminal.
3. `pending_waits`. A row with `signal` and `key` is a wait.
4. `obligations`. `blocked` needs `resolve --blocked-input`. `compensating` is in-flight undo.
5. `recovery`. If `suspended` is true, the store was restored and needs `resolve --ack`.
6. `error`. Domain failure, not a transport timeout.

```sh
graphrun history --run <run-id> --local-dir ./graphrun-data
graphrun replay --run <run-id> --local-dir ./graphrun-data
```

`replay` is read-only. It does not start activities or write the store.
