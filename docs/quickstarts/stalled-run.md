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
graphrun history --run <run-id> --page-size 100 --local-dir ./graphrun-data
graphrun replay --run <run-id> --through-sequence 4 --local-dir ./graphrun-data
```

`history` returns `retained_from`, `retained_through`, `events`, and `next_cursor`. Each event has a per-run `sequence`, a version, a command cause when available, and a verified payload reference. To read the next page, pass its `next_cursor` as `--after-sequence`. A page contains at most 1,000 events.

`replay --through-sequence` returns the run view after that event, including pending waits and compensation progress. Omit the flag to replay through the last retained event. Both the live control socket and the offline `replay` command read recorded facts without running activities or writing to the store. You can also use `--endpoint`, `--ca`, `--cert`, `--tls-key`, and `--server-name` for a cluster member.

Active runs keep their history. Terminal runs keep full history for 30 days, then keep a summary without a typed output for another 60 days. After the full history expires, a history request either reports the unavailable range or returns `unavailable: true` with `unavailable_range: {first, last}` and an empty page. Do not treat a missing page as an empty run.

The engine rejects an older data directory if its runs lack versioned history records or use an unsupported checkpoint format. It reports `FailedPrecondition` before starting workers and leaves the directory unchanged. There is no automatic migration for these records.
