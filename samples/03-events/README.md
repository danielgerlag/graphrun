# 03 Events

The run parks on `wait_signal` until the host delivers an `approval` event with matching correlation key `order-1`. The bin starts the run, calls `Engine::signal`, then `wait_terminal` — it does not sit forever.

This is the Graphrun equivalent of WorkflowCore Sample04 `WaitFor`. Events are addressed by run, signal name, and key. `EventId` is 32 hex characters.

## Run

```sh
cargo run -p graphrun-samples --bin 03-events
```

The bin compiles this YAML, builds the same graph with `RegionBuilder`, checks IR parity, and runs both on `Engine::local`.
