# 10 Timeout Recovery

A timed wait has two ports. If `approval` arrives within one second, the success port goes straight to `finish`. If it does not, the timeout port runs `counter.increment` and then the same `finish`. Both paths return the original counter; only timeout executes recovery.

This is the Graphrun equivalent of WorkflowCore `WaitFor` with a timeout. The shared tail is required: you cannot feed the wait payload into `finish` from both ports.

## Run

```sh
cargo run -p graphrun-samples --bin 10-timeout-recovery
```

The bin compiles this YAML, builds the same graph with `RegionGraphBuilder`, checks IR parity, then runs the success path (signal) and the timeout path (wait) on `Engine::local`.
