# 01 Hello World

A linear sequence of two catalog activities, then `complete`. `counter.increment` adds one each time, so input `{value: 0}` finishes at `{value: 2}`.

This is the Graphrun equivalent of WorkflowCore Sample01: the smallest graph that actually runs. There is no custom handler registration; local mode executes the built-in increment handler.

## Run

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```

The bin compiles this YAML, builds the same graph with `RegionBuilder`, checks IR parity, and runs both on `Engine::local`.
