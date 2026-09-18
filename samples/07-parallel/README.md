# 07 Parallel

Two branches quote tax and shipping at the same time. The join is an all-join: the output is a tuple in branch order, `[{cents: 100}, {cents: 500}]` for amount 1000.

This is the Graphrun equivalent of WorkflowCore Sample13. The typed builder uses `declare_parallel2`.

## Run

```sh
cargo run -p graphrun-samples --bin 07-parallel
```

The bin compiles this YAML against `catalog.json` in this directory, builds the same graph with `RegionGraphBuilder`, checks IR parity, and runs both on `Engine::local`.
