# 05 Foreach

`foreach` maps a body over an array. Each item is incremented independently; the result stays in input order even if items finish out of order.

This is the Graphrun equivalent of WorkflowCore Sample09. The body sees the item as `scope.input`.

## Run

```sh
cargo run -p graphrun-samples --bin 05-foreach
```

The bin compiles this YAML against `catalog.json` in this directory, builds the same graph with `RegionBuilder`, checks IR parity, and runs both on `Engine::local`.
