# 06 Choice / If

`choose` evaluates `when` conditions in order. If `value` equals 1, the bump case increments; otherwise the default echoes the input. Exclusive branch, not a set of independent ifs.

This is the Graphrun equivalent of WorkflowCore Sample11. Case bodies share an input and output schema.

## Run

```sh
cargo run -p graphrun-samples --bin 06-choice
```

The bin compiles this YAML against `catalog.json` in this directory, builds the same graph with `RegionBuilder::choose`, checks IR parity, and runs both the matching case and the default on `Engine::local`.
