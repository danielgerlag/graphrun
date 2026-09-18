# 04 While

A `while` loop increments a counter while `loop.state.value` is less than 3. Starting at `{value: 0}` yields `{value: 3}`. Starting at `{value: 5}` skips the body and returns 5.

This is the Graphrun equivalent of WorkflowCore Sample10. The loop carry is the whole counter object; JSON Pointer paths start with `/`.

## Run

```sh
cargo run -p graphrun-samples --bin 04-while
```

The bin compiles this YAML against `catalog.json` in this directory, builds the same graph with `RegionBuilder`, checks IR parity, and runs both inputs on `Engine::local`.
