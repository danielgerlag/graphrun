# Build a workflow in Rust

This how-to compiles a typed graph that matches `sequence.yaml` and runs it on a local engine.

## Open a catalog

```rust
use graphrun::{Catalog, Engine, Value};

let catalog = Catalog::from_json(include_bytes!(
    "docs/specs/v1/examples/activity-catalog.json"
))?;
```

`activity_ref` binds input and output types. A mismatch fails at compile time. See `graphrun/tests/api.rs` and `graphrun/tests/ui/` for the pass and fail cases.

## Construct the graph

Follow `graphrun/tests/api.rs`. The builder and YAML compiler produce one IR. `cargo test --test api yaml_and_builder_while_match` checks that parity.

## Run it

```rust
let engine = Engine::local("/tmp/graphrun-builder").await?;
let run = engine.start(definition, catalog, input).await?;
let output = engine.wait_terminal(run, std::time::Duration::from_secs(10)).await?;
engine.shutdown().await?;
```

Local mode still commits through one-member Raft. There is no in-memory shortcut.
