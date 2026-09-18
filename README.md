# Graphrun

Graphrun is a durable workflow engine. You write a graph in YAML or with a typed Rust builder. Both compile to the same IR. Every command goes through Raft, then a redb apply. Kill the process. Open the same data directory again. The run is still there.

Local mode is one Raft voter in-process. A cluster uses the same write path with more voters and optional remote workers. There is no in-memory shortcut.

## Install

Library:

```toml
[dependencies]
graphrun = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

CLI (installs the `graphrun` binary):

```sh
cargo install graphrun-cli --locked
```

Rust 1.90 or newer. Linux and macOS.

## Run a workflow

`Engine::local` owns a data directory. This graph reserves inventory, then charges payment. Built-in handlers in the library implement those two activity names.

```yaml
dsl: graphrun/v1
id: sequence
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: reserve
nodes:
  reserve:
    kind: activity
    activity: {name: inventory.reserve, version: 1}
    input: {from: workflow.input}
    retry:
      errors: [inventory.unavailable]
      max_attempts: 5
    next: charge
  charge:
    kind: activity
    activity: {name: payment.charge, version: 1}
    input: {from: nodes.reserve.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.charge.output}
```

```rust
use graphrun::{Catalog, Engine, Value};
use std::collections::BTreeMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> graphrun::Result<()> {
	let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
	let engine = Engine::local("/tmp/graphrun-demo").await?;
	let input = Value::Object(BTreeMap::from([
		("order_id".into(), Value::String("o1".into())),
		("amount".into(), Value::Int(1000)),
	]));
	let run = engine.start_yaml(include_str!("sequence.yaml"), &catalog, input).await?;
	let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
	println!("{output:?}");
	engine.shutdown().await?;
	Ok(())
}
```

The output is `payment_id: pay-1`.

A complete, runnable copy (catalog JSON included) is `graphrun/examples/order.rs`:

```sh
cargo run -p graphrun --example order
```

The catalog is contracts: schema, activity name/version, execution kind, effect kind, recovery, and error codes. YAML and the builder both require it. The example catalog and more graphs live in [`docs/specs/v1/examples`](docs/specs/v1/examples).

## Build the same graph in Rust

`activity_ref` binds input and output types. A mismatch fails at compile time.

```rust
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun::Catalog;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order { order_id: String, amount: i64 }
impl DurablePayload for Order {
	fn schema_ref() -> SchemaRef { SchemaRef::named("order", 1).unwrap() }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder { order_id: String, amount: i64, reservation_id: String }
impl DurablePayload for ReservedOrder {
	fn schema_ref() -> SchemaRef { SchemaRef::named("reserved_order", 1).unwrap() }
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt { order_id: String, amount: i64, payment_id: String }
impl DurablePayload for Receipt {
	fn schema_ref() -> SchemaRef { SchemaRef::named("receipt", 1).unwrap() }
}

let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
let reserve = catalog.activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)?;
let charge = catalog.activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)?;
let mut root = RegionBuilder::<Order>::new();
let reserved = root.activity("reserve", &reserve, root.input())?;
let charged = root.activity("charge", &charge, reserved.output())?;
let root = root.complete("finish", charged.output())?;
let definition = WorkflowBuilder::new("sequence", 1, root).build(&catalog)?;
```

Then `engine.start(definition, catalog, input).await`. YAML and builder produce one IR. `cargo test --test api yaml_and_builder_while_match` checks that parity.

Add `serde = { version = "1", features = ["derive"] }` for the payload types.

## CLI

```sh
graphrun serve --local-dir /tmp/graphrun-demo &

graphrun validate \
	--definition sequence.yaml \
	--catalog catalog.json

graphrun start \
	--definition sequence.yaml \
	--catalog catalog.json \
	--input order.json \
	--local-dir /tmp/graphrun-demo

graphrun inspect --run <run-id> --local-dir /tmp/graphrun-demo
```

`start` waits for a terminal status unless you pass `--no-wait`. Inspect prints `status`, `output`, `blocked_reason`, pending waits, and open scopes.

To deliver an external event:

```sh
graphrun signal \
	--run <run-id> \
	--name approval \
	--key k1 \
	--event-id aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
	--payload approval.json \
	--local-dir /tmp/graphrun-demo
```

`--key` is the wait correlation key. Certificate private keys use `--tls-key`.

Other commands: `cancel`, `list`, `history`, `replay`, `resolve` (blocked compensation, abandon), `backup`, `restore`, `cluster join|promote|remove`.

A cluster member is reached with `--endpoint`, `--ca`, `--cert`, `--tls-key`, and `--server-name` instead of `--local-dir`.

## What the engine does

- **One write path.** Command, Raft quorum, decide, events, evolve, redb apply. Local and clustered runs share that path.
- **Activities.** Catalog contracts are data. `Engine::local` runs the library handlers for the names in the example catalog (`inventory.reserve`, `payment.charge`, `counter.increment`, and the other fixture names). Those handlers are how the examples complete without a separate worker process.
- **Waits and signals.** A run can block on a named signal and a correlation key. `EventId` is a 32-character hex identity. Duplicate ids are idempotent.
- **Sagas.** A failed run compensates open obligations in reverse order. If compensation input cannot be built, the obligation is `blocked` until `resolve` supplies input or `--abandon --confirm`.
- **History.** `inspect` and `history` are read-only. Replay reconstructs state from recorded events. Apply does not call activities, clocks, or RNG.

## Cluster

Three members plus workers is the production shape. Members speak Tonic gRPC over mTLS. Workers claim ready activities from a member. See [Run a three-member cluster](docs/quickstarts/cluster.md).

## Limits

A definition may have 512 nodes. YAML, payloads, and the normalized IR are each capped at 256 KiB. Unapplied log is capped at 4,096 entries or 64 MiB. The full table is [docs/limits.md](docs/limits.md).

## How-tos

- [Run a local YAML workflow](docs/quickstarts/local-yaml.md)
- [Build a workflow in Rust](docs/quickstarts/rust-builder.md)
- [Run a three-member cluster](docs/quickstarts/cluster.md)
- [Deliver an external event](docs/quickstarts/events.md)
- [Compensate, intervene, or abandon](docs/quickstarts/compensation.md)
- [Back up and restore a member](docs/quickstarts/backup-restore.md)
- [Diagnose a stalled run](docs/quickstarts/stalled-run.md)

The v1 spec is [`docs/specs/v1`](docs/specs/v1).

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --skip snapshot_controller_fires
cargo build --release --locked -p graphrun-cli
cargo run --locked -p graphrun-e2e -- verify \
	--cli target/release/graphrun \
	--matrix docs/specs/v1/verification-matrix.tsv \
	--artifacts target/e2e-artifacts
```
