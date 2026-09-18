# Graphrun

Graphrun is a Rust library. You depend on `graphrun`, open [`Engine::local`](https://docs.rs/graphrun/latest/graphrun/engine/struct.Engine.html) on a directory in your process, and drive runs through that API. The library uses your Tokio runtime. It does not start a server, install signal handlers, or require a database daemon.

The graph is data. Write it with the typed builder or with YAML. Both compile to the same IR. Each command is committed through Raft, then applied to a redb file in that directory. Restart the process on the same path and the run is still there.

## Install

```toml
[dependencies]
graphrun = "0.1.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Rust 1.90 or newer. Linux and macOS.

## Embed the engine

This is the whole local runtime: one Raft voter, in-process worker, Unix control socket under the data directory.

```rust
use graphrun::{Catalog, Engine, Value};
use std::collections::BTreeMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> graphrun::Result<()> {
	let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
	let engine = Engine::local("./graphrun-data").await?;
	let input = Value::Object(BTreeMap::from([
		("order_id".into(), Value::String("o1".into())),
		("amount".into(), Value::Int(1000)),
	]));
	let run = engine
		.start_yaml(include_str!("sequence.yaml"), &catalog, input)
		.await?;
	let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
	engine.shutdown().await?;
	println!("{output:?}");
	Ok(())
}
```

`start` takes a compiled [`Definition`](https://docs.rs/graphrun/latest/graphrun/ir/struct.Definition.html) from the builder. `start_yaml` compiles YAML against the same catalog. `inspect`, `signal`, `cancel`, and `history` are methods on `Engine`.

A complete listing, including catalog JSON, is `graphrun/examples/order.rs`. From this repo:

```sh
cargo run -p graphrun --example order
```

That prints `payment_id: pay-1`. The example activity names (`inventory.reserve`, `payment.charge`, and the other fixture names) have handlers inside the library. That is how the example finishes without a worker process of your own.

## Samples

YAML and typed-builder pairs that run on `Engine::local` live in [`samples/`](samples/README.md).

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```

## Typed builder

`activity_ref` checks input and output types against the catalog. A mismatch is a compile error.

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
let run = engine.start(definition, catalog, input).await?;
```

The catalog is contracts: schema, activity key/version, execution kind, effect kind, recovery, error codes. It is not Rust type names. YAML for the same graph is [`docs/specs/v1/examples/sequence.yaml`](docs/specs/v1/examples/sequence.yaml).

## Cluster

`Engine::member` is a storage replica. Activity execution is opt-in. `Engine::run_worker` is a process that claims work over gRPC/mTLS and does not open a data directory. Local and clustered runs use the same write path. See [Run a three-member cluster](docs/quickstarts/cluster.md).

## Optional CLI

[`graphrun-cli`](https://crates.io/crates/graphrun-cli) is an operator binary. It talks to an engine you already opened (`--local-dir`) or to a member (`--endpoint` plus mTLS). Your application does not depend on it.

```sh
cargo install graphrun-cli --locked
graphrun inspect --run <run-id> --local-dir ./graphrun-data
```

## Limits

512 nodes per definition. YAML, payloads, and normalized IR are each 256 KiB. Unapplied log: 4,096 entries or 64 MiB. Full table: [docs/limits.md](docs/limits.md).

## How-tos

- [Build a workflow in Rust](docs/quickstarts/rust-builder.md)
- [Run a local YAML workflow](docs/quickstarts/local-yaml.md)
- [Run a three-member cluster](docs/quickstarts/cluster.md)
- [Deliver an external event](docs/quickstarts/events.md)
- [Compensate, intervene, or abandon](docs/quickstarts/compensation.md)
- [Back up and restore a member](docs/quickstarts/backup-restore.md)
- [Diagnose a stalled run](docs/quickstarts/stalled-run.md)

Spec: [`docs/specs/v1`](docs/specs/v1).

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
