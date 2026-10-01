# Graphrun

[![crates.io](https://img.shields.io/crates/v/graphrun.svg)](https://crates.io/crates/graphrun)
[![docs.rs](https://img.shields.io/docsrs/graphrun)](https://docs.rs/graphrun)
[![CI](https://github.com/danielgerlag/graphrun/actions/workflows/ci.yml/badge.svg)](https://github.com/danielgerlag/graphrun/actions/workflows/ci.yml)

Graphrun is an embedded durable workflow engine for Rust. Depend on `graphrun`, open `Engine::local` on a directory in your process, and drive runs through that API. It uses your Tokio runtime. It does not start a server or need a database daemon.

This is from the author of [Workflow Core](https://github.com/danielgerlag/workflow-core). Same job (long-running graphs, waits, sagas) as an embeddable library: the graph is data, local mode is one Raft voter on a redb file, no separate workflow server.

The graph is data: YAML or a typed builder, both the same IR. Commands go through Raft onto a redb file in that directory. Restart the process on the same path and the run is still there.

Register your own activity handlers with [`Engine::builder`](https://docs.rs/graphrun/latest/graphrun/engine/struct.Engine.html#method.builder). An activity name with no handler fails (`activity.unregistered`); it does not echo input. `Engine::local` enables the built-in sample fixtures (`counter.increment`, `inventory.reserve`, …) for tests and the sample catalog. Production code should register the handlers it needs.

## Install

```toml
[dependencies]
graphrun = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Rust 1.90 or newer. Linux and macOS. `0.1` resolves to the latest unyanked 0.1.x on crates.io.

## First run

Paste this into `src/main.rs`. Input `{value: 0}` finishes at `{value: 2}`. The increment is **your** function.

```rust
use graphrun::{payload, Catalog, Engine, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
struct Counter { value: i64 }
payload!(Counter, "counter");

const CATALOG: &[u8] = br#"{
  "format": "graphrun.catalog/v1",
  "schemas": {
    "counter/v1": {
      "type": "object",
      "required": ["value"],
      "additionalProperties": false,
      "properties": {"value": {"type": "integer"}}
    }
  },
  "activities": [
    {
      "name": "counter.increment",
      "version": 1,
      "input_schema": "counter/v1",
      "output_schema": "counter/v1",
      "execution": "async",
      "effects": "pure",
      "recovery": "RetrySafe",
      "error_codes": []
    }
  ]
}"#;

const YAML: &str = r#"
dsl: graphrun/v1
id: hello_world
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: hello
nodes:
  hello:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: workflow.input}
    next: goodbye
  goodbye:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: nodes.hello.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.goodbye.output}
"#;

#[tokio::main]
async fn main() -> graphrun::Result<()> {
	let catalog = Catalog::from_json(CATALOG)?;
	let engine = Engine::builder("./graphrun-data")
		.activity("counter.increment", |input: Counter| async move {
			Ok(Counter { value: input.value + 1 })
		})?
		.open()
		.await?;
	let input = Value::Object(BTreeMap::from([("value".into(), Value::Int(0))]));
	let run = engine.start_yaml(YAML, &catalog, input).await?;
	let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
	println!("{output:?}");
	engine.shutdown().await?;
	Ok(())
}
```

A catalog is the registry of payload schemas and activity contracts the graph may name. It is not the workflow and not your handler code.

Crate examples (also on docs.rs source):

```sh
cargo run -p graphrun --example hello_world
cargo run -p graphrun --example order
```

`hello_world` prints `{value: 2}`. `order` prints `payment_id: pay-1`.

## What to read next

Do these in order. Each sample folder is self-contained (`workflow.yaml`, `catalog.json`, payload types, builder).

1. [01 Hello World](https://github.com/danielgerlag/graphrun/tree/main/samples/01-hello-world) — the graph above, plus the typed builder
2. [02 Passing Data](https://github.com/danielgerlag/graphrun/tree/main/samples/02-passing-data) — order → reserve → charge
3. [03 Events](https://github.com/danielgerlag/graphrun/tree/main/samples/03-events) — `wait_signal` and `Engine::signal`

Then pick from [04–10](https://github.com/danielgerlag/graphrun/tree/main/samples): while, foreach, choose, parallel, saga, repeat, timeout recovery.

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```

How-tos:

- [Build a workflow in Rust](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/rust-builder.md)
- [Run YAML from your process](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/local-yaml.md)
- [Wait for an external event](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/events.md)
- [Inspect a stalled run](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/stalled-run.md)
- [Compensate or abandon](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/compensation.md)
- [Back up and restore](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/backup-restore.md)
- [Compact an offline member](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/compact-store.md)
- [Run a three-member cluster](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/cluster.md)

## Typed builder

`payload!(Order, "order")` binds a struct to catalog schema `order/v1`. First `.activity` uses the run input; the next uses the previous output. Same IR as the YAML.

The full listing is [samples/02-passing-data](https://github.com/danielgerlag/graphrun/tree/main/samples/02-passing-data).

```rust
let reserve = catalog.activity_v1::<Order, ReservedOrder>("inventory.reserve")?;
let charge = catalog.activity_v1::<ReservedOrder, Receipt>("payment.charge")?;
let definition = workflow::<Order>("passing_data")
	.activity("reserve", &reserve)?
	.activity("charge", &charge)?
	.finish(&catalog)?;
let run = engine.start(definition, catalog, input).await?;
```

## Optional CLI

[`graphrun-cli`](https://crates.io/crates/graphrun-cli) inspects and signals an engine you already opened. Your application does not depend on it.

```sh
cargo install graphrun-cli --locked
graphrun inspect --run <run-id> --local-dir ./graphrun-data
```

Publish a catalog, then a compiled definition, before starting by workflow name.
The catalog version and YAML's explicit workflow version are separate:

```sh
graphrun publish --catalog catalog.json --catalog-version 1 --local-dir ./graphrun-data
graphrun publish --definition workflow.yaml --catalog-version 1 --local-dir ./graphrun-data
graphrun start --workflow my_workflow --version 1 --start-key order-42 \
  --input order.json --local-dir ./graphrun-data
```

Omit `--version` to resolve latest once. A matching start key returns the original
run, even after a newer definition is published; differing input or explicit
version conflicts. Use `--command-id <32-hex>` across retries and
`graphrun command-result --command-id <32-hex> --local-dir ...` to query a
committed result after an uncertain outcome. Publication is immutable: the
same version/content is idempotent, changed same-version content is rejected.
Waiting for a failed run or timing out exits nonzero (use `--no-wait` to submit
only). Existing pre-publication stores require an explicit format migration;
opening them does not silently rewrite or initialize them.

The same operations are available through `Engine::publish_catalog`,
`publish_definition_yaml`, and `start_published_with_command`. For gRPC,
present a cluster-CA-signed URI SAN principal (`issue_principal`): the `admin`
role publishes, the `client` role starts. The cluster ID must match the
member's `identity.json`; a roleless node certificate or caller-supplied
principal text is not authority. The local socket remains owner-only.

## Cluster

After a local run works, `Engine::member` hosts storage without running handlers. An independent process uses `Worker::builder(endpoint, tls, catalog)` to register its application handlers, claim exact pinned contracts, and report results over gRPC/mTLS. It opens no replica or data directory. Fixture handlers are confined to tests and the explicitly enabled `fixture-worker` feature used by the non-published verification driver. [Worker how-to](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/worker.md) and [cluster how-to](https://github.com/danielgerlag/graphrun/blob/main/docs/quickstarts/cluster.md).

## Limits

A definition may have 512 nodes. YAML, payloads, and IR are each 256 KiB. [docs/limits.md](https://github.com/danielgerlag/graphrun/blob/main/docs/limits.md).
