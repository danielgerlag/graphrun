# Build a workflow in Rust

Same graph as the YAML below: reserve inventory, then charge. Typed builder lives in [`samples/02-passing-data/builder.rs`](../../samples/02-passing-data/builder.rs). YAML: [`workflow.yaml`](../../samples/02-passing-data/workflow.yaml). Catalog: [`catalog.json`](../../samples/02-passing-data/catalog.json).

`DurablePayload` on a struct is a catalog schema name (`order/v1`), not the Rust type name. `activity_ref` checks the struct against that contract. Use `workflow_input()` for the run input (YAML `from: workflow.input`). `input()` is the current region’s input (`scope.input`).

## Definition

```yaml
dsl: graphrun/v1
id: passing_data
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: reserve
nodes:
  reserve:
    kind: activity
    activity: {name: inventory.reserve, version: 1}
    input: {from: workflow.input}
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

`inventory.reserve` and `payment.charge` are built-in fixtures. Input `{"order_id":"o1","amount":1000}` finishes with `payment_id: pay-1`.

## Builder

```rust
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun::{Catalog, Engine};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order {
	order_id: String,
	amount: i64,
}
impl DurablePayload for Order {
	fn schema_ref() -> SchemaRef {
		SchemaRef::named("order", 1).unwrap()
	}
}

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder {
	order_id: String,
	amount: i64,
	reservation_id: String,
}
impl DurablePayload for ReservedOrder {
	fn schema_ref() -> SchemaRef {
		SchemaRef::named("reserved_order", 1).unwrap()
	}
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
	order_id: String,
	amount: i64,
	payment_id: String,
}
impl DurablePayload for Receipt {
	fn schema_ref() -> SchemaRef {
		SchemaRef::named("receipt", 1).unwrap()
	}
}

let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
let reserve = catalog.activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)?;
let charge = catalog.activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)?;
let mut root = RegionBuilder::<Order>::new();
let reserved = root.activity("reserve", &reserve, root.workflow_input())?;
let charged = root.activity("charge", &charge, reserved.output())?;
let root = root.complete("finish", charged.output())?;
let definition = WorkflowBuilder::new("passing_data", 1, root).build(&catalog)?;

let engine = Engine::local("./graphrun-data").await?;
let run = engine.start(definition, catalog, input).await?;
let output = engine.wait_terminal(run, std::time::Duration::from_secs(10)).await?;
```

```sh
cargo run -p graphrun-samples --bin 02-passing-data
```
