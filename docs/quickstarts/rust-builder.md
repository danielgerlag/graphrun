# Build a workflow in Rust

Same graph as the YAML below: reserve inventory, then charge. Typed builder: [`samples/02-passing-data/builder.rs`](../../samples/02-passing-data/builder.rs). YAML: [`workflow.yaml`](../../samples/02-passing-data/workflow.yaml). Catalog: [`catalog.json`](../../samples/02-passing-data/catalog.json).

`payload!(Order, "order")` binds the struct to catalog schema `order/v1`. `activity_v1` checks that against the activity contract. First `.activity` uses the run input; the next uses the previous output.

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
use graphrun::{payload, workflow, Catalog};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Order {
	order_id: String,
	amount: i64,
}
payload!(Order, "order");

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder {
	order_id: String,
	amount: i64,
	reservation_id: String,
}
payload!(ReservedOrder, "reserved_order");

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
	order_id: String,
	amount: i64,
	payment_id: String,
}
payload!(Receipt, "receipt");

let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
let reserve = catalog.activity_v1::<Order, ReservedOrder>("inventory.reserve")?;
let charge = catalog.activity_v1::<ReservedOrder, Receipt>("payment.charge")?;
let definition = workflow::<Order>("passing_data")
	.activity("reserve", &reserve)?
	.activity("charge", &charge)?
	.finish(&catalog)?;

let engine = Engine::builder("./graphrun-data")
	.activity("inventory.reserve", |order: Order| async move { /* your code */ Ok(reserved) })?
	.activity("payment.charge", |reserved: ReservedOrder| async move { /* your code */ Ok(receipt) })?
	.open()
	.await?;
// Or Engine::local("./graphrun-data") for the built-in sample fixtures.
let run = engine.start(definition, catalog, input).await?;
let output = engine.wait_terminal(run, std::time::Duration::from_secs(10)).await?;
```

Nested bodies use `region::<T>()` (scope input, complete as `done`):

```rust
let body = region::<Counter>()
	.activity("increment", &increment)?
	.finish()?;
workflow::<Counter>("while_counter")
	.while_lt("count", "/value", 3, body, 10)?
	.finish(&catalog)?;
```

`RegionBuilder` / `RegionGraphBuilder` remain for graphs that are not a straight chain.

```sh
cargo run -p graphrun-samples --bin 02-passing-data
```
