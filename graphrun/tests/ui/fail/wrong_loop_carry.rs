use graphrun::builder::RegionBuilder;
use graphrun::catalog::Catalog;
use graphrun::schema::{DurablePayload, SchemaRef};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Counter {
    value: i64,
}
impl DurablePayload for Counter {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("counter", 1).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
}
impl DurablePayload for Order {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("order", 1).unwrap()
    }
}

fn main() {
    let catalog = Catalog::from_json(include_bytes!("../../../../docs/specs/v1/examples/activity-catalog.json")).unwrap();
    let increment = catalog.activity_ref::<Counter, Counter>("counter.increment", 1).unwrap();
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body.activity("increment", &increment, body.input()).unwrap();
    let body = body.complete("done", bumped.output()).unwrap();
    let mut root = RegionBuilder::<Order>::new();
    let count = root.literal(3_i64).unwrap();
    let _ = root.repeat("repeat", count, root.input(), body, 10);
}
