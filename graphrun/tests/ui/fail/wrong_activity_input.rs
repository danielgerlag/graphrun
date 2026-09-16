use graphrun::builder::RegionBuilder;
use graphrun::catalog::Catalog;
use graphrun::schema::{DurablePayload, SchemaRef};
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
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

fn main() {
    let catalog = Catalog::from_json(include_bytes!("../../../../docs/specs/v1/examples/activity-catalog.json")).unwrap();
    let charge = catalog.activity_ref::<ReservedOrder, Receipt>("payment.charge", 1).unwrap();
    let mut root = RegionBuilder::<Order>::new();
    let _ = root.activity("charge", &charge, root.input());
}
