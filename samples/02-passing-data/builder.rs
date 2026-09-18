use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun_samples::{pretty, run_pair, to_value};
use serde::{Deserialize, Serialize};

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
}

impl DurablePayload for Order {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("order", 1).expect("order/v1")
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
        SchemaRef::named("reserved_order", 1).expect("reserved_order/v1")
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
        SchemaRef::named("receipt", 1).expect("receipt/v1")
    }
}

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let reserve = catalog.activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)?;
    let charge = catalog.activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)?;
    let mut root = RegionBuilder::<Order>::new();
    let reserved = root.activity("reserve", &reserve, root.workflow_input())?;
    let charged = root.activity("charge", &charge, reserved.output())?;
    let root = root.complete("finish", charged.output())?;
    WorkflowBuilder::new("passing_data", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let input = to_value(&Order {
        order_id: "o1".to_owned(),
        amount: 1000,
    })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("passing-data {}", pretty(&output));
    Ok(())
}
