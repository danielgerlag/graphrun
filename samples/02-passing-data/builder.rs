use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun_samples::{Order, Receipt, ReservedOrder, catalog, pretty, run_pair, to_value};

const YAML: &str = include_str!("workflow.yaml");

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
    let catalog = catalog()?;
    let built = build(&catalog)?;
    let input = to_value(&Order {
        order_id: "o1".to_owned(),
        amount: 1000,
        fail_after_payment: None,
    })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("passing-data {}", pretty(&output));
    Ok(())
}
