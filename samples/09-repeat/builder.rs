use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun_samples::{expect_value, pretty, run_pair, to_value};
use serde::{Deserialize, Serialize};

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Counter {
    value: i64,
}

impl DurablePayload for Counter {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("counter", 1).expect("counter/v1")
    }
}

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let increment = catalog.activity_ref::<Counter, Counter>("counter.increment", 1)?;
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body.activity("increment", &increment, body.input())?;
    let body = body.complete("done", bumped.output())?;
    let mut root = RegionBuilder::<Counter>::new();
    let count = root.literal(3_i64)?;
    let repeated = root.repeat("count", count, root.workflow_input(), body, 10)?;
    let root = root.complete("finish", repeated.output())?;
    WorkflowBuilder::new("repeat_counter", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let output = run_pair(YAML, built, &catalog, to_value(&Counter { value: 1 })?).await?;
    expect_value(&output, &to_value(&Counter { value: 4 })?)?;
    println!("repeat {}", pretty(&output));
    Ok(())
}
