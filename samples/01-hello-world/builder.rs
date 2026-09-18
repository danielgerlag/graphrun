use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun_samples::{pretty, run_pair, to_value};
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
    let mut root = RegionBuilder::<Counter>::new();
    let hello = root.activity("hello", &increment, root.workflow_input())?;
    let goodbye = root.activity("goodbye", &increment, hello.output())?;
    let root = root.complete("finish", goodbye.output())?;
    WorkflowBuilder::new("hello_world", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let input = to_value(&Counter { value: 0 })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("hello-world {}", pretty(&output));
    Ok(())
}
