use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun_samples::{Counter, catalog, pretty, run_pair, to_value};

const YAML: &str = include_str!("workflow.yaml");

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
    let catalog = catalog()?;
    let built = build(&catalog)?;
    let input = to_value(&Counter { value: 0 })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("hello-world {}", pretty(&output));
    Ok(())
}
