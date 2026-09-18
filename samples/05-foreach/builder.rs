use graphrun::Catalog;
use graphrun::builder::{RegionBuilder, WorkflowBuilder};
use graphrun_samples::{Counter, catalog, pretty, run_pair, to_value};

const YAML: &str = include_str!("workflow.yaml");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let increment = catalog.activity_ref::<Counter, Counter>("counter.increment", 1)?;
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body.activity("increment", &increment, body.input())?;
    let body = body.complete("done", bumped.output())?;
    let mut root = RegionBuilder::<Vec<Counter>>::new();
    let each = root.foreach("increment_all", root.workflow_input(), body, 100, 4)?;
    let root = root.complete("finish", each.output())?;
    WorkflowBuilder::new("foreach_counter", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = catalog()?;
    let built = build(&catalog)?;
    let input = to_value(&vec![
        Counter { value: 3 },
        Counter { value: 1 },
        Counter { value: 3 },
    ])?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("foreach {}", pretty(&output));
    Ok(())
}
