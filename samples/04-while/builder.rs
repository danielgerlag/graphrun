use graphrun::Catalog;
use graphrun::binding::{Binding, Condition, Reference};
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
    let condition = Condition::Lt {
        left: Binding::from_path(Reference::LoopState, "/value"),
        right: Binding::literal(graphrun::value::Value::Int(3)),
    };
    let looped = root.while_loop("count", root.workflow_input(), condition, body, 10)?;
    let root = root.complete("finish", looped.output())?;
    WorkflowBuilder::new("while_counter", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let from_zero = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 0 })?,
    )
    .await?;
    expect_value(&from_zero, &to_value(&Counter { value: 3 })?)?;
    let from_five = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 5 })?,
    )
    .await?;
    expect_value(&from_five, &to_value(&Counter { value: 5 })?)?;
    println!(
        "while from 0 -> {} ; from 5 -> {}",
        pretty(&from_zero),
        pretty(&from_five)
    );
    Ok(())
}
