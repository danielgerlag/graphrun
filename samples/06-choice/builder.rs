use graphrun::Catalog;
use graphrun::binding::{Binding, Condition, Reference};
use graphrun::builder::{Case, RegionBuilder, WorkflowBuilder};
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
    let echo = catalog.activity_ref::<Counter, Counter>("remote.echo", 1)?;
    let mut bump = RegionBuilder::<Counter>::new();
    let bumped = bump.activity("increment", &increment, bump.input())?;
    let bump = bump.complete("done", bumped.output())?;
    let mut keep = RegionBuilder::<Counter>::new();
    let echoed = keep.activity("echo", &echo, keep.input())?;
    let keep = keep.complete("done", echoed.output())?;
    let mut root = RegionBuilder::<Counter>::new();
    let decided = root.choose(
        "decide",
        root.workflow_input(),
        vec![Case {
            name: "bump".to_owned(),
            when: Condition::Eq {
                left: Binding::from_path(Reference::WorkflowInput, "/value"),
                right: Binding::literal(graphrun::value::Value::Int(1)),
            },
            body: bump,
        }],
        keep,
    )?;
    let root = root.complete("finish", decided.output())?;
    WorkflowBuilder::new("choice", 1, root).build(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let matched = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 1 })?,
    )
    .await?;
    expect_value(&matched, &to_value(&Counter { value: 2 })?)?;
    let defaulted = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 7 })?,
    )
    .await?;
    expect_value(&defaulted, &to_value(&Counter { value: 7 })?)?;
    println!(
        "choice value=1 -> {} ; value=7 -> {}",
        pretty(&matched),
        pretty(&defaulted)
    );
    Ok(())
}
