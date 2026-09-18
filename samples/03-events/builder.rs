use graphrun::builder::{RegionBuilder, SignalRef, WorkflowBuilder};
use graphrun::{Catalog, EventId, Value};
use graphrun_samples::{
    Approval, EventRequest, LocalEngine, assert_same_ir, catalog, expect_value, pretty, to_value,
};
use std::collections::BTreeMap;
use std::time::Duration;

const YAML: &str = include_str!("workflow.yaml");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let approval = SignalRef::<Approval>::new("approval")?;
    let mut root = RegionBuilder::<EventRequest>::new();
    let key = root.literal("order-1".to_owned())?;
    let waited = root.wait_signal("approval", &approval, key)?;
    let root = root.complete("finish", waited.output())?;
    WorkflowBuilder::new("external_approval", 1, root).build(catalog)
}

async fn run_signaled(
    engine: &graphrun::Engine,
    definition: graphrun::Definition,
    catalog: Catalog,
    input: Value,
    event_id: EventId,
) -> graphrun::Result<Value> {
    let run = engine.start(definition, catalog, input).await?;
    engine
        .signal(
            run,
            event_id,
            "approval",
            "order-1",
            Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))])),
        )
        .await?;
    engine.wait_terminal(run, Duration::from_secs(15)).await
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = catalog()?;
    let yaml = graphrun::compile_yaml(YAML, &catalog)?;
    let built = build(&catalog)?;
    assert_same_ir(&yaml, &built)?;

    let input = to_value(&EventRequest {
        key: "order-1".to_owned(),
    })?;
    let expected = to_value(&Approval { approved: true })?;
    let local = LocalEngine::new().await?;
    let yaml_out = run_signaled(
        local.engine(),
        yaml,
        catalog.clone(),
        input.clone(),
        EventId::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").map_err(graphrun::Error::invalid)?,
    )
    .await?;
    let built_out = run_signaled(
        local.engine(),
        built,
        catalog,
        input,
        EventId::from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").map_err(graphrun::Error::invalid)?,
    )
    .await?;
    local.shutdown().await?;
    expect_value(&yaml_out, &expected)?;
    expect_value(&built_out, &expected)?;
    println!("events {}", pretty(&yaml_out));
    Ok(())
}
