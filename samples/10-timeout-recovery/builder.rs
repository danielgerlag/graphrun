use graphrun::builder::{RegionGraphBuilder, SignalRef, WorkflowBuilder};
use graphrun::{Catalog, EventId, Value};
use graphrun_samples::{
    Approval, Counter, LocalEngine, assert_same_ir, catalog, expect_value, pretty, to_value,
};
use std::collections::BTreeMap;
use std::time::Duration;

const YAML: &str = include_str!("workflow.yaml");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let increment = catalog.activity_ref::<Counter, Counter>("counter.increment", 1)?;
    let approval = SignalRef::<Approval>::new("approval")?;
    let mut graph = RegionGraphBuilder::<Counter>::new();
    let input = graph.workflow_input();
    let key = graph.literal("approval".to_owned())?;
    let wait = graph.declare_timed_wait("approval", &approval, key, Duration::from_secs(1))?;
    let recovery = graph.declare_activity("recover", &increment, input.clone())?;
    let finish = graph.declare_complete("finish", input)?;
    graph.start_at(wait.entry())?;
    graph.connect(wait.success_port(), finish.entry())?;
    graph.connect(wait.timeout_port(), recovery.entry())?;
    graph.connect(recovery.exit(), finish.entry())?;
    let region = graph.finish::<Counter>()?;
    WorkflowBuilder::new("timeout_recovery", 1, region).build(catalog)
}

fn recover_succeeded(engine_state: &graphrun::State, run: graphrun::RunId) -> bool {
    engine_state.activations.values().any(|act| {
        act.run == run
            && act.node.as_str() == "recover"
            && matches!(act.status, graphrun::domain::ActivationStatus::Succeeded)
    })
}

async fn run_success(
    engine: &graphrun::Engine,
    definition: graphrun::Definition,
    catalog: Catalog,
    input: Value,
    event_id: EventId,
) -> graphrun::Result<(Value, bool)> {
    let run = engine.start(definition, catalog, input).await?;
    engine
        .signal(
            run,
            event_id,
            "approval",
            "approval",
            Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))])),
        )
        .await?;
    let output = engine.wait_terminal(run, Duration::from_secs(15)).await?;
    let recovered = recover_succeeded(&engine.inspect(run).await?, run);
    Ok((output, recovered))
}

async fn run_timeout(
    engine: &graphrun::Engine,
    definition: graphrun::Definition,
    catalog: Catalog,
    input: Value,
) -> graphrun::Result<(Value, bool)> {
    let run = engine.start(definition, catalog, input).await?;
    let output = engine.wait_terminal(run, Duration::from_secs(15)).await?;
    let recovered = recover_succeeded(&engine.inspect(run).await?, run);
    Ok((output, recovered))
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = catalog()?;
    let yaml = graphrun::compile_yaml(YAML, &catalog)?;
    let built = build(&catalog)?;
    assert_same_ir(&yaml, &built)?;
    let input = to_value(&Counter { value: 0 })?;
    let expected = input.clone();
    let local = LocalEngine::new().await?;
    let (yaml_ok, yaml_ok_recovered) = run_success(
        local.engine(),
        yaml.clone(),
        catalog.clone(),
        input.clone(),
        EventId::from_hex("cccccccccccccccccccccccccccccccc").map_err(graphrun::Error::invalid)?,
    )
    .await?;
    let (built_ok, built_ok_recovered) = run_success(
        local.engine(),
        built.clone(),
        catalog.clone(),
        input.clone(),
        EventId::from_hex("dddddddddddddddddddddddddddddddd").map_err(graphrun::Error::invalid)?,
    )
    .await?;
    let (yaml_timeout, yaml_timeout_recovered) =
        run_timeout(local.engine(), yaml, catalog.clone(), input.clone()).await?;
    let (built_timeout, built_timeout_recovered) =
        run_timeout(local.engine(), built, catalog, input).await?;
    local.shutdown().await?;
    expect_value(&yaml_ok, &expected)?;
    expect_value(&built_ok, &expected)?;
    expect_value(&yaml_timeout, &expected)?;
    expect_value(&built_timeout, &expected)?;
    if yaml_ok_recovered || built_ok_recovered {
        return Err(graphrun::Error::invalid(
            "success path must not run recover",
        ));
    }
    if !yaml_timeout_recovered || !built_timeout_recovered {
        return Err(graphrun::Error::invalid("timeout path must run recover"));
    }
    println!(
        "timeout-recovery success {} ; timeout {}",
        pretty(&yaml_ok),
        pretty(&yaml_timeout)
    );
    Ok(())
}
