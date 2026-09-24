use graphrun::builder::SignalRef;
use graphrun::{Catalog, EventId, Value, payload, workflow};
use graphrun_samples::{LocalEngine, assert_same_ir, expect_value, pretty, to_value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Counter {
    value: i64,
}
payload!(Counter, "counter");

#[derive(Clone, Serialize, Deserialize)]
struct Approval {
    approved: bool,
}
payload!(Approval, "approval");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let increment = catalog.activity_v1::<Counter, Counter>("counter.increment")?;
    let approval = SignalRef::<Approval>::new("approval")?;
    workflow::<Counter>("timeout_recovery")
        .timed_wait("approval", &approval, "approval", Duration::from_secs(1))?
        .on_timeout("recover", &increment)?
        .finish(catalog)
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
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
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
