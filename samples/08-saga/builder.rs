use graphrun::binding::{Binding, Condition, Reference};
use graphrun::builder::{Case, RegionBuilder, RegionGraphBuilder, WorkflowBuilder};
use graphrun::{Catalog, Error, Value};
use graphrun_samples::{LocalEngine, Order, Receipt, ReservedOrder, catalog, pretty, to_value};
use std::time::Duration;

const YAML: &str = include_str!("workflow.yaml");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let reserve = catalog.activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)?;
    let release = catalog.activity_ref::<ReservedOrder, ()>("inventory.release", 1)?;
    let charge = catalog.activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)?;
    let refund = catalog.activity_ref::<Receipt, ()>("payment.refund", 1)?;
    let abort = RegionBuilder::<Receipt>::new();
    let abort = abort.fail("abort", "fixture.failed", "Failure after recorded payment.")?;
    let accept = RegionBuilder::<Receipt>::new();
    let accepted = accept.input();
    let accept = accept.complete("accept", accepted)?;
    let mut body = RegionGraphBuilder::<Order>::new();
    let reserved = body.declare_activity("reserve", &reserve, body.input())?;
    body.attach_compensation(&reserved, &release)?;
    let charged = body.declare_activity("charge", &charge, reserved.output())?;
    body.attach_compensation(&charged, &refund)?;
    let decided = body.declare_choose(
        "decide",
        charged.output(),
        vec![Case {
            name: "force_failure".to_owned(),
            when: Condition::All {
                items: vec![
                    Condition::Exists {
                        binding: Binding::from_path(
                            Reference::WorkflowInput,
                            "/fail_after_payment",
                        ),
                    },
                    Condition::Eq {
                        left: Binding::from_path(Reference::WorkflowInput, "/fail_after_payment"),
                        right: Binding::literal(Value::Bool(true)),
                    },
                ],
            },
            body: abort,
        }],
        accept,
    )?;
    let done = body.declare_complete("done", decided.output())?;
    body.start_at(reserved.entry())?;
    body.connect(reserved.exit(), charged.entry())?;
    body.connect(charged.exit(), decided.entry())?;
    body.connect(decided.exit(), done.entry())?;
    let body = body.finish::<Receipt>()?;
    let mut root = RegionBuilder::<Order>::new();
    let fulfilled = root.saga("fulfill", root.workflow_input(), body)?;
    let root = root.complete("finish", fulfilled.output())?;
    WorkflowBuilder::new("compensating_fulfillment", 1, root).build(catalog)
}

async fn run_failed(
    engine: &graphrun::Engine,
    definition: graphrun::Definition,
    catalog: Catalog,
    input: Value,
    label: &str,
) -> graphrun::Result<()> {
    let run = engine.start(definition, catalog, input).await?;
    match engine.wait_terminal(run, Duration::from_secs(15)).await {
        Ok(output) => {
            return Err(Error::invalid(format!(
                "{label} expected failure, got {}",
                pretty(&output)
            )));
        }
        Err(err) => {
            if !err.to_string().contains("fixture.failed") {
                return Err(err);
            }
        }
    }
    let view = engine.inspect_json(run).await?;
    let status = view
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if status != "failed" {
        return Err(Error::invalid(format!("{label} inspect status={status}")));
    }
    let obligations = view
        .get("obligations")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let compensated = obligations
        .iter()
        .filter(|item| {
            item.get("status").and_then(serde_json::Value::as_str) == Some("compensated")
        })
        .count();
    if compensated < 2 {
        return Err(Error::invalid(format!(
            "{label} expected compensated obligations, got {obligations:?}"
        )));
    }
    println!("{label} failed after payment; compensated={compensated}");
    Ok(())
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = catalog()?;
    let yaml = graphrun::compile_yaml(YAML, &catalog)?;
    let built = build(&catalog)?;
    let input = to_value(&Order {
        order_id: "o1".to_owned(),
        amount: 1000,
        fail_after_payment: Some(true),
    })?;
    let local = LocalEngine::new().await?;
    run_failed(local.engine(), yaml, catalog.clone(), input.clone(), "yaml").await?;
    run_failed(local.engine(), built, catalog, input, "builder").await?;
    local.shutdown().await?;
    Ok(())
}
