use graphrun::{Catalog, Error, Value, payload, region, workflow};
use graphrun_samples::{LocalEngine, pretty, to_value};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fail_after_payment: Option<bool>,
}
payload!(Order, "order");

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder {
    order_id: String,
    amount: i64,
    reservation_id: String,
}
payload!(ReservedOrder, "reserved_order");

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    amount: i64,
    payment_id: String,
}
payload!(Receipt, "receipt");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let reserve = catalog.activity_v1::<Order, ReservedOrder>("inventory.reserve")?;
    let release = catalog.activity_v1::<ReservedOrder, ()>("inventory.release")?;
    let charge = catalog.activity_v1::<ReservedOrder, Receipt>("payment.charge")?;
    let refund = catalog.activity_v1::<Receipt, ()>("payment.refund")?;
    let abort =
        region::<Receipt>().fail("abort", "fixture.failed", "Failure after recorded payment.")?;
    let accept = region::<Receipt>().complete("accept")?;
    let body = region::<Order>()
        .activity("reserve", &reserve)?
        .compensate(&release)?
        .activity("charge", &charge)?
        .compensate(&refund)?
        .choose("decide")
        .when_true("force_failure", "/fail_after_payment", abort)
        .otherwise(accept)?
        .complete("done")?;
    workflow::<Order>("compensating_fulfillment")
        .saga("fulfill", body)?
        .finish(catalog)
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
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
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
