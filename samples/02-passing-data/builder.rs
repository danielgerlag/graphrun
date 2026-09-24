use graphrun::{Catalog, payload, workflow};
use graphrun_samples::{pretty, run_pair, to_value};
use serde::{Deserialize, Serialize};

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
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
    let charge = catalog.activity_v1::<ReservedOrder, Receipt>("payment.charge")?;
    workflow::<Order>("passing_data")
        .activity("reserve", &reserve)?
        .activity("charge", &charge)?
        .finish(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let input = to_value(&Order {
        order_id: "o1".to_owned(),
        amount: 1000,
    })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("passing-data {}", pretty(&output));
    Ok(())
}
