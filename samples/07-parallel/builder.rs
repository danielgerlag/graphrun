use graphrun::{Catalog, payload, region, workflow};
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
struct Tax {
    cents: i64,
}
payload!(Tax, "tax");

#[derive(Clone, Serialize, Deserialize)]
struct Shipping {
    cents: i64,
}
payload!(Shipping, "shipping");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote")?;
    let shipping = catalog.activity_v1::<Order, Shipping>("shipping.quote")?;
    let tax_body = region::<Order>().activity("quote", &tax)?.finish()?;
    let shipping_body = region::<Order>().activity("quote", &shipping)?.finish()?;
    workflow::<Order>("parallel_quotes")
        .parallel("quotes")
        .branch("tax", tax_body)
        .branch("shipping", shipping_body)?
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
    println!("parallel {}", pretty(&output));
    Ok(())
}
