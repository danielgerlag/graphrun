use graphrun::{Catalog, payload, region, workflow};
use graphrun_samples::{expect_value, pretty, run_pair, to_value};
use serde::{Deserialize, Serialize};

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Counter {
    value: i64,
}
payload!(Counter, "counter");

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let increment = catalog.activity_v1::<Counter, Counter>("counter.increment")?;
    let body = region::<Counter>()
        .activity("increment", &increment)?
        .finish()?;
    workflow::<Counter>("while_counter")
        .while_lt("count", "/value", 3, body, 10)?
        .finish(catalog)
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
