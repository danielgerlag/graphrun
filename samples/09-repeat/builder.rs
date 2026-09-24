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
    workflow::<Counter>("repeat_counter")
        .repeat("count", 3, body, 10)?
        .finish(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let output = run_pair(YAML, built, &catalog, to_value(&Counter { value: 1 })?).await?;
    expect_value(&output, &to_value(&Counter { value: 4 })?)?;
    println!("repeat {}", pretty(&output));
    Ok(())
}
