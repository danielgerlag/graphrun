use graphrun::{Catalog, payload, region, workflow};
use graphrun_samples::{pretty, run_pair, to_value};
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
    workflow::<Vec<Counter>>("foreach_counter")
        .foreach("increment_all", body, 100, 4)?
        .finish(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let input = to_value(&vec![
        Counter { value: 3 },
        Counter { value: 1 },
        Counter { value: 3 },
    ])?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("foreach {}", pretty(&output));
    Ok(())
}
