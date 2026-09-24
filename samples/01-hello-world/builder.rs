use graphrun::{Catalog, payload, workflow};
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
    workflow::<Counter>("hello_world")
        .activity("hello", &increment)?
        .activity("goodbye", &increment)?
        .finish(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let built = build(&catalog)?;
    let input = to_value(&Counter { value: 0 })?;
    let output = run_pair(YAML, built, &catalog, input).await?;
    println!("hello-world {}", pretty(&output));
    Ok(())
}
