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
    let echo = catalog.activity_v1::<Counter, Counter>("remote.echo")?;
    let bump = region::<Counter>()
        .activity("increment", &increment)?
        .finish()?;
    let keep = region::<Counter>().activity("echo", &echo)?.finish()?;
    workflow::<Counter>("choice")
        .choose("decide")
        .when_eq("bump", "/value", 1, bump)
        .otherwise(keep)?
        .finish(catalog)
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
    let matched = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 1 })?,
    )
    .await?;
    expect_value(&matched, &to_value(&Counter { value: 2 })?)?;
    let defaulted = run_pair(
        YAML,
        build(&catalog)?,
        &catalog,
        to_value(&Counter { value: 7 })?,
    )
    .await?;
    expect_value(&defaulted, &to_value(&Counter { value: 7 })?)?;
    println!(
        "choice value=1 -> {} ; value=7 -> {}",
        pretty(&matched),
        pretty(&defaulted)
    );
    Ok(())
}
