use graphrun::Catalog;
use graphrun::builder::{Branch, RegionBuilder, RegionGraphBuilder, WorkflowBuilder};
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun_samples::{pretty, run_pair, to_value};
use serde::{Deserialize, Serialize};

const YAML: &str = include_str!("workflow.yaml");

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
}

impl DurablePayload for Order {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("order", 1).expect("order/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Tax {
    cents: i64,
}

impl DurablePayload for Tax {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("tax", 1).expect("tax/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Shipping {
    cents: i64,
}

impl DurablePayload for Shipping {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("shipping", 1).expect("shipping/v1")
    }
}

fn build(catalog: &Catalog) -> graphrun::Result<graphrun::Definition> {
    let tax = catalog.activity_ref::<Order, Tax>("tax.quote", 1)?;
    let shipping = catalog.activity_ref::<Order, Shipping>("shipping.quote", 1)?;
    let mut tax_body = RegionBuilder::<Order>::new();
    let tax_quote = tax_body.activity("quote", &tax, tax_body.input())?;
    let tax_body = tax_body.complete("done", tax_quote.output())?;
    let mut shipping_body = RegionBuilder::<Order>::new();
    let shipping_quote = shipping_body.activity("quote", &shipping, shipping_body.input())?;
    let shipping_body = shipping_body.complete("done", shipping_quote.output())?;
    let mut graph = RegionGraphBuilder::<Order>::new();
    let input = graph.workflow_input();
    let quotes = graph.declare_parallel2(
        "quotes",
        Branch {
            name: "tax".to_owned(),
            input: input.binding().clone(),
            body: tax_body,
        },
        Branch {
            name: "shipping".to_owned(),
            input: input.binding().clone(),
            body: shipping_body,
        },
    )?;
    let finish = graph.declare_complete("finish", quotes.output())?;
    graph.start_at(quotes.entry())?;
    graph.connect(quotes.exit(), finish.entry())?;
    let region = graph.finish::<(Tax, Shipping)>()?;
    WorkflowBuilder::new("parallel_quotes", 1, region).build(catalog)
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
