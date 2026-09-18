use graphrun::{Catalog, Engine, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const CATALOG: &str = r#"{
  "format": "graphrun.catalog/v1",
  "schemas": {
    "order/v1": {
      "type": "object",
      "required": ["order_id", "amount"],
      "additionalProperties": false,
      "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "integer", "minimum": 0}
      }
    },
    "reserved_order/v1": {
      "type": "object",
      "required": ["order_id", "amount", "reservation_id"],
      "additionalProperties": false,
      "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "integer"},
        "reservation_id": {"type": "string"}
      }
    },
    "receipt/v1": {
      "type": "object",
      "required": ["order_id", "amount", "payment_id"],
      "additionalProperties": false,
      "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "integer"},
        "payment_id": {"type": "string"}
      }
    }
  },
  "activities": [
    {
      "name": "inventory.reserve",
      "version": 1,
      "input_schema": "order/v1",
      "output_schema": "reserved_order/v1",
      "execution": "async",
      "effects": "external",
      "recovery": "RetrySafe",
      "error_codes": [{"code": "inventory.unavailable", "retryable": true}],
      "reconciler": {"name": "inventory.lookup", "version": 1}
    },
    {
      "name": "payment.charge",
      "version": 1,
      "input_schema": "reserved_order/v1",
      "output_schema": "receipt/v1",
      "execution": "async",
      "effects": "external",
      "recovery": "RetrySafe",
      "error_codes": [{"code": "payment.declined", "retryable": false}],
      "reconciler": {"name": "payment.lookup", "version": 1}
    }
  ],
  "reconcilers": [
    {"name": "inventory.lookup", "version": 1, "forward_activity": {"name": "inventory.reserve", "version": 1}},
    {"name": "payment.lookup", "version": 1, "forward_activity": {"name": "payment.charge", "version": 1}}
  ]
}"#;

const WORKFLOW: &str = r#"
dsl: graphrun/v1
id: sequence
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: reserve
nodes:
  reserve:
    kind: activity
    activity: {name: inventory.reserve, version: 1}
    input: {from: workflow.input}
    retry:
      errors: [inventory.unavailable]
      max_attempts: 5
    next: charge
  charge:
    kind: activity
    activity: {name: payment.charge, version: 1}
    input: {from: nodes.reserve.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.charge.output}
"#;

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let dir = std::env::temp_dir().join("graphrun-example-order");
    let _ = std::fs::remove_dir_all(&dir);
    let catalog = Catalog::from_json(CATALOG.as_bytes())?;
    let engine = Engine::local(&dir).await?;
    let input = Value::Object(BTreeMap::from([
        ("order_id".into(), Value::String("o1".into())),
        ("amount".into(), Value::Int(1000)),
    ]));
    let run = engine.start_yaml(WORKFLOW, &catalog, input).await?;
    let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
    println!("{output:?}");
    engine.shutdown().await?;
    Ok(())
}
