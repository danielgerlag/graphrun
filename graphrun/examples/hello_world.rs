use graphrun::{Catalog, Engine, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const CATALOG: &[u8] = br#"{
  "format": "graphrun.catalog/v1",
  "schemas": {
    "counter/v1": {
      "type": "object",
      "required": ["value"],
      "additionalProperties": false,
      "properties": {"value": {"type": "integer"}}
    }
  },
  "activities": [
    {
      "name": "counter.increment",
      "version": 1,
      "input_schema": "counter/v1",
      "output_schema": "counter/v1",
      "execution": "async",
      "effects": "pure",
      "recovery": "RetrySafe",
      "error_codes": []
    }
  ]
}"#;

const YAML: &str = r#"
dsl: graphrun/v1
id: hello_world
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: hello
nodes:
  hello:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: workflow.input}
    next: goodbye
  goodbye:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: nodes.hello.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.goodbye.output}
"#;

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let dir = std::env::temp_dir().join("graphrun-example-hello");
    let _ = std::fs::remove_dir_all(&dir);
    let catalog = Catalog::from_json(CATALOG)?;
    let engine = Engine::local(&dir).await?;
    let input = Value::Object(BTreeMap::from([("value".into(), Value::Int(0))]));
    let run = engine.start_yaml(YAML, &catalog, input).await?;
    let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
    println!("{output:?}");
    engine.shutdown().await?;
    Ok(())
}
