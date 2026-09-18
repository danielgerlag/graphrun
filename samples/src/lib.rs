//! Shared catalog, payloads, and `Engine::local` helpers for the sample bins.
//!
//! Each sample bin still shows the graph: YAML in `workflow.yaml` and the
//! typed builder in `builder.rs`. Activities are catalog contracts. Local
//! mode runs the built-in handlers in the library.

use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun::{Catalog, Definition, Engine, Error, Result, Value};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub fn catalog() -> Result<Catalog> {
    Catalog::from_json(include_bytes!("../catalog.json"))
}

pub fn execution_ir(definition: &Definition) -> serde_json::Value {
    let mut value = serde_json::to_value(definition).expect("definition JSON");
    strip_ir(&mut value);
    value
}

fn strip_ir(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.remove("digest");
            if map
                .get("path")
                .is_some_and(|value| value.is_array() || value.is_object())
            {
                map.remove("path");
            }
            for child in map.values_mut() {
                strip_ir(child);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                strip_ir(child);
            }
        }
        _ => {}
    }
}

pub fn assert_same_ir(yaml: &Definition, built: &Definition) -> Result<()> {
    let left = execution_ir(yaml);
    let right = execution_ir(built);
    if left == right {
        Ok(())
    } else {
        Err(Error::invalid(format!(
            "YAML and builder IR differ\nYAML: {}\nBuilder: {}",
            serde_json::to_string_pretty(&left).unwrap_or_default(),
            serde_json::to_string_pretty(&right).unwrap_or_default()
        )))
    }
}

pub fn pretty(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
}

pub fn to_value<T: Serialize>(payload: &T) -> Result<Value> {
    let json = serde_json::to_value(payload).map_err(|err| Error::invalid(err.to_string()))?;
    Value::from_json(json)
}

pub fn expect_value(actual: &Value, expected: &Value) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::invalid(format!(
            "unexpected output: got {} want {}",
            pretty(actual),
            pretty(expected)
        )))
    }
}

pub struct LocalEngine {
    engine: Engine,
    _dir: tempfile::TempDir,
}

impl LocalEngine {
    pub async fn new() -> Result<Self> {
        let dir = tempfile::tempdir().map_err(|err| Error::invalid(format!("tempdir: {err}")))?;
        let engine = Engine::local(dir.path()).await?;
        Ok(Self { engine, _dir: dir })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub async fn run(
        &self,
        definition: Definition,
        catalog: Catalog,
        input: Value,
    ) -> Result<Value> {
        let run = self.engine.start(definition, catalog, input).await?;
        self.engine
            .wait_terminal(run, Duration::from_secs(15))
            .await
    }

    pub async fn shutdown(self) -> Result<()> {
        self.engine.shutdown().await
    }
}

pub async fn run_pair(
    yaml_src: &str,
    built: Definition,
    catalog: &Catalog,
    input: Value,
) -> Result<Value> {
    let yaml = graphrun::compile_yaml(yaml_src, catalog)?;
    assert_same_ir(&yaml, &built)?;
    let local = LocalEngine::new().await?;
    let yaml_out = local.run(yaml, catalog.clone(), input.clone()).await?;
    let built_out = local.run(built, catalog.clone(), input).await?;
    local.shutdown().await?;
    if yaml_out != built_out {
        return Err(Error::invalid(format!(
            "output mismatch yaml={} builder={}",
            pretty(&yaml_out),
            pretty(&built_out)
        )));
    }
    Ok(yaml_out)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Counter {
    pub value: i64,
}

impl DurablePayload for Counter {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("counter", 1).expect("counter/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fail_after_payment: Option<bool>,
}

impl DurablePayload for Order {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("order", 1).expect("order/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReservedOrder {
    pub order_id: String,
    pub amount: i64,
    pub reservation_id: String,
}

impl DurablePayload for ReservedOrder {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("reserved_order", 1).expect("reserved_order/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub order_id: String,
    pub amount: i64,
    pub payment_id: String,
}

impl DurablePayload for Receipt {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("receipt", 1).expect("receipt/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EventRequest {
    pub key: String,
}

impl DurablePayload for EventRequest {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("event_request", 1).expect("event_request/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Approval {
    pub approved: bool,
}

impl DurablePayload for Approval {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("approval", 1).expect("approval/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Tax {
    pub cents: i64,
}

impl DurablePayload for Tax {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("tax", 1).expect("tax/v1")
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Shipping {
    pub cents: i64,
}

impl DurablePayload for Shipping {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("shipping", 1).expect("shipping/v1")
    }
}
