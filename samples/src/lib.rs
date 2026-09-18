//! Shared `Engine::local` harness. Sample graphs, catalogs, and payload types
//! live next to each bin.

use graphrun::{Catalog, Definition, Engine, Error, Result, Value};
use serde::Serialize;
use std::time::Duration;

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
