use crate::error::{Error, Result};
use crate::ids::{ActivityKey, valid_ascii_name};
use crate::schema::{SchemaKey, SchemaRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionKind {
    Async,
    Blocking,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Pure,
    External,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Recovery {
    RetrySafe,
    Manual,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCode {
    pub code: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityContract {
    pub key: ActivityKey,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub execution: ExecutionKind,
    pub effects: EffectKind,
    pub recovery: Recovery,
    pub error_codes: Vec<ErrorCode>,
    pub reconciler: Option<ActivityKey>,
}

impl ActivityContract {
    pub fn retryable_codes(&self) -> Vec<String> {
        self.error_codes
            .iter()
            .filter(|code| code.retryable)
            .map(|code| code.code.clone())
            .collect()
    }

    pub fn known_code(&self, code: &str) -> Option<&ErrorCode> {
        self.error_codes.iter().find(|item| item.code == code)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcilerContract {
    pub key: ActivityKey,
    pub forward: ActivityKey,
}

/// Registry of payload schemas and activity contracts a workflow may name.
///
/// Not the graph and not handler code.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub schemas: BTreeMap<SchemaKey, serde_json::Value>,
    pub activities: BTreeMap<ActivityKey, ActivityContract>,
    pub reconcilers: BTreeMap<ActivityKey, ReconcilerContract>,
}

impl Catalog {
    /// Parse a `graphrun.catalog/v1` JSON document.
    ///
    /// ```
    /// let catalog = graphrun::Catalog::from_json(
    ///     br#"{"format":"graphrun.catalog/v1","schemas":{},"activities":[]}"#,
    /// )
    /// .unwrap();
    /// assert!(catalog.activities.is_empty());
    /// ```
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let raw: RawCatalog = serde_json::from_slice(bytes)
            .map_err(|err| Error::invalid(format!("catalog JSON: {err}")))?;
        if raw.format != "graphrun.catalog/v1" {
            return Err(Error::invalid(format!(
                "unsupported catalog format {}",
                raw.format
            )));
        }
        let mut catalog = Catalog::default();
        catalog.insert_builtin_schemas();
        for (key, schema) in raw.schemas {
            let parsed = SchemaKey::parse(&key)?;
            catalog.schemas.insert(parsed, schema);
        }
        for activity in raw.activities {
            let key = ActivityKey::new(activity.name.clone(), activity.version);
            if !valid_ascii_name(&key.name) {
                return Err(Error::invalid(format!(
                    "invalid activity name {}",
                    key.name
                )));
            }
            let contract = ActivityContract {
                key: key.clone(),
                input_schema: parse_schema_ref_string(&activity.input_schema)?,
                output_schema: parse_schema_ref_string(&activity.output_schema)?,
                execution: activity.execution,
                effects: activity.effects,
                recovery: activity.recovery,
                error_codes: activity.error_codes,
                reconciler: activity
                    .reconciler
                    .map(|r| ActivityKey::new(r.name, r.version)),
            };
            catalog.activities.insert(key, contract);
        }
        for reconciler in raw.reconcilers {
            let key = ActivityKey::new(reconciler.name, reconciler.version);
            catalog.reconcilers.insert(
                key.clone(),
                ReconcilerContract {
                    key,
                    forward: ActivityKey::new(
                        reconciler.forward_activity.name,
                        reconciler.forward_activity.version,
                    ),
                },
            );
        }
        catalog.validate_refs()?;
        Ok(catalog)
    }

    fn insert_builtin_schemas(&mut self) {
        let builtins = [
            (
                SchemaKey::new("integer", 1).expect("integer"),
                serde_json::json!({"type": "integer"}),
            ),
            (
                SchemaKey::new("string", 1).expect("string"),
                serde_json::json!({"type": "string"}),
            ),
            (
                SchemaKey::new("boolean", 1).expect("boolean"),
                serde_json::json!({"type": "boolean"}),
            ),
            (
                SchemaKey::new("unit", 1).expect("unit"),
                serde_json::json!({"type": "null"}),
            ),
        ];
        for (key, schema) in builtins {
            self.schemas.entry(key).or_insert(schema);
        }
    }

    fn validate_refs(&self) -> Result<()> {
        for contract in self.activities.values() {
            self.require_schema(&contract.input_schema)?;
            self.require_schema(&contract.output_schema)?;
            if let Some(recon) = &contract.reconciler {
                if !self.reconcilers.contains_key(recon) {
                    return Err(Error::invalid(format!(
                        "activity {}/v{} refers to missing reconciler {}/v{}",
                        contract.key.name, contract.key.version, recon.name, recon.version
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_for_publication(&self) -> Result<()> {
        for key in self.schemas.keys() {
            if !valid_ascii_name(&key.name) || key.version == 0 {
                return Err(Error::invalid(format!("invalid published schema {key}")));
            }
        }
        for (key, contract) in &self.activities {
            if key != &contract.key || !valid_ascii_name(&key.name) || key.version == 0 {
                return Err(Error::invalid("invalid published activity identity"));
            }
        }
        for (key, contract) in &self.reconcilers {
            if key != &contract.key || !valid_ascii_name(&key.name) || key.version == 0 {
                return Err(Error::invalid("invalid published reconciler identity"));
            }
            if !self.activities.contains_key(&contract.forward) {
                return Err(Error::invalid(format!(
                    "reconciler {key} refers to missing activity"
                )));
            }
        }
        self.validate_refs()
    }

    pub fn require_schema(&self, schema: &SchemaRef) -> Result<()> {
        match schema {
            SchemaRef::Named { key } => {
                if !self.schemas.contains_key(key) {
                    return Err(Error::invalid(format!("unknown schema {key}")));
                }
                Ok(())
            }
            SchemaRef::Array { element } => self.require_schema(element),
            SchemaRef::Tuple { elements } => {
                for element in elements {
                    self.require_schema(element)?;
                }
                Ok(())
            }
            SchemaRef::Integer | SchemaRef::String | SchemaRef::Boolean | SchemaRef::Null => Ok(()),
        }
    }

    pub fn validate_value(&self, schema: &SchemaRef, value: &crate::value::Value) -> Result<()> {
        let json_schema = self.schema_json(schema)?;
        let instance = value.to_json();
        if jsonschema::is_valid(&json_schema, &instance) {
            Ok(())
        } else {
            Err(Error::invalid("output does not match activity schema"))
        }
    }

    fn schema_json(&self, schema: &SchemaRef) -> Result<serde_json::Value> {
        match schema {
            SchemaRef::Named { key } => self
                .schemas
                .get(key)
                .cloned()
                .ok_or_else(|| Error::invalid(format!("unknown schema {key}"))),
            SchemaRef::Array { element } => Ok(serde_json::json!({
                "type": "array",
                "items": self.schema_json(element)?,
            })),
            SchemaRef::Tuple { elements } => {
                let items: Result<Vec<_>> = elements
                    .iter()
                    .map(|element| self.schema_json(element))
                    .collect();
                Ok(serde_json::json!({
                    "type": "array",
                    "items": items?,
                }))
            }
            SchemaRef::Integer => Ok(serde_json::json!({"type": "integer"})),
            SchemaRef::String => Ok(serde_json::json!({"type": "string"})),
            SchemaRef::Boolean => Ok(serde_json::json!({"type": "boolean"})),
            SchemaRef::Null => Ok(serde_json::json!({"type": "null"})),
        }
    }

    pub fn activity(&self, key: &ActivityKey) -> Result<&ActivityContract> {
        self.activities.get(key).ok_or_else(|| {
            Error::invalid(format!("unknown activity {}/v{}", key.name, key.version))
        })
    }

    pub fn activity_ref<I, O>(
        &self,
        name: &str,
        version: u32,
    ) -> Result<crate::builder::ActivityRef<I, O>>
    where
        I: crate::schema::DurablePayload,
        O: crate::schema::DurablePayload,
    {
        crate::builder::ActivityRef::from_catalog(self, name, version)
    }

    /// Lookup `name` at version 1.
    pub fn activity_v1<I, O>(&self, name: &str) -> Result<crate::builder::ActivityRef<I, O>>
    where
        I: crate::schema::DurablePayload,
        O: crate::schema::DurablePayload,
    {
        self.activity_ref(name, 1)
    }

    pub fn named_schema(&self, key: &SchemaKey) -> Result<&serde_json::Value> {
        self.schemas
            .get(key)
            .ok_or_else(|| Error::invalid(format!("unknown schema {key}")))
    }
}

fn parse_schema_ref_string(raw: &str) -> Result<SchemaRef> {
    Ok(SchemaRef::Named {
        key: SchemaKey::parse(raw)?,
    })
}

#[derive(Deserialize)]
struct RawCatalog {
    format: String,
    #[serde(default)]
    schemas: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    activities: Vec<RawActivity>,
    #[serde(default)]
    reconcilers: Vec<RawReconciler>,
}

#[derive(Deserialize)]
struct RawActivity {
    name: String,
    version: u32,
    input_schema: String,
    output_schema: String,
    execution: ExecutionKind,
    effects: EffectKind,
    recovery: Recovery,
    #[serde(default)]
    error_codes: Vec<ErrorCode>,
    reconciler: Option<RawKey>,
}

#[derive(Deserialize)]
struct RawReconciler {
    name: String,
    version: u32,
    forward_activity: RawKey,
}

#[derive(Deserialize)]
struct RawKey {
    name: String,
    version: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_fixture_catalog() {
        let bytes = include_bytes!("../../docs/specs/v1/examples/activity-catalog.json");
        let catalog = Catalog::from_json(bytes).unwrap();
        assert!(
            catalog
                .activities
                .contains_key(&ActivityKey::new("counter.increment", 1))
        );
        assert!(
            catalog
                .schemas
                .contains_key(&SchemaKey::parse("order/v1").unwrap())
        );
    }
}
