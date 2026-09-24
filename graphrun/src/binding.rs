use crate::error::{Error, Result};
use crate::ids::NodeKey;
use crate::schema::SchemaRef;
use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reference {
    WorkflowInput,
    ScopeInput,
    ScopeId,
    NodeOutput { node: NodeKey },
    LoopState,
    LoopIndex,
    ItemValue,
    ItemIndex,
    ForwardInput,
    ForwardOutput,
}

impl Reference {
    pub fn parse(raw: &str) -> Result<Self> {
        if raw == "workflow.input" {
            return Ok(Self::WorkflowInput);
        }
        if raw == "scope.input" {
            return Ok(Self::ScopeInput);
        }
        if raw == "scope.id" {
            return Ok(Self::ScopeId);
        }
        if raw == "loop.state" {
            return Ok(Self::LoopState);
        }
        if raw == "loop.index" {
            return Ok(Self::LoopIndex);
        }
        if raw == "item.value" {
            return Ok(Self::ItemValue);
        }
        if raw == "item.index" {
            return Ok(Self::ItemIndex);
        }
        if raw == "forward.input" {
            return Ok(Self::ForwardInput);
        }
        if raw == "forward.output" {
            return Ok(Self::ForwardOutput);
        }
        if let Some(node) = raw.strip_prefix("nodes.") {
            let key = node
                .strip_suffix(".output")
                .ok_or_else(|| Error::invalid(format!("invalid node reference {raw}")))?;
            return Ok(Self::NodeOutput {
                node: NodeKey::parse(key).map_err(Error::invalid)?,
            });
        }
        Err(Error::invalid(format!("unknown reference {raw}")))
    }

    pub fn as_stable(&self) -> String {
        match self {
            Self::WorkflowInput => "workflow.input".to_owned(),
            Self::ScopeInput => "scope.input".to_owned(),
            Self::ScopeId => "scope.id".to_owned(),
            Self::NodeOutput { node } => format!("nodes.{}.output", node.as_str()),
            Self::LoopState => "loop.state".to_owned(),
            Self::LoopIndex => "loop.index".to_owned(),
            Self::ItemValue => "item.value".to_owned(),
            Self::ItemIndex => "item.index".to_owned(),
            Self::ForwardInput => "forward.input".to_owned(),
            Self::ForwardOutput => "forward.output".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Binding {
    From {
        reference: Reference,
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    Literal {
        value: Value,
    },
    Object {
        fields: BTreeMap<String, Binding>,
    },
    Array {
        items: Vec<Binding>,
    },
}

impl Binding {
    pub fn from_ref(reference: Reference) -> Self {
        Self::From {
            reference,
            path: None,
        }
    }

    pub fn from_path(reference: Reference, path: impl Into<String>) -> Self {
        Self::From {
            reference,
            path: Some(path.into()),
        }
    }

    pub fn literal(value: Value) -> Self {
        Self::Literal { value }
    }

    pub fn depth(&self) -> usize {
        match self {
            Self::From { .. } | Self::Literal { .. } => 1,
            Self::Object { fields } => 1 + fields.values().map(Self::depth).max().unwrap_or(0),
            Self::Array { items } => 1 + items.iter().map(Self::depth).max().unwrap_or(0),
        }
    }

    pub fn references(&self) -> Vec<&Reference> {
        match self {
            Self::From { reference, .. } => vec![reference],
            Self::Literal { .. } => Vec::new(),
            Self::Object { fields } => fields.values().flat_map(Self::references).collect(),
            Self::Array { items } => items.iter().flat_map(Self::references).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Condition {
    Eq { left: Binding, right: Binding },
    Ne { left: Binding, right: Binding },
    Lt { left: Binding, right: Binding },
    Le { left: Binding, right: Binding },
    Gt { left: Binding, right: Binding },
    Ge { left: Binding, right: Binding },
    All { items: Vec<Condition> },
    Any { items: Vec<Condition> },
    Not { inner: Box<Condition> },
    Exists { binding: Binding },
}

impl Condition {
    pub fn eq_input(path: &str, value: Value) -> Self {
        Self::Eq {
            left: Binding::from_path(Reference::WorkflowInput, path),
            right: Binding::literal(value),
        }
    }

    pub fn lt_loop(path: &str, value: i64) -> Self {
        Self::Lt {
            left: Binding::from_path(Reference::LoopState, path),
            right: Binding::literal(Value::Int(value)),
        }
    }

    pub fn flag_true(path: &str) -> Self {
        Self::All {
            items: vec![
                Self::Exists {
                    binding: Binding::from_path(Reference::WorkflowInput, path),
                },
                Self::eq_input(path, Value::Bool(true)),
            ],
        }
    }

    pub fn operator_count(&self) -> usize {
        match self {
            Self::All { items } | Self::Any { items } => {
                1 + items.iter().map(Self::operator_count).sum::<usize>()
            }
            Self::Not { inner } => 1 + inner.operator_count(),
            _ => 1,
        }
    }

    pub fn bindings(&self) -> Vec<&Binding> {
        match self {
            Self::Eq { left, right }
            | Self::Ne { left, right }
            | Self::Lt { left, right }
            | Self::Le { left, right }
            | Self::Gt { left, right }
            | Self::Ge { left, right } => vec![left, right],
            Self::All { items } | Self::Any { items } => {
                items.iter().flat_map(Self::bindings).collect()
            }
            Self::Not { inner } => inner.bindings(),
            Self::Exists { binding } => vec![binding],
        }
    }
}

pub fn infer_literal_schema(value: &Value) -> SchemaRef {
    match value {
        Value::Null => SchemaRef::Null,
        Value::Bool(_) => SchemaRef::Boolean,
        Value::Int(_) => SchemaRef::Integer,
        Value::String(_) => SchemaRef::String,
        Value::Array(items) => {
            if items.is_empty() {
                SchemaRef::array(SchemaRef::Null)
            } else {
                SchemaRef::array(infer_literal_schema(&items[0]))
            }
        }
        Value::Object(_) => SchemaRef::named("object", 1).unwrap_or(SchemaRef::Null),
    }
}
