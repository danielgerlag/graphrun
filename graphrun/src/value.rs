use crate::error::{Error, Result};
use crate::limits::MAX_DATA_DEPTH;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}

impl Value {
    pub fn from_json(json: serde_json::Value) -> Result<Self> {
        from_json(json, 0)
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(v) => serde_json::Value::Bool(*v),
            Self::Int(v) => serde_json::Value::Number((*v).into()),
            Self::String(v) => serde_json::Value::String(v.clone()),
            Self::Array(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_json).collect())
            }
            Self::Object(fields) => serde_json::Value::Object(
                fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect(),
            ),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn pointer(&self, path: &str) -> Result<&Value> {
        if path.is_empty() {
            return Ok(self);
        }
        if !path.starts_with('/') {
            return Err(Error::invalid(format!(
                "JSON Pointer {path} must start with /"
            )));
        }
        let mut current = self;
        for raw in path.split('/').skip(1) {
            let token = unescape_pointer(raw);
            current = match current {
                Self::Object(map) => map.get(&token).ok_or_else(|| {
                    Error::invalid(format!("missing object field {token} at {path}"))
                })?,
                Self::Array(items) => {
                    let index: usize = token.parse().map_err(|_| {
                        Error::invalid(format!("array index {token} is not an integer"))
                    })?;
                    items.get(index).ok_or_else(|| {
                        Error::invalid(format!("array index {index} is out of range"))
                    })?
                }
                _ => {
                    return Err(Error::invalid(format!(
                        "cannot project {token} through a scalar"
                    )));
                }
            };
        }
        Ok(current)
    }
}

fn unescape_pointer(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

fn from_json(json: serde_json::Value, depth: usize) -> Result<Value> {
    if depth > MAX_DATA_DEPTH {
        return Err(Error::invalid("value exceeds maximum nesting depth"));
    }
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(v) => Ok(Value::Bool(v)),
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                Ok(Value::Int(v))
            } else {
                Err(Error::invalid(format!(
                    "non-integer number {n} is not supported"
                )))
            }
        }
        serde_json::Value::String(v) => Ok(Value::String(v)),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(from_json(item, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
        serde_json::Value::Object(map) => {
            let mut out = BTreeMap::new();
            for (key, item) in map {
                out.insert(key, from_json(item, depth + 1)?);
            }
            Ok(Value::Object(out))
        }
    }
}

pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_canonical(&mut out, value, 0)?;
    Ok(out)
}

fn write_canonical(out: &mut Vec<u8>, value: &serde_json::Value, depth: usize) -> Result<()> {
    if depth > MAX_DATA_DEPTH {
        return Err(Error::invalid("canonical JSON exceeds maximum depth"));
    }
    match value {
        serde_json::Value::Null => out.extend_from_slice(b"null"),
        serde_json::Value::Bool(true) => out.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => out.extend_from_slice(b"false"),
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                out.extend_from_slice(v.to_string().as_bytes());
            } else {
                return Err(Error::invalid(format!(
                    "non-integer number {n} cannot be canonicalized"
                )));
            }
        }
        serde_json::Value::String(s) => {
            let encoded = serde_json::to_string(s)
                .map_err(|err| Error::invalid(format!("string encode: {err}")))?;
            out.extend_from_slice(encoded.as_bytes());
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(out, item, depth + 1)?;
            }
            out.push(b']');
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                let encoded = serde_json::to_string(*key)
                    .map_err(|err| Error::invalid(format!("key encode: {err}")))?;
                out.extend_from_slice(encoded.as_bytes());
                out.push(b':');
                write_canonical(out, &map[*key], depth + 1)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_sorts_object_keys() {
        let value = json!({"b": 1, "a": 2});
        assert_eq!(
            String::from_utf8(canonical_json(&value).unwrap()).unwrap(),
            r#"{"a":2,"b":1}"#
        );
    }
}
