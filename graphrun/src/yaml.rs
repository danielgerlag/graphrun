use crate::binding::{Binding, Condition, Reference};
use crate::error::{Error, Result};
use crate::ids::NodeKey;
use crate::schema::{SchemaKey, SchemaRef};
use crate::value::Value;
use std::collections::BTreeMap;
use yaml_rust2::parser::{Event, Parser, Tag};
use yaml_rust2::scanner::{Marker, TScalarStyle};

#[derive(Clone, Debug)]
pub struct Span {
    pub line: usize,
    pub column: usize,
}

#[derive(Clone, Debug)]
pub struct Spanned {
    pub span: Span,
    pub doc: Doc,
}

#[derive(Clone, Debug)]
pub enum Doc {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
    Array(Vec<Spanned>),
    Map(BTreeMap<String, Spanned>),
}

impl Spanned {
    pub fn as_map(&self) -> Result<&BTreeMap<String, Spanned>> {
        match &self.doc {
            Doc::Map(map) => Ok(map),
            _ => {
                Err(Error::invalid("expected a mapping").at_span(self.span.line, self.span.column))
            }
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match &self.doc {
            Doc::String(text) => Ok(text),
            _ => Err(Error::invalid("expected a string").at_span(self.span.line, self.span.column)),
        }
    }

    pub fn as_u32(&self) -> Result<u32> {
        match &self.doc {
            Doc::Int(v) if *v >= 0 && *v <= i64::from(u32::MAX) => Ok(*v as u32),
            _ => Err(Error::invalid("expected a non-negative integer")
                .at_span(self.span.line, self.span.column)),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match &self.doc {
            Doc::Int(v) => Ok(*v),
            _ => {
                Err(Error::invalid("expected an integer").at_span(self.span.line, self.span.column))
            }
        }
    }

    pub fn to_value(&self) -> Result<Value> {
        Ok(match &self.doc {
            Doc::Null => Value::Null,
            Doc::Bool(v) => Value::Bool(*v),
            Doc::Int(v) => Value::Int(*v),
            Doc::String(v) => Value::String(v.clone()),
            Doc::Array(items) => Value::Array(
                items
                    .iter()
                    .map(Self::to_value)
                    .collect::<Result<Vec<_>>>()?,
            ),
            Doc::Map(fields) => {
                let mut out = BTreeMap::new();
                for (k, v) in fields {
                    out.insert(k.clone(), v.to_value()?);
                }
                Value::Object(out)
            }
        })
    }
}

pub fn parse_yaml(text: &str) -> Result<Spanned> {
    if text.len() > crate::limits::MAX_YAML_BYTES {
        return Err(Error::invalid("YAML exceeds 256 KiB"));
    }
    let mut parser = Parser::new_from_str(text);
    expect_event(&mut parser, |ev| matches!(ev, Event::StreamStart))?;
    expect_event(&mut parser, |ev| matches!(ev, Event::DocumentStart))?;
    let doc = parse_node(&mut parser)?;
    expect_event(&mut parser, |ev| matches!(ev, Event::DocumentEnd))?;
    let (ev, mark) = parser.next_token().map_err(scan_error)?;
    match ev {
        Event::StreamEnd => {}
        Event::DocumentStart => {
            return Err(Error::invalid("multiple YAML documents are not allowed")
                .at_span(mark.line(), mark.col() + 1));
        }
        other => {
            return Err(Error::invalid(format!("unexpected YAML event {other:?}"))
                .at_span(mark.line(), mark.col() + 1));
        }
    }
    Ok(doc)
}

fn scan_error(err: yaml_rust2::scanner::ScanError) -> Error {
    Error::invalid(err.to_string())
}

fn expect_event(
    parser: &mut Parser<std::str::Chars<'_>>,
    pred: impl Fn(&Event) -> bool,
) -> Result<(Event, Marker)> {
    let (ev, mark) = parser.next_token().map_err(scan_error)?;
    if pred(&ev) {
        Ok((ev, mark))
    } else {
        Err(Error::invalid(format!("unexpected YAML event {ev:?}"))
            .at_span(mark.line(), mark.col() + 1))
    }
}

fn parse_node(parser: &mut Parser<std::str::Chars<'_>>) -> Result<Spanned> {
    let (ev, mark) = parser.next_token().map_err(scan_error)?;
    let span = Span {
        line: mark.line(),
        column: mark.col() + 1,
    };
    match ev {
        Event::Scalar(text, style, anchor, tag) => {
            reject_anchor_tag(anchor, tag.as_ref(), &span)?;
            Ok(Spanned {
                span,
                doc: parse_scalar(text, style),
            })
        }
        Event::SequenceStart(anchor, tag) => {
            reject_anchor_tag(anchor, tag.as_ref(), &span)?;
            let mut items = Vec::new();
            loop {
                let (peek, peek_mark) = parser.peek().map_err(scan_error)?.clone();
                match peek {
                    Event::SequenceEnd => {
                        parser.next_token().map_err(scan_error)?;
                        break;
                    }
                    Event::StreamEnd => {
                        return Err(Error::invalid("unterminated sequence")
                            .at_span(peek_mark.line(), peek_mark.col() + 1));
                    }
                    _ => items.push(parse_node(parser)?),
                }
            }
            Ok(Spanned {
                span,
                doc: Doc::Array(items),
            })
        }
        Event::MappingStart(anchor, tag) => {
            reject_anchor_tag(anchor, tag.as_ref(), &span)?;
            let mut fields = BTreeMap::new();
            loop {
                let (peek, peek_mark) = parser.peek().map_err(scan_error)?.clone();
                match peek {
                    Event::MappingEnd => {
                        parser.next_token().map_err(scan_error)?;
                        break;
                    }
                    Event::Alias(_) => {
                        return Err(Error::invalid("YAML aliases are not allowed")
                            .at_span(peek_mark.line(), peek_mark.col() + 1));
                    }
                    _ => {
                        let key = parse_node(parser)?;
                        let key_text = key.as_str()?.to_owned();
                        if fields.contains_key(&key_text) {
                            return Err(Error::invalid(format!("duplicate key {key_text}"))
                                .at_span(key.span.line, key.span.column));
                        }
                        let value = parse_node(parser)?;
                        fields.insert(key_text, value);
                    }
                }
            }
            Ok(Spanned {
                span,
                doc: Doc::Map(fields),
            })
        }
        Event::Alias(_) => {
            Err(Error::invalid("YAML aliases are not allowed").at_span(span.line, span.column))
        }
        other => Err(Error::invalid(format!("unexpected YAML event {other:?}"))
            .at_span(span.line, span.column)),
    }
}

fn reject_anchor_tag(anchor: usize, tag: Option<&Tag>, span: &Span) -> Result<()> {
    if anchor != 0 {
        return Err(Error::invalid("YAML anchors are not allowed").at_span(span.line, span.column));
    }
    if let Some(tag) = tag {
        return Err(Error::invalid(format!(
            "YAML tags are not allowed ({}{})",
            tag.handle, tag.suffix
        ))
        .at_span(span.line, span.column));
    }
    Ok(())
}

fn parse_scalar(text: String, style: TScalarStyle) -> Doc {
    if !matches!(style, TScalarStyle::Plain) {
        return Doc::String(text);
    }
    match text.as_str() {
        "" | "null" | "Null" | "NULL" | "~" => Doc::Null,
        "true" | "True" | "TRUE" => Doc::Bool(true),
        "false" | "False" | "FALSE" => Doc::Bool(false),
        _ => {
            if let Ok(v) = text.parse::<i64>() {
                Doc::Int(v)
            } else {
                Doc::String(text)
            }
        }
    }
}

pub fn required<'a>(
    map: &'a BTreeMap<String, Spanned>,
    key: &str,
    span: &Span,
) -> Result<&'a Spanned> {
    map.get(key).ok_or_else(|| {
        Error::invalid(format!("missing field {key}")).at_span(span.line, span.column)
    })
}

pub fn reject_unknown(
    map: &BTreeMap<String, Spanned>,
    allowed: &[&str],
    span: &Span,
) -> Result<()> {
    for key in map.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(
                Error::invalid(format!("unknown field {key}")).at_span(span.line, span.column)
            );
        }
    }
    Ok(())
}

pub fn parse_schema_ref(node: &Spanned) -> Result<SchemaRef> {
    match &node.doc {
        Doc::String(text) => Ok(SchemaRef::Named {
            key: SchemaKey::parse(text)?,
        }),
        Doc::Map(map) => {
            if map.len() != 1 {
                return Err(
                    Error::invalid("schema constructor must have exactly one field")
                        .at_span(node.span.line, node.span.column),
                );
            }
            if let Some(element) = map.get("array") {
                return Ok(SchemaRef::array(parse_schema_ref(element)?));
            }
            if let Some(elements) = map.get("tuple") {
                let Doc::Array(items) = &elements.doc else {
                    return Err(Error::invalid("tuple schema requires an array")
                        .at_span(elements.span.line, elements.span.column));
                };
                let parsed = items
                    .iter()
                    .map(parse_schema_ref)
                    .collect::<Result<Vec<_>>>()?;
                return SchemaRef::tuple(parsed);
            }
            Err(Error::invalid("unknown schema constructor")
                .at_span(node.span.line, node.span.column))
        }
        _ => Err(
            Error::invalid("schema reference must be a string or constructor")
                .at_span(node.span.line, node.span.column),
        ),
    }
}

pub fn parse_binding(node: &Spanned) -> Result<Binding> {
    let map = node.as_map()?;
    let keys: Vec<&str> = map.keys().map(String::as_str).collect();
    if keys.contains(&"from") {
        reject_unknown(map, &["from", "path"], &node.span)?;
        let reference = Reference::parse(required(map, "from", &node.span)?.as_str()?)?;
        let path = match map.get("path") {
            Some(path) => Some(path.as_str()?.to_owned()),
            None => None,
        };
        return Ok(Binding::From { reference, path });
    }
    if keys.contains(&"literal") {
        reject_unknown(map, &["literal"], &node.span)?;
        return Ok(Binding::Literal {
            value: required(map, "literal", &node.span)?.to_value()?,
        });
    }
    if keys.contains(&"object") {
        reject_unknown(map, &["object"], &node.span)?;
        let fields_node = required(map, "object", &node.span)?;
        let fields_map = fields_node.as_map()?;
        let mut fields = BTreeMap::new();
        for (key, value) in fields_map {
            fields.insert(key.clone(), parse_binding(value)?);
        }
        return Ok(Binding::Object { fields });
    }
    if keys.contains(&"array") {
        reject_unknown(map, &["array"], &node.span)?;
        let items_node = required(map, "array", &node.span)?;
        let Doc::Array(items) = &items_node.doc else {
            return Err(Error::invalid("array binding requires a sequence")
                .at_span(items_node.span.line, items_node.span.column));
        };
        return Ok(Binding::Array {
            items: items
                .iter()
                .map(parse_binding)
                .collect::<Result<Vec<_>>>()?,
        });
    }
    Err(
        Error::invalid("binding must be from, literal, object, or array")
            .at_span(node.span.line, node.span.column),
    )
}

pub fn parse_condition(node: &Spanned) -> Result<Condition> {
    let map = node.as_map()?;
    if map.len() != 1 {
        return Err(Error::invalid("condition must have exactly one operator")
            .at_span(node.span.line, node.span.column));
    }
    let (op, value) = map.iter().next().expect("len == 1");
    match op.as_str() {
        "eq" | "ne" | "lt" | "le" | "gt" | "ge" => {
            let Doc::Array(items) = &value.doc else {
                return Err(Error::invalid("comparison expects two bindings")
                    .at_span(value.span.line, value.span.column));
            };
            if items.len() != 2 {
                return Err(Error::invalid("comparison expects two bindings")
                    .at_span(value.span.line, value.span.column));
            }
            let left = parse_binding(&items[0])?;
            let right = parse_binding(&items[1])?;
            Ok(match op.as_str() {
                "eq" => Condition::Eq { left, right },
                "ne" => Condition::Ne { left, right },
                "lt" => Condition::Lt { left, right },
                "le" => Condition::Le { left, right },
                "gt" => Condition::Gt { left, right },
                "ge" => Condition::Ge { left, right },
                _ => unreachable!(),
            })
        }
        "all" | "any" => {
            let Doc::Array(items) = &value.doc else {
                return Err(Error::invalid("all/any expects a nonempty sequence")
                    .at_span(value.span.line, value.span.column));
            };
            if items.is_empty() {
                return Err(Error::invalid("all/any requires at least one condition")
                    .at_span(value.span.line, value.span.column));
            }
            let parsed = items
                .iter()
                .map(parse_condition)
                .collect::<Result<Vec<_>>>()?;
            if op == "all" {
                Ok(Condition::All { items: parsed })
            } else {
                Ok(Condition::Any { items: parsed })
            }
        }
        "not" => Ok(Condition::Not {
            inner: Box::new(parse_condition(value)?),
        }),
        "exists" => Ok(Condition::Exists {
            binding: parse_binding(value)?,
        }),
        other => Err(
            Error::invalid(format!("unknown condition operator {other}"))
                .at_span(node.span.line, node.span.column),
        ),
    }
}

pub fn parse_node_key(node: &Spanned) -> Result<NodeKey> {
    NodeKey::parse(node.as_str()?).map_err(Error::invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_keys() {
        let err = parse_yaml("a: 1\na: 2\n").unwrap_err();
        assert!(err.message.contains("duplicate key"));
    }

    #[test]
    fn parses_sequence_fixture() {
        let text = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let doc = parse_yaml(text).unwrap();
        assert_eq!(
            required(doc.as_map().unwrap(), "id", &doc.span)
                .unwrap()
                .as_str()
                .unwrap(),
            "sequence"
        );
    }
}
