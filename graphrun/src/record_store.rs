use crate::domain::State;
use crate::error::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(crate) const FORMAT: &str = "graphrun.state-record/v1";
const NESTED_FIELDS: &[&str] = &["published_definitions", "start_keys"];
const HISTORY_FIELDS: &[&str] = &["history", "history_records"];
const LIST_FIELDS: &[&str] = &["inbox", "obligations"];

#[derive(Serialize, Deserialize)]
struct VersionedRecord {
    format: String,
    revision: u64,
    value: Value,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::FailedPrecondition, message.into())
}

fn owner(field: &str, key: Option<&str>, value: &Value, state: &State) -> Result<Option<String>> {
    if matches!(
        field,
        "runs"
            | "history"
            | "history_records"
            | "history_dependencies"
            | "checkpoints"
            | "terminal_summaries"
    ) {
        return Ok(key.map(str::to_owned));
    }
    if matches!(
        field,
        "scopes" | "activations" | "waits" | "inbox" | "obligations" | "signal_tombstones"
    ) {
        return value
            .get("run")
            .and_then(Value::as_str)
            .map(|run| Some(run.to_owned()))
            .ok_or_else(|| invalid(format!("{field} record has no run owner")));
    }
    if matches!(
        field,
        "loop_carry"
            | "foreach_items"
            | "foreach_done"
            | "parallel_done"
            | "saga_errors"
            | "interventions"
    ) {
        let id = key.ok_or_else(|| invalid(format!("{field} has no activation identity")))?;
        let activation = crate::ids::ActivationId::from_hex(id)
            .map_err(|err| invalid(format!("invalid {field} activation: {err}")))?;
        return state
            .activations
            .get(&activation)
            .map(|act| Some(act.run.to_hex()))
            .ok_or_else(|| invalid(format!("{field} activation owner missing")));
    }
    Ok(None)
}

fn record_key(
    generation: u64,
    run: Option<&str>,
    field: &str,
    tag: &str,
    key: &str,
    nested: Option<&str>,
) -> String {
    let run = run.map_or_else(|| "global".to_owned(), |run| format!("run-{run}"));
    let primary = hex::encode(key.as_bytes());
    let nested = nested
        .map(|value| hex::encode(value.as_bytes()))
        .unwrap_or_default();
    format!("{generation:016x}/{run}/{field}/{tag}/{primary}/{nested}")
}

fn insert(
    rows: &mut BTreeMap<String, Vec<u8>>,
    key: String,
    value: Value,
    revision: u64,
) -> Result<()> {
    let bytes = serde_json::to_vec(&VersionedRecord {
        format: FORMAT.to_owned(),
        revision,
        value,
    })
    .map_err(|err| Error::invalid(err.to_string()))?;
    if rows.insert(key, bytes).is_some() {
        return Err(invalid("duplicate state record identity"));
    }
    Ok(())
}

pub(crate) fn same_value(left: &[u8], right: &[u8]) -> Result<bool> {
    let left: VersionedRecord = serde_json::from_slice(left)
        .map_err(|err| invalid(format!("stored state record is corrupt: {err}")))?;
    let right: VersionedRecord = serde_json::from_slice(right)
        .map_err(|err| invalid(format!("new state record is corrupt: {err}")))?;
    if left.format != FORMAT || right.format != FORMAT || left.revision == 0 || right.revision == 0
    {
        return Err(invalid("unsupported state record version"));
    }
    Ok(left.value == right.value)
}

pub(crate) fn encode(
    state: &State,
    generation: u64,
    revision: u64,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let Value::Object(defaults) =
        serde_json::to_value(State::default()).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("default state must be an object"));
    };
    let Value::Object(fields) =
        serde_json::to_value(state).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("state must be an object"));
    };
    let mut rows = BTreeMap::new();
    for (field, value) in fields {
        if HISTORY_FIELDS.contains(&field.as_str()) {
            let Value::Object(runs) = value else {
                return Err(invalid(format!("{field} must be indexed by run")));
            };
            for (run, entries) in runs {
                let Value::Array(entries) = entries else {
                    return Err(invalid(format!("{field} must contain ordered events")));
                };
                for (index, entry) in entries.into_iter().enumerate() {
                    let sequence = index as u64 + 1;
                    let key = record_key(
                        generation,
                        Some(&run),
                        &field,
                        "sequence",
                        &format!("{sequence:020}"),
                        None,
                    );
                    insert(&mut rows, key, entry, revision)?;
                }
            }
        } else if NESTED_FIELDS.contains(&field.as_str()) {
            let Value::Object(groups) = value else {
                return Err(invalid(format!("{field} must be an object")));
            };
            for (group, children) in groups {
                let Value::Object(children) = children else {
                    return Err(invalid(format!("{field} group must be an object")));
                };
                for (child, value) in children {
                    let key = record_key(generation, None, &field, "nested", &group, Some(&child));
                    insert(&mut rows, key, value, revision)?;
                }
            }
        } else if LIST_FIELDS.contains(&field.as_str()) {
            let Value::Array(entries) = value else {
                return Err(invalid(format!("{field} must be an array")));
            };
            for (index, value) in entries.into_iter().enumerate() {
                let run = owner(&field, None, &value, state)?;
                let key = record_key(
                    generation,
                    run.as_deref(),
                    &field,
                    "list",
                    &format!("{index:020}"),
                    None,
                );
                insert(&mut rows, key, value, revision)?;
            }
        } else if defaults.get(&field).is_some_and(Value::is_object) {
            let Value::Object(entries) = value else {
                return Err(invalid(format!("{field} must be an object")));
            };
            for (entry, value) in entries {
                let run = owner(&field, Some(&entry), &value, state)?;
                let key = record_key(generation, run.as_deref(), &field, "map", &entry, None);
                insert(&mut rows, key, value, revision)?;
            }
        } else {
            let key = record_key(generation, None, &field, "scalar", "", None);
            insert(&mut rows, key, value, revision)?;
        }
    }
    Ok(rows)
}

fn decode_key(key: &str) -> Result<String> {
    let bytes = hex::decode(key).map_err(|err| invalid(format!("bad record key: {err}")))?;
    String::from_utf8(bytes).map_err(|err| invalid(format!("record key is not UTF-8: {err}")))
}

fn add_to_array(
    arrays: &mut BTreeMap<(String, String), BTreeMap<u64, Value>>,
    field: &str,
    group: &str,
    sequence: &str,
    value: Value,
) -> Result<()> {
    let sequence = sequence
        .parse::<u64>()
        .map_err(|err| invalid(format!("invalid {field} sequence: {err}")))?;
    if arrays
        .entry((field.to_owned(), group.to_owned()))
        .or_default()
        .insert(sequence, value)
        .is_some()
    {
        return Err(invalid("duplicate state array sequence"));
    }
    Ok(())
}

pub(crate) fn decode(
    rows: impl IntoIterator<Item = (String, Vec<u8>)>,
    generation: u64,
) -> Result<State> {
    let Value::Object(mut root) =
        serde_json::to_value(State::default()).map_err(|err| Error::invalid(err.to_string()))?
    else {
        return Err(invalid("default state must be an object"));
    };
    let mut arrays: BTreeMap<(String, String), BTreeMap<u64, Value>> = BTreeMap::new();
    let mut seen_keys = std::collections::BTreeSet::new();
    for (key, bytes) in rows {
        if !seen_keys.insert(key.clone()) {
            return Err(invalid("duplicate state record key"));
        }
        let pieces: Vec<_> = key.split('/').collect();
        if pieces.len() != 6
            || pieces[0] != format!("{generation:016x}")
            || !root.contains_key(pieces[2])
        {
            return Err(invalid("unknown or cross-generation state record key"));
        }
        let value: VersionedRecord = serde_json::from_slice(&bytes)
            .map_err(|err| invalid(format!("corrupt state record: {err}")))?;
        if value.format != FORMAT || value.revision == 0 {
            return Err(invalid("unsupported state record format or revision"));
        }
        let field = pieces[2];
        let primary = decode_key(pieces[4])?;
        let run = pieces[1].strip_prefix("run-");
        match pieces[3] {
            "scalar" if pieces[1] == "global" && primary.is_empty() => {
                root.insert(field.to_owned(), value.value);
            }
            "map" if pieces[5].is_empty() => {
                let map = root
                    .get_mut(field)
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| invalid(format!("{field} is not a map")))?;
                if map.insert(primary, value.value).is_some() {
                    return Err(invalid("duplicate state map record"));
                }
            }
            "nested" if pieces[1] == "global" => {
                let nested = decode_key(pieces[5])?;
                let group = root
                    .get_mut(field)
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| invalid(format!("{field} is not a nested map")))?;
                let members = group
                    .entry(primary)
                    .or_insert_with(|| Value::Object(Map::new()))
                    .as_object_mut()
                    .ok_or_else(|| invalid("nested state record group has wrong type"))?;
                if members.insert(nested, value.value).is_some() {
                    return Err(invalid("duplicate nested state record"));
                }
            }
            "sequence" if HISTORY_FIELDS.contains(&field) && run.is_some() => {
                add_to_array(
                    &mut arrays,
                    field,
                    run.expect("checked"),
                    &primary,
                    value.value,
                )?;
            }
            "list" if LIST_FIELDS.contains(&field) && run.is_some() => {
                add_to_array(&mut arrays, field, "", &primary, value.value)?;
            }
            _ => return Err(invalid("malformed state record key")),
        }
    }
    for ((field, group), values) in arrays {
        let first = if HISTORY_FIELDS.contains(&field.as_str()) {
            1
        } else {
            0
        };
        let mut ordered = Vec::with_capacity(values.len());
        for (sequence, value) in values {
            if sequence != first + ordered.len() as u64 {
                return Err(invalid(format!("{field} has a missing sequence")));
            }
            ordered.push(value);
        }
        if group.is_empty() {
            root.insert(field, Value::Array(ordered));
        } else {
            root.get_mut(&field)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| invalid("history field is not a map"))?
                .insert(group, Value::Array(ordered));
        }
    }
    let state: State = serde_json::from_value(Value::Object(root))
        .map_err(|err| invalid(format!("state records cannot be assembled: {err}")))?;
    let expected = encode(&state, generation, 1)?;
    if expected.keys().ne(seen_keys.iter()) {
        return Err(invalid(
            "state record owner or identity does not match its value",
        ));
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CommandId, RunId, ScopeId};
    use crate::time::EngineTime;

    #[test]
    fn round_trips_versioned_events_and_command_maps() {
        let run = RunId::from_bytes([7; 16]);
        let command = CommandId::from_bytes([9; 16]);
        let mut state = State {
            engine_time_watermark_ms: 42,
            ..State::default()
        };
        state.command_times.insert(command, 42);
        state.history.insert(
            run,
            vec![crate::domain::DomainEvent::RunAdmitted {
                run,
                definition_id: "test".to_owned(),
                definition_version: 1,
                input: crate::Value::Null,
                root: ScopeId::from_bytes([2; 16]),
                policy: crate::policy::CapturedRunPolicy::defaults(None),
                admitted_ms: EngineTime::from_millis(42).as_millis(),
            }],
        );
        let rows = encode(&state, 3, 11).unwrap();
        assert!(rows.keys().any(|key| key.contains("/history/sequence/")));
        let restored = decode(rows, 3).unwrap();
        assert_eq!(
            serde_json::to_value(restored).unwrap(),
            serde_json::to_value(state).unwrap()
        );
    }

    #[test]
    fn rejects_unknown_versions_and_missing_event_sequence() {
        let mut rows = encode(&State::default(), 1, 1).unwrap();
        let key = rows.keys().next().unwrap().clone();
        let mut record: VersionedRecord = serde_json::from_slice(&rows[&key]).unwrap();
        record.format = "graphrun.state-record/v99".to_owned();
        rows.insert(key, serde_json::to_vec(&record).unwrap());
        assert_eq!(
            decode(rows, 1).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn run_records_keep_scope_owners_and_reject_a_foreign_owner() {
        use crate::domain::{Command, CommandBody};

        let catalog = crate::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = crate::compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        let run = RunId::from_bytes([3; 16]);
        let mut state = State::default();
        crate::domain::start_run(
            &mut state,
            Command {
                id: CommandId::from_bytes([4; 16]),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    catalog: Box::new(catalog.clone()),
                    input: crate::Value::Object(
                        [
                            ("order_id".to_owned(), crate::Value::String("r1".to_owned())),
                            ("amount".to_owned(), crate::Value::Int(1)),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                },
                time: EngineTime::from_millis(1),
            },
            definition,
            catalog,
        )
        .unwrap();
        let rows = encode(&state, 1, 1).unwrap();
        assert!(
            rows.keys()
                .any(|key| key.contains(&format!("/run-{}/scopes/map/", run.to_hex())))
        );
        assert_eq!(
            serde_json::to_value(decode(rows.clone(), 1).unwrap()).unwrap(),
            serde_json::to_value(state).unwrap()
        );
        let (key, value) = rows
            .iter()
            .find(|(key, _)| key.contains("/scopes/map/"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .unwrap();
        let mut corrupted = rows;
        corrupted.remove(&key);
        corrupted.insert(key.replace(&run.to_hex(), &"f".repeat(32)), value);
        assert_eq!(
            decode(corrupted, 1).unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }
}
