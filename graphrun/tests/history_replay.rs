use graphrun::domain::{
    self, ActivationStatus, Command, CommandBody, DomainEvent, ObligationStatus, RunStatus,
    ScopeStatus, State,
};
use graphrun::engine::ledger_clear;
use graphrun::ids::CommandId;
use graphrun::publication::{
    CommandKey, CommandResult, Disposition, PublicationOperation, StartKeyRecord,
};
use graphrun::time::EngineTime;
use graphrun::{
    Catalog, ControlRequest, Engine, ErrorKind, EventId, RunId, Value, compile_yaml,
    connect_control, ledger_get,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

const WORKFLOW: &str = "\
dsl: graphrun/v1
id: replay_loop
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  approval: {schema: unit/v1}
start: outer
nodes:
  outer:
    kind: repeat
    count: {literal: 1}
    state_schema: unit/v1
    state: {literal: null}
    max_iterations: 2
    body:
      input_schema: unit/v1
      output_schema: unit/v1
      start: inner
      nodes:
        inner:
          kind: complete
          output: {literal: null}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.outer.output}
";

fn catalog() -> Catalog {
    Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap()
}

fn test_state() -> State {
    let mut state = State {
        current_cluster_id: "11111111111111111111111111111111".to_owned(),
        ..State::default()
    };
    state
        .artifact_origins
        .insert(state.current_cluster_id.clone());
    state
}

fn started() -> (State, RunId) {
    let definition = compile_yaml(WORKFLOW, &catalog()).unwrap();
    let mut state = test_state();
    let run = RunId::generate();
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(1),
            body: CommandBody::Start {
                run,
                definition: Box::new(definition),
                catalog: Box::new(catalog()),
                input: Value::Null,
            },
        },
    )
    .unwrap();
    (state, run)
}

fn checkpointed() -> (State, RunId, u64) {
    let (mut state, run) = started();
    for millis in 2..12 {
        domain::commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(millis),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        if let Some(sequence) = state
            .checkpoints
            .get(&run)
            .map(|cp| cp.through_run_sequence)
        {
            return (state, run, sequence);
        }
    }
    panic!("terminal command did not create a checkpoint");
}

#[test]
fn bounded_command_cleanup_is_identical_with_independent_hash_order() {
    let ids: Vec<_> = (1..=16).map(|n| CommandId::from_bytes([n; 16])).collect();
    for _ in 0..24 {
        let mut state = test_state();
        for (index, id) in ids.iter().enumerate() {
            state.command_times.insert(*id, index as u64 + 1);
        }
        domain::commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(24 * 60 * 60 * 1000 + 100),
                body: CommandBody::PruneHistory { limit: 1 },
            },
        )
        .unwrap();
        assert_eq!(
            ids.iter()
                .filter(|id| !state.command_times.contains_key(id))
                .copied()
                .collect::<Vec<_>>(),
            vec![ids[0]]
        );
    }
}

#[test]
fn bounded_run_cleanup_is_identical_with_independent_hash_order() {
    let (seed, first) = started();
    let template = &seed.runs[&first];
    let ids: Vec<_> = (1..=16).map(|n| RunId::from_bytes([n; 16])).collect();
    for _ in 0..24 {
        let mut state = test_state();
        for (index, id) in ids.iter().enumerate() {
            let mut run = template.clone();
            run.id = *id;
            run.status = RunStatus::Succeeded {
                output: Value::Null,
            };
            run.terminal_ms = index as u64 + 1;
            state.runs.insert(*id, run);
        }
        domain::commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(31 * 24 * 60 * 60 * 1000),
                body: CommandBody::PruneHistory { limit: 1 },
            },
        )
        .unwrap();
        assert_eq!(
            ids.iter()
                .filter(|id| !state.runs.contains_key(id))
                .copied()
                .collect::<Vec<_>>(),
            vec![ids[0]]
        );
    }
}

#[test]
fn bounded_summary_cleanup_is_identical_with_independent_hash_order() {
    let ids: Vec<_> = (1..=16).map(|n| RunId::from_bytes([n; 16])).collect();
    for _ in 0..24 {
        let mut state = test_state();
        for (index, id) in ids.iter().enumerate() {
            state.terminal_summaries.insert(
                *id,
                graphrun::history::TerminalSummary {
                    run: *id,
                    workflow: "old".to_owned(),
                    version: 1,
                    status: "succeeded".to_owned(),
                    terminal_ms: 1,
                    expires_ms: index as u64 + 1,
                    history_through: 3,
                },
            );
        }
        domain::commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(100),
                body: CommandBody::PruneHistory { limit: 1 },
            },
        )
        .unwrap();
        assert_eq!(
            ids.iter()
                .filter(|id| !state.terminal_summaries.contains_key(id))
                .copied()
                .collect::<Vec<_>>(),
            vec![ids[0]]
        );
    }
}

#[test]
fn checkpoint_replay_rejects_corrupt_preceding_event() {
    let (mut state, run, through) = checkpointed();
    state.history_records.get_mut(&run).unwrap()[0]
        .payload
        .sha256 = "damaged".to_owned();
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through, EngineTime::from_millis(12))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
}

#[test]
fn checkpoint_replay_rejects_shortened_records_without_panicking() {
    let (mut state, run, through) = checkpointed();
    state.history_records.get_mut(&run).unwrap().pop();
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through, EngineTime::from_millis(12))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
}

#[test]
fn checkpoint_replay_rejects_invalid_and_altered_sequence() {
    let (mut state, run, through) = checkpointed();
    state.checkpoints.get_mut(&run).unwrap().format = "graphrun.run-checkpoint/v1".to_owned();
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through, EngineTime::from_millis(12))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    state.checkpoints.get_mut(&run).unwrap().format =
        graphrun::history::CHECKPOINT_FORMAT.to_owned();
    state
        .checkpoints
        .get_mut(&run)
        .unwrap()
        .through_run_sequence = through + 1;
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through - 1, EngineTime::from_millis(12))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    state
        .checkpoints
        .get_mut(&run)
        .unwrap()
        .through_run_sequence = through - 1;
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through, EngineTime::from_millis(12))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
}

#[test]
fn checkpoint_labels_complete_multi_event_boundary_and_projection_is_as_of() {
    let (mut state, run) = started();
    let root = state.runs[&run].root;
    let first = state.history[&run].len();
    assert!(first < 256);
    let make_event = |i| DomainEvent::NodeOutputRecorded {
        run,
        scope: root,
        node: "observed".to_owned(),
        output: Value::Int(i),
    };
    let before = 255 - first;
    domain::apply_events_with_cause(
        &mut state,
        &(0..before as i64).map(make_event).collect::<Vec<_>>(),
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(2),
    )
    .unwrap();
    domain::history_or_unavailable(&state, run, EngineTime::from_millis(2)).unwrap();
    graphrun::history::checkpoint_after_command(&mut state, run, EngineTime::from_millis(2))
        .unwrap();
    assert_eq!(
        state
            .checkpoints
            .get(&run)
            .map(|cp| cp.through_run_sequence),
        None
    );

    domain::apply_events_with_cause(
        &mut state,
        &(1..=5).map(make_event).collect::<Vec<_>>(),
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(3),
    )
    .unwrap();
    graphrun::history::checkpoint_after_command(&mut state, run, EngineTime::from_millis(3))
        .unwrap();
    assert_eq!(state.history[&run].len(), 260);
    assert_eq!(state.checkpoints[&run].through_run_sequence, 260);
    assert_eq!(
        state.checkpoints[&run].projection.scopes[&root].outputs["observed"],
        Value::Int(5)
    );
    let at_255 = graphrun::reconstruct_at(&state, run, 255, EngineTime::from_millis(3)).unwrap();
    assert_eq!(
        at_255.scopes[&root].outputs["observed"],
        Value::Int(before as i64 - 1)
    );
    let at_257 = graphrun::reconstruct_at(&state, run, 257, EngineTime::from_millis(3)).unwrap();
    assert_eq!(at_257.scopes[&root].outputs["observed"], Value::Int(2));
    assert!(matches!(at_257.runs[&run].status, RunStatus::Active));

    domain::apply_events_with_cause(
        &mut state,
        &[DomainEvent::RunSucceeded {
            run,
            output: Value::Null,
        }],
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(4),
    )
    .unwrap();
    state.runs.get_mut(&run).unwrap().terminal_ms = 4;
    graphrun::history::checkpoint_after_command(&mut state, run, EngineTime::from_millis(4))
        .unwrap();
    assert_eq!(state.checkpoints[&run].through_run_sequence, 261);
    let at_terminal =
        graphrun::reconstruct_at(&state, run, 261, EngineTime::from_millis(4)).unwrap();
    assert_eq!(at_terminal.runs[&run].terminal_ms, 4);
    assert!(matches!(
        at_terminal.runs[&run].status,
        RunStatus::Succeeded { .. }
    ));
}

#[test]
fn missing_or_unknown_record_and_artifact_versions_fail_closed() {
    let (mut state, run) = started();
    let now = EngineTime::from_millis(2);
    state.history_records.get_mut(&run).unwrap()[0].format = "unknown/v99".to_owned();
    assert_eq!(
        graphrun::history::page(&state, run, 0, 1, now)
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    state.history_records.get_mut(&run).unwrap()[0].format =
        graphrun::history::EVENT_FORMAT.to_owned();
    state
        .history_dependencies
        .get_mut(&run)
        .unwrap()
        .definition
        .sha256 = "missing".to_owned();
    assert_eq!(
        graphrun::reconstruct_at(&state, run, 1, now)
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    let definition_hash = graphrun::history::ArtifactRef::capture_in(
        &state.current_cluster_id,
        "graphrun.definition/v1",
        &state.runs[&run].definition,
    )
    .unwrap()
    .sha256;
    state
        .history_dependencies
        .get_mut(&run)
        .unwrap()
        .definition
        .sha256 = definition_hash;
    state
        .history_dependencies
        .get_mut(&run)
        .unwrap()
        .schemas
        .values_mut()
        .next()
        .unwrap()
        .sha256 = "missing-schema".to_owned();
    assert_eq!(
        graphrun::history::page(&state, run, 0, 1, now)
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    state.history_dependencies.remove(&run);
    assert_eq!(
        graphrun::history::page(&state, run, 0, 1, now)
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
}

#[test]
fn ownerless_workflow_fact_is_rejected_not_dropped_from_history() {
    let (mut state, run) = started();
    let before = state.history[&run].len();
    let err = domain::apply_events_with_cause(
        &mut state,
        &[DomainEvent::EventReserved {
            wait: graphrun::ids::WaitId::from_bytes([3; 16]),
            event_id: EventId::from_bytes([4; 16]),
        }],
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(2),
    )
    .unwrap_err();
    assert_eq!(err.kind, ErrorKind::FailedPrecondition);
    assert_eq!(state.history[&run].len(), before);
}

#[test]
fn committed_retention_keeps_tombstones_and_drops_typed_outputs() {
    let (mut state, run) = started();
    let root = state.runs[&run].root;
    let signal_id = EventId::from_bytes([4; 16]);
    let start_key = "remembered";
    state
        .start_keys
        .entry("replay_loop".to_owned())
        .or_default()
        .insert(
            start_key.to_owned(),
            StartKeyRecord {
                run,
                version: 1,
                input_digest: hex::encode(Sha256::digest(b"null")),
                admitted_ms: 1,
            },
        );
    let id = CommandId::generate();
    let key = CommandKey {
        cluster_id: "test".to_owned(),
        principal_id: "caller".to_owned(),
        command_id: id,
    };
    let result_key = key.storage_key();
    let receipt = CommandResult {
        format: graphrun::publication::RESULT_FORMAT.to_owned(),
        key: key.clone(),
        request_digest: "request".to_owned(),
        operation: "start".to_owned(),
        target: "replay_loop/remembered".to_owned(),
        outcome: Disposition::Applied {
            run: Some(run),
            digest: None,
            version: Some(1),
        },
        event_range: None,
        recorded_ms: 1,
    };
    state
        .command_results
        .insert(result_key.clone(), receipt.clone());
    state
        .command_results
        .insert(format!("{result_key}-second"), receipt);
    domain::apply_events_with_cause(
        &mut state,
        &[
            DomainEvent::EventAccepted {
                run,
                event_id: signal_id,
                signal: "approval".to_owned(),
                key: "order-1".to_owned(),
                payload: Value::Null,
                sequence: 1,
                accepted_ms: 5,
                expires_ms: 5 + 7 * 24 * 60 * 60 * 1000,
            },
            DomainEvent::ScopeCompleted {
                run,
                scope: root,
                output: Value::Null,
            },
            DomainEvent::RunSucceeded {
                run,
                output: Value::Null,
            },
        ],
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(10),
    )
    .unwrap();
    state.runs.get_mut(&run).unwrap().terminal_ms = 10;
    graphrun::history::checkpoint_after_command(&mut state, run, EngineTime::from_millis(10))
        .unwrap();
    let through = state.history[&run].len() as u64;
    let day_ms = 24 * 60 * 60 * 1000;
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(day_ms + 1),
            body: CommandBody::PruneHistory { limit: 1 },
        },
    )
    .unwrap();
    assert_eq!(
        state.command_results.len(),
        1,
        "one row per committed cleanup batch"
    );
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(day_ms + 2),
            body: CommandBody::PruneHistory { limit: 128 },
        },
    )
    .unwrap();
    assert!(state.command_results.is_empty());
    assert!(state.start_keys["replay_loop"].contains_key(start_key));
    let cutoff = 10 + 30 * day_ms;
    let before =
        graphrun::history::page(&state, run, 0, 10, EngineTime::from_millis(cutoff - 1)).unwrap();
    assert_eq!(before.retained_from, 1);
    assert_eq!(before.retained_through, through);
    assert_eq!(
        graphrun::reconstruct_at(&state, run, through, EngineTime::from_millis(cutoff))
            .unwrap_err()
            .kind,
        ErrorKind::Unavailable
    );
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(cutoff),
            body: CommandBody::PruneHistory { limit: 128 },
        },
    )
    .unwrap();
    assert!(!state.runs.contains_key(&run));
    assert!(!state.scopes.contains_key(&root));
    assert!(!state.checkpoints.contains_key(&run));
    let summary = graphrun::history::summary_view(&state.terminal_summaries[&run]);
    assert_eq!(summary["history_unavailable"], true);
    assert!(summary.get("output").is_none());
    let page =
        graphrun::history::page(&state, run, 0, 10, EngineTime::from_millis(cutoff)).unwrap();
    assert!(page.unavailable);
    let range = page.unavailable_range.as_ref().unwrap();
    assert_eq!((range.first, range.last), (1, through));
    assert_eq!(page.retained_from, through + 1);
    assert_eq!(page.retained_through, through);
    assert!(page.events.is_empty());
    assert!(state.signal_tombstones.contains_key(&format!(
        "{}:{}",
        run.to_hex(),
        signal_id.to_hex()
    )));
    let duplicate = domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(cutoff),
            body: CommandBody::Signal {
                run,
                event_id: signal_id,
                signal: "approval".to_owned(),
                key: "order-1".to_owned(),
                payload: Value::Null,
            },
        },
    )
    .unwrap();
    assert!(duplicate.is_empty());
    let operation = PublicationOperation::Start {
        workflow: "replay_loop".to_owned(),
        version: None,
        start_key: start_key.to_owned(),
        input: Value::Null,
    };
    let retry = graphrun::publication::apply(
        &mut state,
        &Command {
            id,
            time: EngineTime::from_millis(cutoff),
            body: CommandBody::Publication {
                key: key.clone(),
                operation: operation.clone(),
            },
        },
        &key,
        &operation,
    );
    assert_eq!(retry.applied_run().unwrap(), run);
    assert!(retry.event_range.is_none());
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(10 + 90 * day_ms),
            body: CommandBody::PruneHistory { limit: 128 },
        },
    )
    .unwrap();
    assert!(!state.terminal_summaries.contains_key(&run));
    assert!(!state.start_keys["replay_loop"].contains_key(start_key));
    assert!(!state.signal_tombstones.contains_key(&format!(
        "{}:{}",
        run.to_hex(),
        signal_id.to_hex()
    )));
    assert_eq!(
        graphrun::history::page(
            &state,
            run,
            0,
            10,
            EngineTime::from_millis(10 + 90 * day_ms)
        )
        .unwrap_err()
        .kind,
        ErrorKind::NotFound
    );
}

#[test]
fn expired_unreserved_input_is_a_recorded_fact_and_keeps_its_dedup_key() {
    let (mut state, run) = started();
    let id = EventId::from_bytes([9; 16]);
    let lifetime_ms = 7 * 24 * 60 * 60 * 1000;
    domain::apply_events_with_cause(
        &mut state,
        &[DomainEvent::EventAccepted {
            run,
            event_id: id,
            signal: "approval".to_owned(),
            key: "order-1".to_owned(),
            payload: Value::Null,
            sequence: 1,
            accepted_ms: 10,
            expires_ms: 10 + lifetime_ms,
        }],
        Some(CommandId::generate()),
        None,
        EngineTime::from_millis(10),
    )
    .unwrap();
    let before = state.history[&run].len() as u64;
    domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(10 + lifetime_ms - 1),
            body: CommandBody::PruneHistory { limit: 128 },
        },
    )
    .unwrap();
    assert_eq!(state.history[&run].len() as u64, before);
    let events = domain::commit_command(
        &mut state,
        Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(10 + lifetime_ms),
            body: CommandBody::PruneHistory { limit: 128 },
        },
    )
    .unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], DomainEvent::EventExpired { event_id, .. } if event_id == id));
    assert!(state.inbox.is_empty());
    assert_eq!(
        graphrun::reconstruct_at(
            &state,
            run,
            before,
            EngineTime::from_millis(10 + lifetime_ms)
        )
        .unwrap()
        .inbox
        .len(),
        1
    );
    assert!(
        graphrun::reconstruct_at(
            &state,
            run,
            before + 1,
            EngineTime::from_millis(10 + lifetime_ms)
        )
        .unwrap()
        .inbox
        .is_empty()
    );
    assert!(
        state
            .signal_tombstones
            .contains_key(&format!("{}:{}", run.to_hex(), id.to_hex()))
    );
}

#[test]
fn failed_committed_cleanup_keeps_prior_results_and_state_intact() {
    let (mut state, run) = started();
    let key = CommandKey {
        cluster_id: "test".to_owned(),
        principal_id: "caller".to_owned(),
        command_id: CommandId::generate(),
    };
    state.command_results.insert(
        key.storage_key(),
        CommandResult {
            format: graphrun::publication::RESULT_FORMAT.to_owned(),
            key,
            request_digest: "old".to_owned(),
            operation: "start".to_owned(),
            target: "run".to_owned(),
            outcome: Disposition::Applied {
                run: Some(run),
                digest: None,
                version: Some(1),
            },
            event_range: None,
            recorded_ms: 1,
        },
    );
    state.inbox.push(domain::InboxEntry {
        event_id: EventId::generate(),
        signal: "approval".to_owned(),
        key: "orphan".to_owned(),
        payload: Value::Null,
        sequence: 1,
        reserved_wait: None,
        consumed: false,
        accepted_ms: 1,
        expires_ms: 2,
        run: None,
    });
    let original = serde_json::to_value(&state).unwrap();
    assert_eq!(
        domain::commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(24 * 60 * 60 * 1000 + 2),
                body: CommandBody::PruneHistory { limit: 128 },
            },
        )
        .unwrap_err()
        .kind,
        ErrorKind::FailedPrecondition
    );
    assert_eq!(serde_json::to_value(&state).unwrap(), original);
}

#[tokio::test]
async fn pages_control_replay_and_restart_agree_without_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::local(dir.path()).await.unwrap();
    let run = engine
        .start_yaml(WORKFLOW, &catalog(), Value::Null)
        .await
        .unwrap();
    engine
        .wait_terminal(run, Duration::from_secs(5))
        .await
        .unwrap();
    let first = engine.history_page(run, 0, 2).await.unwrap();
    assert_eq!(first.retained_from, 1);
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.events[0].record.sequence, 1);
    assert!(first.next_cursor.is_some());
    let mut cursor = first.next_cursor.unwrap();
    let mut total = 2;
    while cursor < first.retained_through {
        let next = engine.history_page(run, cursor, 2).await.unwrap();
        total += next.events.len();
        cursor = next.next_cursor.unwrap_or(next.retained_through);
    }
    assert_eq!(total as u64, first.retained_through);
    let all = engine.history_page(run, 0, 1000).await.unwrap().events;
    let body = all
        .iter()
        .find_map(|entry| match &entry.event {
            DomainEvent::ScopeOpened {
                role: domain::ScopeRole::LoopBody { activation, index },
                input,
                ..
            } => Some((entry.record.sequence, *activation, *index, input.clone())),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        engine.reconstruct_at(run, body.0).await.unwrap().loop_carry[&body.1],
        (body.3, body.2)
    );
    let checkpoint = engine.inspect(run).await.unwrap().checkpoints[&run].clone();
    assert_eq!(checkpoint.through_run_sequence, first.retained_through);
    let before_terminal = engine
        .reconstruct_at(run, first.retained_through - 1)
        .await
        .unwrap();
    assert!(matches!(
        before_terminal.runs[&run].status,
        RunStatus::Active
    ));
    assert!(before_terminal.scopes.values().any(|scope| {
        scope.run == run && matches!(scope.status, ScopeStatus::Completed { .. })
    }));
    let via_socket = connect_control(
        dir.path().join("control.sock"),
        ControlRequest::History {
            run: run.to_hex(),
            after_sequence: 0,
            page_size: 2,
        },
    )
    .await
    .unwrap();
    assert!(via_socket.ok, "{:?}", via_socket.error);
    assert_eq!(via_socket.body["retained_through"], first.retained_through);
    let snapshot = connect_control(dir.path().join("control.sock"), ControlRequest::Snapshot)
        .await
        .unwrap();
    assert!(snapshot.ok, "{:?}", snapshot.error);
    assert_eq!(
        engine
            .history_page(run, 0, 2)
            .await
            .unwrap()
            .retained_through,
        first.retained_through,
        "Raft snapshot and log purge cannot prune workflow history"
    );
    let replay = connect_control(
        dir.path().join("control.sock"),
        ControlRequest::Replay {
            run: run.to_hex(),
            through_sequence: first.retained_through - 1,
        },
    )
    .await
    .unwrap();
    assert!(replay.ok, "{:?}", replay.error);
    assert_eq!(replay.body["status"], "active");
    let state = engine.inspect(run).await.unwrap();
    assert_eq!(state.history[&run].len(), first.retained_through as usize);
    engine.shutdown().await.unwrap();
    let offline = graphrun::replay(dir.path()).unwrap();
    assert_eq!(
        offline.history_records[&run].len() as u64,
        first.retained_through
    );
    let engine = Engine::local(dir.path()).await.unwrap();
    assert_eq!(
        engine
            .history_page(run, 0, 2)
            .await
            .unwrap()
            .retained_through,
        first.retained_through
    );
    assert_eq!(
        engine
            .reconstruct_at(run, first.retained_through)
            .await
            .unwrap()
            .runs[&run]
            .status,
        RunStatus::Succeeded {
            output: Value::Null
        }
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn as_of_wait_reservation_and_delivery_uses_recorded_signal_only() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::local(dir.path()).await.unwrap();
    let input: Value = serde_json::from_value(serde_json::json!({"key":"order-1"})).unwrap();
    let payload: Value = serde_json::from_value(serde_json::json!({"approved":true})).unwrap();
    let run = engine
        .start_yaml(
            include_str!("../../samples/03-events/workflow.yaml"),
            &catalog(),
            input,
        )
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !engine
        .inspect(run)
        .await
        .unwrap()
        .waits
        .values()
        .any(|wait| wait.pending)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "wait was never opened"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let events = engine.history_page(run, 0, 100).await.unwrap().events;
    let waiting = events
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::WaitOpened { .. }))
        .unwrap()
        .record
        .sequence;
    let at_wait = engine.reconstruct_at(run, waiting).await.unwrap();
    assert!(at_wait.waits.values().any(|wait| wait.pending));
    let waiting_activation = at_wait.waits.values().next().unwrap().activation;
    assert_eq!(
        at_wait.activations[&waiting_activation].status,
        ActivationStatus::Ready
    );
    assert!(at_wait.inbox.is_empty());
    engine
        .signal(
            run,
            EventId::from_bytes([1; 16]),
            "approval",
            "order-1",
            payload.clone(),
        )
        .await
        .unwrap();
    let sent = engine.history_page(run, 0, 100).await.unwrap().events;
    let accepted = sent
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::EventAccepted { .. }))
        .unwrap()
        .record
        .sequence;
    let reserved = sent
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::EventReserved { .. }))
        .unwrap()
        .record
        .sequence;
    assert_eq!(reserved, accepted + 1);
    assert_eq!(
        engine.reconstruct_at(run, accepted).await.unwrap().inbox[0].reserved_wait,
        None
    );
    assert!(
        engine.reconstruct_at(run, reserved).await.unwrap().inbox[0]
            .reserved_wait
            .is_some()
    );
    engine
        .wait_terminal(run, Duration::from_secs(5))
        .await
        .unwrap();
    let done = engine.history_page(run, 0, 100).await.unwrap();
    let satisfied = done
        .events
        .iter()
        .find(|entry| matches!(entry.event, DomainEvent::WaitSatisfied { .. }))
        .unwrap()
        .record
        .sequence;
    assert!(engine.reconstruct_at(run, satisfied).await.unwrap().inbox[0].consumed);
    engine
        .signal(
            run,
            EventId::from_bytes([1; 16]),
            "approval",
            "order-1",
            payload,
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .history_page(run, 0, 100)
            .await
            .unwrap()
            .retained_through,
        done.retained_through
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn as_of_compensation_is_not_reexecuted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::local(dir.path()).await.unwrap();
    let input = serde_json::from_value(serde_json::json!({
        "order_id": "o1",
        "amount": 1000,
        "fail_after_payment": true
    }))
    .unwrap();
    let run = engine
        .start_yaml(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            &catalog(),
            input,
        )
        .await
        .unwrap();
    assert!(
        engine
            .wait_terminal(run, Duration::from_secs(10))
            .await
            .is_err()
    );
    let entries = engine.history_page(run, 0, 1000).await.unwrap().events;
    let (start, key) = entries
        .iter()
        .find_map(|entry| match &entry.event {
            DomainEvent::CompensationStarted { .. } => {
                entries.iter().find_map(|claim| match &claim.event {
                    DomainEvent::ClaimGranted { effect_key, .. } => {
                        Some((entry.record.sequence, effect_key.to_hex()))
                    }
                    _ => None,
                })
            }
            _ => None,
        })
        .unwrap();
    let as_of = engine.reconstruct_at(run, start).await.unwrap();
    assert!(
        as_of.obligations.iter().any(|obligation| {
            matches!(obligation.status, ObligationStatus::Compensating { .. })
        })
    );
    assert!(as_of.activations.values().any(|activation| {
        activation.role == graphrun::ids::ExecutionRole::Compensation
            && activation.status == ActivationStatus::Ready
    }));
    assert!(matches!(as_of.runs[&run].status, RunStatus::Active));
    ledger_clear();
    assert!(ledger_get(&key).is_none());
    let terminal = engine
        .reconstruct_at(run, entries.last().unwrap().record.sequence)
        .await
        .unwrap();
    assert!(matches!(
        terminal.runs[&run].status,
        RunStatus::Failed { .. }
    ));
    assert!(
        terminal
            .obligations
            .iter()
            .all(|item| matches!(item.status, ObligationStatus::Compensated))
    );
    assert!(
        ledger_get(&key).is_none(),
        "read-only replay must not invoke handlers"
    );
    engine.shutdown().await.unwrap();
}
