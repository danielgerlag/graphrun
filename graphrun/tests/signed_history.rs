use graphrun::domain::{Command, CommandBody, DomainEvent, State, commit_command};
use graphrun::generated::{ClaimRequest, RegisterRequest, RenewSessionRequest, SignalRequest};
use graphrun::ids::{ActivityKey, CommandId, ExecutionRole, WorkerSessionId};
use graphrun::time::EngineTime;
use graphrun::tls::{ClusterId, PeerRole, PrincipalId, PrincipalIdentity, issue_principal};
use graphrun::{
    ActivityError, Catalog, Engine, EventId, GrpcClient, MemberConfig, Value, Worker, generate_ca,
    issue_node,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

const SIGNAL_YAML: &str = include_str!("../../samples/03-events/workflow.yaml");
const WORKER_YAML: &str = "\
dsl: graphrun/v1
id: signed_worker_history
version: 1
input_schema: integer/v1
output_schema: integer/v1
start: effect
nodes:
  effect:
    kind: activity
    activity: {name: sdk.effect, version: 1}
    input: {from: workflow.input}
    next: done
  done:
    kind: complete
    output: {from: nodes.effect.output}
";

fn worker_catalog() -> Catalog {
    Catalog::from_json(
        br#"{"format":"graphrun.catalog/v1","schemas":{},"activities":[{
            "name":"sdk.effect","version":1,"input_schema":"integer/v1",
            "output_schema":"integer/v1","execution":"blocking","effects":"external",
            "recovery":"RetrySafe","error_codes":[]}]}"#,
    )
    .unwrap()
}

fn reconciliation_catalog() -> Catalog {
    Catalog::from_json(
        br#"{"format":"graphrun.catalog/v1","schemas":{},"activities":[{
            "name":"sdk.effect","version":1,"input_schema":"integer/v1",
            "output_schema":"integer/v1","execution":"blocking","effects":"external",
            "recovery":"Manual","error_codes":[],
            "reconciler":{"name":"sdk.lookup","version":1}}],
            "reconcilers":[{"name":"sdk.lookup","version":1,
            "forward_activity":{"name":"sdk.effect","version":1}}]}"#,
    )
    .unwrap()
}

fn signed(
    ca: &graphrun::CertificateAuthority,
    principal: &str,
    roles: impl IntoIterator<Item = PeerRole>,
    server_name: &str,
) -> graphrun::TlsMaterial {
    let cluster = ClusterId::parse(hex::encode(&Sha256::digest(ca.pem.as_bytes())[..16])).unwrap();
    let identity =
        PrincipalIdentity::new(cluster, PrincipalId::parse(principal).unwrap(), roles).unwrap();
    let mut tls = issue_principal(ca, &identity, "actor.graphrun.local").unwrap();
    tls.server_name = server_name.to_owned();
    tls
}

struct TestDirectory(tempfile::TempDir);

impl TestDirectory {
    fn new() -> Self {
        Self(tempfile::Builder::new().prefix("gr-h-").tempdir().unwrap())
    }
}

fn state_with_origin() -> State {
    let mut state = State {
        current_cluster_id: "22222222222222222222222222222222".to_owned(),
        ..State::default()
    };
    state
        .artifact_origins
        .insert(state.current_cluster_id.clone());
    state
}

#[test]
fn replicated_cause_is_backward_readable_and_scoped_before_dedup() {
    let run = graphrun::RunId::generate();
    let catalog = Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    let legacy = Command {
        id: CommandId::generate(),
        time: EngineTime::from_millis(1),
        body: CommandBody::Start {
            run,
            definition: Box::new(graphrun::compile_yaml(SIGNAL_YAML, &catalog).unwrap()),
            catalog: Box::new(catalog),
            input: serde_json::from_value(serde_json::json!({"key":"order-1"})).unwrap(),
        },
    };
    let encoded = serde_json::to_vec(&legacy).unwrap();
    let mut state = state_with_origin();
    commit_command(&mut state, serde_json::from_slice(&encoded).unwrap()).unwrap();
    assert!(
        state.history_records[&run]
            .iter()
            .all(|record| record.principal_id.is_none())
    );

    let command = Command {
        id: CommandId::generate(),
        time: EngineTime::from_millis(2),
        body: CommandBody::Authenticated {
            principal_id: "alice".to_owned(),
            body: Box::new(CommandBody::Signal {
                run,
                event_id: EventId::generate(),
                signal: "approval".to_owned(),
                key: "unused-alice".to_owned(),
                payload: serde_json::from_value(serde_json::json!({"approved":true})).unwrap(),
            }),
        },
    };
    let events = commit_command(&mut state, command.clone()).unwrap();
    let length = state.history_records[&run].len();
    assert!(!events.is_empty());
    assert_eq!(commit_command(&mut state, command.clone()).unwrap(), events);
    assert_eq!(state.history_records[&run].len(), length);
    let mut different_actor = command.clone();
    if let CommandBody::Authenticated { principal_id, body } = &mut different_actor.body {
        *principal_id = "bob".to_owned();
        if let CommandBody::Signal { event_id, key, .. } = body.as_mut() {
            *event_id = EventId::generate();
            *key = "unused-bob".to_owned();
        }
    }
    let bob_events = commit_command(&mut state, different_actor.clone()).unwrap();
    assert!(
        bob_events
            .iter()
            .any(|event| matches!(event, DomainEvent::EventAccepted { .. }))
    );
    assert_eq!(state.history_records[&run].len(), length + bob_events.len());
    assert!(
        state.history_records[&run][length..]
            .iter()
            .all(|record| record.command_id == Some(command.id)
                && record.principal_id.as_deref() == Some("bob"))
    );
    let alice_key = state
        .command_actors
        .iter()
        .find(|(_, actor)| *actor == "alice")
        .map(|(id, _)| *id)
        .unwrap();
    let bob_key = state
        .command_actors
        .iter()
        .find(|(_, actor)| *actor == "bob")
        .map(|(id, _)| *id)
        .unwrap();
    assert_ne!(alice_key, bob_key);
    assert_eq!(state.command_external_ids[&alice_key], command.id);
    assert_eq!(state.command_external_ids[&bob_key], command.id);
    assert_eq!(
        commit_command(
            &mut state,
            Command {
                id: alice_key,
                time: EngineTime::from_millis(3),
                body: CommandBody::PruneHistory { limit: 1 },
            }
        )
        .unwrap_err()
        .kind,
        graphrun::ErrorKind::AlreadyExists
    );
    assert!(
        state.authenticated_request_digests[&alice_key]
            .starts_with("graphrun.authenticated-request/v1:")
    );
    assert!(
        state.authenticated_request_digests[&bob_key]
            .starts_with("graphrun.authenticated-request/v1:")
    );
    state
        .command_external_ids
        .insert(bob_key, CommandId::generate());
    assert_eq!(
        commit_command(&mut state, different_actor.clone())
            .unwrap_err()
            .kind,
        graphrun::ErrorKind::AlreadyExists
    );
    state.command_external_ids.insert(bob_key, command.id);
    assert_eq!(
        commit_command(&mut state, different_actor).unwrap(),
        bob_events
    );
    let digest = state.authenticated_request_digests[&alice_key].clone();
    state
        .authenticated_request_digests
        .insert(alice_key, digest.replace("/v1:", "/v2:"));
    assert_eq!(
        commit_command(&mut state, command.clone())
            .unwrap_err()
            .kind,
        graphrun::ErrorKind::FailedPrecondition
    );
    state
        .authenticated_request_digests
        .insert(alice_key, digest);
    let mut changed = command.clone();
    if let CommandBody::Authenticated { body, .. } = &mut changed.body
        && let CommandBody::Signal { payload, .. } = body.as_mut()
    {
        *payload = serde_json::from_value(serde_json::json!({"approved":false})).unwrap();
    }
    assert_eq!(
        commit_command(&mut state, changed).unwrap_err().kind,
        graphrun::ErrorKind::AlreadyExists
    );
    let mut invalid = command.clone();
    if let CommandBody::Authenticated { principal_id, .. } = &mut invalid.body {
        *principal_id = "spoofed principal".to_owned();
    }
    assert_eq!(
        commit_command(&mut state, invalid).unwrap_err().kind,
        graphrun::ErrorKind::InvalidArgument
    );
    let CommandBody::Authenticated { body, .. } = command.body else {
        unreachable!();
    };
    assert!(
        commit_command(
            &mut state,
            Command {
                id: command.id,
                time: command.time,
                body: *body,
            }
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(state.history_records[&run].len(), length + bob_events.len());
}

#[test]
fn same_external_start_id_creates_distinct_principal_scoped_execution_ids() {
    let mut state = state_with_origin();
    let external = CommandId::generate();
    let catalog = worker_catalog();
    let definition = graphrun::compile_yaml(WORKER_YAML, &catalog).unwrap();
    let mut roots = Vec::new();
    let mut activations = Vec::new();
    for actor in ["alice", "bob"] {
        let run = graphrun::RunId::generate();
        let events = commit_command(
            &mut state,
            Command {
                id: external,
                time: EngineTime::from_millis(1),
                body: CommandBody::Authenticated {
                    principal_id: actor.to_owned(),
                    body: Box::new(CommandBody::Start {
                        run,
                        definition: Box::new(definition.clone()),
                        catalog: Box::new(catalog.clone()),
                        input: Value::Int(41),
                    }),
                },
            },
        )
        .unwrap();
        let root = events
            .iter()
            .find_map(|event| match event {
                DomainEvent::RunAdmitted { root, .. } => Some(*root),
                _ => None,
            })
            .unwrap();
        roots.push(root);
        activations.push(
            state
                .activations
                .values()
                .find(|activation| activation.run == run)
                .unwrap()
                .id,
        );
        assert!(
            state.history_records[&run]
                .iter()
                .all(|record| record.command_id == Some(external)
                    && record.principal_id.as_deref() == Some(actor))
        );
    }
    assert_ne!(roots[0], roots[1]);
    assert_ne!(activations[0], activations[1]);
    let saved = serde_json::to_vec(&state).unwrap();
    let restored: State = serde_json::from_slice(&saved).unwrap();
    assert_eq!(restored.command_external_ids.len(), 2);
    assert_eq!(restored.authenticated_request_digests.len(), 2);
}

#[test]
fn principal_scoped_worker_claims_keep_distinct_effects_and_durable_receipts() {
    let mut state = state_with_origin();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let catalog = worker_catalog();
    let definition = graphrun::compile_yaml(WORKER_YAML, &catalog).unwrap();
    let start_id = CommandId::generate();
    let mut runs = Vec::new();
    for actor in ["alice", "bob"] {
        let run = graphrun::RunId::generate();
        commit_command(
            &mut state,
            Command {
                id: start_id,
                time: EngineTime::from_millis(now),
                body: CommandBody::Authenticated {
                    principal_id: actor.to_owned(),
                    body: Box::new(CommandBody::Start {
                        run,
                        definition: Box::new(definition.clone()),
                        catalog: Box::new(catalog.clone()),
                        input: Value::Int(41),
                    }),
                },
            },
        )
        .unwrap();
        commit_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(now + 1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        runs.push(run);
    }
    runs.sort();
    let claim_id = CommandId::generate();
    let capability = graphrun::worker_contract::capability_for(
        &catalog,
        &ActivityKey::new("sdk.effect", 1),
        ExecutionRole::Forward,
    )
    .unwrap();
    let mut assignments = Vec::new();
    for (actor, run) in ["worker-a", "worker-b"].into_iter().zip(runs) {
        let session = WorkerSessionId::generate();
        let register = Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(now + 2),
            body: CommandBody::Authenticated {
                principal_id: actor.to_owned(),
                body: Box::new(CommandBody::RegisterWorker {
                    session,
                    principal_id: actor.to_owned(),
                    capabilities: vec![capability.clone()],
                    capacity: 1,
                    protocol_min: 1,
                    protocol_max: 1,
                }),
            },
        };
        commit_command(&mut state, register).unwrap();
        let claim = Command {
            id: claim_id,
            time: EngineTime::from_millis(now + 3),
            body: CommandBody::Authenticated {
                principal_id: actor.to_owned(),
                body: Box::new(CommandBody::Claim {
                    session,
                    capacity: 1,
                }),
            },
        };
        let events = commit_command(&mut state, claim.clone()).unwrap();
        let granted = graphrun::domain::assignments_from(&state, &events).unwrap();
        assert_eq!(
            granted.len(),
            1,
            "claim events: {events:?}, activations: {:?}",
            state
                .activations
                .values()
                .map(|activation| (activation.run, activation.status.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(granted[0].run, run);
        assignments.push(granted[0].clone());
        let history_len = state.history_records[&run].len();
        let restored: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        state = restored;
        assert_eq!(commit_command(&mut state, claim.clone()).unwrap(), events);
        assert_eq!(state.history_records[&run].len(), history_len);
        let mut changed = claim;
        if let CommandBody::Authenticated { body, .. } = &mut changed.body {
            **body = CommandBody::Claim {
                session,
                capacity: 2,
            };
        }
        assert_eq!(
            commit_command(&mut state, changed).unwrap_err().kind,
            graphrun::ErrorKind::AlreadyExists
        );
    }
    assert_ne!(assignments[0].activation, assignments[1].activation);
    assert_ne!(assignments[0].effect_key, assignments[1].effect_key);
}

async fn assert_history(
    engine: &Engine,
    run: graphrun::RunId,
    cancelled: graphrun::RunId,
    worked: graphrun::RunId,
    signal_id: CommandId,
    cancel_id: CommandId,
) {
    let signal_page = engine.history_page(run, 0, 100).await.unwrap();
    let signaled: Vec<_> = signal_page
        .events
        .iter()
        .filter(|entry| entry.record.command_id == Some(signal_id))
        .collect();
    assert!(!signaled.is_empty());
    assert!(
        signaled
            .iter()
            .all(|entry| entry.record.principal_id.as_deref() == Some("alice"))
    );
    assert!(
        signaled
            .iter()
            .any(|entry| matches!(entry.event, DomainEvent::EventAccepted { .. }))
    );
    let terminal = engine.history_page(cancelled, 0, 100).await.unwrap();
    assert!(terminal.events.iter().any(|entry| {
        entry.record.command_id == Some(cancel_id)
            && entry.record.principal_id.as_deref() == Some("alice")
    }));
    let worker_page = engine.history_page(worked, 0, 100).await.unwrap();
    for event in &worker_page.events {
        if matches!(
            event.event,
            DomainEvent::ClaimGranted { .. } | DomainEvent::LeafSucceeded { .. }
        ) {
            assert_eq!(event.record.principal_id.as_deref(), Some("worker-one"));
            assert!(event.record.command_id.is_some());
        }
    }
    assert!(
        worker_page
            .events
            .iter()
            .any(|entry| matches!(entry.event, DomainEvent::ClaimGranted { .. }))
    );
    assert!(
        worker_page
            .events
            .iter()
            .any(|entry| matches!(entry.event, DomainEvent::LeafSucceeded { .. }))
    );
    for page in [&signal_page, &terminal, &worker_page] {
        assert!(matches!(
            page.events.first().map(|entry| &entry.event),
            Some(DomainEvent::RunAdmitted { .. })
        ));
        assert_eq!(page.events[0].record.principal_id.as_deref(), Some("alice"));
        for entry in &page.events {
            assert_eq!(entry.record.format, graphrun::history::EVENT_FORMAT);
            if entry.record.principal_id.is_none() {
                assert_ne!(entry.record.command_id, Some(signal_id));
                assert_ne!(entry.record.command_id, Some(cancel_id));
                assert!(!matches!(
                    entry.event,
                    DomainEvent::ClaimGranted { .. } | DomainEvent::LeafSucceeded { .. }
                ));
            }
        }
        let projection = engine
            .reconstruct_at(page.run, page.retained_through)
            .await
            .unwrap();
        let projected = &projection.history_records[&page.run];
        assert_eq!(
            serde_json::to_value(projected).unwrap(),
            serde_json::to_value(
                page.events
                    .iter()
                    .map(|entry| &entry.record)
                    .collect::<Vec<_>>()
            )
            .unwrap()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_client_and_worker_causes_are_durable_and_not_spoofable() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let server_tls = issue_node(&ca, 1).unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let endpoint = format!("https://{addr}");
    let open = |initialize| MemberConfig {
        data_dir: directory.0.path().join("member"),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: server_tls.clone(),
        host_activities: false,
        initialize,
    };
    let engine = Engine::member(open(true)).await.unwrap();
    let alice_tls = signed(&ca, "alice", [PeerRole::Client], &server_tls.server_name);
    let bob_tls = signed(&ca, "bob", [PeerRole::Client], &server_tls.server_name);
    let worker_tls = signed(
        &ca,
        "worker-one",
        [PeerRole::Worker],
        &server_tls.server_name,
    );
    let mut alice = GrpcClient::connect(&endpoint, &alice_tls).await.unwrap();
    let mut bob = GrpcClient::connect(&endpoint, &bob_tls).await.unwrap();
    let catalog = Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    let input: Value = serde_json::from_value(serde_json::json!({"key":"order-1"})).unwrap();
    let approval: Value = serde_json::from_value(serde_json::json!({"approved":true})).unwrap();
    let start_id = CommandId::generate();
    let run = alice
        .start_with_command(SIGNAL_YAML, &catalog, &input, start_id)
        .await
        .unwrap();
    let bob_run = bob
        .start_with_command(SIGNAL_YAML, &catalog, &input, start_id)
        .await
        .unwrap();
    assert_ne!(run, bob_run);
    let alice_start = engine.history_page(run, 0, 100).await.unwrap();
    let bob_start = engine.history_page(bob_run, 0, 100).await.unwrap();
    let root = |page: &graphrun::history::HistoryPage| match &page.events[0].event {
        DomainEvent::RunAdmitted { root, .. } => *root,
        _ => panic!("missing admission"),
    };
    assert_ne!(root(&alice_start), root(&bob_start));
    assert_eq!(alice_start.events[0].record.command_id, Some(start_id));
    assert_eq!(bob_start.events[0].record.command_id, Some(start_id));
    assert_eq!(
        bob_start.events[0].record.principal_id.as_deref(),
        Some("bob")
    );
    let event = EventId::generate();
    let signal_id = CommandId::generate();
    alice
        .signal_with_command(run, event, "approval", "order-1", &approval, signal_id)
        .await
        .unwrap();
    alice
        .signal_with_command(run, event, "approval", "order-1", &approval, signal_id)
        .await
        .unwrap();
    let bob_event = EventId::generate();
    bob.signal_with_command(
        bob_run, bob_event, "approval", "order-1", &approval, signal_id,
    )
    .await
    .unwrap();
    assert!(
        engine
            .history_page(bob_run, 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(
                |entry| matches!(entry.event, DomainEvent::EventAccepted { .. })
                    && entry.record.command_id == Some(signal_id)
                    && entry.record.principal_id.as_deref() == Some("bob")
            )
    );

    let channel = tonic::transport::Channel::from_shared(endpoint.clone())
        .unwrap()
        .tls_config(graphrun::rpc::client_tls(&worker_tls).unwrap())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut wrong_role = graphrun::generated::client_client::ClientClient::new(channel.clone());
    let mut spoofed = tonic::Request::new(SignalRequest {
        command_id: signal_id.to_hex(),
        run_id: run.to_hex(),
        event_id: event.to_hex(),
        name: "approval".into(),
        key: "order-1".into(),
        payload_json: serde_json::to_vec(&approval).unwrap(),
    });
    spoofed
        .metadata_mut()
        .insert("principal-id", "alice".parse().unwrap());
    assert_eq!(
        wrong_role.signal(spoofed).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut raw_worker = graphrun::generated::worker_client::WorkerClient::new(channel);
    assert_eq!(
        raw_worker
            .register(RegisterRequest {
                session_id: graphrun::ids::WorkerSessionId::generate().to_hex(),
                capacity: 1,
                capabilities: Vec::new(),
                principal_id: "alice".into(),
                protocol_min: 1,
                protocol_max: 1,
                command_id: CommandId::generate().to_hex(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );

    let cancelled = alice.start(SIGNAL_YAML, &catalog, &input).await.unwrap();
    let cancel_id = CommandId::generate();
    alice
        .cancel_with_command(cancelled, "operator request", cancel_id)
        .await
        .unwrap();
    let worker = Worker::builder(endpoint, worker_tls, worker_catalog())
        .blocking("sdk.effect", 1, |input: i64, _| {
            Ok::<_, ActivityError>(input + 1)
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = stopped.await;
    }));
    let worked = alice
        .start(WORKER_YAML, &worker_catalog(), &Value::Int(41))
        .await
        .unwrap();
    assert_eq!(
        engine
            .wait_terminal(worked, Duration::from_secs(20))
            .await
            .unwrap(),
        Value::Int(42)
    );
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();

    assert_history(&engine, run, cancelled, worked, signal_id, cancel_id).await;
    engine.snapshot().await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = Engine::member(open(false)).await.unwrap();
    assert_history(&engine, run, cancelled, worked, signal_id, cancel_id).await;
    assert!(
        engine
            .history_page(bob_run, 0, 100)
            .await
            .unwrap()
            .events
            .iter()
            .any(
                |entry| matches!(entry.event, DomainEvent::EventAccepted { .. })
                    && entry.record.command_id == Some(signal_id)
                    && entry.record.principal_id.as_deref() == Some("bob")
            )
    );
    let before = engine.history_page(run, 0, 100).await.unwrap();
    let mut alice = GrpcClient::connect(&format!("https://{addr}"), &alice_tls)
        .await
        .unwrap();
    alice
        .signal_with_command(run, event, "approval", "order-1", &approval, signal_id)
        .await
        .unwrap();
    assert_eq!(
        alice
            .start_with_command(SIGNAL_YAML, &catalog, &input, start_id)
            .await
            .unwrap(),
        run
    );
    let mut bob = GrpcClient::connect(&format!("https://{addr}"), &bob_tls)
        .await
        .unwrap();
    assert_eq!(
        bob.start_with_command(SIGNAL_YAML, &catalog, &input, start_id)
            .await
            .unwrap(),
        bob_run
    );
    let changed_input: Value =
        serde_json::from_value(serde_json::json!({"key":"changed"})).unwrap();
    assert_eq!(
        alice
            .start_with_command(SIGNAL_YAML, &catalog, &changed_input, start_id)
            .await
            .unwrap_err()
            .kind,
        graphrun::ErrorKind::AlreadyExists
    );
    let changed: Value = serde_json::from_value(serde_json::json!({"approved":false})).unwrap();
    assert_eq!(
        alice
            .signal_with_command(run, event, "approval", "order-1", &changed, signal_id)
            .await
            .unwrap_err()
            .kind,
        graphrun::ErrorKind::AlreadyExists
    );
    assert_eq!(
        alice
            .cancel_with_command(cancelled, "different reason", cancel_id)
            .await
            .unwrap_err()
            .kind,
        graphrun::ErrorKind::AlreadyExists
    );
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(engine.history_page(run, 0, 100).await.unwrap()).unwrap()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_worker_receipts_are_scoped_and_reusable_after_restart() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let server_tls = issue_node(&ca, 1).unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let endpoint = format!("https://{addr}");
    let open = |initialize| MemberConfig {
        data_dir: directory.0.path().join("member"),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: server_tls.clone(),
        host_activities: false,
        initialize,
    };
    let engine = Engine::member(open(true)).await.unwrap();
    let catalog = worker_catalog();
    let client_tls = signed(&ca, "alice", [PeerRole::Client], &server_tls.server_name);
    let mut client = GrpcClient::connect(&endpoint, &client_tls).await.unwrap();
    let mut runs = Vec::new();
    for _ in 0..2 {
        let run = client
            .start(WORKER_YAML, &catalog, &Value::Int(41))
            .await
            .unwrap();
        engine.progress(run).await.unwrap();
        runs.push(run);
    }
    let capability = graphrun::worker_contract::capability_for(
        &catalog,
        &ActivityKey::new("sdk.effect", 1),
        ExecutionRole::Forward,
    )
    .unwrap()
    .to_wire();
    let register_id = CommandId::generate();
    let renew_id = CommandId::generate();
    let claim_id = CommandId::generate();
    let mut registered = Vec::new();
    for actor in ["worker-a", "worker-b"] {
        let tls = signed(&ca, actor, [PeerRole::Worker], &server_tls.server_name);
        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .tls_config(graphrun::rpc::client_tls(&tls).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut worker = graphrun::generated::worker_client::WorkerClient::new(channel);
        let session = WorkerSessionId::generate();
        let registration = RegisterRequest {
            session_id: session.to_hex(),
            capacity: 1,
            capabilities: vec![capability.clone()],
            principal_id: actor.to_owned(),
            protocol_min: 1,
            protocol_max: 1,
            command_id: register_id.to_hex(),
        };
        let first = worker
            .register(registration.clone())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            worker
                .register(registration.clone())
                .await
                .unwrap()
                .into_inner(),
            first
        );
        let mut changed = registration;
        changed.capacity = 2;
        assert_eq!(
            worker.register(changed).await.unwrap_err().code(),
            tonic::Code::AlreadyExists
        );
        let renewal = RenewSessionRequest {
            command_id: renew_id.to_hex(),
            session_id: session.to_hex(),
            revision: 1,
        };
        let renewed = worker
            .renew_session(renewal.clone())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(renewed.revision, 2);
        assert_eq!(
            worker.renew_session(renewal).await.unwrap().into_inner(),
            renewed
        );
        registered.push((session, worker));
    }
    let wrong_session = ClaimRequest {
        command_id: claim_id.to_hex(),
        session_id: registered[0].0.to_hex(),
        capacity: 1,
    };
    assert_eq!(
        registered[1]
            .1
            .claim(wrong_session)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut assigned = Vec::new();
    for (session, worker) in &mut registered {
        let claim = ClaimRequest {
            command_id: claim_id.to_hex(),
            session_id: session.to_hex(),
            capacity: 1,
        };
        let first = worker.claim(claim.clone()).await.unwrap().into_inner();
        assert_eq!(first.assignments.len(), 1);
        assert_eq!(
            worker.claim(claim.clone()).await.unwrap().into_inner(),
            first
        );
        let mut changed = claim;
        changed.capacity = 2;
        assert_eq!(
            worker.claim(changed).await.unwrap_err().code(),
            tonic::Code::AlreadyExists
        );
        assigned.push(first.assignments[0].clone());
    }
    assert_ne!(assigned[0].run_id, assigned[1].run_id);
    assert_ne!(assigned[0].activation_id, assigned[1].activation_id);
    assert_ne!(assigned[0].effect_key, assigned[1].effect_key);
    for run in &runs {
        let page = engine.history_page(*run, 0, 100).await.unwrap();
        assert_eq!(
            page.events
                .iter()
                .filter(|entry| matches!(entry.event, DomainEvent::ClaimGranted { .. }))
                .count(),
            1
        );
    }
    engine.snapshot().await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = Engine::member(open(false)).await.unwrap();
    for (index, ((session, _), assignment)) in registered.iter().zip(&assigned).enumerate() {
        let tls = signed(
            &ca,
            if index == 0 { "worker-a" } else { "worker-b" },
            [PeerRole::Worker],
            &server_tls.server_name,
        );
        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .tls_config(graphrun::rpc::client_tls(&tls).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut worker = graphrun::generated::worker_client::WorkerClient::new(channel);
        let repeat = worker
            .claim(ClaimRequest {
                command_id: claim_id.to_hex(),
                session_id: session.to_hex(),
                capacity: 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(repeat.assignments.len(), 1);
        assert_eq!(repeat.assignments[0].effect_key, assignment.effect_key);
        assert_eq!(
            repeat.assignments[0].activation_id,
            assignment.activation_id
        );
    }
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_reconciliation_cause_survives_restart() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let server_tls = issue_node(&ca, 1).unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let endpoint = format!("https://{addr}");
    let open = |initialize| MemberConfig {
        data_dir: directory.0.path().join("member"),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: server_tls.clone(),
        host_activities: false,
        initialize,
    };
    let engine = Engine::member(open(true)).await.unwrap();
    let tls = signed(
        &ca,
        "worker-one",
        [PeerRole::Worker],
        &server_tls.server_name,
    );
    let released = Arc::new(AtomicBool::new(false));
    let wait = released.clone();
    let worker = Worker::builder(endpoint.clone(), tls.clone(), reconciliation_catalog())
        .blocking("sdk.effect", 1, move |input: i64, _| {
            while !wait.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok::<_, ActivityError>(input + 1)
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let worker_task = tokio::spawn(worker.run());
    let client_tls = signed(&ca, "alice", [PeerRole::Client], &server_tls.server_name);
    let mut client = GrpcClient::connect(&endpoint, &client_tls).await.unwrap();
    let run = client
        .start(WORKER_YAML, &reconciliation_catalog(), &Value::Int(41))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let page = engine.history_page(run, 0, 100).await.unwrap();
            if page
                .events
                .iter()
                .any(|entry| matches!(entry.event, DomainEvent::ClaimGranted { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    worker_task.abort();
    let _ = worker_task.await;
    let recon = Worker::builder(endpoint, tls, reconciliation_catalog())
        .reconciler("sdk.lookup", 1, |_: i64, _| async {
            Ok::<_, ActivityError>(graphrun::Observed::<i64>::Unknown)
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let recon_task = tokio::spawn(recon.run_until(async move {
        let _ = stopped.await;
    }));
    let recorded = tokio::time::timeout(Duration::from_secs(75), async {
        loop {
            let page = engine.history_page(run, 0, 100).await.unwrap();
            if let Some(entry) = page
                .events
                .iter()
                .find(|entry| matches!(entry.event, DomainEvent::ReconciliationRecorded { .. }))
            {
                break (entry.record.command_id.unwrap(), entry.record.sequence);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    recon_task.await.unwrap().unwrap();
    released.store(true, Ordering::SeqCst);
    engine.snapshot().await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = Engine::member(open(false)).await.unwrap();
    let page = engine.history_page(run, 0, 100).await.unwrap();
    let entry = &page.events[(recorded.1 - 1) as usize];
    assert_eq!(entry.record.command_id, Some(recorded.0));
    assert_eq!(entry.record.principal_id.as_deref(), Some("worker-one"));
    assert!(matches!(
        entry.event,
        DomainEvent::ReconciliationRecorded { .. }
    ));
    let projection = engine.reconstruct_at(run, recorded.1).await.unwrap();
    assert_eq!(
        projection.history_records[&run][(recorded.1 - 1) as usize].principal_id,
        entry.record.principal_id
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn owner_socket_commands_have_owner_cause_but_scheduler_progress_does_not() {
    let directory = TestDirectory::new();
    let engine = Engine::local(directory.0.path().join("local"))
        .await
        .unwrap();
    let catalog = Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    let input = serde_json::from_value(serde_json::json!({"key":"order-1"})).unwrap();
    let approval = serde_json::from_value(serde_json::json!({"approved":true})).unwrap();
    let run = engine
        .start_yaml(SIGNAL_YAML, &catalog, input)
        .await
        .unwrap();
    engine.progress(run).await.unwrap();
    engine
        .signal(run, EventId::generate(), "approval", "order-1", approval)
        .await
        .unwrap();
    let page = engine.history_page(run, 0, 100).await.unwrap();
    assert_eq!(
        page.events[0].record.principal_id.as_deref(),
        Some("local-owner")
    );
    assert!(page.events.iter().any(|entry| {
        matches!(entry.event, DomainEvent::WaitOpened { .. })
            && entry.record.principal_id.is_none()
            && entry.record.command_id.is_some()
    }));
    assert!(page.events.iter().any(|entry| {
        matches!(entry.event, DomainEvent::EventAccepted { .. })
            && entry.record.principal_id.as_deref() == Some("local-owner")
    }));
    engine.shutdown().await.unwrap();
}
