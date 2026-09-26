use graphrun::domain::RunStatus;
use graphrun::ids::CommandId;
use graphrun::publication::Disposition;
use graphrun::tls::{
    ClusterId as SignedClusterId, PeerRole, PrincipalId, PrincipalIdentity, issue_principal,
};
use graphrun::{Catalog, Engine, ErrorKind, Value, compile_yaml};
use graphrun::{MemberConfig, generate_ca, issue_node};
use sha2::Digest;
use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::Duration;

fn catalog() -> Catalog {
    Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap()
}

fn workflow(version: u32) -> String {
    include_str!("../../docs/specs/v1/examples/remote.yaml").replacen(
        "version: 1",
        &format!("version: {version}"),
        1,
    )
}

fn input(value: i64) -> Value {
    serde_json::from_value(serde_json::json!({"value":value})).unwrap()
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!("publication-start-{}", CommandId::generate()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn concurrent_immutable_publish_and_pinned_latest_survive_restart() {
    let directory = TestDirectory::new();
    let engine = Engine::local(&directory.0).await.unwrap();
    let c = catalog();
    let (a, b) = tokio::join!(
        engine.publish_catalog(1, c.clone()),
        engine.publish_catalog(1, c.clone())
    );
    assert_eq!(a.unwrap().operation, "publish_catalog");
    assert_eq!(b.unwrap().operation, "publish_catalog");
    let mut changed = c.clone();
    changed.schemas.insert(
        graphrun::schema::SchemaKey::parse("counter/v1").unwrap(),
        serde_json::json!({"type":"integer"}),
    );
    assert_eq!(
        engine.publish_catalog(1, changed).await.unwrap_err().kind,
        ErrorKind::AlreadyExists
    );
    let v1 = compile_yaml(&workflow(1), &c).unwrap();
    let v2 = compile_yaml(&workflow(2), &c).unwrap();
    assert_eq!(v2.version, 2);
    let (a, b) = tokio::join!(
        engine.publish_definition(v1.clone(), 1),
        engine.publish_definition(v1.clone(), 1)
    );
    assert!(a.is_ok() && b.is_ok());
    let altered =
        compile_yaml(&workflow(1).replace("remote.echo", "counter.increment"), &c).unwrap();
    let rejected_publish_id = CommandId::generate();
    assert_eq!(
        engine
            .publish_definition_with_command(altered.clone(), 1, rejected_publish_id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    let rejected_publish = engine.command_result(rejected_publish_id).await.unwrap();
    assert_eq!(rejected_publish.format, "graphrun.command-result/v1");
    assert_eq!(rejected_publish.operation, "publish_definition");
    assert!(rejected_publish.event_range.is_none());
    assert!(matches!(
        &rejected_publish.outcome,
        Disposition::Rejected { code, .. } if code == "already_exists"
    ));
    assert_eq!(
        engine
            .publish_definition_with_command(altered.clone(), 1, rejected_publish_id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        serde_json::to_value(engine.command_result(rejected_publish_id).await.unwrap()).unwrap(),
        serde_json::to_value(&rejected_publish).unwrap()
    );

    let first = engine
        .start_published("external_worker_echo", None, "Key", input(1))
        .await
        .unwrap();
    assert_eq!(
        engine.inspect(first).await.unwrap().runs[&first]
            .published
            .as_ref()
            .unwrap()
            .catalog_version,
        1
    );
    engine.publish_catalog(2, c.clone()).await.unwrap();
    engine.publish_definition(v2, 2).await.unwrap();
    let same = engine
        .start_published("external_worker_echo", None, "Key", input(1))
        .await
        .unwrap();
    assert_eq!(same, first);
    assert_eq!(
        engine
            .start_published("external_worker_echo", Some(2), "Key", input(1))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        engine
            .start_published("external_worker_echo", None, "Key", input(2))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    let second = engine
        .start_published("external_worker_echo", None, "key", input(1))
        .await
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(
        engine.inspect(second).await.unwrap().runs[&second]
            .definition
            .version,
        2
    );
    assert_eq!(
        engine.inspect(second).await.unwrap().runs[&second]
            .published
            .as_ref()
            .unwrap()
            .catalog_version,
        2
    );
    engine.snapshot().await.unwrap();
    engine.shutdown().await.unwrap();

    let engine = Engine::local(&directory.0).await.unwrap();
    assert_eq!(
        engine
            .start_published("external_worker_echo", None, "Key", input(1))
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        engine.inspect(first).await.unwrap().runs[&first]
            .definition
            .version,
        1
    );
    assert_eq!(
        engine
            .publish_definition_with_command(altered, 1, rejected_publish_id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        serde_json::to_value(engine.command_result(rejected_publish_id).await.unwrap()).unwrap(),
        serde_json::to_value(rejected_publish).unwrap()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_range_exactly_covers_immediate_nested_terminal_history() {
    let directory = TestDirectory::new();
    let engine = Engine::local(&directory.0).await.unwrap();
    engine.publish_catalog(1, catalog()).await.unwrap();
    let yaml = "\
dsl: graphrun/v1
id: immediate_nested
version: 1
input_schema: unit/v1
output_schema: unit/v1
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
    engine.publish_definition_yaml(yaml, 1).await.unwrap();
    let command = CommandId::generate();
    let run = engine
        .start_published_with_command("immediate_nested", Some(1), "nested", Value::Null, command)
        .await
        .unwrap();
    let state = engine.inspect(run).await.unwrap();
    assert!(matches!(
        state.runs[&run].status,
        RunStatus::Succeeded { .. }
    ));
    let history = &state.history[&run];
    assert!(
        history.len() > 3,
        "start must commit immediate nested progress"
    );
    assert!(
        history
            .iter()
            .any(|event| matches!(event, graphrun::domain::DomainEvent::RunSucceeded { .. }))
    );
    assert!(
        history
            .iter()
            .filter(|event| matches!(event, graphrun::domain::DomainEvent::ScopeOpened { .. }))
            .count()
            > 1
    );
    let receipt = engine.command_result(command).await.unwrap();
    let range = receipt.event_range.as_ref().unwrap();
    assert_eq!(range.run, run);
    assert_eq!(range.first, 1);
    assert_eq!(range.last, history.len() as u64);
    assert_eq!(range.last - range.first + 1, history.len() as u64);
    assert_eq!(
        engine
            .start_published_with_command(
                "immediate_nested",
                Some(1),
                "nested",
                Value::Null,
                command
            )
            .await
            .unwrap(),
        run
    );
    assert_eq!(
        serde_json::to_value(engine.command_result(command).await.unwrap()).unwrap(),
        serde_json::to_value(receipt).unwrap()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_result_dedup_rejection_and_event_range_are_durable() {
    let directory = TestDirectory::new();
    let engine = Engine::local(&directory.0).await.unwrap();
    engine.publish_catalog(1, catalog()).await.unwrap();
    engine
        .publish_definition_yaml(&workflow(1), 1)
        .await
        .unwrap();
    let id = CommandId::generate();
    let run = engine
        .start_published_with_command("external_worker_echo", None, "stable", input(1), id)
        .await
        .unwrap();
    let before = engine.inspect(run).await.unwrap();
    let result = engine.command_result(id).await.unwrap();
    assert_eq!(result.format, "graphrun.command-result/v1");
    assert_eq!(result.event_range.as_ref().unwrap().first, 1);
    assert!(result.event_range.as_ref().unwrap().last >= 3);
    assert!(result.event_range.as_ref().unwrap().last <= before.history[&run].len() as u64);
    assert!(matches!(result.outcome, Disposition::Applied { run: Some(r), .. } if r == run));
    assert_eq!(
        engine
            .start_published_with_command("external_worker_echo", None, "stable", input(1), id)
            .await
            .unwrap(),
        run
    );
    assert_eq!(
        engine
            .start_published_with_command("external_worker_echo", None, "stable", input(2), id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );
    let after = engine.inspect(run).await.unwrap();
    assert_eq!(before.runs.len(), after.runs.len());
    let admitted = |state: &graphrun::State| {
        state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter(|event| matches!(event, graphrun::domain::DomainEvent::RunAdmitted { .. }))
            .count()
    };
    assert_eq!(admitted(&before), 1);
    assert_eq!(admitted(&after), 1);

    let rejected_id = CommandId::generate();
    assert_eq!(
        engine
            .start_published_with_command(
                "external_worker_echo",
                Some(99),
                "absent",
                input(1),
                rejected_id
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    let rejected = engine.command_result(rejected_id).await.unwrap();
    assert_eq!(rejected.format, "graphrun.command-result/v1");
    assert_eq!(rejected.key.command_id, rejected_id);
    assert!(rejected.event_range.is_none());
    assert!(matches!(
        &rejected.outcome, Disposition::Rejected { code, details }
            if code == "not_found" && details == "workflow version not published"
    ));
    assert_eq!(
        engine
            .start_published_with_command(
                "external_worker_echo",
                Some(99),
                "absent",
                input(1),
                rejected_id
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        serde_json::to_value(engine.command_result(rejected_id).await.unwrap()).unwrap(),
        serde_json::to_value(&rejected).unwrap()
    );
    engine.snapshot().await.unwrap();
    engine.shutdown().await.unwrap();
    let engine = Engine::local(&directory.0).await.unwrap();
    assert_eq!(
        engine
            .start_published_with_command("external_worker_echo", None, "stable", input(1), id)
            .await
            .unwrap(),
        run
    );
    assert_eq!(
        engine
            .start_published_with_command(
                "external_worker_echo",
                Some(99),
                "absent",
                input(1),
                rejected_id
            )
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        serde_json::to_value(engine.command_result(rejected_id).await.unwrap()).unwrap(),
        serde_json::to_value(rejected).unwrap()
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_key_failure_and_timeout_do_not_report_pending_success() {
    let directory = TestDirectory::new();
    let engine = Engine::local(&directory.0).await.unwrap();
    engine.publish_catalog(1, catalog()).await.unwrap();
    let fail = "\
dsl: graphrun/v1
id: fail_test
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: fail
nodes:
  fail:
    kind: fail
    error: {code: publication.failed, message: expected failure}
";
    let wait = "\
dsl: graphrun/v1
id: wait_test
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  approval: {schema: unit/v1}
start: wait
nodes:
  wait:
    kind: wait_signal
    signal: approval
    key: {literal: gate}
    consume_from: buffered
    timeout: '1d'
    next: done
    on_timeout: done
  done:
    kind: complete
    output: {literal: null}
";
    engine.publish_definition_yaml(fail, 1).await.unwrap();
    engine.publish_definition_yaml(wait, 1).await.unwrap();
    let too_big = "é".repeat(65);
    assert_eq!(
        engine
            .start_published("fail_test", None, &too_big, Value::Null)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        engine
            .start_published("fail_test", None, "", Value::Null)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::InvalidArgument
    );
    let run = engine
        .start_published("fail_test", None, "one", Value::Null)
        .await
        .unwrap();
    let error = engine
        .wait_terminal(run, Duration::from_secs(3))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("publication.failed"));
    assert!(matches!(
        engine.inspect(run).await.unwrap().runs[&run].status,
        RunStatus::Failed { .. }
    ));
    let run = engine
        .start_published("wait_test", None, "two", Value::Null)
        .await
        .unwrap();
    assert_eq!(
        engine
            .wait_terminal(run, Duration::from_millis(0))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::DeadlineExceeded
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn pre_publication_member_directory_is_not_modified() {
    let directory = TestDirectory::new();
    let identity = br#"{"mode":"local","node_id":1,"cluster_name":"graphrun-local"}"#;
    std::fs::write(directory.0.join("identity.json"), identity).unwrap();
    std::fs::write(directory.0.join("member.redb"), b"pre-v1").unwrap();
    let result = Engine::local(&directory.0).await;
    assert!(matches!(
        result,
        Err(graphrun::Error {
            kind: ErrorKind::FailedPrecondition,
            ..
        })
    ));
    assert_eq!(
        std::fs::read(directory.0.join("identity.json")).unwrap(),
        identity
    );
    assert_eq!(
        std::fs::read(directory.0.join("member.redb")).unwrap(),
        b"pre-v1"
    );
}

#[test]
fn pre_publication_redb_is_rejected_without_writes() {
    let directory = TestDirectory::new();
    let path = directory.0.join("member.redb");
    drop(redb::Database::create(&path).unwrap());
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        graphrun::storage::StorageHandle::open(&path),
        Err(graphrun::Error {
            kind: ErrorKind::FailedPrecondition,
            ..
        })
    ));
    assert_eq!(std::fs::read(path).unwrap(), before);
}

fn unused_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_command_result_survives_leader_change() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let addrs = [unused_addr(), unused_addr(), unused_addr()];
    let tls = [
        issue_node(&ca, 1).unwrap(),
        issue_node(&ca, 2).unwrap(),
        issue_node(&ca, 3).unwrap(),
    ];
    let mut members = Vec::new();
    for node in 1..=3u64 {
        let peers: BTreeMap<_, _> = (1..=3u64)
            .filter(|other| *other != node)
            .map(|other| {
                (
                    other,
                    (
                        addrs[(other - 1) as usize],
                        tls[(other - 1) as usize].clone(),
                    ),
                )
            })
            .collect();
        members.push(Engine::member(MemberConfig {
            data_dir: directory.0.join(format!("node-{node}")),
            node_id: node,
            bind: addrs[(node - 1) as usize],
            peers,
            tls: tls[(node - 1) as usize].clone(),
            host_activities: false,
            initialize: node == 1,
        }));
    }
    let follower2 = members.remove(1).await.unwrap();
    let follower3 = members.remove(1).await.unwrap();
    let original = members.remove(0).await.unwrap();
    original.publish_catalog(1, catalog()).await.unwrap();
    original
        .publish_definition_yaml(&workflow(1), 1)
        .await
        .unwrap();
    let command = CommandId::generate();
    let run = original
        .start_published_with_command(
            "external_worker_echo",
            None,
            "before-leader-change",
            input(7),
            command,
        )
        .await
        .unwrap();
    let original_page = original.history_page(run, 0, 100).await.unwrap();
    let original_projection = original
        .reconstruct_at(run, original_page.retained_through)
        .await
        .unwrap();
    let committed_index = original.last_applied_index().await;
    let receipt_key = original
        .command_result(command)
        .await
        .unwrap()
        .key
        .storage_key();
    let catchup_deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    loop {
        let second = follower2.last_applied_index().await;
        let third = follower3.last_applied_index().await;
        let second_voters = follower2.voter_ids();
        let third_voters = follower3.voter_ids();
        if second >= committed_index
            && third >= committed_index
            && second_voters == [1, 2, 3]
            && third_voters == [1, 2, 3]
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < catchup_deadline,
            "followers did not apply committed index {committed_index} and voter roster before shutdown: node2 index {second} voters {second_voters:?}, node3 index {third} voters {third_voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for follower in [&follower2, &follower3] {
        assert!(
            follower
                .inspect(run)
                .await
                .unwrap()
                .command_results
                .contains_key(&receipt_key),
            "follower applied the log but not the start receipt"
        );
    }
    original.shutdown().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let leader = loop {
        if follower2.is_leader() {
            break &follower2;
        }
        if follower3.is_leader() {
            break &follower3;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no replacement leader after both followers applied index {committed_index}: node2 index {} voters {:?}, node3 index {} voters {:?}",
            follower2.raft_applied_index(),
            follower2.voter_ids(),
            follower3.raft_applied_index(),
            follower3.voter_ids(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        leader
            .start_published_with_command(
                "external_worker_echo",
                None,
                "before-leader-change",
                input(7),
                command
            )
            .await
            .unwrap(),
        run
    );
    assert_eq!(
        leader
            .command_result(command)
            .await
            .unwrap()
            .applied_run()
            .unwrap(),
        run
    );
    let state = leader.inspect(run).await.unwrap();
    let elected_page = leader.history_page(run, 0, 100).await.unwrap();
    assert_eq!(
        serde_json::to_value(&elected_page).unwrap(),
        serde_json::to_value(&original_page).unwrap()
    );
    let elected_projection = leader
        .reconstruct_at(run, elected_page.retained_through)
        .await
        .unwrap();
    assert_eq!(
        graphrun::history::projection_view(&elected_projection, run),
        graphrun::history::projection_view(&original_projection, run)
    );
    let nonleader = if follower2.is_leader() {
        &follower3
    } else {
        &follower2
    };
    assert_eq!(
        nonleader.history_page(run, 0, 100).await.unwrap_err().kind,
        ErrorKind::Unavailable
    );
    assert_eq!(state.runs.len(), 1);
    assert_eq!(
        state.history[&run]
            .iter()
            .filter(|event| matches!(event, graphrun::domain::DomainEvent::RunAdmitted { .. }))
            .count(),
        1
    );
    follower2.shutdown().await.unwrap();
    follower3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_rpc_duplicate_returns_original_run_and_changed_body_conflicts() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let tls = issue_node(&ca, 1).unwrap();
    let addr = unused_addr();
    let engine = Engine::member(MemberConfig {
        data_dir: directory.0.join("member"),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: tls.clone(),
        host_activities: false,
        initialize: true,
    })
    .await
    .unwrap();
    let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(graphrun::rpc::client_tls(&tls).unwrap())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = graphrun::generated::client_client::ClientClient::new(channel);
    let id = CommandId::generate();
    let mut request = graphrun::generated::StartRequest {
        command_id: id.to_hex(),
        yaml: workflow(1),
        catalog_json: serde_json::to_vec(&catalog()).unwrap(),
        input_json: serde_json::to_vec(&input(1)).unwrap(),
        workflow: String::new(),
        version: 0,
        start_key: String::new(),
    };
    let first = client
        .start(request.clone())
        .await
        .unwrap()
        .into_inner()
        .run_id;
    let repeated = client
        .start(request.clone())
        .await
        .unwrap()
        .into_inner()
        .run_id;
    assert_eq!(first, repeated);
    request.input_json = serde_json::to_vec(&input(2)).unwrap();
    assert_eq!(
        client.start(request).await.unwrap_err().code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        engine
            .inspect(graphrun::RunId::from_hex(&first).unwrap())
            .await
            .unwrap()
            .runs
            .len(),
        1
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_principal_publishes_and_starts_over_grpc_with_scoped_receipts() {
    let directory = TestDirectory::new();
    let ca = generate_ca().unwrap();
    let server_tls = issue_node(&ca, 1).unwrap();
    let addr = unused_addr();
    let engine = Engine::member(MemberConfig {
        data_dir: directory.0.join("member"),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: server_tls.clone(),
        host_activities: false,
        initialize: true,
    })
    .await
    .unwrap();
    let cluster_id =
        SignedClusterId::parse(hex::encode(&sha2::Sha256::digest(ca.pem.as_bytes())[..16]))
            .unwrap();
    let operator = PrincipalIdentity::new(
        cluster_id.clone(),
        PrincipalId::parse("operator").unwrap(),
        [PeerRole::Admin, PeerRole::Client],
    )
    .unwrap();
    let mut operator_tls = issue_principal(&ca, &operator, "operator.graphrun.local").unwrap();
    operator_tls.server_name = server_tls.server_name.clone();
    let mut client = graphrun::GrpcClient::connect(&format!("https://{addr}"), &operator_tls)
        .await
        .unwrap();
    let catalog = catalog();
    let publish_id = CommandId::generate();
    client
        .publish_catalog_with_command(1, &catalog, publish_id)
        .await
        .unwrap();
    client.publish_definition(&workflow(1), 1).await.unwrap();
    let id = CommandId::generate();
    let first = client
        .start_published_with_command("external_worker_echo", None, "same", &input(1), id)
        .await
        .unwrap();
    let remote_page = client.history_page(first, 0, 2).await.unwrap();
    let local_page = engine.history_page(first, 0, 2).await.unwrap();
    assert_eq!(
        serde_json::to_value(&remote_page).unwrap(),
        serde_json::to_value(&local_page).unwrap()
    );
    let remote_view = client
        .reconstruct_at(first, remote_page.retained_through)
        .await
        .unwrap();
    let local_projection = engine
        .reconstruct_at(first, remote_page.retained_through)
        .await
        .unwrap();
    assert_eq!(
        remote_view,
        graphrun::history::projection_view(&local_projection, first)
    );
    let socket_page = graphrun::connect_control(
        directory.0.join("member/control.sock"),
        graphrun::ControlRequest::History {
            run: first.to_hex(),
            after_sequence: 0,
            page_size: 2,
        },
    )
    .await
    .unwrap();
    assert!(socket_page.ok);
    assert_eq!(socket_page.body, serde_json::to_value(remote_page).unwrap());
    assert_eq!(
        client
            .history_page(graphrun::RunId::from_bytes([1; 16]), 0, 2)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        client.reconstruct_at(first, 0).await.unwrap_err().kind,
        ErrorKind::InvalidArgument
    );
    assert_eq!(
        client
            .start_published_with_command("external_worker_echo", None, "same", &input(1), id)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        client
            .command_result(id)
            .await
            .unwrap()
            .applied_run()
            .unwrap(),
        first
    );
    assert_eq!(
        client
            .start_published_with_command("external_worker_echo", None, "same", &input(2), id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );

    let restricted = PrincipalIdentity::new(
        cluster_id.clone(),
        PrincipalId::parse("operator").unwrap(),
        [PeerRole::Client],
    )
    .unwrap();
    let mut restricted_tls =
        issue_principal(&ca, &restricted, "restricted.graphrun.local").unwrap();
    restricted_tls.server_name = server_tls.server_name.clone();
    let mut restricted_client =
        graphrun::GrpcClient::connect(&format!("https://{addr}"), &restricted_tls)
            .await
            .unwrap();
    assert_eq!(
        restricted_client
            .publish_catalog_with_command(1, &catalog, publish_id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PermissionDenied
    );

    let admin_only = PrincipalIdentity::new(
        cluster_id.clone(),
        PrincipalId::parse("operator").unwrap(),
        [PeerRole::Admin],
    )
    .unwrap();
    let mut admin_tls = issue_principal(&ca, &admin_only, "admin.graphrun.local").unwrap();
    admin_tls.server_name = server_tls.server_name.clone();
    let mut admin_client = graphrun::GrpcClient::connect(&format!("https://{addr}"), &admin_tls)
        .await
        .unwrap();
    assert_eq!(
        admin_client
            .start_published_with_command("external_worker_echo", None, "same", &input(1), id)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PermissionDenied
    );

    let other = PrincipalIdentity::new(
        cluster_id,
        PrincipalId::parse("other").unwrap(),
        [PeerRole::Client],
    )
    .unwrap();
    let mut other_tls = issue_principal(&ca, &other, "other.graphrun.local").unwrap();
    other_tls.server_name = server_tls.server_name.clone();
    let mut other_client = graphrun::GrpcClient::connect(&format!("https://{addr}"), &other_tls)
        .await
        .unwrap();
    assert_eq!(
        other_client
            .publish_catalog(2, &catalog)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        other_client.command_result(id).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    let wrong = PrincipalIdentity::new(
        SignedClusterId::parse("other-cluster").unwrap(),
        PrincipalId::parse("operator").unwrap(),
        [PeerRole::Client],
    )
    .unwrap();
    let mut wrong_tls = issue_principal(&ca, &wrong, "cross-cluster.graphrun.local").unwrap();
    wrong_tls.server_name = server_tls.server_name.clone();
    let mut wrong_client = graphrun::GrpcClient::connect(&format!("https://{addr}"), &wrong_tls)
        .await
        .unwrap();
    assert_eq!(
        wrong_client.command_result(id).await.unwrap_err().kind,
        ErrorKind::Unauthenticated
    );
    engine.shutdown().await.unwrap();
}
