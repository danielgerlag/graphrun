use graphrun::catalog::Catalog;
use graphrun::cluster::MemberConfig;
use graphrun::provider::{self, EffectStatus};
use graphrun::tls::{ClusterId, PeerRole, PrincipalId, PrincipalIdentity, issue_principal};
use graphrun::{ActivityError, Engine, Observed, TlsMaterial, Value, Worker, generate_ca};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(serde::Deserialize, serde::Serialize)]
struct Order {
    order_id: String,
    amount: i64,
    #[serde(default)]
    fail_after_payment: Option<bool>,
}
graphrun::payload!(Order, "order");

#[derive(serde::Deserialize, serde::Serialize)]
struct ReservedOrder {
    order_id: String,
    amount: i64,
    reservation_id: String,
}
graphrun::payload!(ReservedOrder, "reserved_order");

#[derive(serde::Deserialize, serde::Serialize)]
struct Receipt {
    order_id: String,
    amount: i64,
    payment_id: String,
}
graphrun::payload!(Receipt, "receipt");

const YAML: &str = r#"
dsl: graphrun/v1
id: independent_worker
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
"#;

fn catalog(output_schema: &str) -> Catalog {
    Catalog::from_json(
        serde_json::json!({
            "format": "graphrun.catalog/v1",
            "schemas": {},
            "activities": [{
                "name": "sdk.effect", "version": 1,
                "input_schema": "integer/v1", "output_schema": output_schema,
                "execution": "blocking", "effects": "external", "recovery": "RetrySafe",
                "error_codes": [{"code": "provider.unavailable", "retryable": true}]
            }]
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap()
}

fn manual_catalog() -> Catalog {
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

fn signed(ca: &graphrun::CertificateAuthority, principal: &str, roles: &[PeerRole]) -> TlsMaterial {
    signed_for_host(ca, principal, roles, "node-1.graphrun.local")
}

fn signed_for_host(
    ca: &graphrun::CertificateAuthority,
    principal: &str,
    roles: &[PeerRole],
    server_name: &str,
) -> TlsMaterial {
    let cluster = ClusterId::parse(hex::encode(&Sha256::digest(ca.pem.as_bytes())[..16])).unwrap();
    let identity = PrincipalIdentity::new(
        cluster,
        PrincipalId::parse(principal).unwrap(),
        roles.iter().copied(),
    )
    .unwrap();
    issue_principal(ca, &identity, server_name).unwrap()
}

fn unused_addr() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn member() -> (Engine, tempfile::TempDir, TlsMaterial, String) {
    let ca = generate_ca().unwrap();
    let addr = unused_addr();
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::member(MemberConfig {
        data_dir: dir.path().to_path_buf(),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: signed(
            &ca,
            "node-1",
            &[PeerRole::Member, PeerRole::Client, PeerRole::Admin],
        ),
        host_activities: false,
        initialize: true,
    })
    .await
    .unwrap();
    (
        engine,
        dir,
        signed(&ca, "app-worker", &[PeerRole::Worker]),
        format!("https://{addr}"),
    )
}

async fn provider() -> (
    tokio::task::JoinHandle<graphrun::Result<()>>,
    tempfile::TempDir,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(provider::serve(listener, dir.path().to_path_buf()));
    (task, dir, url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_worker_matches_pinned_contract_and_calls_provider() {
    let ca = generate_ca().unwrap();
    let addr = unused_addr();
    let member_dir = tempfile::tempdir().unwrap();
    let member = Engine::member(MemberConfig {
        data_dir: member_dir.path().to_path_buf(),
        node_id: 1,
        bind: addr,
        peers: BTreeMap::new(),
        tls: signed(
            &ca,
            "node-1",
            &[PeerRole::Member, PeerRole::Client, PeerRole::Admin],
        ),
        host_activities: false,
        initialize: true,
    })
    .await
    .unwrap();
    let provider_dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let probe_url = url.clone();
    let provider_task = tokio::spawn(provider::serve(listener, provider_dir.path().to_path_buf()));
    let tls = signed(&ca, "app-worker", &[PeerRole::Worker]);
    let endpoint = format!("https://{addr}");
    let incompatible = Worker::builder(endpoint.clone(), tls.clone(), catalog("string/v1"))
        .blocking("sdk.effect", 1, |input: i64, _| {
            Ok::<_, ActivityError>(input.to_string())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (bad_stop, bad_rx) = oneshot::channel::<()>();
    let bad = tokio::spawn(incompatible.run_until(async move {
        let _ = bad_rx.await;
    }));
    let run = member
        .start_yaml(YAML, &catalog("integer/v1"), Value::Int(41))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let state = member.inspect(run).await.unwrap();
    assert!(matches!(
        state.runs.get(&run).unwrap().status,
        graphrun::domain::RunStatus::Active
    ));
    let activation = state
        .activations
        .values()
        .find(|act| act.run == run)
        .unwrap();
    assert_eq!(activation.status, graphrun::domain::ActivationStatus::Ready);
    assert!(
        activation.claim.is_none(),
        "incompatible worker received a claim"
    );
    let effect = Arc::new(Mutex::new(None::<String>));
    let recorded = effect.clone();
    let worker = Worker::builder(endpoint, tls, catalog("integer/v1"))
        .blocking("sdk.effect", 1, move |input: i64, context| {
            assert!(context.can_start_effect());
            *recorded.lock().unwrap() = Some(context.effect_key.clone());
            let response = provider::apply_effect(
                &url,
                &context.effect_key,
                "forward",
                &Value::Int(input + 1),
            )
            .map_err(|error| ActivityError::new("provider.unavailable", error.to_string()))?;
            assert_eq!(response.status, EffectStatus::Applied);
            Ok(response.output.unwrap().as_i64().unwrap())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = rx.await;
    }));
    assert_eq!(
        member
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap(),
        Value::Int(42)
    );
    let key = effect.lock().unwrap().clone().expect("external effect key");
    let ledger = provider::dump_ledger(&probe_url).unwrap();
    assert_eq!(ledger["entries"][&key]["physical"], 1);
    assert_eq!(ledger["entries"][&key]["logical"], 1);
    stop.send(()).unwrap();
    bad_stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    bad.await.unwrap().unwrap();
    provider_task.abort();
    member.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_error_retries_with_same_effect_identity() {
    let (engine, _dir, tls, endpoint) = member().await;
    let (provider_task, _provider_dir, url) = provider().await;
    let query_url = url.clone();
    let keys = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = keys.clone();
    let worker = Worker::builder(endpoint, tls, catalog("integer/v1"))
        .capacity(1)
        .unwrap()
        .blocking("sdk.effect", 1, move |input: i64, ctx| {
            observed.lock().unwrap().push(ctx.effect_key.clone());
            let response =
                provider::apply_effect(&url, &ctx.effect_key, "forward", &Value::Int(input + 1))
                    .map_err(|err| ActivityError::new("provider.unavailable", err.to_string()))?;
            if ctx.attempt == 1 {
                return Err(ActivityError::new(
                    "provider.unavailable",
                    "response lost after effect",
                ));
            }
            Ok(response.output.unwrap().as_i64().unwrap())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = rx.await;
    }));
    let retry_yaml = YAML.replace(
        "    next: done",
        "    retry:\n      errors: [provider.unavailable]\n      max_attempts: 2\n    next: done",
    );
    let run = engine
        .start_yaml(&retry_yaml, &catalog("integer/v1"), Value::Int(8))
        .await
        .unwrap();
    assert_eq!(
        engine
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap(),
        Value::Int(9)
    );
    let keys = keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    let ledger = provider::dump_ledger(&query_url).unwrap();
    assert_eq!(ledger["entries"][&keys[0]]["physical"], 2);
    assert_eq!(ledger["entries"][&keys[0]]["logical"], 1);
    assert!(
        engine
            .history(run)
            .await
            .unwrap()
            .iter()
            .any(|event| matches!(
                event, graphrun::domain::DomainEvent::LeafFailed { code, retry: true, .. }
                    if code == "provider.unavailable"
            ))
    );
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_compensation_uses_distinct_external_effect_key() {
    let (engine, _dir, tls, endpoint) = member().await;
    let (provider_task, _provider_dir, url) = provider().await;
    let catalog = Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    let forward_key = Arc::new(Mutex::new(None::<String>));
    let compensation_key = Arc::new(Mutex::new(None::<String>));
    let forward_record = forward_key.clone();
    let compensation_record = compensation_key.clone();
    let reserve_url = url.clone();
    let release_url = url.clone();
    let worker = Worker::builder(endpoint, tls, catalog.clone())
        .activity("inventory.reserve", 1, move |order: Order, ctx| {
            let url = reserve_url.clone();
            let forward = forward_record.clone();
            async move {
                assert!(ctx.can_start_effect());
                *forward.lock().unwrap() = Some(ctx.effect_key.clone());
                let output = ReservedOrder {
                    reservation_id: format!("res-{}", order.order_id),
                    order_id: order.order_id,
                    amount: order.amount,
                };
                let payload = Value::from_json(serde_json::to_value(&output).unwrap()).unwrap();
                let response = tokio::task::spawn_blocking(move || {
                    provider::apply_effect(&url, &ctx.effect_key, "forward", &payload)
                })
                .await
                .map_err(|err| ActivityError::new("inventory.unavailable", err.to_string()))?
                .map_err(|err| ActivityError::new("inventory.unavailable", err.to_string()))?;
                Ok::<ReservedOrder, ActivityError>(
                    serde_json::from_value(response.output.unwrap().to_json()).unwrap(),
                )
            }
        })
        .unwrap()
        .activity(
            "payment.charge",
            1,
            |_input: ReservedOrder, _ctx| async move {
                Err::<Receipt, ActivityError>(ActivityError::new(
                    "payment.declined",
                    "charge refused",
                ))
            },
        )
        .unwrap()
        .compensation("inventory.release", 1, move |_input: ReservedOrder, ctx| {
            let url = release_url.clone();
            let compensation = compensation_record.clone();
            async move {
                assert_eq!(ctx.role, graphrun::ids::ExecutionRole::Compensation);
                assert!(ctx.can_start_effect());
                *compensation.lock().unwrap() = Some(ctx.effect_key.clone());
                tokio::task::spawn_blocking(move || {
                    provider::apply_effect(&url, &ctx.effect_key, "compensation", &Value::Null)
                })
                .await
                .map_err(|err| {
                    ActivityError::new("inventory.release_unavailable", err.to_string())
                })?
                .map_err(|err| {
                    ActivityError::new("inventory.release_unavailable", err.to_string())
                })?;
                Ok::<(), ActivityError>(())
            }
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = rx.await;
    }));
    let input = Value::Object(
        [
            ("order_id".to_owned(), Value::String("o-1".to_owned())),
            ("amount".to_owned(), Value::Int(1000)),
        ]
        .into_iter()
        .collect(),
    );
    let run = engine
        .start_yaml(
            include_str!("../../samples/08-saga/workflow.yaml"),
            &catalog,
            input,
        )
        .await
        .unwrap();
    let state = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let state = engine.inspect(run).await.unwrap();
            if matches!(
                state.runs.get(&run).unwrap().status,
                graphrun::domain::RunStatus::Failed { .. }
            ) {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(state.obligations.iter().any(|obligation| {
        obligation.run == run
            && obligation.status == graphrun::domain::ObligationStatus::Compensated
    }));
    let forward = forward_key.lock().unwrap().clone().unwrap();
    let compensation = compensation_key.lock().unwrap().clone().unwrap();
    assert_ne!(forward, compensation);
    let ledger = provider::dump_ledger(&url).unwrap();
    assert_eq!(ledger["entries"][&forward]["logical"], 1);
    assert_eq!(ledger["entries"][&compensation]["logical"], 1);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_handler_renews_past_initial_lease() {
    let (engine, _dir, tls, endpoint) = member().await;
    let (provider_task, _provider_dir, url) = provider().await;
    let worker = Worker::builder(endpoint, tls, catalog("integer/v1"))
        .capacity(1)
        .unwrap()
        .blocking("sdk.effect", 1, move |input: i64, ctx| {
            std::thread::sleep(Duration::from_secs(32));
            assert!(
                ctx.can_start_effect(),
                "session and claim must still be live"
            );
            let response =
                provider::apply_effect(&url, &ctx.effect_key, "forward", &Value::Int(input + 1))
                    .map_err(|err| ActivityError::new("provider.unavailable", err.to_string()))?;
            Ok(response.output.unwrap().as_i64().unwrap())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = rx.await;
    }));
    let run = engine
        .start_yaml(YAML, &catalog("integer/v1"), Value::Int(7))
        .await
        .unwrap();
    let outcome = engine.wait_terminal(run, Duration::from_secs(45)).await;
    if let Err(err) = &outcome {
        let state = engine.inspect(run).await.unwrap();
        let worker = if task.is_finished() {
            format!("{:?}", task.await)
        } else {
            "still running".to_owned()
        };
        panic!(
            "long activity: {err}; worker={worker}; sessions={:?}; activations={:?}",
            state.sessions, state.activations
        );
    }
    assert_eq!(outcome.unwrap(), Value::Int(8));
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_result_retries_same_external_effect_key() {
    let (engine, _dir, tls, endpoint) = member().await;
    let (provider_task, _provider_dir, url) = provider().await;
    let first_seen = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let first_key = Arc::new(Mutex::new(None::<String>));
    let seen = first_seen.clone();
    let pending = release.clone();
    let key = first_key.clone();
    let first_url = url.clone();
    let first = Worker::builder(endpoint.clone(), tls.clone(), catalog("integer/v1"))
        .capacity(1)
        .unwrap()
        .blocking("sdk.effect", 1, move |input: i64, ctx| {
            let response = provider::apply_effect(
                &first_url,
                &ctx.effect_key,
                "forward",
                &Value::Int(input + 1),
            )
            .map_err(|err| ActivityError::new("provider.unavailable", err.to_string()))?;
            *key.lock().unwrap() = Some(ctx.effect_key);
            seen.store(true, Ordering::SeqCst);
            while !pending.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(response.output.unwrap().as_i64().unwrap())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let first_task = tokio::spawn(first.run());
    let run = engine
        .start_yaml(YAML, &catalog("integer/v1"), Value::Int(14))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !first_seen.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    first_task.abort();
    let _ = first_task.await;
    release.store(true, Ordering::SeqCst);
    let second_url = url.clone();
    let second = Worker::builder(endpoint, tls, catalog("integer/v1"))
        .capacity(1)
        .unwrap()
        .blocking("sdk.effect", 1, move |input: i64, ctx| {
            let response = provider::apply_effect(
                &second_url,
                &ctx.effect_key,
                "forward",
                &Value::Int(input + 1),
            )
            .map_err(|err| ActivityError::new("provider.unavailable", err.to_string()))?;
            Ok(response.output.unwrap().as_i64().unwrap())
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(second.run_until(async move {
        let _ = rx.await;
    }));
    let outcome = engine.wait_terminal(run, Duration::from_secs(50)).await;
    if let Err(err) = &outcome {
        let state = engine.inspect(run).await.unwrap();
        let worker = if task.is_finished() {
            format!("{:?}", task.await)
        } else {
            "still running".to_owned()
        };
        panic!(
            "lost result: {err}; worker={worker}; sessions={:?}; activations={:?}",
            state.sessions, state.activations
        );
    }
    assert_eq!(outcome.unwrap(), Value::Int(15));
    let key = first_key.lock().unwrap().clone().unwrap();
    let ledger = provider::dump_ledger(&url).unwrap();
    assert_eq!(ledger["entries"][&key]["physical"], 2);
    assert_eq!(ledger["entries"][&key]["logical"], 1);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_reconciler_records_all_three_outcomes() {
    let (engine, _dir, tls, endpoint) = member().await;
    let (provider_task, _provider_dir, url) = provider().await;
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let applied = Arc::new(Mutex::new(None::<String>));
    let applied_key = applied.clone();
    let seen_handler = seen.clone();
    let release_handler = release.clone();
    let first_url = url.clone();
    let first = Worker::builder(endpoint.clone(), tls.clone(), manual_catalog())
        .capacity(4)
        .unwrap()
        .blocking("sdk.effect", 1, move |input: i64, ctx| {
            if input == 1 {
                let response = provider::apply_effect(
                    &first_url,
                    &ctx.effect_key,
                    "forward",
                    &Value::Int(input + 1),
                )
                .map_err(|err| ActivityError::new("worker.provider", err.to_string()))?;
                assert_eq!(response.status, EffectStatus::Applied);
                *applied_key.lock().unwrap() = Some(ctx.effect_key);
            } else if input == 3 || input == 4 {
                provider::hold_effect(&first_url, &ctx.effect_key)
                    .map_err(|err| ActivityError::new("worker.provider", err.to_string()))?;
            }
            seen_handler.fetch_add(1, Ordering::SeqCst);
            while !release_handler.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(input + 1)
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let first_task = tokio::spawn(first.run());
    let mut runs = Vec::new();
    for input in 1..=4 {
        runs.push(
            engine
                .start_yaml(YAML, &manual_catalog(), Value::Int(input))
                .await
                .unwrap(),
        );
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while seen.load(Ordering::SeqCst) != 4 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    first_task.abort();
    let _ = first_task.await;
    release.store(true, Ordering::SeqCst);
    let probe_url = url.clone();
    let recon = Worker::builder(endpoint, tls, manual_catalog())
        .capacity(4)
        .unwrap()
        .reconciler("sdk.lookup", 1, move |input: i64, ctx| {
            let url = probe_url.clone();
            async move {
                if input == 3 {
                    return Err(ActivityError::new(
                        "worker.probe_unavailable",
                        "provider query failed",
                    ));
                }
                let response = tokio::task::spawn_blocking(move || {
                    provider::probe_effect(&url, &ctx.effect_key)
                })
                .await
                .map_err(|err| ActivityError::new("worker.probe", err.to_string()))?
                .map_err(|err| ActivityError::new("worker.probe", err.to_string()))?;
                Ok::<Observed<i64>, ActivityError>(match response.status {
                    EffectStatus::Applied => {
                        Observed::Applied(response.output.unwrap().as_i64().unwrap())
                    }
                    EffectStatus::NotApplied => Observed::NotApplied,
                    EffectStatus::Unknown | EffectStatus::Pending => Observed::Unknown,
                })
            }
        })
        .unwrap()
        .open()
        .await
        .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(recon.run_until(async move {
        let _ = rx.await;
    }));
    assert_eq!(
        engine
            .wait_terminal(runs[0], Duration::from_secs(45))
            .await
            .unwrap(),
        Value::Int(2)
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let not_applied = engine.history(runs[1]).await.unwrap();
        let failed_probe = engine.history(runs[2]).await.unwrap();
        let unknown = engine.history(runs[3]).await.unwrap();
        if not_applied.iter().any(|event| {
            matches!(
                event,
                graphrun::domain::DomainEvent::ReconciliationRecorded {
                    outcome: graphrun::domain::ReconcileOutcome::NotApplied,
                    ..
                }
            )
        }) && failed_probe.iter().any(|event| {
            matches!(
                event,
                graphrun::domain::DomainEvent::ReconciliationFailed { code, .. }
                    if code == "worker.probe_unavailable"
            )
        }) && failed_probe.iter().any(|event| {
            matches!(
                event,
                graphrun::domain::DomainEvent::ReconciliationRecorded {
                    outcome: graphrun::domain::ReconcileOutcome::Unknown,
                    ..
                }
            )
        }) && unknown.iter().any(|event| {
            matches!(
                event,
                graphrun::domain::DomainEvent::ReconciliationRecorded {
                    outcome: graphrun::domain::ReconcileOutcome::Unknown,
                    ..
                }
            )
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing not-applied, unknown, or failed probe"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let applied_key = applied.lock().unwrap().clone().unwrap();
    let ledger = provider::dump_ledger(&url).unwrap();
    assert_eq!(ledger["entries"][&applied_key]["logical"], 1);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    engine.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn worker_keeps_assignment_across_leader_change() {
    let ca = generate_ca().unwrap();
    let addresses = [unused_addr(), unused_addr(), unused_addr()];
    let members_tls = [1, 2, 3].map(|id| {
        signed_for_host(
            &ca,
            &format!("node-{id}"),
            &[PeerRole::Member, PeerRole::Client, PeerRole::Admin],
            &format!("node-{id}.graphrun.local"),
        )
    });
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let open = |index: usize, initialize: bool| {
        let peers = (0..3)
            .filter(|other| *other != index)
            .map(|other| {
                (
                    (other + 1) as u64,
                    (addresses[other], members_tls[other].clone()),
                )
            })
            .collect();
        MemberConfig {
            data_dir: dirs[index].path().to_path_buf(),
            node_id: (index + 1) as u64,
            bind: addresses[index],
            peers,
            tls: members_tls[index].clone(),
            host_activities: false,
            initialize,
        }
    };
    let second = Engine::member(open(1, false)).await.unwrap();
    let third = Engine::member(open(2, false)).await.unwrap();
    let first = Engine::member(open(0, true)).await.unwrap();
    let (provider_task, _provider_dir, url) = provider().await;
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handler_started = started.clone();
    let handler_release = release.clone();
    let worker = Worker::builder(
        format!("https://{}", addresses[0]),
        signed(&ca, "app-worker", &[PeerRole::Worker]),
        catalog("integer/v1"),
    )
    .seed_with_server_name(format!("https://{}", addresses[1]), "node-2.graphrun.local")
    .seed_with_server_name(format!("https://{}", addresses[2]), "node-3.graphrun.local")
    .capacity(1)
    .unwrap()
    .blocking("sdk.effect", 1, move |input: i64, ctx| {
        handler_started.store(true, Ordering::SeqCst);
        while !handler_release.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            ctx.can_start_effect(),
            "worker must renew its claim with new leader"
        );
        let response =
            provider::apply_effect(&url, &ctx.effect_key, "forward", &Value::Int(input + 1))
                .map_err(|err| ActivityError::new("provider.unavailable", err.to_string()))?;
        Ok(response.output.unwrap().as_i64().unwrap())
    })
    .unwrap()
    .open()
    .await
    .unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(worker.run_until(async move {
        let _ = rx.await;
    }));
    let run = first
        .start_yaml(YAML, &catalog("integer/v1"), Value::Int(99))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !started.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    first.shutdown().await.unwrap();
    let survivor = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if second.is_leader() {
                break &second;
            }
            if third.is_leader() {
                break &third;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    release.store(true, Ordering::SeqCst);
    let outcome = survivor.wait_terminal(run, Duration::from_secs(35)).await;
    if let Err(err) = &outcome {
        let worker = if task.is_finished() {
            format!("{:?}", task.await)
        } else {
            "still running".to_owned()
        };
        let state = survivor.inspect(run).await.unwrap();
        panic!(
            "leader change: {err}; worker={worker}; sessions={:?}; activations={:?}",
            state.sessions, state.activations
        );
    }
    assert_eq!(outcome.unwrap(), Value::Int(100));
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    provider_task.abort();
    second.shutdown().await.unwrap();
    third.shutdown().await.unwrap();
}
