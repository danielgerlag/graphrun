use crate::generated::{Blob, raft_client::RaftClient};
use crate::rpc::client_tls;
use crate::storage::TypeConfig;
use crate::tls::TlsMaterial;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tonic::transport::Channel;

#[derive(Clone)]
pub struct MemberConfig {
    pub data_dir: PathBuf,
    pub node_id: u64,
    pub bind: SocketAddr,
    pub peers: BTreeMap<u64, (SocketAddr, TlsMaterial)>,
    pub tls: TlsMaterial,
    pub host_activities: bool,
    pub initialize: bool,
}

#[derive(Clone)]
pub struct ClusterNetwork {
    peers: Arc<Mutex<BTreeMap<u64, (SocketAddr, TlsMaterial)>>>,
}

impl ClusterNetwork {
    pub fn new(peers: BTreeMap<u64, (SocketAddr, TlsMaterial)>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(peers)),
        }
    }

    pub fn insert_peer(&self, id: u64, addr: SocketAddr, tls: TlsMaterial) {
        self.peers.lock().unwrap().insert(id, (addr, tls));
    }
}

impl RaftNetworkFactory<TypeConfig> for ClusterNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        let peer = self.peers.lock().unwrap().get(&target).cloned();
        PeerClient { target, peer }
    }
}

pub struct PeerClient {
    target: u64,
    peer: Option<(SocketAddr, TlsMaterial)>,
}

impl PeerClient {
    async fn client(&self) -> std::io::Result<RaftClient<Channel>> {
        let Some((addr, tls)) = &self.peer else {
            return Err(io::Error::other(format!("unknown peer {}", self.target)));
        };
        let endpoint = format!("https://{addr}");
        let channel = Channel::from_shared(endpoint)
            .map_err(io::Error::other)?
            .tls_config(client_tls(tls).map_err(io::Error::other)?)
            .map_err(io::Error::other)?
            .connect()
            .await
            .map_err(io::Error::other)?;
        Ok(RaftClient::new(channel))
    }

    async fn call(&mut self, kind: &str, rpc: impl serde::Serialize) -> std::io::Result<Vec<u8>> {
        let mut client = self.client().await?;
        let blob = Blob {
            json: serde_json::to_vec(&rpc).map_err(io::Error::other)?,
        };
        let resp = match kind {
            "append" => client.append_entries(blob).await,
            "vote" => client.vote(blob).await,
            "snapshot" => client.install_snapshot(blob).await,
            _ => return Err(io::Error::other("unknown raft rpc")),
        }
        .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(resp.into_inner().json)
    }
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        match self.call("append", rpc).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&io::Error::other(err)))),
            Err(err) => Err(RPCError::Unreachable(Unreachable::new(&err))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        match self.call("snapshot", rpc).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&io::Error::other(err)))),
            Err(err) => Err(RPCError::Unreachable(Unreachable::new(&err))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.call("vote", rpc).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|err| RPCError::Unreachable(Unreachable::new(&io::Error::other(err)))),
            Err(err) => Err(RPCError::Unreachable(Unreachable::new(&err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::engine::Engine;
    use crate::tls::{generate_ca, issue_node};
    use crate::value::Value;
    use std::time::Duration;

    fn unused_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    async fn three_voters(
        host_activities: bool,
    ) -> (
        Engine,
        Engine,
        Engine,
        [tempfile::TempDir; 3],
        [SocketAddr; 3],
        [crate::tls::TlsMaterial; 3],
        crate::tls::CertificateAuthority,
    ) {
        let ca = generate_ca().unwrap();
        let addrs = [unused_addr(), unused_addr(), unused_addr()];
        let materials = [
            issue_node(&ca, 1).unwrap(),
            issue_node(&ca, 2).unwrap(),
            issue_node(&ca, 3).unwrap(),
        ];
        let dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let mut members = Vec::new();
        for node in 1u64..=3 {
            let mut peers = BTreeMap::new();
            for other in 1u64..=3 {
                if other == node {
                    continue;
                }
                peers.insert(
                    other,
                    (
                        addrs[(other - 1) as usize],
                        materials[(other - 1) as usize].clone(),
                    ),
                );
            }
            members.push(Engine::member(MemberConfig {
                data_dir: dirs[(node - 1) as usize].path().to_path_buf(),
                node_id: node,
                bind: addrs[(node - 1) as usize],
                peers,
                tls: materials[(node - 1) as usize].clone(),
                host_activities,
                initialize: node == 1,
            }));
        }
        let engine2 = members.remove(1).await.unwrap();
        let engine3 = members.remove(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let engine1 = members.remove(0).await.unwrap();
        (engine1, engine2, engine3, dirs, addrs, materials, ca)
    }

    async fn wait_leader<'a>(a: &'a Engine, b: &'a Engine) -> &'a Engine {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if a.is_leader() {
                return a;
            }
            if b.is_leader() {
                return b;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no survivor became leader");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_voters_run_sequence() {
        let ca = generate_ca().unwrap();
        let addrs = [unused_addr(), unused_addr(), unused_addr()];
        let materials = [
            issue_node(&ca, 1).unwrap(),
            issue_node(&ca, 2).unwrap(),
            issue_node(&ca, 3).unwrap(),
        ];
        let dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let mut members = Vec::new();
        for node in 1u64..=3 {
            let mut peers = BTreeMap::new();
            for other in 1u64..=3 {
                if other == node {
                    continue;
                }
                peers.insert(
                    other,
                    (
                        addrs[(other - 1) as usize],
                        materials[(other - 1) as usize].clone(),
                    ),
                );
            }
            members.push(Engine::member(MemberConfig {
                data_dir: dirs[(node - 1) as usize].path().to_path_buf(),
                node_id: node,
                bind: addrs[(node - 1) as usize],
                peers,
                tls: materials[other_tls(node)].clone(),
                host_activities: true,
                initialize: node == 1,
            }));
        }
        let engine2 = members.remove(1).await.unwrap();
        let engine3 = members.remove(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let engine1 = members.remove(0).await.unwrap();
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let yaml = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let input = Value::Object(
            [
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ]
            .into_iter()
            .collect(),
        );
        let run = engine1.start_yaml(yaml, &catalog, input).await.unwrap();
        let output = engine1
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        engine1.shutdown().await.unwrap();
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }

    fn other_tls(node: u64) -> usize {
        (node - 1) as usize
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn grpc_client_starts_sequence() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: true,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut client = crate::client::GrpcClient::connect(&format!("https://{addr}"), &tls)
            .await
            .unwrap();
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = client
            .start(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                &Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_worker_completes_sequence() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: false,
            initialize: true,
        })
        .await
        .unwrap();
        let endpoint = format!("https://{addr}");
        let worker = tokio::spawn(Engine::run_worker(endpoint, tls));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        worker.abort();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_loss_keeps_accepted_state() {
        let ca = generate_ca().unwrap();
        let addrs = [unused_addr(), unused_addr(), unused_addr()];
        let materials = [
            issue_node(&ca, 1).unwrap(),
            issue_node(&ca, 2).unwrap(),
            issue_node(&ca, 3).unwrap(),
        ];
        let dirs = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let mut members = Vec::new();
        for node in 1u64..=3 {
            let mut peers = BTreeMap::new();
            for other in 1u64..=3 {
                if other == node {
                    continue;
                }
                peers.insert(
                    other,
                    (
                        addrs[(other - 1) as usize],
                        materials[(other - 1) as usize].clone(),
                    ),
                );
            }
            members.push(Engine::member(MemberConfig {
                data_dir: dirs[(node - 1) as usize].path().to_path_buf(),
                node_id: node,
                bind: addrs[(node - 1) as usize],
                peers,
                tls: materials[other_tls(node)].clone(),
                host_activities: true,
                initialize: node == 1,
            }));
        }
        let engine2 = members.remove(1).await.unwrap();
        let engine3 = members.remove(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let engine1 = members.remove(0).await.unwrap();
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine1
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let expected = engine1
            .wait_terminal(run, Duration::from_secs(15))
            .await
            .unwrap();
        engine1.shutdown().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let leader = loop {
            if engine2.is_leader() {
                break &engine2;
            }
            if engine3.is_leader() {
                break &engine3;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no survivor became leader");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let catch_up = tokio::time::Instant::now() + Duration::from_secs(10);
        let output = loop {
            let state = leader.inspect(run).await.expect("survivor has the run");
            if let Some(out) = crate::domain::run_output(&state, run) {
                break out;
            }
            if tokio::time::Instant::now() >= catch_up {
                panic!("survivor completed the run");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(output, expected);
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_ca_is_rejected() {
        let ca = generate_ca().unwrap();
        let other = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: true,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let wrong = issue_node(&other, 1).unwrap();
        let err = crate::client::GrpcClient::connect(&format!("https://{addr}"), &wrong)
            .await
            .err()
            .expect("wrong CA must not authenticate");
        assert!(
            !err.to_string().is_empty(),
            "expected TLS failure, got empty error"
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_loss_mid_event_wait() {
        let (engine1, engine2, engine3, _dirs, _addrs, _tls, _ca) = three_voters(true).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine1
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/events.yaml"),
                &catalog,
                Value::Object(
                    [("key".to_owned(), Value::String("k1".to_owned()))]
                        .into_iter()
                        .collect(),
                ),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let state = engine1.inspect(run).await.unwrap();
            if state.waits.values().any(|wait| wait.pending) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("wait never opened");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine1.shutdown().await.unwrap();
        let leader = wait_leader(&engine2, &engine3).await;
        leader
            .signal(
                run,
                crate::ids::EventId::generate(),
                "approval",
                "k1",
                Value::Object(
                    [("approved".to_owned(), Value::Bool(true))]
                        .into_iter()
                        .collect(),
                ),
            )
            .await
            .unwrap();
        let output = leader
            .wait_terminal(run, Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(output.pointer("/approved").unwrap(), &Value::Bool(true));
        let consumed = leader
            .inspect(run)
            .await
            .unwrap()
            .inbox
            .iter()
            .filter(|entry| entry.consumed)
            .count();
        assert_eq!(consumed, 1);
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn nested_controls_survive_leader_loss() {
        let (engine1, engine2, engine3, _dirs, addrs, materials, _ca) = three_voters(false).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let input = Value::Array(vec![
            Value::Object([("value".to_owned(), Value::Int(1))].into_iter().collect()),
            Value::Object([("value".to_owned(), Value::Int(4))].into_iter().collect()),
        ]);
        let run = engine1
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/nested-controls.yaml"),
                &catalog,
                input,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        engine1.shutdown().await.unwrap();
        let leader = wait_leader(&engine2, &engine3).await;
        let (endpoint, tls) = if std::ptr::eq(leader, &engine2) {
            (format!("https://{}", addrs[1]), materials[1].clone())
        } else {
            (format!("https://{}", addrs[2]), materials[2].clone())
        };
        let worker = tokio::spawn(Engine::run_worker(endpoint, tls));
        let output = leader
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        let Value::Array(items) = output else {
            panic!("nested output");
        };
        assert_eq!(items.len(), 2);
        worker.abort();
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_loss_does_not_reset_ready_work() {
        let (engine1, engine2, engine3, dirs, addrs, materials, _ca) = three_voters(false).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine1
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let state = engine1.inspect(run).await.unwrap();
            if !crate::domain::ready_activations(&state, run).is_empty() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no ready work before leader loss");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let before = engine1.inspect(run).await.unwrap();
        let ready_before = crate::domain::ready_activations(&before, run);
        let replicated = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if engine2.inspect(run).await.is_ok() && engine3.inspect(run).await.is_ok() {
                break;
            }
            if tokio::time::Instant::now() >= replicated {
                panic!("followers did not replicate the run before leader loss");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine1.shutdown().await.unwrap();
        let leader = wait_leader(&engine2, &engine3).await;
        let after = loop {
            match leader.inspect(run).await {
                Ok(state) => break state,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(err) => panic!("leader inspect after loss: {err}"),
            }
        };
        assert!(matches!(
            after.runs.get(&run).unwrap().status,
            crate::domain::RunStatus::Active
        ));
        let ready_after = crate::domain::ready_activations(&after, run);
        assert_eq!(ready_after, ready_before);
        let (endpoint, tls) = if std::ptr::eq(leader, &engine2) {
            (format!("https://{}", addrs[1]), materials[1].clone())
        } else {
            (format!("https://{}", addrs[2]), materials[2].clone())
        };
        let worker = tokio::spawn(Engine::run_worker(endpoint, tls));
        let output = leader
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        worker.abort();
        let mut peers = BTreeMap::new();
        for other in 2u64..=3 {
            peers.insert(
                other,
                (
                    addrs[(other - 1) as usize],
                    materials[(other - 1) as usize].clone(),
                ),
            );
        }
        let returned = Engine::member(MemberConfig {
            data_dir: dirs[0].path().to_path_buf(),
            node_id: 1,
            bind: addrs[0],
            peers,
            tls: materials[0].clone(),
            host_activities: false,
            initialize: false,
        })
        .await
        .unwrap();
        let catch_up = tokio::time::Instant::now() + Duration::from_secs(10);
        let restored_out = loop {
            let restored = returned.inspect(run).await.unwrap();
            if let Some(out) = crate::domain::run_output(&restored, run) {
                break out;
            }
            if tokio::time::Instant::now() >= catch_up {
                panic!(
                    "old owner did not catch up; status={:?}",
                    restored.runs.get(&run).map(|item| &item.status)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(restored_out, output);
        returned.shutdown().await.unwrap();
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workers_scale_without_membership_change() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: false,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(engine.voter_ids(), vec![1]);
        let endpoint = format!("https://{addr}");
        let worker1 = tokio::spawn(Engine::run_worker(endpoint.clone(), tls.clone()));
        let worker2 = tokio::spawn(Engine::run_worker(endpoint, tls));
        tokio::time::sleep(Duration::from_millis(200)).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        assert_eq!(engine.voter_ids(), vec![1]);
        worker1.abort();
        worker2.abort();
        engine.shutdown().await.unwrap();
    }

    async fn worker_rpc(
        addr: SocketAddr,
        tls: &crate::tls::TlsMaterial,
    ) -> crate::generated::worker_client::WorkerClient<tonic::transport::Channel> {
        crate::tls::install_provider();
        let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(crate::rpc::client_tls(tls).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        crate::generated::worker_client::WorkerClient::new(channel)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forged_worker_results_are_rejected() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: false,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let mut client = worker_rpc(addr, &tls).await;
        let session = crate::ids::WorkerSessionId::generate();
        let _ = client
            .register(crate::generated::RegisterRequest {
                session_id: session.to_hex(),
                activities: vec!["*".to_owned()],
                capacity: 8,
            })
            .await
            .unwrap();
        let claimed = loop {
            let resp = client
                .claim(crate::generated::ClaimRequest {
                    command_id: crate::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    capacity: 8,
                })
                .await
                .unwrap()
                .into_inner();
            if !resp.assignments.is_empty() {
                break resp.assignments;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let assignment = &claimed[0];
        let stale = client
            .report(crate::generated::ReportRequest {
                command_id: crate::ids::CommandId::generate().to_hex(),
                session_id: session.to_hex(),
                run_id: assignment.run_id.clone(),
                activation_id: assignment.activation_id.clone(),
                generation: assignment.generation.saturating_add(9),
                revision: assignment.revision,
                output_json: serde_json::to_vec(&Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                        (
                            "reservation_id".to_owned(),
                            Value::String("forged".to_owned()),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ))
                .unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            !stale.error.is_empty(),
            "stale generation must be rejected: {}",
            stale.error
        );
        let invalid = client
            .report(crate::generated::ReportRequest {
                command_id: crate::ids::CommandId::generate().to_hex(),
                session_id: session.to_hex(),
                run_id: assignment.run_id.clone(),
                activation_id: assignment.activation_id.clone(),
                generation: assignment.generation,
                revision: assignment.revision,
                output_json: serde_json::to_vec(&Value::Null).unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            !invalid.error.is_empty(),
            "schema-invalid output must be rejected: {}",
            invalid.error
        );
        let state = engine.inspect(run).await.unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            crate::domain::RunStatus::Active
        ));
        let accepted = client
            .report(crate::generated::ReportRequest {
                command_id: crate::ids::CommandId::generate().to_hex(),
                session_id: session.to_hex(),
                run_id: assignment.run_id.clone(),
                activation_id: assignment.activation_id.clone(),
                generation: assignment.generation,
                revision: assignment.revision,
                output_json: serde_json::to_vec(&Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                        (
                            "reservation_id".to_owned(),
                            Value::String("res-1".to_owned()),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ))
                .unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            accepted.error.is_empty(),
            "valid result must apply: {}",
            accepted.error
        );
        let worker = tokio::spawn(Engine::run_worker(format!("https://{addr}"), tls));
        let output = engine
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        worker.abort();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quorum_loss_does_not_invent_authority() {
        let (engine1, engine2, engine3, _dirs, _addrs, _tls, _ca) = three_voters(true).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = engine1
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/events.yaml"),
                &catalog,
                Value::Object(
                    [("key".to_owned(), Value::String("k1".to_owned()))]
                        .into_iter()
                        .collect(),
                ),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if engine1
                .inspect(run)
                .await
                .unwrap()
                .waits
                .values()
                .any(|wait| wait.pending)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("wait never opened");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let err = tokio::time::timeout(
            Duration::from_secs(3),
            engine1.signal(
                run,
                crate::ids::EventId::generate(),
                "approval",
                "k1",
                Value::Object(
                    [("approved".to_owned(), Value::Bool(true))]
                        .into_iter()
                        .collect(),
                ),
            ),
        )
        .await;
        assert!(
            matches!(err, Err(_) | Ok(Err(_))),
            "writes without quorum must fail or time out"
        );
        let state = engine1.inspect(run).await.unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            crate::domain::RunStatus::Active
        ));
        assert!(run_output_missing(&state, run));
        engine1.shutdown().await.unwrap();
    }

    fn run_output_missing(state: &crate::domain::State, run: crate::ids::RunId) -> bool {
        crate::domain::run_output(state, run).is_none()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clock_rollback_fails_closed() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: true,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 10_000;
        crate::write::inject_clock_watermark(future);
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let err = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .expect_err("clock rollback must fail closed");
        assert!(err.to_string().contains("clock rollback"));
        crate::write::clear_clock_watermark();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn learner_catchup_then_voter_replacement() {
        let (engine1, engine2, engine3, _dirs, addrs, materials, ca) = three_voters(true).await;
        let addr4 = unused_addr();
        let tls4 = issue_node(&ca, 4).unwrap();
        let dir4 = tempfile::tempdir().unwrap();
        let mut peers4 = BTreeMap::new();
        for node in 1u64..=3 {
            peers4.insert(
                node,
                (
                    addrs[(node - 1) as usize],
                    materials[(node - 1) as usize].clone(),
                ),
            );
        }
        let engine4 = Engine::member(MemberConfig {
            data_dir: dir4.path().to_path_buf(),
            node_id: 4,
            bind: addr4,
            peers: peers4,
            tls: tls4.clone(),
            host_activities: true,
            initialize: false,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        for engine in [&engine1, &engine2, &engine3] {
            engine.insert_peer(4, addr4, tls4.clone());
        }
        let leader = if engine1.is_leader() {
            &engine1
        } else if engine2.is_leader() {
            &engine2
        } else {
            &engine3
        };
        leader.add_learner(4, addr4).await.unwrap();
        leader.add_voter(4).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if leader.voter_ids().contains(&4) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("node 4 never became a voter");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let run = leader
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let output = leader
            .wait_terminal(run, Duration::from_secs(20))
            .await
            .unwrap();
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        engine1.shutdown().await.unwrap();
        let catch_up = tokio::time::Instant::now() + Duration::from_secs(10);
        let restored = loop {
            let from_two = engine2
                .inspect(run)
                .await
                .ok()
                .and_then(|state| crate::domain::run_output(&state, run));
            let from_three = engine3
                .inspect(run)
                .await
                .ok()
                .and_then(|state| crate::domain::run_output(&state, run));
            if let Some(out) = from_two.or(from_three) {
                break out;
            }
            if tokio::time::Instant::now() >= catch_up {
                panic!("accepted state missing after voter replacement");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(restored, output);
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
        engine4.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_recon_and_idempotent_effect() {
        crate::engine::ledger_clear();
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: false,
            initialize: true,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let manual = r#"
dsl: graphrun/v1
id: manual_probe
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: step
nodes:
  step:
    kind: activity
    activity: {name: test.manual, version: 1}
    input: {from: workflow.input}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.step.output}
"#;
        let input = Value::Object([("value".to_owned(), Value::Int(2))].into_iter().collect());
        let applied = engine
            .start_yaml(manual, &catalog, input.clone())
            .await
            .unwrap();
        let not_applied = engine
            .start_yaml(manual, &catalog, input.clone())
            .await
            .unwrap();
        let unknown = engine
            .start_yaml(manual, &catalog, input.clone())
            .await
            .unwrap();
        let lost = engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let mut client = worker_rpc(addr, &tls).await;
        let session = crate::ids::WorkerSessionId::generate();
        let _ = client
            .register(crate::generated::RegisterRequest {
                session_id: session.to_hex(),
                activities: vec!["*".to_owned()],
                capacity: 8,
            })
            .await
            .unwrap();
        let mut claimed = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while claimed.len() < 4 {
            let resp = client
                .claim(crate::generated::ClaimRequest {
                    command_id: crate::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    capacity: 8,
                })
                .await
                .unwrap()
                .into_inner();
            claimed.extend(resp.assignments);
            if tokio::time::Instant::now() >= deadline {
                panic!("did not claim four activations");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let lost_assign = claimed
            .iter()
            .find(|item| item.run_id == lost.to_hex())
            .cloned()
            .expect("lost sequence claim");
        let first = crate::engine::dispatch_handler(
            &lost_assign.activity_name,
            &serde_json::from_slice(&lost_assign.input_json).unwrap_or(Value::Null),
            Some(&lost_assign.effect_key),
        )
        .unwrap();
        let first_row = crate::engine::ledger_get(&lost_assign.effect_key).unwrap();
        assert_eq!(first_row.physical, 1);
        assert_eq!(first_row.output, first);
        tokio::time::sleep(crate::policy::SESSION_LEASE + Duration::from_secs(1)).await;
        let _ = client
            .register(crate::generated::RegisterRequest {
                session_id: session.to_hex(),
                activities: vec!["*".to_owned()],
                capacity: 8,
            })
            .await
            .unwrap();
        let mut second = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while second.len() < 4 {
            let resp = client
                .claim(crate::generated::ClaimRequest {
                    command_id: crate::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    capacity: 8,
                })
                .await
                .unwrap()
                .into_inner();
            second.extend(resp.assignments);
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let lost2 = second
            .iter()
            .find(|item| item.run_id == lost.to_hex())
            .cloned()
            .expect("retry claim");
        assert_eq!(lost2.effect_key, lost_assign.effect_key);
        let second_out = crate::engine::dispatch_handler(
            &lost2.activity_name,
            &serde_json::from_slice(&lost2.input_json).unwrap_or(Value::Null),
            Some(&lost2.effect_key),
        )
        .unwrap();
        let row = crate::engine::ledger_get(&lost2.effect_key).unwrap();
        assert_eq!(row.physical, 2);
        assert_eq!(row.output, first);
        assert_eq!(second_out, first);
        let _ = client
            .report(crate::generated::ReportRequest {
                command_id: crate::ids::CommandId::generate().to_hex(),
                session_id: session.to_hex(),
                run_id: lost2.run_id,
                activation_id: lost2.activation_id,
                generation: lost2.generation,
                revision: lost2.revision,
                output_json: serde_json::to_vec(&second_out).unwrap(),
            })
            .await;
        fn send_recon(
            assignment: &crate::generated::Assignment,
            session: &str,
            outcome: &str,
            output: Option<Value>,
        ) -> crate::generated::ReconcileRequest {
            crate::generated::ReconcileRequest {
                command_id: crate::ids::CommandId::generate().to_hex(),
                session_id: session.to_owned(),
                run_id: assignment.run_id.clone(),
                activation_id: assignment.activation_id.clone(),
                outcome: outcome.to_owned(),
                output_json: output
                    .map(|value| serde_json::to_vec(&value).unwrap_or_default())
                    .unwrap_or_default(),
                generation: assignment.generation,
                revision: assignment.revision,
            }
        }
        let applied_a = second
            .iter()
            .find(|item| item.run_id == applied.to_hex())
            .expect("applied recon");
        let not_a = second
            .iter()
            .find(|item| item.run_id == not_applied.to_hex())
            .expect("not_applied recon");
        let unk_a = second
            .iter()
            .find(|item| item.run_id == unknown.to_hex())
            .expect("unknown recon");
        assert!(applied_a.role.contains("reconcil"));
        let _ = client
            .reconcile(send_recon(
                applied_a,
                &session.to_hex(),
                "applied",
                Some(input.clone()),
            ))
            .await;
        let _ = client
            .reconcile(send_recon(not_a, &session.to_hex(), "not_applied", None))
            .await;
        for _ in 0..3 {
            let _ = client
                .reconcile(send_recon(unk_a, &session.to_hex(), "unknown", None))
                .await;
        }
        let worker = tokio::spawn(Engine::run_worker(format!("https://{addr}"), tls));
        let applied_out = engine
            .wait_terminal(applied, Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(applied_out.pointer("/value").unwrap().as_i64(), Some(2));
        let lost_out = engine
            .wait_terminal(lost, Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(
            lost_out.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        let state = engine.inspect(unknown).await.unwrap();
        assert!(!state.interventions.is_empty());
        let not_out = engine
            .wait_terminal(not_applied, Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(not_out.pointer("/value").unwrap().as_i64(), Some(2));
        worker.abort();
        engine.shutdown().await.unwrap();
    }
}
