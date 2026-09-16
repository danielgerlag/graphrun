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
use std::sync::Arc;
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
    peers: Arc<BTreeMap<u64, (SocketAddr, TlsMaterial)>>,
}

impl ClusterNetwork {
    pub fn new(peers: BTreeMap<u64, (SocketAddr, TlsMaterial)>) -> Self {
        Self {
            peers: Arc::new(peers),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for ClusterNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        let peer = self.peers.get(&target).cloned();
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
        let state = leader.inspect(run).await.expect("survivor has the run");
        let output = crate::domain::run_output(&state, run).expect("survivor completed the run");
        assert_eq!(output, expected);
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        engine2.shutdown().await.unwrap();
        engine3.shutdown().await.unwrap();
    }
}
