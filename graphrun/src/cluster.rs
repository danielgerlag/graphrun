use crate::error::{Error, Result};
use crate::storage::TypeConfig;
use crate::tls::{TlsMaterial, client_config, rpc_timeout, server_config};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

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

#[derive(Serialize, Deserialize)]
enum RaftRpc {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
enum RaftReply {
    Append(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    Snapshot(InstallSnapshotResponse<u64>),
    Error(String),
}

impl PeerClient {
    async fn call(&mut self, rpc: RaftRpc) -> std::io::Result<RaftReply> {
        let Some((addr, tls)) = &self.peer else {
            return Err(io::Error::other(format!("unknown peer {}", self.target)));
        };
        let connector = TlsConnector::from(client_config(tls).map_err(io::Error::other)?);
        let stream = TcpStream::connect(addr).await?;
        let name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(io::Error::other)?;
        let mut tls_stream = connector.connect(name, stream).await?;
        write_frame(
            &mut tls_stream,
            &serde_json::to_vec(&rpc).map_err(io::Error::other)?,
        )
        .await?;
        let bytes = read_frame(&mut tls_stream).await?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

impl RaftNetwork<TypeConfig> for PeerClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        match self.call(RaftRpc::Append(rpc)).await {
            Ok(RaftReply::Append(resp)) => Ok(resp),
            Ok(RaftReply::Error(err)) => Err(RPCError::Unreachable(Unreachable::new(
                &io::Error::other(err),
            ))),
            Ok(_) => Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "unexpected raft reply",
            )))),
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
        match self.call(RaftRpc::Snapshot(rpc)).await {
            Ok(RaftReply::Snapshot(resp)) => Ok(resp),
            Ok(RaftReply::Error(err)) => Err(RPCError::Unreachable(Unreachable::new(
                &io::Error::other(err),
            ))),
            Ok(_) => Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "unexpected raft reply",
            )))),
            Err(err) => Err(RPCError::Unreachable(Unreachable::new(&err))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.call(RaftRpc::Vote(rpc)).await {
            Ok(RaftReply::Vote(resp)) => Ok(resp),
            Ok(RaftReply::Error(err)) => Err(RPCError::Unreachable(Unreachable::new(
                &io::Error::other(err),
            ))),
            Ok(_) => Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "unexpected raft reply",
            )))),
            Err(err) => Err(RPCError::Unreachable(Unreachable::new(&err))),
        }
    }
}

pub async fn serve_raft(bind: SocketAddr, tls: TlsMaterial, raft: Raft<TypeConfig>) -> Result<()> {
    let acceptor = TlsAcceptor::from(server_config(&tls)?);
    let listener = TcpListener::bind(bind)
        .await
        .map_err(|err| Error::invalid(err.to_string()))?;
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let acceptor = acceptor.clone();
        let raft = raft.clone();
        tokio::spawn(async move {
            let Ok(mut tls_stream) = acceptor.accept(stream).await else {
                return;
            };
            let Ok(bytes) = read_frame(&mut tls_stream).await else {
                return;
            };
            let Ok(rpc) = serde_json::from_slice::<RaftRpc>(&bytes) else {
                return;
            };
            let reply = match rpc {
                RaftRpc::Append(req) => match raft.append_entries(req).await {
                    Ok(resp) => RaftReply::Append(resp),
                    Err(err) => RaftReply::Error(err.to_string()),
                },
                RaftRpc::Vote(req) => match raft.vote(req).await {
                    Ok(resp) => RaftReply::Vote(resp),
                    Err(err) => RaftReply::Error(err.to_string()),
                },
                RaftRpc::Snapshot(req) => match raft.install_snapshot(req).await {
                    Ok(resp) => RaftReply::Snapshot(resp),
                    Err(err) => RaftReply::Error(err.to_string()),
                },
            };
            if let Ok(payload) = serde_json::to_vec(&reply) {
                let _ = write_frame(&mut tls_stream, &payload).await;
            }
        });
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    if data.len() > 8 * 1024 * 1024 {
        return Err(io::Error::other("rpc exceeds 8MiB envelope"));
    }
    writer.write_u32(data.len() as u32).await?;
    writer.write_all(data).await?;
    writer.flush().await
}

async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u32().await? as usize;
    if len > 8 * 1024 * 1024 {
        return Err(io::Error::other("rpc exceeds 8MiB envelope"));
    }
    let mut buf = vec![0; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

pub fn _rpc_timeout() -> std::time::Duration {
    rpc_timeout()
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
                tls: materials[(node - 1) as usize].clone(),
                host_activities: true,
                initialize: node == 1,
            }));
        }
        let engine2 = members.remove(1).await.unwrap();
        let engine3 = members.remove(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
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
}
