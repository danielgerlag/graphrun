use crate::generated::{Blob, ClockHealthRequest, ElectionRequest, raft_client::RaftClient};
#[cfg(test)]
use crate::rpc::client_tls;
use crate::storage::TypeConfig;
use crate::tls::TlsMaterial;
use hyper_util::rt::TokioIo;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};

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
    local_id: u64,
    local_tls: TlsMaterial,
}

#[derive(Clone, Copy)]
pub(crate) struct ElectionEvidence {
    pub expected_term: u64,
    pub membership_index: u64,
    pub min_applied_index: u64,
    pub min_last_log_index: u64,
}

impl ClusterNetwork {
    pub fn new(
        local_id: u64,
        local_tls: TlsMaterial,
        peers: BTreeMap<u64, (SocketAddr, TlsMaterial)>,
    ) -> Self {
        Self {
            peers: Arc::new(Mutex::new(peers)),
            local_id,
            local_tls,
        }
    }

    pub fn insert_peer(&self, id: u64, addr: SocketAddr, tls: TlsMaterial) {
        self.peers.lock().unwrap().insert(id, (addr, tls));
    }

    pub fn local_id(&self) -> u64 {
        self.local_id
    }

    pub fn peer(&self, id: u64) -> Option<(SocketAddr, String)> {
        self.peers
            .lock()
            .unwrap()
            .get(&id)
            .map(|(addr, tls)| (*addr, tls.server_name.clone()))
    }

    pub async fn probe_clock(&self, target: u64, committed_endpoint: &str) -> io::Result<()> {
        let peer = self.peers.lock().unwrap().get(&target).cloned();
        let Some((addr, _)) = &peer else {
            return Err(io::Error::other("clock member is not configured"));
        };
        if addr.to_string() != committed_endpoint {
            return Err(io::Error::other(
                "clock member endpoint differs from committed roster",
            ));
        }
        let peer = PeerClient {
            target,
            peer,
            expected_endpoint: committed_endpoint.to_owned(),
            local_id: self.local_id,
            local_tls: self.local_tls.clone(),
        };
        let started = crate::time::boot_millis().map_err(io::Error::other)?;
        let (before_wall, result) =
            tokio::time::timeout(std::time::Duration::from_millis(500), async {
                let mut client = peer.client().await?;
                let before_wall = crate::time::wall_millis().map_err(io::Error::other)?;
                let response = client
                    .clock_health(ClockHealthRequest {
                        sender_id: self.local_id,
                    })
                    .await
                    .map_err(io::Error::other)?
                    .into_inner();
                Ok::<_, io::Error>((before_wall, response))
            })
            .await
            .map_err(io::Error::other)??;
        let finished = crate::time::boot_millis().map_err(io::Error::other)?;
        let after_wall = crate::time::wall_millis().map_err(io::Error::other)?;
        if result.member_id != target
            || finished.saturating_sub(started) > 500
            || result.wall_ms < before_wall.saturating_sub(2_000)
            || result.wall_ms > after_wall.saturating_add(2_000)
        {
            return Err(io::Error::other(
                "clock member returned stale or skewed sample",
            ));
        }
        Ok(())
    }

    pub(crate) async fn request_election(
        &self,
        target: u64,
        committed_endpoint: &str,
        evidence: ElectionEvidence,
    ) -> io::Result<()> {
        if target == self.local_id {
            return Err(io::Error::other("cannot request a local leader transfer"));
        }
        let peer = self.peers.lock().unwrap().get(&target).cloned();
        let Some((addr, _)) = &peer else {
            return Err(io::Error::other("election target is not configured"));
        };
        if addr.to_string() != committed_endpoint {
            return Err(io::Error::other(
                "election target endpoint differs from committed roster",
            ));
        }
        let peer = PeerClient {
            target,
            peer,
            expected_endpoint: committed_endpoint.to_owned(),
            local_id: self.local_id,
            local_tls: self.local_tls.clone(),
        };
        let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            peer.client()
                .await?
                .request_election(ElectionRequest {
                    sender_id: self.local_id,
                    expected_term: evidence.expected_term,
                    membership_index: evidence.membership_index,
                    min_applied_index: evidence.min_applied_index,
                    min_last_log_index: evidence.min_last_log_index,
                })
                .await
                .map_err(io::Error::other)
        })
        .await
        .map_err(io::Error::other)??
        .into_inner();
        if response.member_id != target || response.requested_from_term != evidence.expected_term {
            return Err(io::Error::other(
                "election receipt identity or term mismatch",
            ));
        }
        Ok(())
    }
}

impl RaftNetworkFactory<TypeConfig> for ClusterNetwork {
    type Network = PeerClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let peer = self.peers.lock().unwrap().get(&target).cloned();
        PeerClient {
            target,
            peer,
            expected_endpoint: node.addr.clone(),
            local_id: self.local_id,
            local_tls: self.local_tls.clone(),
        }
    }
}

pub struct PeerClient {
    target: u64,
    peer: Option<(SocketAddr, TlsMaterial)>,
    expected_endpoint: String,
    local_id: u64,
    local_tls: TlsMaterial,
}

struct VerifiedMemberConnector {
    addr: SocketAddr,
    tls: tokio_rustls::TlsConnector,
    server_name: rustls::pki_types::ServerName<'static>,
    ca_pem: Arc<String>,
    member_id: u64,
}

impl Service<Uri> for VerifiedMemberConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let addr = self.addr;
        let connector = self.tls.clone();
        let server_name = self.server_name.clone();
        let ca_pem = self.ca_pem.clone();
        let member_id = self.member_id;
        Box::pin(async move {
            let socket = tokio::net::TcpStream::connect(addr).await?;
            let stream = connector
                .connect(server_name, socket)
                .await
                .map_err(io::Error::other)?;
            let chain = stream
                .get_ref()
                .1
                .peer_certificates()
                .ok_or_else(|| io::Error::other("member server certificate missing"))?;
            let cluster = crate::tls::cluster_id_from_ca(&ca_pem).map_err(io::Error::other)?;
            let identity = crate::tls::verify_peer_identity(&ca_pem, &cluster, chain)
                .map_err(io::Error::other)?;
            identity
                .require_member_identity(
                    &cluster,
                    &crate::tls::PrincipalId::parse(member_id.to_string())
                        .map_err(io::Error::other)?,
                )
                .map_err(io::Error::other)?;
            Ok(TokioIo::new(stream))
        })
    }
}

impl PeerClient {
    async fn client(&self) -> std::io::Result<RaftClient<Channel>> {
        let Some((addr, tls)) = &self.peer else {
            return Err(io::Error::other(format!("unknown peer {}", self.target)));
        };
        if addr.to_string() != self.expected_endpoint {
            return Err(io::Error::other(
                "peer endpoint differs from committed roster",
            ));
        }
        if tls.ca_pem != self.local_tls.ca_pem {
            return Err(io::Error::other("peer belongs to another CA"));
        }
        let expected_cluster =
            crate::tls::cluster_id_from_ca(&self.local_tls.ca_pem).map_err(io::Error::other)?;
        let identity = crate::tls::verify_peer_identity(
            &self.local_tls.ca_pem,
            &expected_cluster,
            &crate::tls::load_certs(&tls.cert_pem).map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;
        identity
            .require_member_identity(
                &expected_cluster,
                &crate::tls::PrincipalId::parse(self.target.to_string())
                    .map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
        let mut rustls =
            (*crate::tls::client_config(&self.local_tls).map_err(io::Error::other)?).clone();
        rustls.alpn_protocols = vec![b"h2".to_vec()];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(rustls));
        let server_name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(io::Error::other)?;
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(io::Error::other)?
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(5))
            .connect_with_connector(VerifiedMemberConnector {
                addr: *addr,
                tls: connector,
                server_name,
                ca_pem: Arc::new(self.local_tls.ca_pem.clone()),
                member_id: self.target,
            })
            .await
            .map_err(io::Error::other)?;
        Ok(RaftClient::new(channel))
    }

    async fn call(&mut self, kind: &str, rpc: impl serde::Serialize) -> std::io::Result<Vec<u8>> {
        let mut client = self.client().await?;
        let blob = Blob {
            json: serde_json::to_vec(&rpc).map_err(io::Error::other)?,
            sender_id: self.local_id,
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
    use crate::generated::{
        Blob, ClockHealthRequest, ElectionRequest, ListRequest, PublishCatalogRequest,
        StartRequest, WatchMemberFaultRequest, client_client::ClientClient,
        raft_client::RaftClient,
    };
    use crate::tls::{
        ClusterId, PeerRole, PrincipalId, PrincipalIdentity, generate_ca, issue_node,
        issue_principal,
    };
    use crate::value::Value;
    use std::time::Duration;
    use tonic::Code;

    fn unused_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    fn register_worker(
        session: crate::ids::WorkerSessionId,
        catalog: &Catalog,
    ) -> crate::generated::RegisterRequest {
        let mut capabilities = Vec::new();
        for key in catalog.activities.keys() {
            for role in [
                crate::ids::ExecutionRole::Forward,
                crate::ids::ExecutionRole::Compensation,
            ] {
                capabilities.push(
                    crate::worker_contract::capability_for(catalog, key, role)
                        .unwrap()
                        .to_wire(),
                );
            }
        }
        for key in catalog.reconcilers.keys() {
            capabilities.push(
                crate::worker_contract::capability_for(
                    catalog,
                    key,
                    crate::ids::ExecutionRole::Reconciliation,
                )
                .unwrap()
                .to_wire(),
            );
        }
        crate::generated::RegisterRequest {
            session_id: session.to_hex(),
            capacity: 8,
            capabilities,
            principal_id: "1".to_owned(),
            protocol_min: 1,
            protocol_max: 1,
            command_id: crate::ids::CommandId::generate().to_hex(),
        }
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
        for _ in 0..3 {
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
            let engine2 = match members.remove(1).await {
                Ok(engine) => engine,
                Err(error) if error.message.starts_with("member listener") => {
                    eprintln!("retrying fixture after {error}");
                    continue;
                }
                Err(error) => panic!("member 2 startup: {error}"),
            };
            let engine3 = match members.remove(1).await {
                Ok(engine) => engine,
                Err(error) if error.message.starts_with("member listener") => {
                    eprintln!("retrying fixture after {error}");
                    engine2.shutdown().await.unwrap();
                    continue;
                }
                Err(error) => panic!("member 3 startup: {error}"),
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            let engine1 = match members.remove(0).await {
                Ok(engine) => engine,
                Err(error) if error.message.starts_with("member listener") => {
                    eprintln!("retrying fixture after {error}");
                    engine2.shutdown().await.unwrap();
                    engine3.shutdown().await.unwrap();
                    continue;
                }
                Err(error) => panic!("member 1 startup: {error}"),
            };
            return (engine1, engine2, engine3, dirs, addrs, materials, ca);
        }
        panic!("three-member fixture listener unavailable after three attempts")
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
    async fn clock_fault_leaves_committed_state_and_a_healthy_voter_takes_over() {
        let (old, second, third, dirs, addrs, tls, _ca) = three_voters(false).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let definition = include_str!("../../docs/specs/v1/examples/events.yaml");
        let input = Value::Object(
            [("key".to_owned(), Value::String("clock-fault".to_owned()))]
                .into_iter()
                .collect(),
        );
        let committed = old
            .start_yaml(definition, &catalog, input.clone())
            .await
            .unwrap();
        let applied_before = old.raft_applied_index();
        let started = tokio::time::Instant::now();
        old.inject_clock_watermark(crate::time::wall_millis().unwrap() + 10_000);
        assert_eq!(old.health().await["clock_safe"], false);
        let survivor = wait_leader(&second, &third).await;
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(old.health().await["clock_safe"], false);
        assert!(
            old.start_yaml(definition, &catalog, input.clone())
                .await
                .is_err()
        );
        let observed = survivor.inspect(committed).await.unwrap();
        assert!(observed.runs.contains_key(&committed));
        old.shutdown().await.unwrap();

        let mut peers = BTreeMap::new();
        peers.insert(2, (addrs[1], tls[1].clone()));
        peers.insert(3, (addrs[2], tls[2].clone()));
        let returned = Engine::member(MemberConfig {
            data_dir: dirs[0].path().to_path_buf(),
            node_id: 1,
            bind: addrs[0],
            peers,
            tls: tls[0].clone(),
            host_activities: false,
            initialize: false,
        })
        .await
        .unwrap();
        assert!(!returned.is_leader());
        assert_eq!(returned.health().await["clock_safe"], false);
        assert!(returned.raft_applied_index() >= applied_before);
        assert!(
            returned
                .start_yaml(definition, &catalog, input.clone())
                .await
                .is_err()
        );
        assert!(returned.acknowledge_clock("clock repaired").await.is_err());
        survivor
            .start_yaml(definition, &catalog, input)
            .await
            .unwrap();
        returned.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn election_request_rejects_unsigned_sender_identity() {
        let (first, second, third, _dirs, addrs, tls, _ca) = three_voters(false).await;
        let mut caller = tls[0].clone();
        caller.server_name = tls[1].server_name.clone();
        let channel = tonic::transport::Channel::from_shared(format!("https://{}", addrs[1]))
            .unwrap()
            .tls_config(client_tls(&caller).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = RaftClient::new(channel);
        let err = client
            .request_election(ElectionRequest {
                sender_id: 3,
                expected_term: 1,
                membership_index: 1,
                min_applied_index: 1,
                min_last_log_index: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn election_request_rejects_caught_up_claim_for_stale_candidate() {
        let (first, second, third, _dirs, addrs, tls, _ca) = three_voters(false).await;
        let (term, membership_index) = first.raft_epoch();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while second.voter_ids() != [1, 2, 3]
            || second.raft_epoch().1 != membership_index
            || second.raft_applied_index() < first.raft_applied_index()
            || !second.membership_applied().await
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "candidate never caught up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut caller = tls[0].clone();
        caller.server_name = tls[1].server_name.clone();
        let channel = tonic::transport::Channel::from_shared(format!("https://{}", addrs[1]))
            .unwrap()
            .tls_config(client_tls(&caller).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let err = RaftClient::new(channel)
            .request_election(ElectionRequest {
                sender_id: 1,
                expected_term: term,
                membership_index,
                min_applied_index: 0,
                min_last_log_index: u64::MAX,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("not caught up"), "{err:?}");
        assert!(first.is_leader());
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn committed_voter_accepts_request_only_after_fresh_clock_quorum() {
        let (first, second, third, _dirs, addrs, tls, _ca) = three_voters(false).await;
        let (term, membership_index) = first.raft_epoch();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while second.voter_ids() != [1, 2, 3]
            || !second.membership_applied().await
            || second.raft_epoch().1 != membership_index
            || second.raft_applied_index() < first.raft_applied_index()
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut caller = tls[0].clone();
        caller.server_name = tls[1].server_name.clone();
        let channel = tonic::transport::Channel::from_shared(format!("https://{}", addrs[1]))
            .unwrap()
            .tls_config(client_tls(&caller).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let receipt = RaftClient::new(channel)
            .request_election(ElectionRequest {
                sender_id: 1,
                expected_term: term,
                membership_index,
                min_applied_index: first.raft_applied_index(),
                min_last_log_index: first.raft_applied_index(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(receipt.member_id, 2);
        assert_eq!(receipt.requested_from_term, term);
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn faulted_member_and_missing_voter_cannot_certify_transfer() {
        let (first, second, third, _dirs, _addrs, _tls, _ca) = three_voters(false).await;
        third.shutdown().await.unwrap();
        first.inject_clock_watermark(crate::time::wall_millis().unwrap() + 10_000);
        assert_eq!(first.health().await["clock_safe"], false);
        assert!(!second.has_fresh_clock_quorum().await);
        assert!(!first.has_fresh_clock_quorum().await);
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn signed_worker_receives_fault_even_with_no_free_capacity() {
        let (first, second, third, _dirs, addrs, tls, _ca) = three_voters(false).await;
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        first
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("watch".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .await
            .unwrap();
        let session = crate::ids::WorkerSessionId::generate();
        let mut worker = worker_rpc(addrs[0], &tls[0]).await;
        let mut registration = register_worker(session, &catalog);
        registration.capacity = 1;
        worker.register(registration).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let claimed = worker
                .claim(crate::generated::ClaimRequest {
                    command_id: crate::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    capacity: 1,
                })
                .await
                .unwrap()
                .into_inner();
            if !claimed.assignments.is_empty() {
                assert_eq!(claimed.assignments.len(), 1);
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no ready assignment"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut wrong_identity = tls[1].clone();
        wrong_identity.server_name = tls[0].server_name.clone();
        let mut outsider = worker_rpc(addrs[0], &wrong_identity).await;
        let err = outsider
            .watch_member_fault(WatchMemberFaultRequest {
                session_id: session.to_hex(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);

        let mut watching = worker.clone();
        let response = tokio::spawn(async move {
            watching
                .watch_member_fault(WatchMemberFaultRequest {
                    session_id: session.to_hex(),
                })
                .await
        });
        first.inject_clock_watermark(crate::time::wall_millis().unwrap() + 10_000);
        assert_eq!(first.health().await["clock_safe"], false);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), response)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .into_inner()
                .faulted
        );
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_handler_observes_fault_without_reporting_a_result() {
        const YAML: &str = r#"
dsl: graphrun/v1
id: faulted_remote_handler
version: 1
input_schema: integer/v1
output_schema: integer/v1
start: effect
nodes:
  effect:
    kind: activity
    activity: {name: sdk.hold, version: 1}
    input: {from: workflow.input}
    next: done
  done:
    kind: complete
    output: {from: nodes.effect.output}
"#;
        let (first, second, third, _dirs, addrs, tls, _ca) = three_voters(false).await;
        let catalog = Catalog::from_json(
            br#"{"format":"graphrun.catalog/v1","schemas":{},"activities":[{
              "name":"sdk.hold","version":1,"input_schema":"integer/v1",
              "output_schema":"integer/v1","execution":"async","effects":"external",
              "recovery":"RetrySafe","error_codes":[]}]}"#,
        )
        .unwrap();
        let run = first
            .start_yaml(YAML, &catalog, Value::Int(7))
            .await
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let cancelled = Arc::new(Mutex::new(Some(cancelled_tx)));
        let worker = crate::worker::Worker::builder(
            format!("https://{}", addrs[0]),
            tls[0].clone(),
            catalog,
        )
        .capacity(1)
        .unwrap()
        .activity("sdk.hold", 1, move |input: i64, context| {
            let entered = entered.clone();
            let cancelled = cancelled.clone();
            async move {
                if let Some(tx) = entered.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                context.cancelled().await;
                if let Some(tx) = cancelled.lock().unwrap().take() {
                    let _ = tx.send(context.can_start_effect());
                }
                Ok::<_, crate::worker::ActivityError>(input)
            }
        })
        .unwrap()
        .open()
        .await
        .unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let worker_task = tokio::spawn(worker.run_until(async move {
            let _ = stop_rx.await;
        }));
        tokio::time::timeout(Duration::from_secs(8), entered_rx)
            .await
            .unwrap()
            .unwrap();
        first.inject_clock_watermark(crate::time::wall_millis().unwrap() + 10_000);
        assert_eq!(first.health().await["clock_safe"], false);
        assert!(
            !tokio::time::timeout(Duration::from_secs(3), cancelled_rx)
                .await
                .unwrap()
                .unwrap()
        );
        let survivor = wait_leader(&second, &third).await;
        assert!(matches!(
            survivor
                .inspect(run)
                .await
                .unwrap()
                .runs
                .get(&run)
                .unwrap()
                .status,
            crate::domain::RunStatus::Active
        ));
        stop_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        third.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn signed_roles_and_cluster_are_checked_before_request_decoding() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let server_tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let server = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: server_tls.clone(),
            host_activities: false,
            initialize: true,
        })
        .await
        .unwrap();
        let cluster = crate::tls::cluster_id_from_ca(&ca.pem).unwrap();
        let worker = PrincipalIdentity::new(
            cluster,
            PrincipalId::parse("1").unwrap(),
            [PeerRole::Worker],
        )
        .unwrap();
        let mut worker_tls = issue_principal(&ca, &worker, "worker.graphrun.local").unwrap();
        worker_tls.server_name = server_tls.server_name.clone();
        let rejected = Engine::member(MemberConfig {
            data_dir: dir.path().join("forged-member"),
            node_id: 1,
            bind: unused_addr(),
            peers: BTreeMap::new(),
            tls: worker_tls.clone(),
            host_activities: false,
            initialize: false,
        })
        .await
        .err()
        .expect("worker-only certificate cannot start a member");
        assert_eq!(rejected.kind, crate::error::ErrorKind::PermissionDenied);
        let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(client_tls(&worker_tls).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ClientClient::new(channel.clone());
        let mut request = tonic::Request::new(ListRequest {});
        request
            .metadata_mut()
            .insert("x-graphrun-role", "admin".parse().unwrap());
        assert_eq!(
            client.list(request).await.unwrap_err().code(),
            Code::PermissionDenied
        );
        assert_eq!(
            client
                .start(StartRequest {
                    command_id: String::new(),
                    yaml: String::new(),
                    catalog_json: Vec::new(),
                    input_json: Vec::new(),
                    workflow: String::new(),
                    version: 0,
                    start_key: String::new(),
                })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
        );
        assert_eq!(
            client
                .publish_catalog(PublishCatalogRequest {
                    command_id: String::new(),
                    version: 0,
                    catalog_json: vec![0xff],
                })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            RaftClient::new(channel.clone())
                .clock_health(ClockHealthRequest { sender_id: 1 })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut raft_client = RaftClient::new(channel);
        let invalid_raft = Blob {
            json: vec![0xff],
            sender_id: 1,
        };
        for reply in [
            raft_client.append_entries(invalid_raft.clone()).await,
            raft_client.vote(invalid_raft.clone()).await,
            raft_client.install_snapshot(invalid_raft).await,
        ] {
            assert_eq!(reply.unwrap_err().code(), Code::PermissionDenied);
        }
        let foreign = PrincipalIdentity::new(
            ClusterId::parse("foreign-cluster").unwrap(),
            PrincipalId::parse("client-1").unwrap(),
            [PeerRole::Client],
        )
        .unwrap();
        let mut foreign_tls = issue_principal(&ca, &foreign, "foreign.graphrun.local").unwrap();
        foreign_tls.server_name = server_tls.server_name;
        let channel = tonic::transport::Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(client_tls(&foreign_tls).unwrap())
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert_eq!(
            ClientClient::new(channel)
                .list(ListRequest {})
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_member_bind_does_not_open_or_initialize_storage() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ca = generate_ca().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let error = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: listener.local_addr().unwrap(),
            peers: BTreeMap::new(),
            tls: issue_node(&ca, 1).unwrap(),
            host_activities: false,
            initialize: true,
        })
        .await
        .err()
        .expect("the occupied listener cannot be served");
        assert_eq!(error.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(!dir.path().join("identity.json").exists());
        assert!(!dir.path().join("member.redb").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mismatched_member_private_key_rejects_before_storage_open() {
        let ca = generate_ca().unwrap();
        let mut invalid = issue_node(&ca, 1).unwrap();
        invalid.key_pem = issue_node(&ca, 2).unwrap().key_pem;
        let dir = tempfile::tempdir().unwrap();
        let error = Engine::member(MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: unused_addr(),
            peers: BTreeMap::new(),
            tls: invalid,
            host_activities: false,
            initialize: true,
        })
        .await
        .err()
        .expect("the member's TLS key must match its signed certificate");
        assert_eq!(error.kind, crate::error::ErrorKind::InvalidArgument);
        assert!(!dir.path().join("identity.json").exists());
        assert!(!dir.path().join("member.redb").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outbound_member_checks_the_presented_certificate_not_only_dns() {
        let ca = generate_ca().unwrap();
        let local = issue_node(&ca, 1).unwrap();
        let expected = issue_node(&ca, 2).unwrap();
        for (principal, role) in [("2", PeerRole::Worker), ("3", PeerRole::Member)] {
            let identity = PrincipalIdentity::new(
                crate::tls::cluster_id_from_ca(&ca.pem).unwrap(),
                PrincipalId::parse(principal).unwrap(),
                [role],
            )
            .unwrap();
            let server_tls = issue_principal(&ca, &identity, &expected.server_name).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let config = crate::tls::server_config(&server_tls).unwrap();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                tokio_rustls::TlsAcceptor::from(config).accept(socket).await
            });
            let peer = PeerClient {
                target: 2,
                peer: Some((addr, expected.clone())),
                expected_endpoint: addr.to_string(),
                local_id: 1,
                local_tls: local.clone(),
            };
            assert!(
                peer.client().await.is_err(),
                "member {principal}/{role:?} must not impersonate member 2"
            );
            assert!(
                server.await.unwrap().is_ok(),
                "TLS handshake should have succeeded"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn raft_clock_rpc_rejects_forged_and_removed_members() {
        let (leader, member2, member3, _dirs, addrs, materials, ca) = three_voters(false).await;
        let peer_client = |mut tls: TlsMaterial| {
            tls.server_name = materials[0].server_name.clone();
            async move {
                let channel =
                    tonic::transport::Channel::from_shared(format!("https://{}", addrs[0]))
                        .unwrap()
                        .tls_config(client_tls(&tls).unwrap())
                        .unwrap()
                        .connect()
                        .await
                        .unwrap();
                RaftClient::new(channel)
            }
        };
        let mut member = peer_client(materials[1].clone()).await;
        assert_eq!(
            member
                .clock_health(ClockHealthRequest { sender_id: 2 })
                .await
                .unwrap()
                .into_inner()
                .member_id,
            1
        );
        assert_eq!(
            member
                .clock_health(ClockHealthRequest { sender_id: 3 })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let foreign_member = PrincipalIdentity::new(
            crate::tls::cluster_id_from_ca(&ca.pem).unwrap(),
            PrincipalId::parse("4").unwrap(),
            [PeerRole::Member],
        )
        .unwrap();
        let mut missing =
            peer_client(issue_principal(&ca, &foreign_member, "node-4.graphrun.local").unwrap())
                .await;
        assert_eq!(
            missing
                .clock_health(ClockHealthRequest { sender_id: 4 })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        leader
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
        leader.remove_voter(3).await.unwrap();
        let mut old_owner = peer_client(materials[2].clone()).await;
        assert_eq!(
            old_owner
                .clock_health(ClockHealthRequest { sender_id: 3 })
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        leader.shutdown().await.unwrap();
        member2.shutdown().await.unwrap();
        member3.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn grpc_client_redirects_follower_reads_and_writes() {
        let (leader, member2, member3, _dirs, addrs, materials, _ca) = three_voters(false).await;
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
        let mut client =
            crate::client::GrpcClient::connect(&format!("https://{}", addrs[1]), &materials[1])
                .await
                .unwrap();
        assert_eq!(client.inspect(run).await.unwrap()["run"], run.to_hex());
        let list = client.list().await.unwrap();
        assert!(
            list.as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["run"] == run.to_hex())
        );
        assert!(
            !client
                .history(run)
                .await
                .unwrap()
                .as_array()
                .unwrap()
                .is_empty()
        );
        let mut follower =
            crate::client::GrpcClient::connect(&format!("https://{}", addrs[2]), &materials[2])
                .await
                .unwrap();
        let other = follower
            .start_with_command(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                &Value::Object(
                    [
                        ("order_id".to_owned(), Value::String("o2".to_owned())),
                        ("amount".to_owned(), Value::Int(2000)),
                    ]
                    .into_iter()
                    .collect(),
                ),
                crate::ids::CommandId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(
            follower.inspect(other).await.unwrap()["run"],
            other.to_hex()
        );
        leader.shutdown().await.unwrap();
        member2.shutdown().await.unwrap();
        member3.shutdown().await.unwrap();
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
        let index = engine1.last_applied_index().await;
        let replicated = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if engine2.last_applied_index().await >= index
                && engine3.last_applied_index().await >= index
            {
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
        assert_eq!(
            returned.inspect(run).await.unwrap_err().kind,
            crate::error::ErrorKind::Unavailable
        );
        let leader_index = leader.last_applied_index().await;
        loop {
            if returned.last_applied_index().await >= leader_index {
                break;
            }
            if tokio::time::Instant::now() >= catch_up {
                panic!("old owner did not catch up");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        returned.shutdown().await.unwrap();
        let restored = crate::engine::replay(dirs[0].path()).unwrap();
        let restored_out = crate::domain::run_output(&restored, run).unwrap();
        assert_eq!(restored_out, output);
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
            .register(register_worker(session, &catalog))
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
                output_schema_digest: assignment.output_schema_digest.clone(),
                ..Default::default()
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
                output_schema_digest: assignment.output_schema_digest.clone(),
                ..Default::default()
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
        let valid_report = crate::generated::ReportRequest {
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
            output_schema_digest: assignment.output_schema_digest.clone(),
            ..Default::default()
        };
        let accepted = client
            .report(valid_report.clone())
            .await
            .unwrap()
            .into_inner();
        assert!(
            accepted.error.is_empty(),
            "valid result must apply: {}",
            accepted.error
        );
        assert!(
            client
                .report(valid_report.clone())
                .await
                .unwrap()
                .into_inner()
                .error
                .is_empty()
        );
        let mut schema_conflict = valid_report.clone();
        schema_conflict.output_schema_digest = "f".repeat(64);
        assert_eq!(
            client.report(schema_conflict).await.unwrap_err().code(),
            tonic::Code::AlreadyExists,
        );
        let mut conflict = valid_report;
        conflict.output_json = serde_json::to_vec(&Value::Null).unwrap();
        assert_eq!(
            client.report(conflict).await.unwrap_err().code(),
            tonic::Code::AlreadyExists,
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
        assert_eq!(
            engine1.inspect(run).await.unwrap_err().kind,
            crate::error::ErrorKind::Unavailable
        );
        let health = engine1.health().await;
        assert_eq!(health["quorum_safe"], false);
        assert_eq!(health["status"], "unavailable");
        engine1.shutdown().await.unwrap();
        let state = crate::engine::replay(engine1.data_dir()).unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            crate::domain::RunStatus::Active
        ));
        assert!(run_output_missing(&state, run));
    }

    fn run_output_missing(state: &crate::domain::State, run: crate::ids::RunId) -> bool {
        crate::domain::run_output(state, run).is_none()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clock_rollback_fails_closed() {
        let ca = generate_ca().unwrap();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut attempts = 0;
        let engine = loop {
            attempts += 1;
            match Engine::member(MemberConfig {
                data_dir: dir.path().to_path_buf(),
                node_id: 1,
                bind: unused_addr(),
                peers: BTreeMap::new(),
                tls: tls.clone(),
                host_activities: true,
                initialize: true,
            })
            .await
            {
                Ok(engine) => break engine,
                Err(error) if error.message.starts_with("member listener") && attempts < 5 => {}
                Err(error) => panic!("clock test member startup: {error}"),
            }
        };
        tokio::time::sleep(Duration::from_millis(150)).await;
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 10_000;
        engine.inject_clock_watermark(future);
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
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(err.to_string().contains("clock unsafe"));
        let health = engine.health().await;
        assert_eq!(health["clock_safe"], false);
        assert_eq!(health["status"], "unavailable");
        engine.clear_clock_watermark();
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clock_fault_persists_until_admin_ack_after_healthy_window() {
        let ca = generate_ca().unwrap();
        let addr = unused_addr();
        let tls = issue_node(&ca, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = MemberConfig {
            data_dir: dir.path().to_path_buf(),
            node_id: 1,
            bind: addr,
            peers: BTreeMap::new(),
            tls: tls.clone(),
            host_activities: false,
            initialize: true,
        };
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let input = Value::Object(
            [
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ]
            .into_iter()
            .collect(),
        );
        let engine = Engine::member(config.clone()).await.unwrap();
        engine
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                input.clone(),
            )
            .await
            .unwrap();
        let watermark = engine.health().await["engine_time_watermark_ms"]
            .as_u64()
            .unwrap();
        engine.inject_clock_watermark(crate::time::wall_millis().unwrap() + 10_000);
        assert_eq!(
            engine
                .start_yaml(
                    include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                    &catalog,
                    input.clone(),
                )
                .await
                .unwrap_err()
                .kind,
            crate::error::ErrorKind::FailedPrecondition
        );
        engine.clear_clock_watermark();
        engine.shutdown().await.unwrap();
        let restored = Engine::member(MemberConfig {
            initialize: false,
            ..config
        })
        .await
        .unwrap();
        assert!(
            restored.health().await["engine_time_watermark_ms"]
                .as_u64()
                .unwrap()
                >= watermark,
            "restart must retain the committed engine-time watermark"
        );
        assert_eq!(
            restored
                .start_yaml(
                    include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                    &catalog,
                    input.clone(),
                )
                .await
                .unwrap_err()
                .kind,
            crate::error::ErrorKind::FailedPrecondition
        );
        let identity = PrincipalIdentity::new(
            crate::tls::cluster_id_from_ca(&ca.pem).unwrap(),
            PrincipalId::parse("operator").unwrap(),
            [PeerRole::Admin],
        )
        .unwrap();
        let mut operator_tls = issue_principal(&ca, &identity, "operator.graphrun.local").unwrap();
        operator_tls.server_name = tls.server_name;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut operator =
            crate::client::GrpcClient::connect(&format!("https://{addr}"), &operator_tls)
                .await
                .unwrap();
        assert_eq!(
            operator
                .acknowledge_clock("clock checked")
                .await
                .unwrap_err()
                .kind,
            crate::error::ErrorKind::FailedPrecondition
        );
        tokio::time::sleep(Duration::from_millis(10_300)).await;
        operator.acknowledge_clock("clock checked").await.unwrap();
        restored
            .start_yaml(
                include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                &catalog,
                input,
            )
            .await
            .unwrap();
        assert!(
            restored.health().await["engine_time_watermark_ms"]
                .as_u64()
                .unwrap()
                >= watermark,
            "acknowledgement cannot lower the committed engine-time watermark"
        );
        restored.shutdown().await.unwrap();
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
            if let Some(survivor) = [&engine2, &engine3, &engine4]
                .into_iter()
                .find(|engine| engine.is_leader())
            {
                if let Ok(state) = survivor.inspect(run).await {
                    if let Some(output) = crate::domain::run_output(&state, run) {
                        break output;
                    }
                }
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
        let mut session = crate::ids::WorkerSessionId::generate();
        let _ = client
            .register(register_worker(session, &catalog))
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
        session = crate::ids::WorkerSessionId::generate();
        let _ = client
            .register(register_worker(session, &catalog))
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
                output_schema_digest: lost2.output_schema_digest.clone(),
                ..Default::default()
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
                output_schema_digest: assignment.output_schema_digest.clone(),
                ..Default::default()
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
