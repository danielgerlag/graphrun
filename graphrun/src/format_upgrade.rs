use crate::error::{Error, ErrorKind, Result};
use openraft::{BasicNode, CommittedLeaderId, LogId, StoredMembership};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const CURRENT_READER: u16 = 5;
pub const CURRENT_WRITER: u16 = 4;
pub const BASE_FORMAT: u16 = 4;
pub const NEXT_FORMAT: u16 = 5;
pub const PEER_WRITER_HEADER: &str = "graphrun-writer-format";
pub const POLICY_FORMAT: &str = "graphrun.writer-policy/v1";
pub const PROOF_FORMAT: &str = "graphrun.reader-proof/v1";
pub const RECEIPT_FORMAT: &str = "graphrun.writer-format-receipt/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub term: u64,
    pub index: u64,
}

impl From<LogId<u64>> for Position {
    fn from(log: LogId<u64>) -> Self {
        Self {
            term: log.leader_id.term,
            index: log.index,
        }
    }
}

impl Position {
    pub fn raft_log_id(self) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(self.term, 0), self.index)
    }
}

pub fn position(id: Option<LogId<u64>>) -> Option<Position> {
    id.map(Position::from)
}

pub const fn base_format() -> u16 {
    BASE_FORMAT
}

pub fn ensure_writer(active: u16) -> Result<()> {
    if active > CURRENT_WRITER {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            format!(
                "binary writer capability {} is below committed writer format {active}",
                CURRENT_WRITER
            ),
        ));
    }
    Ok(())
}

pub fn attach_writer<T>(request: &mut tonic::Request<T>) {
    request.metadata_mut().insert(
        PEER_WRITER_HEADER,
        CURRENT_WRITER
            .to_string()
            .parse()
            .expect("writer format is ASCII"),
    );
}

pub async fn verify_peer_writer<T>(
    storage: &crate::storage::StorageHandle,
    request: &tonic::Request<T>,
) -> std::result::Result<(), tonic::Status> {
    let active = storage
        .writer_format()
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
    if active == BASE_FORMAT {
        return Ok(());
    }
    let writer = request
        .metadata()
        .get(PEER_WRITER_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| tonic::Status::failed_precondition("member writer format is missing"))?;
    if writer < active {
        return Err(tonic::Status::failed_precondition(format!(
            "member writer capability {writer} is below committed format {active}"
        )));
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderProof {
    pub format: String,
    pub cluster_id: String,
    pub member_id: u64,
    pub roster_log_id: Option<Position>,
    pub active_generation: u64,
    pub applied_log_id: Option<Position>,
    pub reader_floor: u16,
    pub command_readers: Vec<u16>,
    pub domain_readers: Vec<String>,
    pub graph_readers: Vec<u32>,
    pub state_records: Vec<String>,
    pub event_readers: Vec<String>,
    pub checkpoint_readers: Vec<String>,
    pub artifact_readers: Vec<String>,
    pub snapshot_readers: Vec<u16>,
}

impl ReaderProof {
    pub fn supports(&self, target: u16) -> bool {
        self.format == PROOF_FORMAT
            && target == NEXT_FORMAT
            && self.reader_floor >= target
            && self.command_readers == [BASE_FORMAT, NEXT_FORMAT]
            && self
                .domain_readers
                .iter()
                .any(|version| version == "graphrun.domain/v1")
            && self.graph_readers.contains(&crate::ir::FORMAT_VERSION)
            && self
                .state_records
                .iter()
                .any(|version| version == crate::record_store::NEXT_FORMAT)
            && self
                .state_records
                .iter()
                .any(|version| version == crate::record_store::FORMAT)
            && self
                .event_readers
                .iter()
                .any(|version| version == crate::history::EVENT_FORMAT)
            && self
                .checkpoint_readers
                .iter()
                .any(|version| version == crate::history::CHECKPOINT_FORMAT)
            && self
                .artifact_readers
                .iter()
                .any(|version| version == crate::history::ARTIFACT_FORMAT)
            && self.snapshot_readers.contains(&1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatPolicy {
    pub format: String,
    pub active_writer: u16,
    pub activation_log_id: Option<Position>,
    pub roster_log_id: Option<Position>,
    pub prepared: BTreeMap<u64, ReaderProof>,
}

impl Default for FormatPolicy {
    fn default() -> Self {
        Self {
            format: POLICY_FORMAT.to_owned(),
            active_writer: BASE_FORMAT,
            activation_log_id: None,
            roster_log_id: None,
            prepared: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatReceipt {
    pub format: String,
    pub principal_id: String,
    pub command_id: crate::ids::CommandId,
    pub request_digest: String,
    pub applied_log_id: Position,
    pub active_writer: u16,
    pub applied: bool,
    pub message: String,
}

pub fn apply(
    policy: &mut FormatPolicy,
    receipts: &mut BTreeMap<String, FormatReceipt>,
    command: &crate::domain::Command,
    roster: &StoredMembership<u64, BasicNode>,
    log_id: LogId<u64>,
    principal: &str,
    cluster_id: &str,
) -> Result<FormatReceipt> {
    use crate::domain::CommandBody;
    let body = match &command.body {
        CommandBody::Authenticated { body, .. } => body.as_ref(),
        body => body,
    };
    let (target, requested_roster) = match body {
        CommandBody::PrepareWriterFormat { roster_log_id, .. } => (NEXT_FORMAT, roster_log_id),
        CommandBody::ActivateWriterFormat {
            roster_log_id,
            target,
        } => (*target, roster_log_id),
        _ => return Err(Error::invalid("not a writer format command")),
    };
    if principal.is_empty() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "format change requires a verified admin principal",
        ));
    }
    let key = format!("{principal}/{}", command.id.to_hex());
    let digest = match body {
        CommandBody::PrepareWriterFormat { .. } => {
            request_digest(&("prepare", target, requested_roster))?
        }
        CommandBody::ActivateWriterFormat { .. } => {
            request_digest(&("activate", target, requested_roster))?
        }
        _ => unreachable!("format operation checked above"),
    };
    if let Some(receipt) = receipts.get(&key) {
        if receipt.request_digest != digest {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "format command identity reused with a different request",
            ));
        }
        return Ok(receipt.clone());
    }
    let attempt = (|| -> Result<()> {
        if principal.is_empty()
            || target != NEXT_FORMAT
            || *requested_roster != position(*roster.log_id())
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "writer format target or committed roster changed",
            ));
        }
        match body {
            CommandBody::PrepareWriterFormat { proofs, .. } => {
                validate_roster(roster, proofs, target, cluster_id)?;
                policy.roster_log_id = position(*roster.log_id());
                policy.prepared = proofs
                    .iter()
                    .map(|proof| (proof.member_id, proof.clone()))
                    .collect();
                Ok(())
            }
            CommandBody::ActivateWriterFormat { .. } => {
                if policy.active_writer == NEXT_FORMAT {
                    return Err(Error::new(
                        ErrorKind::FailedPrecondition,
                        "writer format activation is monotonic",
                    ));
                }
                if policy.roster_log_id != position(*roster.log_id()) {
                    return Err(Error::new(
                        ErrorKind::FailedPrecondition,
                        "reader readiness belongs to an older roster",
                    ));
                }
                let proofs: Vec<_> = policy.prepared.values().cloned().collect();
                validate_roster(roster, &proofs, target, cluster_id)?;
                policy.active_writer = target;
                policy.activation_log_id = Some(log_id.into());
                Ok(())
            }
            _ => unreachable!("format operation checked above"),
        }
    })();
    let receipt = FormatReceipt {
        format: RECEIPT_FORMAT.to_owned(),
        principal_id: principal.to_owned(),
        command_id: command.id,
        request_digest: digest,
        applied_log_id: log_id.into(),
        active_writer: policy.active_writer,
        applied: attempt.is_ok(),
        message: attempt.err().map_or(String::new(), |err| err.message),
    };
    receipts.insert(key, receipt.clone());
    Ok(receipt)
}

pub fn request_digest<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    let bytes = crate::value::canonical_json(&value)?;
    let mut sha = Sha256::new();
    sha.update(b"graphrun.writer-activation/v1\0");
    sha.update(&bytes);
    Ok(hex::encode(sha.finalize()))
}

pub fn validate_roster(
    roster: &StoredMembership<u64, BasicNode>,
    proofs: &[ReaderProof],
    target: u16,
    cluster_id: &str,
) -> Result<()> {
    if proofs.len() != roster.membership().nodes().count() {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "every configured data-bearing voter and learner must prepare readers",
        ));
    }

    let mut seen = BTreeSet::new();
    for proof in proofs {
        if !seen.insert(proof.member_id)
            || proof.cluster_id != cluster_id
            || proof.roster_log_id != position(*roster.log_id())
            || proof.active_generation == 0
            || !proof.supports(target)
            || !roster
                .membership()
                .nodes()
                .any(|(member, _)| *member == proof.member_id)
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "reader preparation does not match the committed roster and required versions",
            ));
        }
    }
    for voters in roster.membership().get_joint_config() {
        if voters.is_empty() || !voters.iter().all(|member| seen.contains(member)) {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "joint voter configuration has an unprepared member",
            ));
        }
    }
    Ok(())
}

pub fn validate_persisted_state(state: &crate::domain::State) -> Result<()> {
    let policy = &state.format_policy;
    if policy.format != POLICY_FORMAT
        || !matches!(policy.active_writer, BASE_FORMAT | NEXT_FORMAT)
        || (policy.active_writer == NEXT_FORMAT && policy.activation_log_id.is_none())
        || policy.prepared.iter().any(|(id, proof)| {
            *id != proof.member_id
                || proof.format != PROOF_FORMAT
                || proof.cluster_id != state.current_cluster_id
                || proof.roster_log_id != policy.roster_log_id
        })
        || state.format_receipts.iter().any(|(key, receipt)| {
            receipt.format != RECEIPT_FORMAT
                || *key != format!("{}/{}", receipt.principal_id, receipt.command_id.to_hex())
                || receipt.request_digest.len() != 64
                || hex::decode(&receipt.request_digest).is_err()
        })
    {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "unsupported or inconsistent retained writer-format policy",
        ));
    }
    Ok(())
}

pub async fn status(
    raft: &openraft::Raft<crate::storage::TypeConfig>,
    storage: &crate::storage::StorageHandle,
) -> Result<serde_json::Value> {
    crate::write::linearizable_read(raft).await?;
    let metrics = raft.metrics().borrow().clone();
    let roster = storage.applied_membership().await?;
    if roster != *metrics.membership_config {
        return Err(Error::new(
            ErrorKind::Unavailable,
            "applied roster differs from the active Raft membership",
        ));
    }
    let policy = storage.query_state().await.format_policy;
    let local_writer = storage.writer_format().await?;
    if policy.active_writer != local_writer {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "committed writer policy differs from the local store manifest",
        ));
    }
    Ok(serde_json::json!({
        "writer_format": policy.active_writer,
        "local_writer_capability": CURRENT_WRITER,
        "reader_capability": CURRENT_READER,
        "roster_log_id": roster.log_id(),
        "joint_voters": roster.membership().get_joint_config(),
        "configured_members": roster.membership().nodes().map(|(id, node)| (*id, &node.addr)).collect::<BTreeMap<_, _>>(),
        "prepared": policy.prepared,
        "activation_log_id": policy.activation_log_id,
        "quorum": true
    }))
}

pub async fn membership_barrier(
    raft: &openraft::Raft<crate::storage::TypeConfig>,
    storage: &crate::storage::StorageHandle,
) -> Result<FormatPolicy> {
    crate::write::linearizable_read(raft).await?;
    let metrics = raft.metrics().borrow().clone();
    if metrics.last_applied.map_or(0, |id| id.index) < metrics.last_log_index.unwrap_or(0)
        || storage.applied_membership().await? != *metrics.membership_config
    {
        return Err(Error::new(
            ErrorKind::Unavailable,
            "membership change waits for all pending entries and the applied roster",
        ));
    }
    let policy = storage.query_state().await.format_policy;
    if policy.active_writer != storage.writer_format().await? {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "committed writer policy differs from the local store manifest",
        ));
    }
    ensure_writer(policy.active_writer)?;
    Ok(policy)
}

pub async fn verify_candidate(
    raft: &openraft::Raft<crate::storage::TypeConfig>,
    storage: &crate::storage::StorageHandle,
    network: Option<&crate::cluster::ClusterNetwork>,
    id: u64,
    endpoint: &str,
) -> Result<()> {
    let policy = membership_barrier(raft, storage).await?;
    let network = network
        .ok_or_else(|| Error::new(ErrorKind::FailedPrecondition, "cluster network unavailable"))?;
    let cluster = storage.query_state().await.current_cluster_id;
    network
        .candidate_readers(id, endpoint, policy.active_writer, &cluster)
        .await
        .map_err(|err| {
            Error::new(
                ErrorKind::FailedPrecondition,
                format!("candidate {id} cannot join the active writer format: {err}"),
            )
        })
}

pub async fn prepare(
    raft: &openraft::Raft<crate::storage::TypeConfig>,
    storage: &crate::storage::StorageHandle,
    network: Option<&crate::cluster::ClusterNetwork>,
    auth: &crate::publication::AuthContext,
    command_id: crate::ids::CommandId,
    target: u16,
) -> Result<FormatReceipt> {
    let _guard = storage.format_admin_guard().await;
    if target != NEXT_FORMAT {
        return Err(Error::invalid("unsupported target writer format"));
    }
    crate::write::linearizable_read(raft).await?;
    let metrics = raft.metrics().borrow().clone();
    let roster = storage.applied_membership().await?;
    if roster != *metrics.membership_config {
        return Err(Error::new(
            ErrorKind::Unavailable,
            "applied membership differs from the Raft roster",
        ));
    }
    let expected = *roster.log_id();
    let members: Vec<_> = roster
        .membership()
        .nodes()
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();
    for (id, endpoint) in &members {
        if *id == metrics.id {
            storage.probe_format_readers(target, expected).await?;
        } else {
            let net = network.ok_or_else(|| {
                Error::new(ErrorKind::FailedPrecondition, "cluster network unavailable")
            })?;
            net.reader_request(*id, endpoint, expected, target, false)
                .await
                .map_err(|err| {
                    Error::new(
                        ErrorKind::Unavailable,
                        format!("member {id} cannot prove installed readers: {err}"),
                    )
                })?;
        }
    }
    let mut proofs = Vec::with_capacity(members.len());
    for (id, endpoint) in members {
        let proof = if id == metrics.id {
            storage.prepare_format_readers(target, expected).await?
        } else {
            network
                .expect("remote member has a cluster network")
                .reader_request(id, &endpoint, expected, target, true)
                .await
                .map_err(|err| {
                    Error::new(
                        ErrorKind::Unavailable,
                        format!("member {id} did not durably prepare readers: {err}"),
                    )
                })?
        };
        proofs.push(proof);
    }
    let cluster = storage.query_state().await.current_cluster_id;
    validate_roster(&roster, &proofs, target, &cluster)?;
    crate::write::linearizable_read(raft).await?;
    if storage.applied_membership().await? != roster {
        return Err(Error::new(
            ErrorKind::Unavailable,
            "membership changed during reader preparation; retry against current roster",
        ));
    }
    let command = crate::domain::Command {
        id: command_id,
        time: crate::write::now(),
        body: crate::domain::CommandBody::PrepareWriterFormat {
            proofs,
            roster_log_id: position(expected),
        },
    }
    .authenticated_context(auth);
    format_reply(crate::write::write_raft_response(raft, storage, command).await?)
}

pub async fn activate(
    raft: &openraft::Raft<crate::storage::TypeConfig>,
    storage: &crate::storage::StorageHandle,
    auth: &crate::publication::AuthContext,
    command_id: crate::ids::CommandId,
    target: u16,
) -> Result<FormatReceipt> {
    let _guard = storage.format_admin_guard().await;
    if target != NEXT_FORMAT || CURRENT_WRITER < target {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "activation requires the new writer binary and a supported target format",
        ));
    }
    crate::write::linearizable_read(raft).await?;
    let metrics = raft.metrics().borrow().clone();
    let roster = storage.applied_membership().await?;
    if roster != *metrics.membership_config {
        return Err(Error::new(
            ErrorKind::Unavailable,
            "applied membership differs from the Raft roster",
        ));
    }
    let command = crate::domain::Command {
        id: command_id,
        time: crate::write::now(),
        body: crate::domain::CommandBody::ActivateWriterFormat {
            roster_log_id: position(*roster.log_id()),
            target,
        },
    }
    .authenticated_context(auth);
    format_reply(crate::write::write_raft_response(raft, storage, command).await?)
}

fn format_reply(response: crate::storage::RaftResponse) -> Result<FormatReceipt> {
    if let Some(receipt) = response.format_receipt {
        return Ok(receipt);
    }
    if let Some(error) = response.error {
        return Err(Error::new(
            response.error_kind.unwrap_or(ErrorKind::FailedPrecondition),
            error,
        ));
    }
    Err(Error::new(
        ErrorKind::Unavailable,
        "format command outcome unknown; retry using the same command ID",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Command, CommandBody};
    use crate::ids::CommandId;
    use crate::time::EngineTime;
    use openraft::{CommittedLeaderId, Membership};

    fn roster() -> StoredMembership<u64, BasicNode> {
        let members = (1..=4)
            .map(|id| (id, BasicNode::new(format!("127.0.0.1:{}", 9000 + id))))
            .collect::<BTreeMap<_, _>>();
        StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(2, 1), 12)),
            Membership::new(
                vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3, 4])],
                members,
            ),
        )
    }

    fn proof(member: u64, roster: &StoredMembership<u64, BasicNode>) -> ReaderProof {
        ReaderProof {
            format: PROOF_FORMAT.to_owned(),
            cluster_id: "11111111111111111111111111111111".to_owned(),
            member_id: member,
            roster_log_id: position(*roster.log_id()),
            active_generation: 1,
            applied_log_id: Some(LogId::new(CommittedLeaderId::new(2, 1), 12).into()),
            reader_floor: 5,
            command_readers: vec![4, 5],
            domain_readers: vec!["graphrun.domain/v1".to_owned()],
            graph_readers: vec![crate::ir::FORMAT_VERSION],
            state_records: vec![
                crate::record_store::FORMAT.to_owned(),
                crate::record_store::NEXT_FORMAT.to_owned(),
            ],
            event_readers: vec![crate::history::EVENT_FORMAT.to_owned()],
            checkpoint_readers: vec![crate::history::CHECKPOINT_FORMAT.to_owned()],
            artifact_readers: vec![crate::history::ARTIFACT_FORMAT.to_owned()],
            snapshot_readers: vec![1],
        }
    }

    #[test]
    fn requires_every_joint_voter_and_learner_from_exact_roster() {
        let roster = roster();
        let cluster = "11111111111111111111111111111111";
        let mut proofs = (1..=4).map(|id| proof(id, &roster)).collect::<Vec<_>>();
        validate_roster(&roster, &proofs, 5, cluster).unwrap();
        proofs.pop();
        assert_eq!(
            validate_roster(&roster, &proofs, 5, cluster)
                .unwrap_err()
                .kind,
            ErrorKind::FailedPrecondition
        );
        proofs.push(proof(4, &roster));
        proofs[0].roster_log_id = None;
        assert_eq!(
            validate_roster(&roster, &proofs, 5, cluster)
                .unwrap_err()
                .kind,
            ErrorKind::FailedPrecondition
        );
        proofs[0] = proof(1, &roster);
        proofs[3].state_records.pop();
        assert_eq!(
            validate_roster(&roster, &proofs, 5, cluster)
                .unwrap_err()
                .kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn activation_is_committed_monotonic_and_idempotent() {
        let roster = roster();
        let proofs = (1..=4).map(|id| proof(id, &roster)).collect();
        let mut policy = FormatPolicy::default();
        let mut receipts = BTreeMap::new();
        let prepare = Command {
            id: CommandId::from_bytes([11; 16]),
            time: EngineTime::from_millis(1),
            body: CommandBody::Authenticated {
                principal_id: "operator".to_owned(),
                body: Box::new(CommandBody::PrepareWriterFormat {
                    proofs,
                    roster_log_id: position(*roster.log_id()),
                }),
            },
        };
        let prepared = apply(
            &mut policy,
            &mut receipts,
            &prepare,
            &roster,
            LogId::new(CommittedLeaderId::new(2, 1), 13),
            "operator",
            "11111111111111111111111111111111",
        )
        .unwrap();
        assert!(prepared.applied);
        let activated_at = LogId::new(CommittedLeaderId::new(2, 1), 14);
        let activate = Command {
            id: CommandId::from_bytes([12; 16]),
            time: EngineTime::from_millis(2),
            body: CommandBody::Authenticated {
                principal_id: "operator".to_owned(),
                body: Box::new(CommandBody::ActivateWriterFormat {
                    target: 5,
                    roster_log_id: position(*roster.log_id()),
                }),
            },
        };
        let first = apply(
            &mut policy,
            &mut receipts,
            &activate,
            &roster,
            activated_at,
            "operator",
            "11111111111111111111111111111111",
        )
        .unwrap();
        assert!(first.applied);
        assert_eq!(policy.active_writer, 5);
        assert_eq!(policy.activation_log_id, Some(activated_at.into()));
        let retry = apply(
            &mut policy,
            &mut receipts,
            &activate,
            &roster,
            LogId::new(CommittedLeaderId::new(2, 1), 15),
            "operator",
            "11111111111111111111111111111111",
        )
        .unwrap();
        assert_eq!(retry, first);
        let duplicate = Command {
            body: CommandBody::Authenticated {
                principal_id: "operator".to_owned(),
                body: Box::new(CommandBody::ActivateWriterFormat {
                    target: 4,
                    roster_log_id: position(*roster.log_id()),
                }),
            },
            ..activate
        };
        assert_eq!(
            apply(
                &mut policy,
                &mut receipts,
                &duplicate,
                &roster,
                activated_at,
                "operator",
                "11111111111111111111111111111111",
            )
            .unwrap_err()
            .kind,
            ErrorKind::AlreadyExists
        );
    }
}
