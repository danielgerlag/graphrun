use crate::binding::{Binding, Condition, Reference};
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{
    ActivationId, AttemptNo, CommandId, EffectKey, EventId, ExecutionRole, LeaseRevision, NodeKey,
    OwnerGeneration, RunId, RunSequence, ScopeId, WaitId, WorkerSessionId,
};
use crate::ir::{ConsumeFrom, Definition, FailError, Node, Region};
use crate::policy::CapturedRunPolicy;
use crate::time::EngineTime;
use crate::value::Value;
use serde::{Deserialize, Serialize};
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

pub static READY_SCANS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DomainEvent {
    RunAdmitted {
        run: RunId,
        definition_id: String,
        definition_version: u32,
        input: Value,
        root: ScopeId,
        policy: CapturedRunPolicy,
        #[serde(default)]
        admitted_ms: u64,
    },
    ScopeOpened {
        run: RunId,
        scope: ScopeId,
        parent: Option<ActivationId>,
        role: ScopeRole,
        input: Value,
    },
    ScopeCompleted {
        run: RunId,
        scope: ScopeId,
        output: Value,
    },
    ScopeFailed {
        run: RunId,
        scope: ScopeId,
        error: FailError,
    },
    ActivationOpened {
        run: RunId,
        scope: ScopeId,
        activation: ActivationId,
        node: String,
    },
    NodeOutputRecorded {
        run: RunId,
        scope: ScopeId,
        node: String,
        output: Value,
    },
    GuardRecorded {
        run: RunId,
        activation: ActivationId,
        result: bool,
        index: u32,
        carry: Value,
    },
    LeafSucceeded {
        run: RunId,
        activation: ActivationId,
        attempt: AttemptNo,
        output: Value,
        role: ExecutionRole,
    },
    LeafFailed {
        run: RunId,
        activation: ActivationId,
        attempt: AttemptNo,
        code: String,
        message: String,
        retry: bool,
    },
    WaitOpened {
        run: RunId,
        wait: WaitId,
        activation: ActivationId,
        signal: String,
        key: String,
        deadline_ms: Option<u64>,
        consume_from: ConsumeFrom,
    },
    WaitSatisfied {
        run: RunId,
        wait: WaitId,
        event_id: EventId,
        payload: Value,
    },
    WaitTimedOut {
        run: RunId,
        wait: WaitId,
    },
    EventAccepted {
        run: RunId,
        event_id: EventId,
        signal: String,
        key: String,
        payload: Value,
        sequence: u64,
        #[serde(default)]
        accepted_ms: u64,
        #[serde(default)]
        expires_ms: u64,
    },
    EventReserved {
        wait: WaitId,
        event_id: EventId,
    },
    EventExpired {
        run: RunId,
        event_id: EventId,
    },
    ReservationReleased {
        wait: WaitId,
        event_id: EventId,
    },
    ObligationRegistered {
        run: RunId,
        forward: ActivationId,
        handler: String,
        handler_version: u32,
        input: Value,
    },
    ObligationBlocked {
        run: RunId,
        forward: ActivationId,
        handler: String,
        handler_version: u32,
        reason: String,
    },
    CompensationStarted {
        run: RunId,
        saga: ActivationId,
        forward: ActivationId,
        activation: ActivationId,
        handler: String,
        handler_version: u32,
        input: Value,
        error: FailError,
    },
    ObligationTransferred {
        forward: ActivationId,
        from_saga: ActivationId,
        to_saga: ActivationId,
    },
    ObligationReleased {
        run: RunId,
        forward: ActivationId,
    },
    SessionRegistered {
        session: WorkerSessionId,
        activities: Vec<String>,
        capacity: u32,
        expires_ms: u64,
    },
    WorkerRegistered {
        session: WorkerSessionId,
        principal_id: String,
        capabilities: Vec<crate::worker_contract::WorkerCapability>,
        capacity: u32,
        protocol_min: u32,
        protocol_max: u32,
        expires_ms: u64,
    },
    WorkerSessionRenewed {
        session: WorkerSessionId,
        revision: LeaseRevision,
        expires_ms: u64,
    },
    ClaimGranted {
        run: RunId,
        scope: ScopeId,
        activation: ActivationId,
        handler: String,
        handler_version: u32,
        input: Value,
        attempt: u32,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        lease_expiry_ms: u64,
        attempt_deadline_ms: u64,
        effect_key: EffectKey,
        role: ExecutionRole,
    },
    ClaimRenewed {
        activation: ActivationId,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        lease_expiry_ms: u64,
    },
    ClaimCleared {
        activation: ActivationId,
    },
    ReconciliationRecorded {
        run: RunId,
        activation: ActivationId,
        outcome: ReconcileOutcome,
        output: Option<Value>,
        probes: u32,
    },
    ReconciliationFailed {
        run: RunId,
        activation: ActivationId,
        code: String,
        message: String,
    },
    InterventionRequired {
        run: RunId,
        activation: ActivationId,
        reason: String,
    },
    RunSucceeded {
        run: RunId,
        output: Value,
    },
    RunFailed {
        run: RunId,
        error: FailError,
    },
    RecoveryAuthorized {
        reason: String,
    },
    AbortIntent {
        run: RunId,
        saga: ActivationId,
        error: FailError,
    },
    CompensationAbandoned {
        run: RunId,
        reason: String,
        unresolved: Vec<ActivationId>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeRole {
    Root,
    LoopBody {
        activation: ActivationId,
        index: u32,
    },
    ForeachItem {
        activation: ActivationId,
        index: u32,
    },
    ParallelBranch {
        activation: ActivationId,
        name: String,
    },
    ChooseBody {
        activation: ActivationId,
        name: String,
    },
    SagaBody {
        activation: ActivationId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CommandBody {
    Publication {
        key: crate::publication::CommandKey,
        operation: crate::publication::PublicationOperation,
    },
    Start {
        run: RunId,
        definition: Box<Definition>,
        input: Value,
        catalog: Box<Catalog>,
    },
    ReportLeaf {
        run: RunId,
        activation: ActivationId,
        output: Value,
    },
    ReportError {
        run: RunId,
        activation: ActivationId,
        code: String,
        message: String,
    },
    ResolveBlocked {
        run: RunId,
        forward: ActivationId,
        input: Value,
    },
    Signal {
        run: RunId,
        event_id: EventId,
        signal: String,
        key: String,
        payload: Value,
    },
    ResolveTimer {
        run: RunId,
        wait: WaitId,
    },
    Progress {
        run: RunId,
    },
    Cancel {
        run: RunId,
        reason: String,
    },
    RegisterSession {
        session: WorkerSessionId,
        activities: Vec<String>,
        capacity: u32,
    },
    RegisterWorker {
        session: WorkerSessionId,
        principal_id: String,
        capabilities: Vec<crate::worker_contract::WorkerCapability>,
        capacity: u32,
        protocol_min: u32,
        protocol_max: u32,
    },
    RenewWorkerSession {
        session: WorkerSessionId,
        revision: LeaseRevision,
    },
    Claim {
        session: WorkerSessionId,
        capacity: u32,
    },
    Renew {
        session: WorkerSessionId,
        activation: ActivationId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
    },
    ReportAssigned {
        run: RunId,
        activation: ActivationId,
        output: Value,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
    },
    ReportWorker {
        run: RunId,
        activation: ActivationId,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        schema_digest: String,
        result: WorkerResult,
    },
    Reconcile {
        run: RunId,
        activation: ActivationId,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        outcome: ReconcileOutcome,
        output: Option<Value>,
    },
    ReconcileWorker {
        run: RunId,
        activation: ActivationId,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        schema_digest: String,
        result: WorkerProbe,
    },
    AcknowledgeRecovery {
        reason: String,
    },
    AbandonCompensation {
        run: RunId,
        reason: String,
    },
    PruneHistory {
        limit: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    Applied,
    NotApplied,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerResult {
    Success { output: Value },
    Error { code: String, message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerProbe {
    Observed {
        outcome: ReconcileOutcome,
        output: Option<Value>,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub id: CommandId,
    pub body: CommandBody,
    pub time: EngineTime,
}

struct IdGen {
    command: CommandId,
    n: u32,
}

impl IdGen {
    fn new(command: CommandId) -> Self {
        Self { command, n: 0 }
    }

    fn next_bytes(&mut self) -> [u8; 16] {
        let mut hasher = Sha256::new();
        hasher.update(self.command.as_bytes());
        hasher.update(self.n.to_le_bytes());
        self.n += 1;
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes
    }

    fn scope(&mut self) -> ScopeId {
        ScopeId::from_bytes(self.next_bytes())
    }

    fn activation(&mut self) -> ActivationId {
        ActivationId::from_bytes(self.next_bytes())
    }

    fn wait(&mut self) -> WaitId {
        WaitId::from_bytes(self.next_bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Active,
    Succeeded { output: Value },
    Failed { error: FailError },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunState {
    pub id: RunId,
    pub definition: Definition,
    pub catalog: Catalog,
    pub input: Value,
    pub policy: CapturedRunPolicy,
    pub status: RunStatus,
    pub root: ScopeId,
    pub next_sequence: RunSequence,
    #[serde(default)]
    pub admitted_ms: u64,
    #[serde(default)]
    pub terminal_ms: u64,
    #[serde(default)]
    pub published: Option<crate::publication::PinnedPublication>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeStatus {
    Open,
    Completed { output: Value },
    Failed { error: FailError },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScopeState {
    pub id: ScopeId,
    pub run: RunId,
    pub parent: Option<ActivationId>,
    pub role: ScopeRole,
    pub input: Value,
    pub status: ScopeStatus,
    pub outputs: BTreeMap<String, Value>,
    pub current: Option<NodeKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivationStatus {
    Open,
    Ready,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivationState {
    pub id: ActivationId,
    pub run: RunId,
    pub scope: ScopeId,
    pub node: NodeKey,
    pub status: ActivationStatus,
    #[serde(default)]
    pub role: ExecutionRole,
    #[serde(default)]
    pub claim: Option<ClaimState>,
    #[serde(default)]
    pub attempts: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClaimState {
    pub session: WorkerSessionId,
    pub generation: OwnerGeneration,
    pub revision: LeaseRevision,
    pub lease_expiry_ms: u64,
    pub attempt_deadline_ms: u64,
    pub effect_key: EffectKey,
    #[serde(default)]
    pub role: ExecutionRole,
    #[serde(default)]
    pub probes: u32,
    #[serde(default)]
    pub unknown_reported: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSession {
    pub id: WorkerSessionId,
    pub activities: Vec<String>,
    pub capacity: u32,
    pub expires_ms: u64,
    #[serde(default)]
    pub principal_id: String,
    #[serde(default)]
    pub capabilities: Vec<crate::worker_contract::WorkerCapability>,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub protocol_min: u32,
    #[serde(default)]
    pub protocol_max: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObligationStatus {
    Open,
    Compensating { activation: ActivationId },
    Compensated,
    Released,
    Blocked { reason: String },
    Irreversible { reason: String },
    Abandoned,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Obligation {
    pub forward: ActivationId,
    pub run: RunId,
    pub owner: ActivationId,
    pub handler: String,
    pub handler_version: u32,
    pub input: Value,
    pub status: ObligationStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitState {
    pub id: WaitId,
    pub run: RunId,
    pub activation: ActivationId,
    pub signal: String,
    pub key: String,
    pub deadline_ms: Option<u64>,
    pub consume_from: ConsumeFrom,
    pub opened_sequence: u64,
    pub pending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboxEntry {
    pub event_id: EventId,
    pub signal: String,
    pub key: String,
    pub payload: Value,
    pub sequence: u64,
    pub reserved_wait: Option<WaitId>,
    pub consumed: bool,
    #[serde(default)]
    pub accepted_ms: u64,
    #[serde(default)]
    pub expires_ms: u64,
    #[serde(default)]
    pub run: Option<RunId>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub engine_time_watermark_ms: u64,
    pub runs: HashMap<RunId, RunState>,
    pub scopes: HashMap<ScopeId, ScopeState>,
    pub activations: HashMap<ActivationId, ActivationState>,
    pub waits: HashMap<WaitId, WaitState>,
    pub inbox: Vec<InboxEntry>,
    pub commands: HashMap<CommandId, Vec<DomainEvent>>,
    #[serde(default)]
    pub legacy_start_digests: HashMap<CommandId, String>,
    #[serde(default)]
    pub worker_command_digests: HashMap<CommandId, String>,
    #[serde(default)]
    pub published_catalogs: BTreeMap<u32, crate::publication::PublishedCatalog>,
    #[serde(default)]
    pub published_definitions:
        BTreeMap<String, BTreeMap<u32, crate::publication::PublishedDefinition>>,
    #[serde(default)]
    pub start_keys: BTreeMap<String, BTreeMap<String, crate::publication::StartKeyRecord>>,
    #[serde(default)]
    pub command_results: BTreeMap<String, crate::publication::CommandResult>,
    pub loop_carry: HashMap<ActivationId, (Value, u32)>,
    pub foreach_items: HashMap<ActivationId, Vec<Value>>,
    pub foreach_done: HashMap<ActivationId, Vec<(u32, Value)>>,
    pub parallel_done: HashMap<ActivationId, BTreeMap<String, Value>>,
    #[serde(default)]
    pub history: HashMap<RunId, Vec<DomainEvent>>,
    #[serde(default)]
    pub history_records: HashMap<RunId, Vec<crate::history::HistoryEntry>>,
    #[serde(default)]
    pub history_dependencies: HashMap<RunId, crate::history::RequiredArtifacts>,
    #[serde(default)]
    pub checkpoints: HashMap<RunId, crate::history::RunCheckpoint>,
    #[serde(default)]
    pub terminal_summaries: HashMap<RunId, crate::history::TerminalSummary>,
    #[serde(default)]
    pub signal_tombstones: HashMap<String, crate::history::SignalTombstone>,
    #[serde(default)]
    pub command_times: HashMap<CommandId, u64>,
    #[serde(default)]
    pub obligations: Vec<Obligation>,
    #[serde(default)]
    pub saga_errors: HashMap<ActivationId, FailError>,
    #[serde(default)]
    pub sessions: HashMap<WorkerSessionId, WorkerSession>,
    #[serde(default)]
    pub next_generation: u64,
    #[serde(default)]
    pub interventions: HashMap<ActivationId, String>,
    #[serde(default)]
    pub recovery: Option<RecoveryHold>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryHold {
    pub reason: String,
    pub authorized: bool,
}

#[derive(Clone, Debug)]
pub struct Decision {
    pub events: Vec<DomainEvent>,
}

pub fn decide(state: &State, command: &Command) -> Result<Decision> {
    if let Some(events) = state.commands.get(&command.id) {
        return Ok(Decision {
            events: events.clone(),
        });
    }
    let mut ids = IdGen::new(command.id);
    match &command.body {
        CommandBody::Publication { .. } => {
            Err(Error::invalid("publication requires replicated apply"))
        }
        CommandBody::PruneHistory { .. } => {
            Err(Error::invalid("retention requires replicated apply"))
        }
        CommandBody::Start {
            run,
            definition,
            input,
            catalog: _,
        } => {
            if execution_suspended(state) {
                return Err(Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "execution suspended until recovery is acknowledged",
                ));
            }
            decide_start(*run, definition, input.clone(), command.time, &mut ids)
        }
        CommandBody::ReportLeaf {
            run,
            activation,
            output,
        } => decide_report(
            state,
            *run,
            *activation,
            output.clone(),
            command.time,
            &mut ids,
        ),
        CommandBody::ReportError {
            run,
            activation,
            code,
            message,
        } => decide_report_error(
            state,
            *run,
            *activation,
            code,
            message,
            command.time,
            &mut ids,
        ),
        CommandBody::ResolveBlocked {
            run,
            forward,
            input,
        } => decide_resolve_blocked(state, *run, *forward, input.clone()),
        CommandBody::Signal {
            run,
            event_id,
            signal,
            key,
            payload,
        } => decide_signal(
            state,
            *run,
            *event_id,
            signal,
            key,
            payload.clone(),
            command.time,
            &mut ids,
        ),
        CommandBody::ResolveTimer { run, wait } => {
            decide_timer(state, *run, *wait, command.time, &mut ids)
        }
        CommandBody::Progress { run } => decide_progress(state, *run, command.time, &mut ids),
        CommandBody::Cancel { run, reason } => decide_cancel(state, *run, reason, &mut ids),
        CommandBody::RegisterSession {
            session,
            activities,
            capacity,
        } => decide_register(state, *session, activities, *capacity, command.time),
        CommandBody::RegisterWorker {
            session,
            principal_id,
            capabilities,
            capacity,
            protocol_min,
            protocol_max,
        } => decide_register_worker(
            state,
            *session,
            principal_id,
            capabilities,
            *capacity,
            *protocol_min,
            *protocol_max,
            command.time,
        ),
        CommandBody::RenewWorkerSession { session, revision } => {
            decide_renew_worker_session(state, *session, *revision, command.time)
        }
        CommandBody::Claim { session, capacity } => {
            if execution_suspended(state) {
                return Ok(Decision { events: Vec::new() });
            }
            decide_claim(state, *session, *capacity, command.time, &mut ids)
        }
        CommandBody::Renew {
            session,
            activation,
            generation,
            revision,
        } => decide_renew(
            state,
            *session,
            *activation,
            *generation,
            *revision,
            command.time,
        ),
        CommandBody::ReportAssigned {
            run,
            activation,
            output,
            session,
            generation,
            revision,
        } => decide_report_assigned(
            state,
            *run,
            *activation,
            output.clone(),
            *session,
            *generation,
            *revision,
            command.time,
            &mut ids,
        ),
        CommandBody::ReportWorker {
            run,
            activation,
            session,
            generation,
            revision,
            schema_digest,
            result,
        } => {
            let claim = validate_claim_owner(
                state,
                *run,
                *activation,
                *session,
                *generation,
                *revision,
                command.time,
            )?;
            if claim.role == ExecutionRole::Reconciliation {
                return Err(Error::invalid("reconciliation claim must use reconcile"));
            }
            let (name, version, _) = activity_key(state, *activation)
                .ok_or_else(|| Error::invalid("claimed activity unavailable"))?;
            let catalog = &state
                .runs
                .get(run)
                .ok_or_else(|| Error::invalid("unknown run"))?
                .catalog;
            let key = crate::ids::ActivityKey::new(name, version);
            let capability = crate::worker_contract::capability_for(catalog, &key, claim.role)?;
            if &capability.output_schema_digest != schema_digest {
                return Err(Error::invalid("worker result schema digest mismatch"));
            }
            match result {
                WorkerResult::Success { output } => decide_report_assigned(
                    state,
                    *run,
                    *activation,
                    output.clone(),
                    *session,
                    *generation,
                    *revision,
                    command.time,
                    &mut ids,
                ),
                WorkerResult::Error { code, message } => {
                    if message.is_empty()
                        || (catalog.activity(&key)?.known_code(code).is_none()
                            && !code.starts_with("worker."))
                    {
                        return Err(Error::invalid(format!(
                            "undeclared handler error code {code}"
                        )));
                    }
                    decide_report_error(
                        state,
                        *run,
                        *activation,
                        code,
                        message,
                        command.time,
                        &mut ids,
                    )
                }
            }
        }
        CommandBody::AcknowledgeRecovery { reason } => decide_acknowledge_recovery(state, reason),
        CommandBody::AbandonCompensation { run, reason } => {
            decide_abandon_compensation(state, *run, reason)
        }
        CommandBody::Reconcile {
            run,
            activation,
            session,
            generation,
            revision,
            outcome,
            output,
        } => decide_reconcile(
            state,
            *run,
            *activation,
            *session,
            *generation,
            *revision,
            *outcome,
            output.clone(),
            command.time,
            &mut ids,
        ),
        CommandBody::ReconcileWorker {
            run,
            activation,
            session,
            generation,
            revision,
            schema_digest,
            result,
        } => {
            let claim = validate_claim_owner(
                state,
                *run,
                *activation,
                *session,
                *generation,
                *revision,
                command.time,
            )?;
            if claim.role != ExecutionRole::Reconciliation {
                return Err(Error::invalid("claim is not a reconciliation probe"));
            }
            let (name, version, _) = activity_key(state, *activation)
                .ok_or_else(|| Error::invalid("claimed reconciler unavailable"))?;
            let catalog = &state
                .runs
                .get(run)
                .ok_or_else(|| Error::invalid("unknown run"))?
                .catalog;
            let capability = crate::worker_contract::capability_for(
                catalog,
                &crate::ids::ActivityKey::new(name, version),
                claim.role,
            )?;
            if &capability.output_schema_digest != schema_digest {
                return Err(Error::invalid("worker probe schema digest mismatch"));
            }
            let (outcome, output, error) = match result {
                WorkerProbe::Observed { outcome, output } => (*outcome, output.clone(), None),
                WorkerProbe::Error { code, message } if !code.is_empty() && !message.is_empty() => {
                    (ReconcileOutcome::Unknown, None, Some((code, message)))
                }
                WorkerProbe::Error { .. } => {
                    return Err(Error::invalid("probe error requires code and message"));
                }
            };
            let mut decision = decide_reconcile(
                state,
                *run,
                *activation,
                *session,
                *generation,
                *revision,
                outcome,
                output,
                command.time,
                &mut ids,
            )?;
            if let Some((code, message)) = error {
                decision.events.insert(
                    0,
                    DomainEvent::ReconciliationFailed {
                        run: *run,
                        activation: *activation,
                        code: code.clone(),
                        message: message.clone(),
                    },
                );
            }
            Ok(decision)
        }
    }
}

fn execution_suspended(state: &State) -> bool {
    state.recovery.as_ref().is_some_and(|hold| !hold.authorized)
}

fn decide_acknowledge_recovery(state: &State, reason: &str) -> Result<Decision> {
    let Some(hold) = &state.recovery else {
        return Err(Error::invalid("no recovery hold"));
    };
    if hold.authorized {
        return Ok(Decision { events: Vec::new() });
    }
    if reason.is_empty() {
        return Err(Error::invalid("recovery acknowledgement requires a reason"));
    }
    Ok(Decision {
        events: vec![DomainEvent::RecoveryAuthorized {
            reason: reason.to_owned(),
        }],
    })
}

fn decide_start(
    run: RunId,
    definition: &Definition,
    input: Value,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let root = ids.scope();
    let start_act = ids.activation();
    let start = definition.root.start.as_str().to_owned();
    Ok(Decision {
        events: vec![
            DomainEvent::RunAdmitted {
                run,
                definition_id: definition.id.clone(),
                definition_version: definition.version,
                input: input.clone(),
                root,
                policy: CapturedRunPolicy::defaults(definition.run_timeout_ms),
                admitted_ms: time.as_millis(),
            },
            DomainEvent::ScopeOpened {
                run,
                scope: root,
                parent: None,
                role: ScopeRole::Root,
                input,
            },
            DomainEvent::ActivationOpened {
                run,
                scope: root,
                activation: start_act,
                node: start,
            },
        ],
    })
}

fn decide_report(
    state: &State,
    run: RunId,
    activation: ActivationId,
    output: Value,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.run != run {
        return Err(Error::invalid("activation does not belong to run"));
    }
    if act.status != ActivationStatus::Ready {
        return Err(Error::invalid("activation is not awaiting a result"));
    }
    if let Some(claim) = &act.claim {
        if time.as_millis() < claim.lease_expiry_ms {
            return Err(Error::new(
                crate::error::ErrorKind::FailedPrecondition,
                "activation is claimed",
            ));
        }
    }
    report_success(state, run, activation, output, ids)
}

fn report_success(
    state: &State,
    run: RunId,
    activation: ActivationId,
    output: Value,
    ids: &mut IdGen,
) -> Result<Decision> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.role == ExecutionRole::Compensation {
        validate_leaf_output(state, act, &output)?;
        return decide_compensation_result(state, run, activation, output);
    }
    validate_leaf_output(state, act, &output)?;
    if run_is_aborting(state, run) {
        let mut events = vec![DomainEvent::LeafSucceeded {
            run,
            activation,
            attempt: AttemptNo::new(1),
            output: output.clone(),
            role: ExecutionRole::Forward,
        }];
        events.push(DomainEvent::NodeOutputRecorded {
            run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: output.clone(),
        });
        events.extend(compensation_events(state, act, &output));
        return Ok(Decision { events });
    }
    let mut events = vec![DomainEvent::LeafSucceeded {
        run,
        activation,
        attempt: AttemptNo::new(1),
        output: output.clone(),
        role: ExecutionRole::Forward,
    }];
    events.push(DomainEvent::NodeOutputRecorded {
        run,
        scope: act.scope,
        node: act.node.as_str().to_owned(),
        output: output.clone(),
    });
    events.extend(compensation_events(state, act, &output));
    events.extend(follow_next(state, act.scope, &act.node, &output, ids)?);
    Ok(Decision { events })
}

fn decide_report_error(
    state: &State,
    run: RunId,
    activation: ActivationId,
    code: &str,
    message: &str,
    _time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.run != run {
        return Err(Error::invalid("activation does not belong to run"));
    }
    if act.status != ActivationStatus::Ready {
        return Err(Error::invalid("activation is not awaiting a result"));
    }
    let attempt = AttemptNo::new(u64::from(act.attempts.max(1)));
    if act.role == ExecutionRole::Compensation {
        let retryable = compensation_error_retryable(state, activation, code);
        let mut events = vec![DomainEvent::LeafFailed {
            run,
            activation,
            attempt,
            code: code.to_owned(),
            message: message.to_owned(),
            retry: retryable,
        }];
        if retryable {
            events.push(DomainEvent::ClaimCleared { activation });
            return Ok(Decision { events });
        }
        if let Some(obligation) = state.obligations.iter().find(|item| {
            matches!(
                item.status,
                ObligationStatus::Compensating { activation: current } if current == activation
            )
        }) {
            events.push(DomainEvent::ObligationBlocked {
                run,
                forward: obligation.forward,
                handler: obligation.handler.clone(),
                handler_version: obligation.handler_version,
                reason: format!("{code}: {message}"),
            });
        }
        return Ok(Decision { events });
    }
    let retryable = forward_error_retryable(state, act, code);
    let mut events = vec![DomainEvent::LeafFailed {
        run,
        activation,
        attempt,
        code: code.to_owned(),
        message: message.to_owned(),
        retry: retryable,
    }];
    if retryable {
        events.push(DomainEvent::ClaimCleared { activation });
        return Ok(Decision { events });
    }
    events.extend(fail_scope(
        state,
        act.scope,
        FailError {
            code: code.to_owned(),
            message: message.to_owned(),
        },
    )?);
    let _ = ids;
    Ok(Decision { events })
}

fn forward_error_retryable(state: &State, act: &ActivationState, code: &str) -> bool {
    let Some(run) = state.runs.get(&act.run) else {
        return false;
    };
    let Some(region) = region_for_scope(state, act.scope) else {
        return false;
    };
    let Some(Node::Activity {
        activity, retry, ..
    }) = region.nodes.get(act.node.as_str())
    else {
        return false;
    };
    let Ok(contract) = run.catalog.activity(activity) else {
        return false;
    };
    let Some(spec) = contract.known_code(code) else {
        return false;
    };
    if !spec.retryable {
        return false;
    }
    if !retry.errors.contains(&code.to_owned()) {
        return false;
    }
    let attempts = act.attempts.max(1);
    attempts < retry.max_attempts
}

fn compensation_error_retryable(state: &State, activation: ActivationId, code: &str) -> bool {
    let Some(act) = state.activations.get(&activation) else {
        return false;
    };
    let Some(run) = state.runs.get(&act.run) else {
        return false;
    };
    let Some(obligation) = state.obligations.iter().find(|item| {
        matches!(
            item.status,
            ObligationStatus::Compensating { activation: current } if current == activation
        )
    }) else {
        return false;
    };
    let Some(forward) = state.activations.get(&obligation.forward) else {
        return obligation_handler_retryable(run, obligation, code);
    };
    let Some(region) = region_for_scope(state, forward.scope) else {
        return obligation_handler_retryable(run, obligation, code);
    };
    let Some(Node::Activity {
        compensation:
            Some(crate::ir::Compensation::Activity {
                retry, activity, ..
            }),
        ..
    }) = region.nodes.get(forward.node.as_str())
    else {
        return obligation_handler_retryable(run, obligation, code);
    };
    let Ok(contract) = run.catalog.activity(activity) else {
        return false;
    };
    let Some(spec) = contract.known_code(code) else {
        return false;
    };
    if !spec.retryable {
        return false;
    }
    let policy = retry.clone().unwrap_or_else(|| {
        crate::policy::RetryPolicy::compensation_default(contract.retryable_codes())
    });
    if policy.errors.is_empty() {
        return false;
    }
    policy.errors.contains(&code.to_owned()) && act.attempts.max(1) < policy.max_attempts
}

fn obligation_handler_retryable(run: &RunState, obligation: &Obligation, code: &str) -> bool {
    let key = crate::ids::ActivityKey::new(&obligation.handler, obligation.handler_version);
    let Ok(contract) = run.catalog.activity(&key) else {
        return false;
    };
    contract.known_code(code).is_some_and(|spec| spec.retryable)
}

fn decide_abandon_compensation(state: &State, run: RunId, reason: &str) -> Result<Decision> {
    if reason.is_empty() {
        return Err(Error::invalid("abandon requires a reason"));
    }
    let run_state = state
        .runs
        .get(&run)
        .ok_or_else(|| Error::invalid("unknown run"))?;
    if matches!(
        &run_state.status,
        RunStatus::Failed { error } if error.code == "saga.abandoned"
    ) {
        return Ok(Decision { events: Vec::new() });
    }
    let unresolved: Vec<ActivationId> = state
        .obligations
        .iter()
        .filter(|item| {
            item.run == run
                && !matches!(
                    item.status,
                    ObligationStatus::Compensated
                        | ObligationStatus::Released
                        | ObligationStatus::Abandoned
                )
        })
        .map(|item| item.forward)
        .collect();
    if unresolved.is_empty() {
        return Err(Error::invalid("no unresolved obligations to abandon"));
    }
    let mut events = vec![DomainEvent::CompensationAbandoned {
        run,
        reason: reason.to_owned(),
        unresolved: unresolved.clone(),
    }];
    if matches!(run_state.status, RunStatus::Active) {
        if let Some(root) = state
            .scopes
            .values()
            .find(|scope| scope.run == run && matches!(scope.role, ScopeRole::Root))
        {
            events.extend(fail_scope(
                state,
                root.id,
                FailError {
                    code: "saga.abandoned".to_owned(),
                    message: format!(
                        "unresolved obligations remain after operator abandonment: {}",
                        unresolved
                            .iter()
                            .map(ActivationId::to_hex)
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                },
            )?);
        }
    }
    Ok(Decision { events })
}

fn decide_resolve_blocked(
    state: &State,
    run: RunId,
    forward: ActivationId,
    input: Value,
) -> Result<Decision> {
    let Some(obligation) = state
        .obligations
        .iter()
        .find(|item| item.forward == forward)
    else {
        return Err(Error::invalid("unknown obligation"));
    };
    if obligation.run != run {
        return Err(Error::invalid("obligation does not belong to run"));
    }
    if !matches!(obligation.status, ObligationStatus::Blocked { .. }) {
        return Err(Error::invalid("obligation is not blocked"));
    }
    Ok(Decision {
        events: vec![DomainEvent::ObligationRegistered {
            run,
            forward,
            handler: obligation.handler.clone(),
            handler_version: obligation.handler_version,
            input,
        }],
    })
}

fn decide_register(
    state: &State,
    session: WorkerSessionId,
    activities: &[String],
    capacity: u32,
    time: EngineTime,
) -> Result<Decision> {
    if capacity == 0 || capacity > crate::limits::CLAIM_BATCH {
        return Err(Error::invalid("invalid worker capacity"));
    }
    if let Some(existing) = state.sessions.get(&session) {
        if existing.expires_ms > time.as_millis() {
            return Ok(Decision { events: Vec::new() });
        }
    }
    let expires_ms = time
        .as_millis()
        .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64);
    Ok(Decision {
        events: vec![DomainEvent::SessionRegistered {
            session,
            activities: activities.to_vec(),
            capacity,
            expires_ms,
        }],
    })
}

fn decide_register_worker(
    state: &State,
    session: WorkerSessionId,
    principal_id: &str,
    capabilities: &[crate::worker_contract::WorkerCapability],
    capacity: u32,
    protocol_min: u32,
    protocol_max: u32,
    time: EngineTime,
) -> Result<Decision> {
    use crate::worker_contract::{CODEC_VERSION, PROTOCOL_VERSION};
    if principal_id.is_empty() || state.sessions.contains_key(&session) {
        return Err(Error::invalid(
            "worker session identity must be new and authenticated",
        ));
    }
    if capacity == 0
        || capacity > crate::limits::MAX_ACTIVE_LEAVES_PER_WORKER
        || capabilities.is_empty()
        || protocol_min > PROTOCOL_VERSION
        || protocol_max < PROTOCOL_VERSION
    {
        return Err(Error::invalid(
            "invalid worker capacity, protocol range, or capabilities",
        ));
    }
    let mut unique = std::collections::HashSet::new();
    for capability in capabilities {
        if capability.codec_version != CODEC_VERSION
            || !unique.insert((
                &capability.activity_name,
                capability.activity_version,
                capability.role as u8,
            ))
        {
            return Err(Error::invalid("duplicate or unsupported worker capability"));
        }
    }
    Ok(Decision {
        events: vec![DomainEvent::WorkerRegistered {
            session,
            principal_id: principal_id.to_owned(),
            capabilities: capabilities.to_vec(),
            capacity,
            protocol_min,
            protocol_max,
            expires_ms: time
                .as_millis()
                .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64),
        }],
    })
}

fn decide_renew_worker_session(
    state: &State,
    session: WorkerSessionId,
    revision: LeaseRevision,
    time: EngineTime,
) -> Result<Decision> {
    let worker = state.sessions.get(&session).ok_or_else(|| {
        Error::new(
            crate::error::ErrorKind::Unauthenticated,
            "unknown worker session",
        )
    })?;
    if worker.principal_id.is_empty()
        || worker.revision != revision.get()
        || time.as_millis() >= worker.expires_ms
    {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale or expired worker session",
        ));
    }
    Ok(Decision {
        events: vec![DomainEvent::WorkerSessionRenewed {
            session,
            revision: revision.next(),
            expires_ms: time
                .as_millis()
                .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64),
        }],
    })
}

fn claimed_activity(
    state: &State,
    activation: ActivationId,
    role: ExecutionRole,
) -> Result<(crate::ids::ActivityKey, Value)> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("missing activation"))?;
    let run = state
        .runs
        .get(&act.run)
        .ok_or_else(|| Error::invalid("missing run"))?;
    match role {
        ExecutionRole::Forward | ExecutionRole::Reconciliation => {
            let region = lookup_region(state, act.scope)
                .ok_or_else(|| Error::invalid("missing activity scope"))?;
            let Node::Activity {
                activity, input, ..
            } = region
                .nodes
                .get(act.node.as_str())
                .ok_or_else(|| Error::invalid("missing activity node"))?
            else {
                return Err(Error::invalid("ready node is not an activity"));
            };
            let scope = state
                .scopes
                .get(&act.scope)
                .ok_or_else(|| Error::invalid("missing activity scope state"))?;
            let bound = eval_binding(input, &eval_ctx(state, scope))?;
            let key = if role == ExecutionRole::Reconciliation {
                run.catalog
                    .activity(activity)?
                    .reconciler
                    .clone()
                    .ok_or_else(|| Error::invalid("missing reconciliation contract"))?
            } else {
                activity.clone()
            };
            Ok((key, bound))
        }
        ExecutionRole::Compensation => {
            let obligation = state.obligations.iter().find(|item| matches!(
                item.status, ObligationStatus::Compensating { activation: current } if current == activation
            )).ok_or_else(|| Error::invalid("missing compensation obligation"))?;
            Ok((
                crate::ids::ActivityKey::new(&obligation.handler, obligation.handler_version),
                obligation.input.clone(),
            ))
        }
    }
}

fn worker_matches(
    state: &State,
    worker: &WorkerSession,
    activation: ActivationId,
    role: ExecutionRole,
) -> Result<bool> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("missing activation"))?;
    let run = state
        .runs
        .get(&act.run)
        .ok_or_else(|| Error::invalid("missing run"))?;
    let (key, _) = claimed_activity(state, activation, role)?;
    if worker.principal_id.is_empty() {
        return Ok(worker
            .activities
            .iter()
            .any(|name| name == "*" || name == &key.name));
    }
    let required = crate::worker_contract::capability_for(&run.catalog, &key, role)?;
    Ok(worker.capabilities.contains(&required))
}

pub fn worker_has_ready(state: &State, session: WorkerSessionId, time: EngineTime) -> Result<bool> {
    let worker = state
        .sessions
        .get(&session)
        .ok_or_else(|| Error::invalid("unknown worker session"))?;
    if worker.principal_id.is_empty() || time.as_millis() >= worker.expires_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "worker session expired",
        ));
    }
    if worker_in_flight(state, session, time) >= worker.capacity {
        return Ok(false);
    }
    for run in active_runs(state) {
        for activation in unclaimed_ready(state, run, time) {
            let act = state
                .activations
                .get(&activation)
                .expect("ready activation");
            let role = grant_role(state, act, time);
            if role == ExecutionRole::Forward
                && (run_past_deadline(state, run, time) || run_is_aborting(state, run))
            {
                continue;
            }
            if worker_matches(state, worker, activation, role)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn worker_in_flight(state: &State, session: WorkerSessionId, time: EngineTime) -> u32 {
    state
        .activations
        .values()
        .filter(|act| {
            act.status == ActivationStatus::Ready
                && act.claim.as_ref().is_some_and(|claim| {
                    claim.session == session && claim.lease_expiry_ms > time.as_millis()
                })
        })
        .count() as u32
}

fn decide_claim(
    state: &State,
    session: WorkerSessionId,
    capacity: u32,
    time: EngineTime,
    _ids: &mut IdGen,
) -> Result<Decision> {
    let worker = state
        .sessions
        .get(&session)
        .ok_or_else(|| Error::new(crate::error::ErrorKind::Unauthenticated, "unknown session"))?;
    if time.as_millis() >= worker.expires_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "session expired",
        ));
    }
    let mut events = if worker.principal_id.is_empty() {
        vec![DomainEvent::SessionRegistered {
            session,
            activities: worker.activities.clone(),
            capacity: worker.capacity,
            expires_ms: time
                .as_millis()
                .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64),
        }]
    } else {
        Vec::new()
    };
    let cap = capacity
        .min(
            worker
                .capacity
                .saturating_sub(worker_in_flight(state, session, time)),
        )
        .min(crate::limits::CLAIM_BATCH);
    let mut granted = 0u32;
    let mut runs = active_runs(state);
    if runs.is_empty() {
        return Ok(Decision { events });
    }
    runs.sort();
    let mut granted_ids = Vec::new();
    while granted < cap {
        let mut progressed = false;
        for run in &runs {
            if granted >= cap {
                break;
            }
            let mut ready = unclaimed_ready(state, *run, time);
            ready.sort();
            let mut selected = None;
            for id in ready {
                if granted_ids.contains(&id) {
                    continue;
                }
                let Some(act) = state.activations.get(&id) else {
                    continue;
                };
                let role = grant_role(state, act, time);
                if role == ExecutionRole::Forward && run_past_deadline(state, *run, time) {
                    continue;
                }
                if role == ExecutionRole::Forward && run_is_aborting(state, *run) {
                    continue;
                }
                if worker_matches(state, worker, id, role)? {
                    selected = Some(id);
                    break;
                }
            }
            let Some(activation) = selected else {
                continue;
            };
            let Some(act) = state.activations.get(&activation) else {
                continue;
            };
            let role = grant_role(state, act, time);
            let (handler, input) = claimed_activity(state, activation, role)?;
            let (input_schema, _) = crate::worker_contract::contract_schemas(
                &state
                    .runs
                    .get(run)
                    .ok_or_else(|| Error::invalid("missing run"))?
                    .catalog,
                &handler,
                role,
            )?;
            state
                .runs
                .get(run)
                .expect("checked run")
                .catalog
                .validate_value(input_schema, &input)?;
            let lease_expiry_ms = time
                .as_millis()
                .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64);
            let timeout = match role {
                ExecutionRole::Reconciliation => crate::policy::RECONCILIATION_ATTEMPT_TIMEOUT,
                ExecutionRole::Compensation => crate::policy::COMPENSATION_ATTEMPT_TIMEOUT,
                ExecutionRole::Forward => crate::policy::FORWARD_ATTEMPT_TIMEOUT,
            };
            let attempt_deadline_ms = time.as_millis().saturating_add(timeout.as_millis() as u64);
            let effect_key = if role == ExecutionRole::Reconciliation {
                act.claim.as_ref().map(|claim| claim.effect_key)
            } else {
                None
            }
            .unwrap_or_else(|| stable_effect_key(*run, activation, role));
            events.push(DomainEvent::ClaimGranted {
                run: *run,
                scope: act.scope,
                activation,
                handler: handler.name,
                handler_version: handler.version,
                input,
                attempt: act.attempts.saturating_add(1),
                session,
                generation: OwnerGeneration::new(
                    state.next_generation.saturating_add(u64::from(granted) + 1),
                ),
                revision: LeaseRevision::new(1),
                lease_expiry_ms,
                attempt_deadline_ms,
                effect_key,
                role,
            });
            granted_ids.push(activation);
            granted += 1;
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
    Ok(Decision { events })
}

fn stable_effect_key(run: RunId, activation: ActivationId, role: ExecutionRole) -> EffectKey {
    let mut hash = Sha256::new();
    hash.update(b"graphrun.effect-key/v1\0");
    hash.update(run.as_bytes());
    hash.update(activation.as_bytes());
    hash.update(crate::worker_contract::role_name(role).as_bytes());
    let bytes: [u8; 16] = hash.finalize()[..16].try_into().expect("sha256 prefix");
    EffectKey::from_bytes(bytes)
}

fn uncertain_forwards(state: &State, run: RunId) -> bool {
    state.activations.values().any(|act| {
        act.run == run
            && act.role == ExecutionRole::Forward
            && act.status == ActivationStatus::Ready
            && (act.claim.is_some() || state.interventions.contains_key(&act.id))
    })
}

fn run_is_aborting(state: &State, run: RunId) -> bool {
    state.saga_errors.keys().any(|saga| {
        state
            .activations
            .get(saga)
            .is_some_and(|act| act.run == run)
    })
}

fn run_past_deadline(state: &State, run: RunId, time: EngineTime) -> bool {
    let Some(run_state) = state.runs.get(&run) else {
        return false;
    };
    let Some(ms) = run_state.policy.run_timeout_ms else {
        return false;
    };
    if run_state.admitted_ms == 0 {
        return false;
    }
    time.as_millis() >= run_state.admitted_ms.saturating_add(ms)
}

fn decide_renew(
    state: &State,
    session: WorkerSessionId,
    activation: ActivationId,
    generation: OwnerGeneration,
    revision: LeaseRevision,
    time: EngineTime,
) -> Result<Decision> {
    let worker = state
        .sessions
        .get(&session)
        .ok_or_else(|| Error::new(crate::error::ErrorKind::Unauthenticated, "unknown session"))?;
    if time.as_millis() >= worker.expires_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "session expired",
        ));
    }
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    let Some(claim) = &act.claim else {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "no claim",
        ));
    };
    if claim.session != session || claim.generation != generation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale claim",
        ));
    }
    if claim.revision != revision {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale revision",
        ));
    }
    if time.as_millis() >= claim.lease_expiry_ms || time.as_millis() >= claim.attempt_deadline_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim expired",
        ));
    }
    let lease_expiry_ms = time
        .as_millis()
        .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64);
    Ok(Decision {
        events: vec![DomainEvent::ClaimRenewed {
            activation,
            session,
            generation,
            revision: revision.next(),
            lease_expiry_ms,
        }],
    })
}

fn decide_report_assigned(
    state: &State,
    run: RunId,
    activation: ActivationId,
    output: Value,
    session: WorkerSessionId,
    generation: OwnerGeneration,
    revision: LeaseRevision,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let claim = validate_claim_owner(state, run, activation, session, generation, revision, time)?;
    if claim.role == ExecutionRole::Reconciliation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "reconciliation claim must use reconcile",
        ));
    }
    report_success(state, run, activation, output, ids)
}

fn validate_claim_owner(
    state: &State,
    run: RunId,
    activation: ActivationId,
    session: WorkerSessionId,
    generation: OwnerGeneration,
    revision: LeaseRevision,
    time: EngineTime,
) -> Result<&ClaimState> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.run != run || act.status != ActivationStatus::Ready {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "activation is not awaiting this result",
        ));
    }
    let claim = act
        .claim
        .as_ref()
        .ok_or_else(|| Error::new(crate::error::ErrorKind::FailedPrecondition, "no claim"))?;
    if claim.session != session || claim.generation != generation || claim.revision != revision {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale claim",
        ));
    }
    let worker = state
        .sessions
        .get(&session)
        .ok_or_else(|| Error::new(crate::error::ErrorKind::Unauthenticated, "unknown session"))?;
    if time.as_millis() >= worker.expires_ms
        || time.as_millis() >= claim.lease_expiry_ms
        || time.as_millis() >= claim.attempt_deadline_ms
    {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim or session expired",
        ));
    }
    Ok(claim)
}

fn grant_role(state: &State, act: &ActivationState, time: EngineTime) -> ExecutionRole {
    if act.role == ExecutionRole::Compensation {
        return ExecutionRole::Compensation;
    }
    let expired = act
        .claim
        .as_ref()
        .is_some_and(|claim| time.as_millis() >= claim.lease_expiry_ms);
    if !expired {
        return ExecutionRole::Forward;
    }
    let Some(run) = state.runs.get(&act.run) else {
        return ExecutionRole::Forward;
    };
    let Some(Node::Activity { activity, .. }) =
        lookup_region(state, act.scope).and_then(|region| region.nodes.get(act.node.as_str()))
    else {
        return ExecutionRole::Forward;
    };
    let Ok(contract) = run.catalog.activity(activity) else {
        return ExecutionRole::Forward;
    };
    if contract.reconciler.is_none() {
        return ExecutionRole::Forward;
    }
    if contract.recovery == crate::catalog::Recovery::Manual || run_is_aborting(state, act.run) {
        ExecutionRole::Reconciliation
    } else {
        ExecutionRole::Forward
    }
}

fn decide_reconcile(
    state: &State,
    run: RunId,
    activation: ActivationId,
    session: WorkerSessionId,
    generation: OwnerGeneration,
    revision: LeaseRevision,
    outcome: ReconcileOutcome,
    output: Option<Value>,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.run != run {
        return Err(Error::invalid("activation does not belong to run"));
    }
    if act.status != ActivationStatus::Ready {
        return Err(Error::invalid("activation is not awaiting a result"));
    }
    let claim = validate_claim_owner(state, run, activation, session, generation, revision, time)?;
    if claim.role != ExecutionRole::Reconciliation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim is not a reconciliation probe",
        ));
    }
    if (outcome == ReconcileOutcome::Applied) != output.is_some() {
        return Err(Error::invalid(
            "applied reconciliation requires output; other outcomes forbid it",
        ));
    }
    if let Some(value) = &output {
        validate_leaf_output(state, act, value)?;
    }
    if outcome == ReconcileOutcome::Unknown && claim.unknown_reported {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "unknown reconciliation already reported for claim",
        ));
    }
    let probes = claim.probes.saturating_add(1);
    let mut events = vec![DomainEvent::ReconciliationRecorded {
        run,
        activation,
        outcome,
        output: output.clone(),
        probes,
    }];
    match outcome {
        ReconcileOutcome::Applied => {
            let value = output.expect("validated applied output");
            events.extend(report_success(state, run, activation, value, ids)?.events);
        }
        ReconcileOutcome::NotApplied => {
            events.push(DomainEvent::ClaimCleared { activation });
            if run_is_aborting(state, run) {
                events.push(DomainEvent::LeafFailed {
                    run,
                    activation,
                    attempt: AttemptNo::new(u64::from(act.attempts.max(1))),
                    code: "not_applied".to_owned(),
                    message: "forward effect conclusively did not apply".to_owned(),
                    retry: false,
                });
            }
        }
        ReconcileOutcome::Unknown => {
            if probes >= crate::policy::RECONCILIATION_PROBES {
                events.push(DomainEvent::InterventionRequired {
                    run,
                    activation,
                    reason: "reconciliation probes exhausted".to_owned(),
                });
                events.push(DomainEvent::ClaimCleared { activation });
            }
        }
    }
    Ok(Decision { events })
}

fn unclaimed_ready(state: &State, run: RunId, time: EngineTime) -> Vec<ActivationId> {
    READY_SCANS.fetch_add(1, Ordering::Relaxed);
    ready_activations(state, run)
        .into_iter()
        .filter(|id| {
            if state.interventions.contains_key(id) {
                return false;
            }
            let Some(act) = state.activations.get(id) else {
                return false;
            };
            match &act.claim {
                None => true,
                Some(claim) => time.as_millis() >= claim.lease_expiry_ms,
            }
        })
        .collect()
}

fn decide_compensation_result(
    state: &State,
    run: RunId,
    activation: ActivationId,
    output: Value,
) -> Result<Decision> {
    let Some(obligation) = state.obligations.iter().find(|item| {
        matches!(
            item.status,
            ObligationStatus::Compensating { activation: current } if current == activation
        )
    }) else {
        return Err(Error::invalid("compensation activation has no obligation"));
    };
    if obligation.run != run {
        return Err(Error::invalid("compensation does not belong to run"));
    }
    Ok(Decision {
        events: vec![DomainEvent::LeafSucceeded {
            run,
            activation,
            attempt: AttemptNo::new(1),
            output,
            role: ExecutionRole::Compensation,
        }],
    })
}

fn decide_cancel(state: &State, run: RunId, reason: &str, ids: &mut IdGen) -> Result<Decision> {
    let run_state = state
        .runs
        .get(&run)
        .ok_or_else(|| Error::invalid("unknown run"))?;
    if !matches!(run_state.status, RunStatus::Active) {
        return Err(Error::invalid("run is not active"));
    }
    let error = FailError {
        code: "run.cancelled".to_owned(),
        message: reason.to_owned(),
    };
    let mut events = Vec::new();
    for wait in state
        .waits
        .values()
        .filter(|wait| wait.run == run && wait.pending)
    {
        if let Some(entry) = state
            .inbox
            .iter()
            .find(|entry| entry.reserved_wait == Some(wait.id) && !entry.consumed)
        {
            events.push(DomainEvent::ReservationReleased {
                wait: wait.id,
                event_id: entry.event_id,
            });
        }
    }
    let sagas: Vec<ActivationId> = state
        .activations
        .values()
        .filter(|act| {
            act.run == run
                && matches!(
                    region_for_scope(state, act.scope)
                        .and_then(|region| region.nodes.get(act.node.as_str())),
                    Some(Node::Saga { .. })
                )
                && act.status == ActivationStatus::Open
        })
        .map(|act| act.id)
        .collect();
    if sagas.is_empty() {
        if let Some(root) = state
            .scopes
            .values()
            .find(|scope| scope.run == run && matches!(scope.role, ScopeRole::Root))
        {
            events.extend(fail_scope(state, root.id, error)?);
        }
        return Ok(Decision { events });
    }
    for saga in sagas {
        events.extend(compensate_or_fail(state, saga, error.clone(), ids)?);
    }
    Ok(Decision { events })
}

fn compensation_events(state: &State, act: &ActivationState, output: &Value) -> Vec<DomainEvent> {
    let Some(region) = region_for_scope(state, act.scope) else {
        return Vec::new();
    };
    let Some(node) = region.nodes.get(act.node.as_str()) else {
        return Vec::new();
    };
    match node {
        Node::Activity {
            compensation:
                Some(crate::ir::Compensation::Activity {
                    activity, input, ..
                }),
            ..
        } => {
            let Some(scope) = state.scopes.get(&act.scope) else {
                return Vec::new();
            };
            let mut ctx = eval_ctx(state, scope);
            ctx.forward_input = act_input_binding(region, &act.node)
                .and_then(|binding| eval_binding(binding, &ctx).ok());
            ctx.forward_output = Some(output.clone());
            match eval_binding(input, &ctx) {
                Ok(bound) => vec![DomainEvent::ObligationRegistered {
                    run: act.run,
                    forward: act.id,
                    handler: activity.name.clone(),
                    handler_version: activity.version,
                    input: bound,
                }],
                Err(err) => vec![DomainEvent::ObligationBlocked {
                    run: act.run,
                    forward: act.id,
                    handler: activity.name.clone(),
                    handler_version: activity.version,
                    reason: err.message,
                }],
            }
        }
        Node::Activity {
            compensation: Some(crate::ir::Compensation::Irreversible { reason }),
            ..
        } => vec![DomainEvent::ObligationBlocked {
            run: act.run,
            forward: act.id,
            handler: "irreversible".to_owned(),
            handler_version: 1,
            reason: reason.clone(),
        }],
        _ => Vec::new(),
    }
}

fn act_input_binding<'a>(region: &'a Region, node: &NodeKey) -> Option<&'a Binding> {
    match region.nodes.get(node.as_str())? {
        Node::Activity { input, .. } => Some(input),
        _ => None,
    }
}

fn decide_signal(
    state: &State,
    run: RunId,
    event_id: EventId,
    signal: &str,
    key: &str,
    payload: Value,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    if let Some(existing) =
        state
            .signal_tombstones
            .get(&format!("{}:{}", run.to_hex(), event_id.to_hex()))
    {
        if existing.signal == signal
            && existing.key == key
            && existing.payload
                == crate::history::ArtifactRef::capture("graphrun.signal-payload/v1", &payload)?
        {
            return Ok(Decision { events: Vec::new() });
        }
        return Err(Error::new(
            crate::error::ErrorKind::AlreadyExists,
            "event id payload conflict",
        ));
    }
    let run_state = state
        .runs
        .get(&run)
        .ok_or_else(|| Error::invalid("unknown run"))?;
    if let Some(existing) = state.inbox.iter().find(|entry| entry.event_id == event_id) {
        if existing.signal == signal && existing.key == key && existing.payload == payload {
            return Ok(Decision { events: Vec::new() });
        }
        return Err(Error::new(
            crate::error::ErrorKind::AlreadyExists,
            "event id payload conflict",
        ));
    }
    if matches!(
        run_state.status,
        RunStatus::Succeeded { .. } | RunStatus::Failed { .. }
    ) {
        return Err(Error::invalid("run is terminal"));
    }
    if !run_state.definition.signals.contains_key(signal) {
        return Err(Error::invalid(format!("unknown signal {signal}")));
    }
    let buffered = state
        .inbox
        .iter()
        .filter(|entry| entry.run == Some(run) && !entry.consumed)
        .count();
    if buffered >= crate::limits::MAX_BUFFERED_EVENTS_PER_RUN as usize {
        return Err(Error::new(
            crate::error::ErrorKind::ResourceExhausted,
            "inbox quota exceeded",
        ));
    }
    let same_addr = state
        .inbox
        .iter()
        .filter(|entry| !entry.consumed && entry.signal == signal && entry.key == key)
        .count();
    if same_addr >= crate::limits::MAX_EVENTS_PER_ADDRESS as usize {
        return Err(Error::new(
            crate::error::ErrorKind::ResourceExhausted,
            "inbox address quota exceeded",
        ));
    }
    let sequence = run_state.next_sequence.get();
    let accepted_ms = time.as_millis();
    let expires_ms = accepted_ms.saturating_add(
        run_state
            .policy
            .unreserved_event_days
            .saturating_mul(24 * 60 * 60 * 1000),
    );
    let mut events = vec![DomainEvent::EventAccepted {
        run,
        event_id,
        signal: signal.to_owned(),
        key: key.to_owned(),
        payload: payload.clone(),
        sequence,
        accepted_ms,
        expires_ms,
    }];
    if let Some(wait) = pending_wait(state, run, signal, key) {
        events.push(DomainEvent::EventReserved {
            wait: wait.id,
            event_id,
        });
    }
    let _ = ids;
    Ok(Decision { events })
}

fn pending_wait<'a>(
    state: &'a State,
    run: RunId,
    signal: &str,
    key: &str,
) -> Option<&'a WaitState> {
    state
        .waits
        .values()
        .find(|wait| wait.run == run && wait.pending && wait.signal == signal && wait.key == key)
}

fn decide_timer(
    state: &State,
    run: RunId,
    wait: WaitId,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let wait_state = state
        .waits
        .get(&wait)
        .ok_or_else(|| Error::invalid("unknown wait"))?;
    if wait_state.run != run || !wait_state.pending {
        return Err(Error::invalid("wait is not pending"));
    }
    let Some(deadline) = wait_state.deadline_ms else {
        return Err(Error::invalid("wait has no deadline"));
    };
    if time.as_millis() < deadline {
        return Err(Error::invalid("wait deadline has not been reached"));
    }
    if let Some(entry) = reserved_or_eligible(state, wait_state, time.as_millis()) {
        let act = state.activations.get(&wait_state.activation).unwrap();
        return Ok(Decision {
            events: vec![
                DomainEvent::WaitSatisfied {
                    run,
                    wait,
                    event_id: entry.event_id,
                    payload: entry.payload.clone(),
                },
                DomainEvent::NodeOutputRecorded {
                    run,
                    scope: act.scope,
                    node: act.node.as_str().to_owned(),
                    output: entry.payload.clone(),
                },
            ]
            .into_iter()
            .chain(follow_next(
                state,
                act.scope,
                &act.node,
                &entry.payload,
                ids,
            )?)
            .collect(),
        });
    }
    let act = state.activations.get(&wait_state.activation).unwrap();
    let mut events = vec![DomainEvent::WaitTimedOut { run, wait }];
    events.extend(follow_timeout(state, act, ids)?);
    Ok(Decision { events })
}

fn reserved_or_eligible<'a>(
    state: &'a State,
    wait: &WaitState,
    now_ms: u64,
) -> Option<&'a InboxEntry> {
    if let Some(entry) = state
        .inbox
        .iter()
        .find(|entry| !entry.consumed && entry.reserved_wait == Some(wait.id))
    {
        return Some(entry);
    }
    state.inbox.iter().find(|entry| {
        !entry.consumed
            && entry.reserved_wait.is_none()
            && entry.signal == wait.signal
            && entry.key == wait.key
            && match wait.consume_from {
                ConsumeFrom::Buffered => true,
                ConsumeFrom::AfterActivation => entry.sequence >= wait.opened_sequence,
            }
            && (wait.deadline_ms.is_none()
                || entry.accepted_ms == 0
                || wait
                    .deadline_ms
                    .is_some_and(|deadline| entry.accepted_ms <= deadline))
            && (entry.expires_ms == 0 || now_ms < entry.expires_ms)
    })
}

fn follow_timeout(
    state: &State,
    act: &ActivationState,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let region =
        region_for_scope(state, act.scope).ok_or_else(|| Error::invalid("missing region"))?;
    let Node::WaitSignal {
        on_timeout: Some(next),
        ..
    } = region.nodes.get(act.node.as_str()).unwrap()
    else {
        return Err(Error::invalid("wait has no timeout edge"));
    };
    open_node(state, act.scope, next, ids)
}

fn decide_progress(
    state: &State,
    run: RunId,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let mut events = Vec::new();
    let mut working = state.clone();
    for _ in 0..crate::limits::PROGRESS_BATCH {
        let Some(next) = next_progress(&working, run, time, ids)? else {
            break;
        };
        apply_events(&mut working, &next)?;
        events.extend(next);
    }
    Ok(Decision { events })
}

fn next_progress(
    state: &State,
    run: RunId,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Option<Vec<DomainEvent>>> {
    let Some(run_state) = state.runs.get(&run) else {
        return Ok(None);
    };
    if !matches!(run_state.status, RunStatus::Active) {
        return Ok(None);
    }
    if let Some(events) = due_wait(state, run, time, ids)? {
        return Ok(Some(events));
    }
    for act in state.activations.values().filter(|act| act.run == run) {
        if act.role != ExecutionRole::Forward {
            continue;
        }
        if act.status != ActivationStatus::Open {
            continue;
        }
        let region = region_for_scope(state, act.scope).unwrap();
        let node = region.nodes.get(act.node.as_str()).unwrap();
        match node {
            Node::Activity { .. } => {
                return Ok(Some(vec![DomainEvent::ActivationOpened {
                    run,
                    scope: act.scope,
                    activation: act.id,
                    node: act.node.as_str().to_owned(),
                }]));
            }
            Node::Complete { output } => {
                let scope = state.scopes.get(&act.scope).unwrap();
                let value = eval_binding(output, &eval_ctx(state, scope))?;
                return Ok(Some(complete_scope(state, scope, value)?));
            }
            Node::Fail { error } => {
                return Ok(Some(fail_scope(state, act.scope, error.clone())?));
            }
            Node::Delay { duration, next } => {
                if state
                    .waits
                    .values()
                    .any(|wait| wait.activation == act.id && wait.pending)
                {
                    continue;
                }
                let scope = state.scopes.get(&act.scope).unwrap();
                let ms = eval_duration_ms(duration, &eval_ctx(state, scope))?;
                let wait = ids.wait();
                let _ = next;
                return Ok(Some(vec![DomainEvent::WaitOpened {
                    run,
                    wait,
                    activation: act.id,
                    signal: "__timer".to_owned(),
                    key: act.id.to_hex(),
                    deadline_ms: Some(time.as_millis().saturating_add(ms)),
                    consume_from: ConsumeFrom::AfterActivation,
                }]));
            }
            Node::WaitUntil { at, next } => {
                let scope = state.scopes.get(&act.scope).unwrap();
                let at = match eval_binding(at, &eval_ctx(state, scope))? {
                    Value::Int(v) if v >= 0 => v as u64,
                    _ => {
                        return Err(Error::invalid(
                            "wait_until at must be a non-negative integer",
                        ));
                    }
                };
                if time.as_millis() >= at {
                    return Ok(Some(open_node(state, act.scope, next, ids)?));
                }
            }
            Node::WaitSignal { .. } => {
                if state
                    .waits
                    .values()
                    .any(|wait| wait.activation == act.id && wait.pending)
                {
                    continue;
                }
                return Ok(Some(open_wait(state, act, time, ids)?));
            }
            Node::While {
                state: st,
                condition,
                max_iterations,
                body,
                next,
                ..
            } => {
                let events = progress_while(
                    state,
                    act,
                    st,
                    condition,
                    *max_iterations,
                    body,
                    next,
                    false,
                    ids,
                )?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::DoWhile {
                state: st,
                condition,
                max_iterations,
                body,
                next,
                ..
            } => {
                let events = progress_while(
                    state,
                    act,
                    st,
                    condition,
                    *max_iterations,
                    body,
                    next,
                    true,
                    ids,
                )?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::Repeat {
                count,
                state: st,
                max_iterations,
                body,
                next,
                ..
            } => {
                let events =
                    progress_repeat(state, act, count, st, *max_iterations, body, next, ids)?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::Foreach {
                items,
                max_items,
                max_concurrency,
                body,
                next,
                ..
            } => {
                let events = progress_foreach(
                    state,
                    act,
                    items,
                    *max_items,
                    *max_concurrency,
                    body,
                    next,
                    ids,
                )?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::Parallel { branches, next } => {
                let events = progress_parallel(state, act, branches, next, ids)?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::Choose {
                input,
                cases,
                default,
                next,
            } => {
                let events = progress_choose(state, act, input, cases, default, next, ids)?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            Node::Saga { input, body, next } => {
                let events = progress_saga(state, act, input, body, next, ids)?;
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
        }
    }
    Ok(None)
}

fn due_wait(
    state: &State,
    run: RunId,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Option<Vec<DomainEvent>>> {
    let now = time.as_millis();
    let mut due: Vec<&WaitState> = state
        .waits
        .values()
        .filter(|wait| {
            wait.run == run
                && wait.pending
                && (wait.deadline_ms.is_some_and(|deadline| now >= deadline)
                    || reserved_or_eligible(state, wait, now).is_some())
        })
        .collect();
    due.sort_by_key(|wait| wait.deadline_ms.unwrap_or(0));
    let Some(wait) = due.first().copied() else {
        return Ok(None);
    };
    if let Some(entry) = reserved_or_eligible(state, wait, now) {
        let act = state.activations.get(&wait.activation).unwrap();
        let mut events = vec![
            DomainEvent::WaitSatisfied {
                run,
                wait: wait.id,
                event_id: entry.event_id,
                payload: entry.payload.clone(),
            },
            DomainEvent::NodeOutputRecorded {
                run,
                scope: act.scope,
                node: act.node.as_str().to_owned(),
                output: entry.payload.clone(),
            },
        ];
        events.extend(follow_next(
            state,
            act.scope,
            &act.node,
            &entry.payload,
            ids,
        )?);
        return Ok(Some(events));
    }
    let act = state.activations.get(&wait.activation).unwrap();
    if wait.signal == "__timer" {
        let region = region_for_scope(state, act.scope).unwrap();
        let Node::Delay { next, .. } = region.nodes.get(act.node.as_str()).unwrap() else {
            return Err(Error::invalid("timer wait is not a delay"));
        };
        let mut events = vec![DomainEvent::WaitTimedOut { run, wait: wait.id }];
        events.extend(open_node(state, act.scope, next, ids)?);
        return Ok(Some(events));
    }
    Ok(Some(decide_timer(state, run, wait.id, time, ids)?.events))
}

fn open_wait(
    state: &State,
    act: &ActivationState,
    time: EngineTime,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let region = region_for_scope(state, act.scope).unwrap();
    let Node::WaitSignal {
        signal,
        key,
        timeout_ms,
        consume_from,
        ..
    } = region.nodes.get(act.node.as_str()).unwrap()
    else {
        unreachable!()
    };
    let scope = state.scopes.get(&act.scope).unwrap();
    let key_val = eval_binding(key, &eval_ctx(state, scope))?;
    let key_str = key_val
        .as_str()
        .ok_or_else(|| Error::invalid("wait key must be a string"))?
        .to_owned();
    if state.waits.values().any(|existing| {
        existing.run == act.run
            && existing.pending
            && existing.signal == *signal
            && existing.key == key_str
    }) {
        return fail_scope(
            state,
            act.scope,
            FailError {
                code: "WaitKeyConflict".to_owned(),
                message: format!("conflicting active wait address {signal}/{key_str}"),
            },
        );
    }
    let wait = ids.wait();
    let deadline = timeout_ms.map(|ms| time.as_millis().saturating_add(ms));
    let mut events = vec![DomainEvent::WaitOpened {
        run: act.run,
        wait,
        activation: act.id,
        signal: signal.clone(),
        key: key_str.clone(),
        deadline_ms: deadline,
        consume_from: *consume_from,
    }];
    let fake = WaitState {
        id: wait,
        run: act.run,
        activation: act.id,
        signal: signal.clone(),
        key: key_str,
        deadline_ms: deadline,
        consume_from: *consume_from,
        opened_sequence: state.runs.get(&act.run).unwrap().next_sequence.get(),
        pending: true,
    };
    if let Some(entry) = reserved_or_eligible(state, &fake, time.as_millis()) {
        events.push(DomainEvent::WaitSatisfied {
            run: act.run,
            wait,
            event_id: entry.event_id,
            payload: entry.payload.clone(),
        });
        events.push(DomainEvent::NodeOutputRecorded {
            run: act.run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: entry.payload.clone(),
        });
        events.extend(follow_next(
            state,
            act.scope,
            &act.node,
            &entry.payload,
            ids,
        )?);
    }
    Ok(events)
}

fn progress_while(
    state: &State,
    act: &ActivationState,
    state_binding: &Binding,
    condition: &Condition,
    max_iterations: u32,
    body: &Region,
    next: &NodeKey,
    do_while: bool,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let scope = state.scopes.get(&act.scope).unwrap();
    let (mut carry, completed) = state.loop_carry.get(&act.id).cloned().unwrap_or_else(|| {
        (
            eval_binding(state_binding, &eval_ctx(state, scope)).unwrap(),
            0,
        )
    });
    if let Some(child) = child_of(state, act.id) {
        match &child.status {
            ScopeStatus::Open => return Ok(Vec::new()), // waiting on child
            ScopeStatus::Failed { error } => return fail_scope(state, act.scope, error.clone()),
            ScopeStatus::Completed { output } => {
                carry = output.clone();
                let completed = completed + 1;
                let mut ctx = eval_ctx(state, scope);
                ctx.loop_state = Some(carry.clone());
                ctx.loop_index = Some(i64::from(completed));
                let cont = eval_condition(condition, &ctx)?;
                let mut events = vec![DomainEvent::GuardRecorded {
                    run: act.run,
                    activation: act.id,
                    result: cont,
                    index: completed,
                    carry: carry.clone(),
                }];
                if !cont {
                    events.push(DomainEvent::NodeOutputRecorded {
                        run: act.run,
                        scope: act.scope,
                        node: act.node.as_str().to_owned(),
                        output: carry.clone(),
                    });
                    events.extend(follow_next(state, act.scope, &act.node, &carry, ids)?);
                    return Ok(events);
                }
                if completed >= max_iterations {
                    return fail_scope(
                        state,
                        act.scope,
                        FailError {
                            code: "LoopLimitExceeded".to_owned(),
                            message: "loop continuation at the iteration limit".to_owned(),
                        },
                    );
                }
                events.extend(open_loop_body(act, body, carry, completed, ids)?);
                return Ok(events);
            }
        }
    }
    let mut ctx = eval_ctx(state, scope);
    ctx.loop_state = Some(carry.clone());
    ctx.loop_index = Some(i64::from(completed));
    if completed == 0 && !do_while {
        let cont = eval_condition(condition, &ctx)?;
        let mut events = vec![DomainEvent::GuardRecorded {
            run: act.run,
            activation: act.id,
            result: cont,
            index: 0,
            carry: carry.clone(),
        }];
        if !cont {
            events.push(DomainEvent::NodeOutputRecorded {
                run: act.run,
                scope: act.scope,
                node: act.node.as_str().to_owned(),
                output: carry.clone(),
            });
            events.extend(follow_next(state, act.scope, &act.node, &carry, ids)?);
            return Ok(events);
        }
        events.extend(open_loop_body(act, body, carry, 0, ids)?);
        return Ok(events);
    }
    if completed == 0 && do_while {
        return open_loop_body(act, body, carry, 0, ids);
    }
    let _ = next;
    Ok(Vec::new())
}

fn open_loop_body(
    act: &ActivationState,
    body: &Region,
    carry: Value,
    index: u32,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let scope = ids.scope();
    let start = ids.activation();
    Ok(vec![
        DomainEvent::ScopeOpened {
            run: act.run,
            scope,
            parent: Some(act.id),
            role: ScopeRole::LoopBody {
                activation: act.id,
                index,
            },
            input: carry,
        },
        DomainEvent::ActivationOpened {
            run: act.run,
            scope,
            activation: start,
            node: body.start.as_str().to_owned(),
        },
    ])
}

fn progress_repeat(
    state: &State,
    act: &ActivationState,
    count: &Binding,
    state_binding: &Binding,
    max_iterations: u32,
    body: &Region,
    next: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let scope = state.scopes.get(&act.scope).unwrap();
    let ctx = eval_ctx(state, scope);
    let count_v = eval_binding(count, &ctx)?;
    let count_n = count_v
        .as_i64()
        .ok_or_else(|| Error::invalid("repeat count must be an integer"))?;
    if count_n < 0 || count_n as u32 > max_iterations {
        return Err(Error::invalid("repeat count is out of range"));
    }
    let (carry, completed) = state
        .loop_carry
        .get(&act.id)
        .cloned()
        .unwrap_or_else(|| (eval_binding(state_binding, &ctx).unwrap(), 0));
    if count_n == 0 && completed == 0 {
        let mut events = vec![DomainEvent::NodeOutputRecorded {
            run: act.run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: carry.clone(),
        }];
        events.extend(follow_next(state, act.scope, &act.node, &carry, ids)?);
        return Ok(events);
    }
    if let Some(child) = child_of(state, act.id) {
        match &child.status {
            ScopeStatus::Open => return Ok(Vec::new()), // waiting on child
            ScopeStatus::Failed { error } => return fail_scope(state, act.scope, error.clone()),
            ScopeStatus::Completed { output } => {
                if completed as i64 == count_n {
                    let mut events = vec![DomainEvent::NodeOutputRecorded {
                        run: act.run,
                        scope: act.scope,
                        node: act.node.as_str().to_owned(),
                        output: output.clone(),
                    }];
                    events.extend(follow_next(state, act.scope, &act.node, output, ids)?);
                    return Ok(events);
                }
                if completed as i64 > count_n {
                    return Ok(Vec::new());
                }
                return open_loop_body(act, body, output.clone(), completed, ids);
            }
        }
    }
    let _ = next;
    open_loop_body(act, body, carry, completed, ids)
}

fn progress_foreach(
    state: &State,
    act: &ActivationState,
    items: &Binding,
    max_items: u32,
    max_concurrency: u32,
    body: &Region,
    next: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let scope = state.scopes.get(&act.scope).unwrap();
    let value = eval_binding(items, &eval_ctx(state, scope))?;
    let Value::Array(list) = value else {
        return Err(Error::invalid("foreach items must be an array"));
    };
    if list.len() as u32 > max_items {
        return Err(Error::invalid("foreach exceeds max_items"));
    }
    if list.is_empty() {
        let empty = Value::Array(Vec::new());
        let mut events = vec![DomainEvent::NodeOutputRecorded {
            run: act.run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: empty.clone(),
        }];
        events.extend(follow_next(state, act.scope, &act.node, &empty, ids)?);
        return Ok(events);
    }
    let done = state.foreach_done.get(&act.id).cloned().unwrap_or_default();
    if done.len() == list.len() {
        let mut ordered = Vec::new();
        for i in 0..list.len() {
            ordered.push(foreach_get(&done, i as u32).cloned().unwrap());
        }
        let output = Value::Array(ordered);
        let mut events = vec![DomainEvent::NodeOutputRecorded {
            run: act.run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: output.clone(),
        }];
        events.extend(follow_next(state, act.scope, &act.node, &output, ids)?);
        return Ok(events);
    }
    if let Some(failed) = children_of(state, act.id)
        .into_iter()
        .find(|child| matches!(child.status, ScopeStatus::Failed { .. }))
    {
        if let ScopeStatus::Failed { error } = failed.status.clone() {
            let mut events = Vec::new();
            for sibling in children_of(state, act.id) {
                if sibling.id == failed.id || sibling.status != ScopeStatus::Open {
                    continue;
                }
                events.extend(fail_scope(
                    state,
                    sibling.id,
                    FailError {
                        code: "item.cancelled".to_owned(),
                        message: "sibling foreach item failed".to_owned(),
                    },
                )?);
            }
            events.extend(fail_scope(state, act.scope, error)?);
            return Ok(events);
        }
    }
    let open_children = children_of(state, act.id)
        .into_iter()
        .filter(|child| child.status == ScopeStatus::Open)
        .count();
    if open_children >= max_concurrency as usize {
        return Ok(Vec::new());
    }
    let mut next_index = 0u32;
    let taken: Vec<u32> = children_of(state, act.id)
        .into_iter()
        .filter_map(|child| match child.role {
            ScopeRole::ForeachItem { index, .. } => Some(index),
            _ => None,
        })
        .collect();
    while taken.contains(&next_index) || foreach_has(&done, next_index) {
        next_index += 1;
    }
    if next_index as usize >= list.len() {
        return Ok(Vec::new());
    }
    let scope_id = ids.scope();
    let start = ids.activation();
    let _ = next;
    Ok(vec![
        DomainEvent::ScopeOpened {
            run: act.run,
            scope: scope_id,
            parent: Some(act.id),
            role: ScopeRole::ForeachItem {
                activation: act.id,
                index: next_index,
            },
            input: list[next_index as usize].clone(),
        },
        DomainEvent::ActivationOpened {
            run: act.run,
            scope: scope_id,
            activation: start,
            node: body.start.as_str().to_owned(),
        },
    ])
}

fn progress_parallel(
    state: &State,
    act: &ActivationState,
    branches: &[crate::ir::ParallelBranch],
    next: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let done = state
        .parallel_done
        .get(&act.id)
        .cloned()
        .unwrap_or_default();
    if done.len() == branches.len() {
        let mut ordered = Vec::new();
        for branch in branches {
            ordered.push(done.get(&branch.name).cloned().unwrap());
        }
        let output = Value::Array(ordered);
        let mut events = vec![DomainEvent::NodeOutputRecorded {
            run: act.run,
            scope: act.scope,
            node: act.node.as_str().to_owned(),
            output: output.clone(),
        }];
        events.extend(follow_next(state, act.scope, &act.node, &output, ids)?);
        return Ok(events);
    }
    let existing: Vec<String> = children_of(state, act.id)
        .into_iter()
        .filter_map(|child| match &child.role {
            ScopeRole::ParallelBranch { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    if existing.len() < branches.len() {
        let scope = state.scopes.get(&act.scope).unwrap();
        let ctx = eval_ctx(state, scope);
        for branch in branches {
            if existing.contains(&branch.name) {
                continue;
            }
            let input = eval_binding(&branch.input, &ctx)?;
            let scope_id = ids.scope();
            let start = ids.activation();
            return Ok(vec![
                DomainEvent::ScopeOpened {
                    run: act.run,
                    scope: scope_id,
                    parent: Some(act.id),
                    role: ScopeRole::ParallelBranch {
                        activation: act.id,
                        name: branch.name.clone(),
                    },
                    input,
                },
                DomainEvent::ActivationOpened {
                    run: act.run,
                    scope: scope_id,
                    activation: start,
                    node: branch.body.start.as_str().to_owned(),
                },
            ]);
        }
    }
    if let Some(failed) = children_of(state, act.id)
        .into_iter()
        .find(|child| matches!(child.status, ScopeStatus::Failed { .. }))
    {
        if let ScopeStatus::Failed { error } = failed.status.clone() {
            let mut events = Vec::new();
            let mut uncertain = false;
            for sibling in children_of(state, act.id) {
                if sibling.id == failed.id {
                    continue;
                }
                if sibling.status != ScopeStatus::Open {
                    continue;
                }
                let claimed = state.activations.values().any(|item| {
                    item.scope == sibling.id
                        && item.claim.as_ref().is_some_and(|claim| {
                            // live claim means the effect may still apply
                            claim.lease_expiry_ms > 0
                        })
                });
                if claimed {
                    uncertain = true;
                    continue;
                }
                events.extend(fail_scope(
                    state,
                    sibling.id,
                    FailError {
                        code: "branch.cancelled".to_owned(),
                        message: "sibling branch failed".to_owned(),
                    },
                )?);
            }
            if uncertain {
                return Ok(events);
            }
            events.extend(fail_scope(state, act.scope, error)?);
            return Ok(events);
        }
    }
    let _ = next;
    Ok(Vec::new())
}

fn progress_choose(
    state: &State,
    act: &ActivationState,
    input: &Binding,
    cases: &[crate::ir::ChooseCase],
    default: &Region,
    next: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    if let Some(child) = child_of(state, act.id) {
        return match &child.status {
            ScopeStatus::Open => Ok(Vec::new()),
            ScopeStatus::Failed { error } => fail_scope(state, act.scope, error.clone()),
            ScopeStatus::Completed { output } => {
                let mut events = vec![DomainEvent::NodeOutputRecorded {
                    run: act.run,
                    scope: act.scope,
                    node: act.node.as_str().to_owned(),
                    output: output.clone(),
                }];
                events.extend(follow_next(state, act.scope, &act.node, output, ids)?);
                Ok(events)
            }
        };
    }
    let scope = state.scopes.get(&act.scope).unwrap();
    let ctx = eval_ctx(state, scope);
    let captured = eval_binding(input, &ctx)?;
    let mut chosen = default;
    let mut name = "default".to_owned();
    for case in cases {
        if eval_condition(&case.when, &ctx)? {
            chosen = &case.body;
            name = case.name.clone();
            break;
        }
    }
    let scope_id = ids.scope();
    let start = ids.activation();
    let _ = next;
    Ok(vec![
        DomainEvent::ScopeOpened {
            run: act.run,
            scope: scope_id,
            parent: Some(act.id),
            role: ScopeRole::ChooseBody {
                activation: act.id,
                name,
            },
            input: captured,
        },
        DomainEvent::ActivationOpened {
            run: act.run,
            scope: scope_id,
            activation: start,
            node: chosen.start.as_str().to_owned(),
        },
    ])
}

fn progress_saga(
    state: &State,
    act: &ActivationState,
    input: &Binding,
    body: &Region,
    next: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    if compensating_in_flight(state, act.id) {
        return Ok(Vec::new());
    }
    if state.saga_errors.contains_key(&act.id) {
        let error = state.saga_errors.get(&act.id).unwrap().clone();
        return compensate_or_fail(state, act.id, error, ids);
    }
    if let Some(child) = child_of(state, act.id) {
        return match &child.status {
            ScopeStatus::Open => Ok(Vec::new()),
            ScopeStatus::Failed { error } => compensate_or_fail(state, act.id, error.clone(), ids),
            ScopeStatus::Completed { output } => {
                let mut events = settle_saga_success(state, act.id);
                events.push(DomainEvent::NodeOutputRecorded {
                    run: act.run,
                    scope: act.scope,
                    node: act.node.as_str().to_owned(),
                    output: output.clone(),
                });
                events.extend(follow_next(state, act.scope, &act.node, output, ids)?);
                Ok(events)
            }
        };
    }
    let scope = state.scopes.get(&act.scope).unwrap();
    let captured = eval_binding(input, &eval_ctx(state, scope))?;
    let scope_id = ids.scope();
    let start = ids.activation();
    let _ = next;
    Ok(vec![
        DomainEvent::ScopeOpened {
            run: act.run,
            scope: scope_id,
            parent: Some(act.id),
            role: ScopeRole::SagaBody { activation: act.id },
            input: captured,
        },
        DomainEvent::ActivationOpened {
            run: act.run,
            scope: scope_id,
            activation: start,
            node: body.start.as_str().to_owned(),
        },
    ])
}

fn compensating_in_flight(state: &State, saga: ActivationId) -> bool {
    state.obligations.iter().any(|item| {
        item.owner == saga && matches!(item.status, ObligationStatus::Compensating { .. })
    })
}

fn compensate_or_fail(
    state: &State,
    saga: ActivationId,
    error: FailError,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let Some(act) = state.activations.get(&saga) else {
        return Err(Error::invalid("unknown saga"));
    };
    if state
        .obligations
        .iter()
        .any(|item| item.owner == saga && matches!(item.status, ObligationStatus::Blocked { .. }))
    {
        return Ok(vec![DomainEvent::InterventionRequired {
            run: act.run,
            activation: saga,
            reason: "compensation input is blocked".to_owned(),
        }]);
    }
    let pending: Vec<&Obligation> = state
        .obligations
        .iter()
        .filter(|item| item.owner == saga && matches!(item.status, ObligationStatus::Open))
        .collect();
    if pending.is_empty() {
        if uncertain_forwards(state, act.run) {
            if state.saga_errors.contains_key(&saga) {
                return Ok(Vec::new());
            }
            return Ok(vec![DomainEvent::AbortIntent {
                run: act.run,
                saga,
                error,
            }]);
        }
        if state.obligations.iter().any(|item| {
            item.owner == saga && matches!(item.status, ObligationStatus::Irreversible { .. })
        }) {
            return fail_scope(
                state,
                act.scope,
                FailError {
                    code: "saga.irreversible".to_owned(),
                    message: error.message,
                },
            );
        }
        return fail_scope(state, act.scope, error);
    }
    let obligation = pending[pending.len() - 1];
    Ok(vec![DomainEvent::CompensationStarted {
        run: act.run,
        saga,
        forward: obligation.forward,
        activation: ids.activation(),
        handler: obligation.handler.clone(),
        handler_version: obligation.handler_version,
        input: obligation.input.clone(),
        error,
    }])
}

fn settle_saga_success(state: &State, saga: ActivationId) -> Vec<DomainEvent> {
    let run = state.activations.get(&saga).map(|act| act.run);
    let owned: Vec<ActivationId> = state
        .obligations
        .iter()
        .filter(|item| item.owner == saga && matches!(item.status, ObligationStatus::Open))
        .map(|item| item.forward)
        .collect();
    if let Some(parent) = parent_saga(state, saga) {
        owned
            .into_iter()
            .map(|forward| DomainEvent::ObligationTransferred {
                forward,
                from_saga: saga,
                to_saga: parent,
            })
            .collect()
    } else if let Some(run) = run {
        owned
            .into_iter()
            .map(|forward| DomainEvent::ObligationReleased { run, forward })
            .collect()
    } else {
        Vec::new()
    }
}

fn parent_saga(state: &State, saga: ActivationId) -> Option<ActivationId> {
    let act = state.activations.get(&saga)?;
    saga_owner_from_scope(state, act.scope, Some(saga))
}

fn saga_owner(state: &State, forward: ActivationId) -> Option<ActivationId> {
    let act = state.activations.get(&forward)?;
    saga_owner_from_scope(state, act.scope, None)
}

fn saga_owner_from_scope(
    state: &State,
    mut scope_id: ScopeId,
    skip: Option<ActivationId>,
) -> Option<ActivationId> {
    loop {
        let scope = state.scopes.get(&scope_id)?;
        if let ScopeRole::SagaBody { activation } = scope.role {
            if skip != Some(activation) {
                return Some(activation);
            }
        }
        let parent_act = scope.parent?;
        let parent = state.activations.get(&parent_act)?;
        scope_id = parent.scope;
    }
}

fn follow_next(
    state: &State,
    scope: ScopeId,
    node: &NodeKey,
    _output: &Value,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let region = region_for_scope(state, scope).ok_or_else(|| Error::invalid("missing region"))?;
    let Some((_, next)) = region
        .nodes
        .get(node.as_str())
        .unwrap()
        .successors()
        .into_iter()
        .find(|(name, _)| *name == "next")
    else {
        return Ok(Vec::new());
    };
    open_node(state, scope, next, ids)
}

fn open_node(
    state: &State,
    scope: ScopeId,
    node: &NodeKey,
    ids: &mut IdGen,
) -> Result<Vec<DomainEvent>> {
    let run = state.scopes.get(&scope).unwrap().run;
    Ok(vec![DomainEvent::ActivationOpened {
        run,
        scope,
        activation: ids.activation(),
        node: node.as_str().to_owned(),
    }])
}

fn complete_scope(_state: &State, scope: &ScopeState, output: Value) -> Result<Vec<DomainEvent>> {
    let mut events = vec![DomainEvent::ScopeCompleted {
        run: scope.run,
        scope: scope.id,
        output: output.clone(),
    }];
    match &scope.role {
        ScopeRole::Root => events.push(DomainEvent::RunSucceeded {
            run: scope.run,
            output,
        }),
        ScopeRole::ForeachItem { activation, index } => {
            let _ = (activation, index, output);
        }
        ScopeRole::ParallelBranch { .. }
        | ScopeRole::LoopBody { .. }
        | ScopeRole::ChooseBody { .. }
        | ScopeRole::SagaBody { .. } => {}
    }
    Ok(events)
}

fn fail_scope(state: &State, scope: ScopeId, error: FailError) -> Result<Vec<DomainEvent>> {
    let scope_state = state.scopes.get(&scope).unwrap();
    if matches!(scope_state.status, ScopeStatus::Failed { .. }) {
        return Ok(Vec::new());
    }
    let mut events = vec![DomainEvent::ScopeFailed {
        run: scope_state.run,
        scope,
        error: error.clone(),
    }];
    if matches!(scope_state.role, ScopeRole::Root) {
        events.push(DomainEvent::RunFailed {
            run: scope_state.run,
            error,
        });
    }
    Ok(events)
}

fn child_of(state: &State, parent: ActivationId) -> Option<&ScopeState> {
    let mut children = children_of(state, parent);
    if let Some(open) = children
        .iter()
        .find(|child| child.status == ScopeStatus::Open)
    {
        return Some(*open);
    }
    children.sort_by_key(|child| match child.role {
        ScopeRole::LoopBody { index, .. } | ScopeRole::ForeachItem { index, .. } => index,
        _ => 0,
    });
    children.pop()
}

fn children_of(state: &State, parent: ActivationId) -> Vec<&ScopeState> {
    state
        .scopes
        .values()
        .filter(|scope| scope.parent == Some(parent))
        .collect()
}

fn foreach_get(done: &[(u32, Value)], index: u32) -> Option<&Value> {
    done.iter()
        .find(|(i, _)| *i == index)
        .map(|(_, value)| value)
}

fn foreach_has(done: &[(u32, Value)], index: u32) -> bool {
    done.iter().any(|(i, _)| *i == index)
}

fn region_for_scope(state: &State, scope: ScopeId) -> Option<&Region> {
    lookup_region(state, scope)
}

struct EvalCtx {
    workflow_input: Value,
    scope_input: Value,
    scope_id: String,
    node_outputs: BTreeMap<String, Value>,
    loop_state: Option<Value>,
    loop_index: Option<i64>,
    item_value: Option<Value>,
    item_index: Option<i64>,
    forward_input: Option<Value>,
    forward_output: Option<Value>,
}

fn eval_ctx(state: &State, scope: &ScopeState) -> EvalCtx {
    let run = state.runs.get(&scope.run).unwrap();
    let (loop_state, loop_index, item_value, item_index) = match &scope.role {
        ScopeRole::LoopBody { activation, index } => {
            let carry = state
                .loop_carry
                .get(activation)
                .map(|(v, _)| v.clone())
                .unwrap_or_else(|| scope.input.clone());
            (Some(carry), Some(i64::from(*index)), None, None)
        }
        ScopeRole::ForeachItem { index, .. } => (
            None,
            None,
            Some(scope.input.clone()),
            Some(i64::from(*index)),
        ),
        _ => (None, None, None, None),
    };
    EvalCtx {
        workflow_input: run.input.clone(),
        scope_input: scope.input.clone(),
        scope_id: scope.id.to_hex(),
        node_outputs: scope.outputs.clone(),
        loop_state,
        loop_index,
        item_value,
        item_index,
        forward_input: None,
        forward_output: None,
    }
}

fn eval_binding(binding: &Binding, ctx: &EvalCtx) -> Result<Value> {
    match binding {
        Binding::Literal { value } => Ok(value.clone()),
        Binding::From { reference, path } => {
            let mut value = eval_ref(reference, ctx)?;
            if let Some(pointer) = path {
                value = value.pointer(pointer)?.clone();
            }
            Ok(value)
        }
        Binding::Object { fields } => {
            let mut out = BTreeMap::new();
            for (k, v) in fields {
                out.insert(k.clone(), eval_binding(v, ctx)?);
            }
            Ok(Value::Object(out))
        }
        Binding::Array { items } => {
            let mut out = Vec::new();
            for item in items {
                out.push(eval_binding(item, ctx)?);
            }
            Ok(Value::Array(out))
        }
    }
}

fn eval_ref(reference: &Reference, ctx: &EvalCtx) -> Result<Value> {
    Ok(match reference {
        Reference::WorkflowInput => ctx.workflow_input.clone(),
        Reference::ScopeInput => ctx.scope_input.clone(),
        Reference::ScopeId => Value::String(ctx.scope_id.clone()),
        Reference::NodeOutput { node } => ctx
            .node_outputs
            .get(node.as_str())
            .cloned()
            .ok_or_else(|| Error::invalid(format!("missing output {}", node.as_str())))?,
        Reference::LoopState => ctx
            .loop_state
            .clone()
            .ok_or_else(|| Error::invalid("loop.state is not in context"))?,
        Reference::LoopIndex => Value::Int(
            ctx.loop_index
                .ok_or_else(|| Error::invalid("loop.index is not in context"))?,
        ),
        Reference::ItemValue => ctx
            .item_value
            .clone()
            .ok_or_else(|| Error::invalid("item.value is not in context"))?,
        Reference::ItemIndex => Value::Int(
            ctx.item_index
                .ok_or_else(|| Error::invalid("item.index is not in context"))?,
        ),
        Reference::ForwardInput => ctx
            .forward_input
            .clone()
            .ok_or_else(|| Error::invalid("forward.input is not in context"))?,
        Reference::ForwardOutput => ctx
            .forward_output
            .clone()
            .ok_or_else(|| Error::invalid("forward.output is not in context"))?,
    })
}

fn eval_condition(condition: &Condition, ctx: &EvalCtx) -> Result<bool> {
    match condition {
        Condition::Eq { left, right } => Ok(eval_binding(left, ctx)? == eval_binding(right, ctx)?),
        Condition::Ne { left, right } => Ok(eval_binding(left, ctx)? != eval_binding(right, ctx)?),
        Condition::Lt { left, right } => cmp_num(left, right, ctx, |a, b| a < b),
        Condition::Le { left, right } => cmp_num(left, right, ctx, |a, b| a <= b),
        Condition::Gt { left, right } => cmp_num(left, right, ctx, |a, b| a > b),
        Condition::Ge { left, right } => cmp_num(left, right, ctx, |a, b| a >= b),
        Condition::All { items } => {
            for item in items {
                if !eval_condition(item, ctx)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Any { items } => {
            for item in items {
                if eval_condition(item, ctx)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Not { inner } => Ok(!eval_condition(inner, ctx)?),
        Condition::Exists { binding } => match eval_binding(binding, ctx) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        },
    }
}

fn cmp_num(
    left: &Binding,
    right: &Binding,
    ctx: &EvalCtx,
    op: impl Fn(i64, i64) -> bool,
) -> Result<bool> {
    let Value::Int(a) = eval_binding(left, ctx)? else {
        return Err(Error::invalid("numeric comparison requires integers"));
    };
    let Value::Int(b) = eval_binding(right, ctx)? else {
        return Err(Error::invalid("numeric comparison requires integers"));
    };
    Ok(op(a, b))
}

fn eval_duration_ms(binding: &Binding, ctx: &EvalCtx) -> Result<u64> {
    match eval_binding(binding, ctx)? {
        Value::Int(v) if v > 0 => Ok(v as u64),
        Value::String(text) => Ok(crate::time::duration_to_millis(
            crate::time::parse_duration(&text)?,
        )?),
        _ => Err(Error::invalid(
            "duration must be a positive integer ms or string",
        )),
    }
}

pub fn apply_events(state: &mut State, events: &[DomainEvent]) -> Result<()> {
    apply_events_with_cause(state, events, None, None, EngineTime::from_millis(0))
}

pub fn apply_events_with_cause(
    state: &mut State,
    events: &[DomainEvent],
    command_id: Option<CommandId>,
    principal_id: Option<&str>,
    time: EngineTime,
) -> Result<()> {
    for event in events {
        let run = event_owner(state, event)?;
        evolve(state, event);
        if let Some(run) = run {
            crate::history::append(
                state,
                run,
                event,
                command_id,
                principal_id,
                time.as_millis(),
            )?;
        }
    }
    Ok(())
}

fn event_owner(state: &State, event: &DomainEvent) -> Result<Option<RunId>> {
    let owner = match event {
        DomainEvent::RunAdmitted { run, .. }
        | DomainEvent::ScopeOpened { run, .. }
        | DomainEvent::ScopeCompleted { run, .. }
        | DomainEvent::ScopeFailed { run, .. }
        | DomainEvent::ActivationOpened { run, .. }
        | DomainEvent::NodeOutputRecorded { run, .. }
        | DomainEvent::GuardRecorded { run, .. }
        | DomainEvent::LeafSucceeded { run, .. }
        | DomainEvent::LeafFailed { run, .. }
        | DomainEvent::WaitOpened { run, .. }
        | DomainEvent::WaitSatisfied { run, .. }
        | DomainEvent::WaitTimedOut { run, .. }
        | DomainEvent::EventAccepted { run, .. }
        | DomainEvent::EventExpired { run, .. }
        | DomainEvent::ObligationRegistered { run, .. }
        | DomainEvent::ObligationBlocked { run, .. }
        | DomainEvent::CompensationStarted { run, .. }
        | DomainEvent::RunSucceeded { run, .. }
        | DomainEvent::RunFailed { run, .. }
        | DomainEvent::ObligationReleased { run, .. }
        | DomainEvent::ClaimGranted { run, .. }
        | DomainEvent::ReconciliationRecorded { run, .. }
        | DomainEvent::ReconciliationFailed { run, .. }
        | DomainEvent::InterventionRequired { run, .. }
        | DomainEvent::AbortIntent { run, .. }
        | DomainEvent::CompensationAbandoned { run, .. } => *run,
        DomainEvent::ObligationTransferred { forward, .. } => state
            .obligations
            .iter()
            .find(|item| item.forward == *forward)
            .map(|item| item.run)
            .ok_or_else(|| {
                Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "transferred obligation has no run",
                )
            })?,
        DomainEvent::ClaimRenewed { activation, .. } | DomainEvent::ClaimCleared { activation } => {
            state
                .activations
                .get(activation)
                .map(|act| act.run)
                .ok_or_else(|| {
                    Error::new(
                        crate::error::ErrorKind::FailedPrecondition,
                        "claim lifecycle event has no activation",
                    )
                })?
        }
        DomainEvent::EventReserved { wait, .. } | DomainEvent::ReservationReleased { wait, .. } => {
            state.waits.get(wait).map(|wait| wait.run).ok_or_else(|| {
                Error::new(
                    crate::error::ErrorKind::FailedPrecondition,
                    "signal reservation has no wait",
                )
            })?
        }
        DomainEvent::SessionRegistered { .. }
        | DomainEvent::WorkerRegistered { .. }
        | DomainEvent::WorkerSessionRenewed { .. }
        | DomainEvent::RecoveryAuthorized { .. } => {
            return Ok(None);
        }
    };
    Ok(Some(owner))
}

pub fn evolve(state: &mut State, event: &DomainEvent) {
    match event {
        DomainEvent::RunAdmitted {
            run,
            input,
            root,
            policy,
            admitted_ms,
            ..
        } => {
            if let Some(run_state) = state.runs.get_mut(run) {
                run_state.input = input.clone();
                run_state.root = *root;
                run_state.policy = policy.clone();
                run_state.status = RunStatus::Active;
                run_state.admitted_ms = *admitted_ms;
            }
        }
        DomainEvent::ScopeOpened {
            run,
            scope,
            parent,
            role,
            input,
        } => {
            state.scopes.insert(
                *scope,
                ScopeState {
                    id: *scope,
                    run: *run,
                    parent: *parent,
                    role: role.clone(),
                    input: input.clone(),
                    status: ScopeStatus::Open,
                    outputs: BTreeMap::new(),
                    current: None,
                },
            );
            if let ScopeRole::LoopBody { activation, index } = role {
                state
                    .loop_carry
                    .insert(*activation, (input.clone(), *index));
            }
        }
        DomainEvent::ScopeCompleted { scope, output, .. } => {
            for act in state.activations.values_mut() {
                if act.scope == *scope {
                    act.status = ActivationStatus::Succeeded;
                }
            }
            if let Some(scope_state) = state.scopes.get_mut(scope) {
                scope_state.status = ScopeStatus::Completed {
                    output: output.clone(),
                };
                if let ScopeRole::ForeachItem { activation, index } = scope_state.role {
                    let slot = state.foreach_done.entry(activation).or_default();
                    if let Some(existing) = slot.iter_mut().find(|(i, _)| *i == index) {
                        existing.1 = output.clone();
                    } else {
                        slot.push((index, output.clone()));
                    }
                }
                if let ScopeRole::ParallelBranch { activation, name } = &scope_state.role {
                    state
                        .parallel_done
                        .entry(*activation)
                        .or_default()
                        .entry(name.clone())
                        .or_insert(output.clone());
                }
                if let ScopeRole::LoopBody { activation, index } = scope_state.role {
                    state
                        .loop_carry
                        .insert(activation, (output.clone(), index + 1));
                }
            }
        }
        DomainEvent::ScopeFailed { scope, error, .. } => {
            if let Some(scope_state) = state.scopes.get_mut(scope) {
                scope_state.status = ScopeStatus::Failed {
                    error: error.clone(),
                };
            }
            for act in state.activations.values_mut() {
                if act.scope == *scope && act.status != ActivationStatus::Succeeded {
                    act.status = ActivationStatus::Failed;
                }
            }
        }
        DomainEvent::ActivationOpened {
            run,
            scope,
            activation,
            node,
        } => {
            let status = {
                let region = state
                    .scopes
                    .get(scope)
                    .and_then(|s| state.runs.get(&s.run).map(|r| &r.definition.root));
                if let Some(region) = region {
                    if let Some(Node::Activity { .. }) = region.nodes.get(node) {
                        ActivationStatus::Ready
                    } else {
                        ActivationStatus::Open
                    }
                } else {
                    ActivationStatus::Open
                }
            };
            // Nested bodies need the child region, not the root. Mark activity nodes Ready
            // by scanning the owning scope's region more carefully below.
            let ready = is_activity_node(state, *scope, node);
            state.activations.insert(
                *activation,
                ActivationState {
                    id: *activation,
                    run: *run,
                    scope: *scope,
                    node: NodeKey(node.clone()),
                    status: if ready {
                        ActivationStatus::Ready
                    } else {
                        ActivationStatus::Open
                    },
                    role: ExecutionRole::Forward,
                    claim: None,
                    attempts: 0,
                },
            );
            if let Some(scope_state) = state.scopes.get_mut(scope) {
                scope_state.current = Some(NodeKey(node.clone()));
            }
            let _ = status;
        }
        DomainEvent::NodeOutputRecorded {
            scope,
            node,
            output,
            ..
        } => {
            if let Some(scope_state) = state.scopes.get_mut(scope) {
                scope_state.outputs.insert(node.clone(), output.clone());
            }
            for act in state.activations.values_mut() {
                if act.scope == *scope && act.node.as_str() == node {
                    if matches!(act.status, ActivationStatus::Open | ActivationStatus::Ready) {
                        act.status = ActivationStatus::Succeeded;
                    }
                }
            }
        }
        DomainEvent::GuardRecorded {
            activation,
            carry,
            index,
            ..
        } => {
            state
                .loop_carry
                .insert(*activation, (carry.clone(), *index));
        }
        DomainEvent::LeafFailed {
            activation, retry, ..
        } => {
            if let Some(act) = state.activations.get_mut(activation) {
                if *retry {
                    act.status = ActivationStatus::Ready;
                    act.claim = None;
                } else {
                    act.status = ActivationStatus::Failed;
                }
            }
        }
        DomainEvent::LeafSucceeded {
            activation, role, ..
        } => {
            if let Some(act) = state.activations.get_mut(activation) {
                act.status = ActivationStatus::Succeeded;
            }
            if *role == ExecutionRole::Compensation {
                if let Some(obligation) = state.obligations.iter_mut().find(|item| {
                    matches!(
                        item.status,
                        ObligationStatus::Compensating { activation: current } if current == *activation
                    )
                }) {
                    obligation.status = ObligationStatus::Compensated;
                }
            }
        }
        DomainEvent::WaitOpened {
            wait,
            run,
            activation,
            signal,
            key,
            deadline_ms,
            consume_from,
        } => {
            let seq = state
                .runs
                .get(run)
                .map(|r| r.next_sequence.get())
                .unwrap_or(0);
            state.waits.insert(
                *wait,
                WaitState {
                    id: *wait,
                    run: *run,
                    activation: *activation,
                    signal: signal.clone(),
                    key: key.clone(),
                    deadline_ms: *deadline_ms,
                    consume_from: *consume_from,
                    opened_sequence: seq,
                    pending: true,
                },
            );
            if let Some(act) = state.activations.get_mut(activation) {
                act.status = ActivationStatus::Ready;
            }
        }
        DomainEvent::WaitSatisfied { wait, event_id, .. } => {
            if let Some(wait_state) = state.waits.get_mut(wait) {
                wait_state.pending = false;
            }
            if let Some(act_id) = state.waits.get(wait).map(|w| w.activation) {
                if let Some(act) = state.activations.get_mut(&act_id) {
                    act.status = ActivationStatus::Succeeded;
                }
            }
            if let Some(entry) = state
                .inbox
                .iter_mut()
                .find(|entry| entry.event_id == *event_id)
            {
                entry.consumed = true;
                entry.reserved_wait = Some(*wait);
            }
        }
        DomainEvent::WaitTimedOut { wait, .. } => {
            if let Some(wait_state) = state.waits.get_mut(wait) {
                wait_state.pending = false;
            }
        }
        DomainEvent::EventAccepted {
            event_id,
            signal,
            key,
            payload,
            sequence,
            accepted_ms,
            expires_ms,
            run,
            ..
        } => {
            state.inbox.push(InboxEntry {
                event_id: *event_id,
                signal: signal.clone(),
                key: key.clone(),
                payload: payload.clone(),
                sequence: *sequence,
                reserved_wait: None,
                consumed: false,
                accepted_ms: *accepted_ms,
                expires_ms: *expires_ms,
                run: Some(*run),
            });
            if let Some(run_state) = state.runs.get_mut(run) {
                run_state.next_sequence = RunSequence::new(sequence + 1);
            }
        }
        DomainEvent::EventExpired { event_id, .. } => {
            state.inbox.retain(|entry| entry.event_id != *event_id);
        }
        DomainEvent::ObligationRegistered {
            run,
            forward,
            handler,
            handler_version,
            input,
        } => {
            if let Some(obligation) = state
                .obligations
                .iter_mut()
                .find(|item| item.forward == *forward)
            {
                obligation.input = input.clone();
                obligation.status = ObligationStatus::Open;
            } else {
                let owner = saga_owner(state, *forward).unwrap_or(*forward);
                state.obligations.push(Obligation {
                    forward: *forward,
                    run: *run,
                    owner,
                    handler: handler.clone(),
                    handler_version: *handler_version,
                    input: input.clone(),
                    status: ObligationStatus::Open,
                });
            }
        }
        DomainEvent::CompensationStarted {
            run,
            saga,
            forward,
            activation,
            handler,
            handler_version,
            input,
            error,
        } => {
            state.saga_errors.insert(*saga, error.clone());
            if let Some(obligation) = state
                .obligations
                .iter_mut()
                .find(|item| item.forward == *forward)
            {
                obligation.status = ObligationStatus::Compensating {
                    activation: *activation,
                };
            }
            let scope = state
                .activations
                .get(saga)
                .map(|act| act.scope)
                .or_else(|| state.scopes.values().find(|s| s.run == *run).map(|s| s.id));
            if let Some(scope) = scope {
                state.activations.insert(
                    *activation,
                    ActivationState {
                        id: *activation,
                        run: *run,
                        scope,
                        node: NodeKey("__compensate".to_owned()),
                        status: ActivationStatus::Ready,
                        role: ExecutionRole::Compensation,
                        claim: None,
                        attempts: 0,
                    },
                );
            }
            let _ = (handler, handler_version, input);
        }
        DomainEvent::ObligationTransferred {
            forward, to_saga, ..
        } => {
            if let Some(obligation) = state
                .obligations
                .iter_mut()
                .find(|item| item.forward == *forward)
            {
                obligation.owner = *to_saga;
            }
        }
        DomainEvent::ObligationReleased { forward, .. } => {
            if let Some(obligation) = state
                .obligations
                .iter_mut()
                .find(|item| item.forward == *forward)
            {
                obligation.status = ObligationStatus::Released;
            }
        }
        DomainEvent::ObligationBlocked {
            run,
            forward,
            handler,
            handler_version,
            reason,
        } => {
            if let Some(obligation) = state
                .obligations
                .iter_mut()
                .find(|item| item.forward == *forward)
            {
                obligation.status = if handler == "irreversible" {
                    ObligationStatus::Irreversible {
                        reason: reason.clone(),
                    }
                } else {
                    ObligationStatus::Blocked {
                        reason: reason.clone(),
                    }
                };
            } else {
                let owner = saga_owner(state, *forward).unwrap_or(*forward);
                let status = if handler == "irreversible" {
                    ObligationStatus::Irreversible {
                        reason: reason.clone(),
                    }
                } else {
                    ObligationStatus::Blocked {
                        reason: reason.clone(),
                    }
                };
                state.obligations.push(Obligation {
                    forward: *forward,
                    run: *run,
                    owner,
                    handler: handler.clone(),
                    handler_version: *handler_version,
                    input: Value::Null,
                    status,
                });
            }
        }
        DomainEvent::EventReserved { wait, event_id } => {
            if let Some(entry) = state
                .inbox
                .iter_mut()
                .find(|entry| entry.event_id == *event_id)
            {
                entry.reserved_wait = Some(*wait);
            }
        }
        DomainEvent::ReservationReleased { event_id, .. } => {
            if let Some(entry) = state
                .inbox
                .iter_mut()
                .find(|entry| entry.event_id == *event_id)
            {
                entry.reserved_wait = None;
            }
        }
        DomainEvent::SessionRegistered {
            session,
            activities,
            capacity,
            expires_ms,
        } => {
            state.sessions.insert(
                *session,
                WorkerSession {
                    id: *session,
                    activities: activities.clone(),
                    capacity: *capacity,
                    expires_ms: *expires_ms,
                    principal_id: String::new(),
                    capabilities: Vec::new(),
                    revision: 0,
                    protocol_min: 0,
                    protocol_max: 0,
                },
            );
        }
        DomainEvent::WorkerRegistered {
            session,
            principal_id,
            capabilities,
            capacity,
            protocol_min,
            protocol_max,
            expires_ms,
        } => {
            state.sessions.insert(
                *session,
                WorkerSession {
                    id: *session,
                    activities: Vec::new(),
                    capacity: *capacity,
                    expires_ms: *expires_ms,
                    principal_id: principal_id.clone(),
                    capabilities: capabilities.clone(),
                    revision: 1,
                    protocol_min: *protocol_min,
                    protocol_max: *protocol_max,
                },
            );
        }
        DomainEvent::WorkerSessionRenewed {
            session,
            revision,
            expires_ms,
        } => {
            if let Some(worker) = state.sessions.get_mut(session) {
                worker.revision = revision.get();
                worker.expires_ms = *expires_ms;
            }
        }
        DomainEvent::ClaimGranted {
            activation,
            session,
            generation,
            revision,
            lease_expiry_ms,
            attempt_deadline_ms,
            effect_key,
            role,
            ..
        } => {
            state.next_generation = state.next_generation.saturating_add(1);
            if let Some(act) = state.activations.get_mut(activation) {
                act.claim = Some(ClaimState {
                    session: *session,
                    generation: *generation,
                    revision: *revision,
                    lease_expiry_ms: *lease_expiry_ms,
                    attempt_deadline_ms: *attempt_deadline_ms,
                    effect_key: *effect_key,
                    role: *role,
                    probes: if *role == ExecutionRole::Reconciliation {
                        act.claim.as_ref().map_or(0, |claim| claim.probes)
                    } else {
                        0
                    },
                    unknown_reported: false,
                });
                act.attempts = act.attempts.saturating_add(1);
            }
        }
        DomainEvent::ClaimRenewed {
            activation,
            revision,
            lease_expiry_ms,
            ..
        } => {
            if let Some(act) = state.activations.get_mut(activation) {
                if let Some(claim) = &mut act.claim {
                    claim.revision = *revision;
                    claim.lease_expiry_ms = *lease_expiry_ms;
                }
            }
        }
        DomainEvent::ClaimCleared { activation } => {
            if let Some(act) = state.activations.get_mut(activation) {
                act.claim = None;
            }
        }
        DomainEvent::ReconciliationFailed { .. } => {}
        DomainEvent::ReconciliationRecorded {
            activation,
            outcome,
            probes,
            ..
        } => {
            if let Some(act) = state.activations.get_mut(activation) {
                if let Some(claim) = &mut act.claim {
                    claim.probes = *probes;
                    if *outcome == ReconcileOutcome::Unknown {
                        claim.unknown_reported = true;
                    }
                }
            }
        }
        DomainEvent::InterventionRequired {
            activation, reason, ..
        } => {
            state.interventions.insert(*activation, reason.clone());
        }
        DomainEvent::RunSucceeded { run, output } => {
            if let Some(run_state) = state.runs.get_mut(run) {
                run_state.status = RunStatus::Succeeded {
                    output: output.clone(),
                };
            }
        }
        DomainEvent::RunFailed { run, error } => {
            if let Some(run_state) = state.runs.get_mut(run) {
                run_state.status = RunStatus::Failed {
                    error: error.clone(),
                };
            }
        }
        DomainEvent::RecoveryAuthorized { .. } => {
            if let Some(hold) = &mut state.recovery {
                hold.authorized = true;
            }
        }
        DomainEvent::AbortIntent { saga, error, .. } => {
            state.saga_errors.insert(*saga, error.clone());
        }
        DomainEvent::CompensationAbandoned {
            run, unresolved, ..
        } => {
            for obligation in &mut state.obligations {
                if obligation.run == *run && unresolved.contains(&obligation.forward) {
                    obligation.status = ObligationStatus::Abandoned;
                }
            }
            for act in state.activations.values_mut() {
                if act.run == *run {
                    act.claim = None;
                }
            }
        }
    }
}

fn is_activity_node(state: &State, scope: ScopeId, node: &str) -> bool {
    matches!(
        lookup_region(state, scope).and_then(|region| region.nodes.get(node)),
        Some(Node::Activity { .. })
    )
}

pub fn start_run(
    state: &mut State,
    command: Command,
    definition: Definition,
    catalog: Catalog,
) -> Result<Vec<DomainEvent>> {
    let CommandBody::Start { run, .. } = &command.body else {
        return Err(Error::invalid("start_run requires Start"));
    };
    if let Some(events) = state.commands.get(&command.id) {
        return Ok(events.clone());
    }
    state.runs.insert(
        *run,
        RunState {
            id: *run,
            definition,
            catalog,
            input: Value::Null,
            policy: CapturedRunPolicy::defaults(None),
            status: RunStatus::Active,
            root: ScopeId::from_bytes([0; 16]),
            next_sequence: RunSequence::new(1),
            admitted_ms: 0,
            terminal_ms: 0,
            published: None,
        },
    );
    let decision = decide(state, &command)?;
    apply_events_with_cause(
        state,
        &decision.events,
        Some(command.id),
        None,
        command.time,
    )?;
    state.commands.insert(command.id, decision.events.clone());
    state
        .command_times
        .insert(command.id, command.time.as_millis());
    crate::history::checkpoint_after_command(state, *run, command.time)?;
    Ok(decision.events)
}

pub fn apply_command(state: &mut State, command: Command) -> Result<Vec<DomainEvent>> {
    let worker_digest = worker_request_digest(&command.body)?;
    if let Some(events) = state.commands.get(&command.id) {
        verify_worker_retry(state, &command, worker_digest.as_deref())?;
        return Ok(events.clone());
    }
    let mut command = command;
    command.time =
        EngineTime::from_millis(command.time.as_millis().max(state.engine_time_watermark_ms));
    state.engine_time_watermark_ms = command.time.as_millis();
    let decision = decide(state, &command)?;
    apply_events_with_cause(
        state,
        &decision.events,
        Some(command.id),
        None,
        command.time,
    )?;
    for event in &decision.events {
        if matches!(
            event,
            DomainEvent::RunSucceeded { .. } | DomainEvent::RunFailed { .. }
        ) {
            if let Some(run) = event_owner(state, event)? {
                if let Some(run_state) = state.runs.get_mut(&run) {
                    run_state.terminal_ms = command.time.as_millis();
                }
            }
        }
    }
    state.commands.insert(command.id, decision.events.clone());
    state
        .command_times
        .insert(command.id, command.time.as_millis());
    if let Some(digest) = worker_digest {
        state.worker_command_digests.insert(command.id, digest);
    }
    let mut affected = std::collections::HashSet::new();
    for event in &decision.events {
        if let Some(run) = event_owner(state, event)? {
            affected.insert(run);
        }
    }
    for run in affected {
        crate::history::checkpoint_after_command(state, run, command.time)?;
    }
    Ok(decision.events)
}

pub(crate) fn worker_request_digest(body: &CommandBody) -> Result<Option<String>> {
    if !matches!(
        body,
        CommandBody::RegisterWorker { .. }
            | CommandBody::RenewWorkerSession { .. }
            | CommandBody::Claim { .. }
            | CommandBody::Renew { .. }
            | CommandBody::ReportAssigned { .. }
            | CommandBody::ReportWorker { .. }
            | CommandBody::Reconcile { .. }
            | CommandBody::ReconcileWorker { .. }
    ) {
        return Ok(None);
    }
    let json = serde_json::to_value(body).map_err(|err| Error::invalid(err.to_string()))?;
    Ok(Some(hex::encode(Sha256::digest(
        crate::value::canonical_json(&json)?,
    ))))
}

fn verify_worker_retry(state: &State, command: &Command, digest: Option<&str>) -> Result<()> {
    match (state.worker_command_digests.get(&command.id), digest) {
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        (None, None) => Ok(()),
        _ => Err(Error::new(
            crate::error::ErrorKind::AlreadyExists,
            "worker command ID reused with different request",
        )),
    }
}

pub fn commit_command(state: &mut State, command: Command) -> Result<Vec<DomainEvent>> {
    let mut command = command;
    if let CommandBody::PruneHistory { limit } = &command.body {
        if *limit == 0 || *limit > 1024 {
            return Err(Error::invalid("retention batch must be 1..=1024"));
        }
        if let Some(events) = state.commands.get(&command.id) {
            return Ok(events.clone());
        }
        command.time =
            EngineTime::from_millis(command.time.as_millis().max(state.engine_time_watermark_ms));
        let mut provisional = state.clone();
        provisional.engine_time_watermark_ms = command.time.as_millis();
        let events =
            crate::history::prune(&mut provisional, command.id, command.time, *limit as usize)?;
        provisional.commands.insert(command.id, events.clone());
        provisional
            .command_times
            .insert(command.id, command.time.as_millis());
        *state = provisional;
        return Ok(events);
    }
    if let CommandBody::Publication { key, operation } = &command.body {
        if key.command_id != command.id || key.cluster_id.is_empty() || key.principal_id.is_empty()
        {
            return Err(Error::invalid("invalid authenticated command identity"));
        }
        if !state.command_results.contains_key(&key.storage_key()) {
            command.time = EngineTime::from_millis(
                command.time.as_millis().max(state.engine_time_watermark_ms),
            );
            state.engine_time_watermark_ms = command.time.as_millis();
        }
        let receipt = crate::publication::apply(state, &command, key, operation);
        receipt.ensure_applied()?;
        return Ok(Vec::new());
    }
    let start_digest = if let CommandBody::Start {
        definition,
        input,
        catalog,
        ..
    } = &command.body
    {
        let logical = serde_json::to_value((&**definition, input, &**catalog))
            .map_err(|err| Error::invalid(err.to_string()))?;
        let bytes = crate::value::canonical_json(&logical)?;
        Some(hex::encode(Sha256::digest(bytes)))
    } else {
        None
    };
    let worker_digest = worker_request_digest(&command.body)?;
    if let Some(events) = state.commands.get(&command.id) {
        verify_worker_retry(state, &command, worker_digest.as_deref())?;
        if let (Some(expected), Some(actual)) = (
            state.legacy_start_digests.get(&command.id),
            start_digest.as_ref(),
        ) {
            if expected != actual {
                return Err(Error::new(
                    crate::error::ErrorKind::AlreadyExists,
                    "command ID reused with a different inline start request",
                ));
            }
        }
        return Ok(events.clone());
    }
    if let CommandBody::Start {
        run,
        definition,
        catalog,
        ..
    } = &command.body
    {
        if !state.runs.contains_key(run) {
            state.runs.insert(
                *run,
                RunState {
                    id: *run,
                    definition: (**definition).clone(),
                    catalog: (**catalog).clone(),
                    input: Value::Null,
                    policy: CapturedRunPolicy::defaults(None),
                    status: RunStatus::Active,
                    root: ScopeId::from_bytes([0; 16]),
                    next_sequence: RunSequence::new(1),
                    admitted_ms: 0,
                    terminal_ms: 0,
                    published: None,
                },
            );
        }
    }
    let id = command.id;
    let events = apply_command(state, command)?;
    if let Some(digest) = start_digest {
        state.legacy_start_digests.insert(id, digest);
    }
    Ok(events)
}

pub fn active_runs(state: &State) -> Vec<RunId> {
    state
        .runs
        .values()
        .filter(|run| matches!(run.status, RunStatus::Active))
        .map(|run| run.id)
        .collect()
}

pub fn ready_activations(state: &State, run: RunId) -> Vec<ActivationId> {
    state
        .activations
        .values()
        .filter(|act| act.run == run && act.status == ActivationStatus::Ready)
        .filter(|act| {
            state
                .waits
                .values()
                .all(|wait| wait.activation != act.id || !wait.pending)
        })
        .map(|act| act.id)
        .collect()
}

pub fn assignments_from(state: &State, events: &[DomainEvent]) -> Result<Vec<AssignmentView>> {
    let mut assignments = Vec::new();
    for event in events {
        let DomainEvent::ClaimGranted {
            run,
            scope,
            activation,
            handler,
            handler_version,
            input,
            attempt,
            session,
            generation,
            attempt_deadline_ms,
            effect_key,
            role,
            ..
        } = event
        else {
            continue;
        };
        let Some(claim) = state
            .activations
            .get(activation)
            .and_then(|act| act.claim.as_ref())
        else {
            continue;
        };
        if claim.session != *session
            || claim.generation != *generation
            || claim.role != *role
            || claim.lease_expiry_ms <= crate::write::now().as_millis()
        {
            continue;
        }
        let catalog = &state
            .runs
            .get(run)
            .ok_or_else(|| Error::invalid("claimed run unavailable"))?
            .catalog;
        let capability = crate::worker_contract::capability_for(
            catalog,
            &crate::ids::ActivityKey::new(handler, *handler_version),
            *role,
        )?;
        assignments.push(AssignmentView {
            run: *run,
            scope: *scope,
            activation: *activation,
            activity_name: handler.clone(),
            activity_version: *handler_version,
            input: input.clone(),
            role: *role,
            capability,
            attempt: *attempt,
            effect_key: *effect_key,
            generation: *generation,
            revision: claim.revision,
            lease_expiry_ms: claim.lease_expiry_ms,
            attempt_deadline_ms: *attempt_deadline_ms,
            session: *session,
        });
    }
    Ok(assignments)
}

#[derive(Clone, Debug)]
pub struct AssignmentView {
    pub run: RunId,
    pub scope: ScopeId,
    pub activation: ActivationId,
    pub activity_name: String,
    pub activity_version: u32,
    pub input: Value,
    pub role: ExecutionRole,
    pub capability: crate::worker_contract::WorkerCapability,
    pub attempt: u32,
    pub effect_key: EffectKey,
    pub generation: OwnerGeneration,
    pub revision: LeaseRevision,
    pub lease_expiry_ms: u64,
    pub attempt_deadline_ms: u64,
    pub session: WorkerSessionId,
}

pub fn activity_key(state: &State, activation: ActivationId) -> Option<(String, u32, Value)> {
    let act = state.activations.get(&activation)?;
    if act
        .claim
        .as_ref()
        .is_some_and(|claim| claim.role == ExecutionRole::Reconciliation)
    {
        let region = lookup_region(state, act.scope)?;
        let Node::Activity {
            activity, input, ..
        } = region.nodes.get(act.node.as_str())?
        else {
            return None;
        };
        let scope = state.scopes.get(&act.scope)?;
        let bound = eval_binding(input, &eval_ctx(state, scope)).ok()?;
        let run = state.runs.get(&act.run)?;
        let contract = run.catalog.activity(activity).ok()?;
        let recon = contract.reconciler.as_ref()?;
        return Some((recon.name.clone(), recon.version, bound));
    }
    if act.role == ExecutionRole::Compensation {
        let obligation = state.obligations.iter().find(|item| {
            matches!(
                item.status,
                ObligationStatus::Compensating { activation: current } if current == activation
            )
        })?;
        return Some((
            obligation.handler.clone(),
            obligation.handler_version,
            obligation.input.clone(),
        ));
    }
    let region = lookup_region(state, act.scope)?;
    let Node::Activity {
        activity, input, ..
    } = region.nodes.get(act.node.as_str())?
    else {
        return None;
    };
    let scope = state.scopes.get(&act.scope)?;
    let bound = eval_binding(input, &eval_ctx(state, scope)).ok()?;
    Some((activity.name.clone(), activity.version, bound))
}

pub fn run_events(state: &State, run: RunId) -> &[DomainEvent] {
    state.history.get(&run).map(Vec::as_slice).unwrap_or(&[])
}

pub fn history_or_unavailable(
    state: &State,
    run: RunId,
    now: EngineTime,
) -> Result<&[DomainEvent]> {
    let Some(run_state) = state.runs.get(&run) else {
        return Err(Error::new(crate::error::ErrorKind::NotFound, "unknown run"));
    };
    if matches!(run_state.status, RunStatus::Active) {
        return Ok(run_events(state, run));
    }
    if run_state.terminal_ms == 0 {
        return Ok(run_events(state, run));
    }
    let retain_ms = run_state
        .policy
        .terminal_history_days
        .saturating_mul(24 * 60 * 60 * 1000);
    if now.as_millis() > run_state.terminal_ms.saturating_add(retain_ms) {
        return Err(Error::new(
            crate::error::ErrorKind::Unavailable,
            "history range is unavailable",
        ));
    }
    Ok(run_events(state, run))
}

fn validate_leaf_output(state: &State, act: &ActivationState, output: &Value) -> Result<()> {
    let Some(run) = state.runs.get(&act.run) else {
        return Ok(());
    };
    if act.role == ExecutionRole::Compensation {
        let obligation = state.obligations.iter().find(|item| matches!(
            item.status, ObligationStatus::Compensating { activation } if activation == act.id
        )).ok_or_else(|| Error::invalid("compensation obligation missing"))?;
        let contract = run.catalog.activity(&crate::ids::ActivityKey::new(
            &obligation.handler,
            obligation.handler_version,
        ))?;
        return run.catalog.validate_value(&contract.output_schema, output);
    }
    let Some(region) = lookup_region(state, act.scope) else {
        return Ok(());
    };
    let Some(Node::Activity { activity, .. }) = region.nodes.get(act.node.as_str()) else {
        return Ok(());
    };
    let Ok(contract) = run.catalog.activity(activity) else {
        return Ok(());
    };
    run.catalog.validate_value(&contract.output_schema, output)
}

pub fn reconstruct(
    definition: Definition,
    catalog: Catalog,
    events: &[DomainEvent],
) -> Result<State> {
    let Some(DomainEvent::RunAdmitted {
        run,
        input,
        root,
        policy,
        ..
    }) = events.first()
    else {
        return Err(Error::invalid("history must start with run admission"));
    };
    let mut state = State::default();
    state.runs.insert(
        *run,
        RunState {
            id: *run,
            definition,
            catalog,
            input: input.clone(),
            policy: policy.clone(),
            status: RunStatus::Active,
            root: *root,
            next_sequence: RunSequence::new(1),
            admitted_ms: 0,
            terminal_ms: 0,
            published: None,
        },
    );
    apply_events(&mut state, events)?;
    Ok(state)
}

fn lookup_region(state: &State, scope: ScopeId) -> Option<&Region> {
    let scope_state = state.scopes.get(&scope)?;
    let run = state.runs.get(&scope_state.run)?;
    match &scope_state.role {
        ScopeRole::Root => Some(&run.definition.root),
        ScopeRole::LoopBody { activation, .. }
        | ScopeRole::ForeachItem { activation, .. }
        | ScopeRole::SagaBody { activation } => {
            let parent = state.activations.get(activation)?;
            let parent_region = lookup_region(state, parent.scope)?;
            match parent_region.nodes.get(parent.node.as_str())? {
                Node::While { body, .. }
                | Node::DoWhile { body, .. }
                | Node::Repeat { body, .. }
                | Node::Foreach { body, .. }
                | Node::Saga { body, .. } => Some(body),
                _ => None,
            }
        }
        ScopeRole::ParallelBranch { activation, name } => {
            let parent = state.activations.get(activation)?;
            let parent_region = lookup_region(state, parent.scope)?;
            match parent_region.nodes.get(parent.node.as_str())? {
                Node::Parallel { branches, .. } => {
                    branches.iter().find(|b| b.name == *name).map(|b| &b.body)
                }
                _ => None,
            }
        }
        ScopeRole::ChooseBody { activation, name } => {
            let parent = state.activations.get(activation)?;
            let parent_region = lookup_region(state, parent.scope)?;
            match parent_region.nodes.get(parent.node.as_str())? {
                Node::Choose { cases, default, .. } => {
                    if name == "default" {
                        Some(default)
                    } else {
                        cases.iter().find(|c| c.name == *name).map(|c| &c.body)
                    }
                }
                _ => None,
            }
        }
    }
}

pub fn run_output(state: &State, run: RunId) -> Option<Value> {
    match state.runs.get(&run)?.status {
        RunStatus::Succeeded { ref output } => Some(output.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::compiler::compile_yaml;

    fn catalog() -> Catalog {
        Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap()
    }

    fn apply(
        state: &mut State,
        time: u64,
        body: CommandBody,
    ) -> crate::error::Result<Vec<DomainEvent>> {
        apply_command(
            state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(time),
                body,
            },
        )
    }

    fn start_yaml(yaml: &str, input: Value) -> (State, RunId) {
        let catalog = catalog();
        let definition = compile_yaml(yaml, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        (state, run)
    }

    #[test]
    fn retrying_start_command_with_different_run_preserves_state() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let id = CommandId::generate();
        let original_run = RunId::generate();
        let retry_run = RunId::generate();
        let events = commit_command(
            &mut state,
            Command {
                id,
                time: EngineTime::from_millis(10),
                body: CommandBody::Start {
                    run: original_run,
                    definition: Box::new(definition.clone()),
                    input: Value::String("original".to_owned()),
                    catalog: Box::new(catalog.clone()),
                },
            },
        )
        .unwrap();
        let Some(DomainEvent::RunAdmitted { run, root, .. }) = events.first() else {
            panic!("initial start did not admit a run");
        };
        assert_eq!(*run, original_run);
        assert_eq!(state.runs[&original_run].root, *root);
        assert_eq!(state.scopes[root].run, original_run);
        assert_eq!(state.history[&original_run], events);
        assert_eq!(state.commands[&id], events);
        let before_retry = serde_json::to_value(&state).unwrap();

        let replayed = commit_command(
            &mut state,
            Command {
                id,
                time: EngineTime::from_millis(20),
                body: CommandBody::Start {
                    run: retry_run,
                    definition: Box::new(definition.clone()),
                    input: Value::String("original".to_owned()),
                    catalog: Box::new(catalog.clone()),
                },
            },
        )
        .unwrap();
        assert_eq!(replayed, events);
        assert_eq!(state.commands[&id], events);
        assert!(!state.runs.contains_key(&retry_run));
        assert_eq!(serde_json::to_value(&state).unwrap(), before_retry);

        let conflict = commit_command(
            &mut state,
            Command {
                id,
                time: EngineTime::from_millis(25),
                body: CommandBody::Start {
                    run: retry_run,
                    definition: Box::new(definition.clone()),
                    input: Value::String("different".to_owned()),
                    catalog: Box::new(catalog.clone()),
                },
            },
        )
        .unwrap_err();
        assert_eq!(conflict.kind, crate::error::ErrorKind::AlreadyExists);
        assert!(!state.runs.contains_key(&retry_run));
        assert_eq!(serde_json::to_value(&state).unwrap(), before_retry);

        let other_run = RunId::generate();
        let replayed = start_run(
            &mut state,
            Command {
                id,
                time: EngineTime::from_millis(30),
                body: CommandBody::Start {
                    run: other_run,
                    definition: Box::new(definition.clone()),
                    input: Value::Null,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        assert_eq!(replayed, events);
        assert!(!state.runs.contains_key(&other_run));
        assert_eq!(serde_json::to_value(&state).unwrap(), before_retry);
    }

    fn handler(name: &str, input: &Value) -> Value {
        match name {
            "counter.increment" => {
                let Value::Object(fields) = input else {
                    panic!("counter");
                };
                let value = fields.get("value").unwrap().as_i64().unwrap() + 1;
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(value))]))
            }
            "inventory.reserve" => {
                let Value::Object(fields) = input else {
                    panic!("order");
                };
                Value::Object(BTreeMap::from([
                    (
                        "order_id".to_owned(),
                        fields
                            .get("order_id")
                            .cloned()
                            .unwrap_or(Value::String(String::new())),
                    ),
                    (
                        "amount".to_owned(),
                        fields.get("amount").cloned().unwrap_or(Value::Int(0)),
                    ),
                    (
                        "reservation_id".to_owned(),
                        Value::String("res-1".to_owned()),
                    ),
                ]))
            }
            "payment.charge" => {
                let Value::Object(fields) = input else {
                    panic!("reserved");
                };
                let mut out = BTreeMap::new();
                out.insert(
                    "order_id".to_owned(),
                    fields.get("order_id").unwrap().clone(),
                );
                out.insert("amount".to_owned(), fields.get("amount").unwrap().clone());
                out.insert("payment_id".to_owned(), Value::String("pay-1".to_owned()));
                Value::Object(out)
            }
            "tax.quote" => {
                let Value::Object(fields) = input else {
                    panic!("order");
                };
                let amount = fields.get("amount").unwrap().as_i64().unwrap();
                Value::Object(BTreeMap::from([(
                    "cents".to_owned(),
                    Value::Int(amount / 10),
                )]))
            }
            "shipping.quote" => {
                Value::Object(BTreeMap::from([("cents".to_owned(), Value::Int(500))]))
            }
            "remote.echo" | "test.gate" => input.clone(),
            "inventory.release" | "payment.refund" => Value::Null,
            _ => input.clone(),
        }
    }

    fn drive(yaml: &str, input: Value) -> Value {
        let catalog = catalog();
        let definition = compile_yaml(yaml, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        for i in 0..64 {
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1_000_000 + i * 1_000),
                    body: CommandBody::Progress { run },
                },
            )
            .unwrap();
            if !matches!(state.runs.get(&run).unwrap().status, RunStatus::Active) {
                break;
            }
            let ready = ready_activations(&state, run);
            if ready.is_empty() {
                continue;
            }
            for activation in ready {
                let Some((name, _, input)) = activity_key(&state, activation) else {
                    continue;
                };
                let output = handler(&name, &input);
                apply_command(
                    &mut state,
                    Command {
                        id: CommandId::generate(),
                        time: EngineTime::from_millis(1_000_000),
                        body: CommandBody::ReportLeaf {
                            run,
                            activation,
                            output,
                        },
                    },
                )
                .unwrap();
            }
        }
        run_output(&state, run).expect("run completed")
    }

    #[test]
    fn sequence_fixture_completes() {
        let output = drive(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let Value::Object(fields) = output else {
            panic!("receipt");
        };
        assert_eq!(fields.get("payment_id").unwrap().as_str(), Some("pay-1"));
        assert_eq!(fields.get("amount").unwrap().as_i64(), Some(1000));
    }

    #[test]
    fn while_fixture_counts_to_three() {
        let output = drive(
            include_str!("../../docs/specs/v1/examples/while.yaml"),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
        );
        let Value::Object(fields) = output else {
            panic!("counter");
        };
        assert_eq!(fields.get("value").unwrap().as_i64(), Some(3));
    }

    #[test]
    fn do_while_counts_to_three() {
        let dw = drive(
            include_str!("../../docs/specs/v1/examples/do-while.yaml"),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
        );
        assert_eq!(dw.pointer("/value").unwrap().as_i64(), Some(3));
    }

    #[test]
    fn repeat_three_times() {
        let rp = drive(
            include_str!("../../docs/specs/v1/examples/repeat.yaml"),
            Value::Object(BTreeMap::from([
                ("count".to_owned(), Value::Int(3)),
                (
                    "counter".to_owned(),
                    Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                ),
            ])),
        );
        assert_eq!(rp.pointer("/value").unwrap().as_i64(), Some(4));
    }

    #[test]
    fn foreach_preserves_order() {
        let fe = drive(
            include_str!("../../docs/specs/v1/examples/foreach.yaml"),
            Value::Array(vec![
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(3))])),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(3))])),
            ]),
        );
        let Value::Array(items) = fe else {
            panic!("array");
        };
        assert_eq!(items[0].pointer("/value").unwrap().as_i64(), Some(4));
        assert_eq!(items[1].pointer("/value").unwrap().as_i64(), Some(2));
        assert_eq!(items[2].pointer("/value").unwrap().as_i64(), Some(4));
    }

    #[test]
    fn parallel_quotes() {
        let output = drive(
            include_str!("../../docs/specs/v1/examples/parallel.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let Value::Array(items) = output else {
            panic!("tuple");
        };
        assert_eq!(items[0].pointer("/cents").unwrap().as_i64(), Some(100));
        assert_eq!(items[1].pointer("/cents").unwrap().as_i64(), Some(500));
    }

    #[test]
    fn timers_and_nested_controls() {
        let timers = drive(
            include_str!("../../docs/specs/v1/examples/timers.yaml"),
            Value::Null,
        );
        assert_eq!(timers, Value::Null);
        let nested = drive(
            include_str!("../../docs/specs/v1/examples/nested-controls.yaml"),
            Value::Array(vec![
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(4))])),
            ]),
        );
        let Value::Array(items) = nested else {
            panic!("nested");
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn saga_success_path() {
        let output = drive(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        assert_eq!(
            output.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
    }

    fn drive_state(yaml: &str, input: Value) -> State {
        let catalog = catalog();
        let definition = compile_yaml(yaml, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        for i in 0..64 {
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1_000_000 + i * 1_000),
                    body: CommandBody::Progress { run },
                },
            )
            .unwrap();
            if !matches!(state.runs.get(&run).unwrap().status, RunStatus::Active) {
                break;
            }
            for activation in ready_activations(&state, run) {
                let Some((name, _, input)) = activity_key(&state, activation) else {
                    continue;
                };
                let output = handler(&name, &input);
                apply_command(
                    &mut state,
                    Command {
                        id: CommandId::generate(),
                        time: EngineTime::from_millis(1_000_000),
                        body: CommandBody::ReportLeaf {
                            run,
                            activation,
                            output,
                        },
                    },
                )
                .unwrap();
            }
        }
        state
    }

    #[test]
    fn saga_failure_compensates_in_reverse() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
                ("fail_after_payment".to_owned(), Value::Bool(true)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
        let handlers: Vec<_> = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                DomainEvent::CompensationStarted { handler, .. } => Some(handler.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(handlers, ["payment.refund", "inventory.release"]);
        assert!(
            state
                .obligations
                .iter()
                .all(|item| matches!(item.status, ObligationStatus::Compensated))
        );
    }

    #[test]
    fn nested_saga_transfers_then_compensates() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/nested-saga.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
        let handlers: Vec<_> = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                DomainEvent::CompensationStarted { handler, .. } => Some(handler.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(handlers, ["payment.refund", "inventory.release"]);
    }

    #[test]
    fn reconstruct_matches_live_state() {
        let live = drive_state(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *live.runs.keys().next().unwrap();
        let run_state = live.runs.get(&run).unwrap();
        let rebuilt = reconstruct(
            run_state.definition.clone(),
            run_state.catalog.clone(),
            live.history.get(&run).unwrap(),
        )
        .unwrap();
        let live_out = run_output(&live, run).unwrap();
        let rebuilt_out = run_output(&rebuilt, run).unwrap();
        assert_eq!(live_out, rebuilt_out);
        assert_eq!(
            live_out.pointer("/payment_id").unwrap().as_str(),
            Some("pay-1")
        );
        assert_eq!(
            live.runs.get(&run).unwrap().status,
            rebuilt.runs.get(&run).unwrap().status
        );
        assert_eq!(live.scopes.len(), rebuilt.scopes.len());
        assert_eq!(
            live.history.get(&run).map(Vec::len),
            rebuilt.history.get(&run).map(Vec::len)
        );
    }

    #[test]
    fn while_already_done() {
        let output = drive(
            include_str!("../../docs/specs/v1/examples/while.yaml"),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(5))])),
        );
        let Value::Object(fields) = output else {
            panic!("counter");
        };
        assert_eq!(fields.get("value").unwrap().as_i64(), Some(5));
    }

    #[test]
    fn duplicate_command_does_not_reapply() {
        let mut state = drive_state(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        let command = Command {
            id: CommandId::generate(),
            time: EngineTime::from_millis(1_000_000),
            body: CommandBody::Progress { run },
        };
        let first = apply_command(&mut state, command.clone()).unwrap();
        let count = state.history.get(&run).map(Vec::len).unwrap_or(0);
        let second = apply_command(&mut state, command).unwrap();
        assert_eq!(first, second);
        assert_eq!(count, state.history.get(&run).map(Vec::len).unwrap_or(0));
    }

    #[test]
    fn claimed_activation_rejects_unassigned_report() {
        let mut state = State::default();
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let session = WorkerSessionId::generate();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let err = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Null,
                },
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
    }

    fn manual_recon_claim() -> (State, RunId, ActivationId, WorkerSessionId) {
        let catalog = catalog();
        let definition = compile_yaml(manual_yaml(), &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let session = WorkerSessionId::generate();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let later = crate::policy::SESSION_LEASE.as_millis() as u64 + 2;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        (state, run, activation, session)
    }

    fn manual_yaml() -> &'static str {
        r#"
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
"#
    }

    #[test]
    fn expired_manual_claim_is_reconciled() {
        let catalog = catalog();
        let definition = compile_yaml(manual_yaml(), &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        let input = Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))]));
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let session = WorkerSessionId::generate();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        assert_eq!(
            state.activations[&activation].claim.as_ref().unwrap().role,
            ExecutionRole::Forward
        );
        let later = crate::policy::SESSION_LEASE.as_millis() as u64 + 2;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let claim = state.activations[&activation].claim.clone().unwrap();
        assert_eq!(claim.role, ExecutionRole::Reconciliation);
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(crate::policy::SESSION_LEASE.as_millis() as u64 + 2),
                body: CommandBody::Reconcile {
                    run,
                    activation,
                    session,
                    generation: claim.generation,
                    revision: claim.revision,
                    outcome: ReconcileOutcome::Applied,
                    output: Some(Value::Object(BTreeMap::from([(
                        "value".to_owned(),
                        Value::Int(2),
                    )]))),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(crate::policy::SESSION_LEASE.as_millis() as u64 + 3),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert_eq!(
            run_output(&state, run)
                .unwrap()
                .pointer("/value")
                .unwrap()
                .as_i64(),
            Some(2)
        );
    }

    #[test]
    fn unknown_probes_enter_intervention() {
        let (mut state, run, activation, first_session) = manual_recon_claim();
        let lease = crate::policy::SESSION_LEASE.as_millis() as u64;
        let budget = crate::policy::RECONCILIATION_PROBES;
        assert_eq!(budget, 3);
        let effect_key = state.activations[&activation]
            .claim
            .as_ref()
            .unwrap()
            .effect_key;
        let mut previous = None;

        for probe in 1..=budget {
            let now = (lease + 2) * u64::from(probe);
            let session = if probe == 1 {
                first_session
            } else {
                let session = WorkerSessionId::generate();
                apply(
                    &mut state,
                    now,
                    CommandBody::RegisterSession {
                        session,
                        activities: vec!["*".to_owned()],
                        capacity: 8,
                    },
                )
                .unwrap();
                apply(
                    &mut state,
                    now,
                    CommandBody::Claim {
                        session,
                        capacity: 8,
                    },
                )
                .unwrap();
                session
            };
            let claim = state.activations[&activation].claim.clone().unwrap();
            assert_eq!(claim.role, ExecutionRole::Reconciliation);
            assert_eq!(claim.probes, probe - 1);
            assert!(!claim.unknown_reported);
            assert_eq!(claim.effect_key, effect_key);

            if let Some((old_session, old_generation, old_revision)) = previous {
                assert_ne!(claim.generation, old_generation);
                let err = apply(
                    &mut state,
                    now,
                    CommandBody::Reconcile {
                        run,
                        activation,
                        session: old_session,
                        generation: old_generation,
                        revision: old_revision,
                        outcome: ReconcileOutcome::Unknown,
                        output: None,
                    },
                )
                .unwrap_err();
                assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
                assert_eq!(
                    state.activations[&activation]
                        .claim
                        .as_ref()
                        .unwrap()
                        .probes,
                    probe - 1
                );
            }

            let command = Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(now),
                body: CommandBody::Reconcile {
                    run,
                    activation,
                    session,
                    generation: claim.generation,
                    revision: claim.revision,
                    outcome: ReconcileOutcome::Unknown,
                    output: None,
                },
            };
            let events = apply_command(&mut state, command.clone()).unwrap();
            assert!(matches!(
                events.as_slice(),
                [DomainEvent::ReconciliationRecorded {
                    outcome: ReconcileOutcome::Unknown,
                    probes,
                    ..
                }, ..] if *probes == probe
            ));
            assert_eq!(
                state.interventions.contains_key(&activation),
                probe == budget
            );
            let history_len = state.history[&run].len();
            assert_eq!(apply_command(&mut state, command).unwrap(), events);
            assert_eq!(state.history[&run].len(), history_len);

            if probe < budget {
                assert_eq!(
                    state.activations[&activation]
                        .claim
                        .as_ref()
                        .unwrap()
                        .probes,
                    probe
                );
                let err = apply(
                    &mut state,
                    now + 1,
                    CommandBody::Reconcile {
                        run,
                        activation,
                        session,
                        generation: claim.generation,
                        revision: claim.revision,
                        outcome: ReconcileOutcome::Unknown,
                        output: None,
                    },
                )
                .unwrap_err();
                assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
                assert_eq!(
                    state.activations[&activation]
                        .claim
                        .as_ref()
                        .unwrap()
                        .probes,
                    probe
                );
                assert_eq!(state.history[&run].len(), history_len);
            } else {
                assert!(state.activations[&activation].claim.is_none());
            }

            let run_state = &state.runs[&run];
            let rebuilt = reconstruct(
                run_state.definition.clone(),
                run_state.catalog.clone(),
                &state.history[&run],
            )
            .unwrap();
            assert_eq!(rebuilt.next_generation, state.next_generation);
            assert_eq!(rebuilt.interventions, state.interventions);
            assert_eq!(rebuilt.history[&run], state.history[&run]);
            assert_eq!(
                rebuilt.activations[&activation]
                    .claim
                    .as_ref()
                    .map(|claim| claim.probes),
                state.activations[&activation]
                    .claim
                    .as_ref()
                    .map(|claim| claim.probes)
            );
            let snapshot: State =
                serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
            assert_eq!(
                snapshot.activations[&activation]
                    .claim
                    .as_ref()
                    .map(|claim| claim.probes),
                rebuilt.activations[&activation]
                    .claim
                    .as_ref()
                    .map(|claim| claim.probes)
            );
            state = rebuilt;
            previous = Some((session, claim.generation, claim.revision));
        }

        let history = &state.history[&run];
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event, DomainEvent::ReconciliationRecorded { .. }))
                .count(),
            budget as usize
        );
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(
                    event,
                    DomainEvent::ClaimGranted {
                        role: ExecutionRole::Forward,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(event, DomainEvent::InterventionRequired { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn terminal_reconciliation_after_unknown_does_not_repeat_forward_effect() {
        for outcome in [ReconcileOutcome::Applied, ReconcileOutcome::NotApplied] {
            let (mut state, run, activation, first_session) = manual_recon_claim();
            let now = crate::policy::SESSION_LEASE.as_millis() as u64 + 2;
            let first_claim = state.activations[&activation].claim.clone().unwrap();
            apply(
                &mut state,
                now,
                CommandBody::Reconcile {
                    run,
                    activation,
                    session: first_session,
                    generation: first_claim.generation,
                    revision: first_claim.revision,
                    outcome: ReconcileOutcome::Unknown,
                    output: None,
                },
            )
            .unwrap();

            let session = WorkerSessionId::generate();
            let later = now + crate::policy::SESSION_LEASE.as_millis() as u64 + 2;
            apply(
                &mut state,
                later,
                CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            )
            .unwrap();
            apply(
                &mut state,
                later,
                CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            )
            .unwrap();
            let claim = state.activations[&activation].claim.clone().unwrap();
            assert_eq!(claim.role, ExecutionRole::Reconciliation);
            assert_eq!(claim.probes, 1);
            assert_eq!(claim.effect_key, first_claim.effect_key);
            assert_eq!(
                state.history[&run]
                    .iter()
                    .filter(|event| matches!(
                        event,
                        DomainEvent::ClaimGranted {
                            role: ExecutionRole::Forward,
                            ..
                        }
                    ))
                    .count(),
                1
            );

            let output = Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))]));
            let events = apply(
                &mut state,
                later,
                CommandBody::Reconcile {
                    run,
                    activation,
                    session,
                    generation: claim.generation,
                    revision: claim.revision,
                    outcome,
                    output: (outcome == ReconcileOutcome::Applied).then_some(output.clone()),
                },
            )
            .unwrap();
            assert!(matches!(
                events.first(),
                Some(DomainEvent::ReconciliationRecorded { probes: 2, .. })
            ));
            assert!(!state.interventions.contains_key(&activation));
            if outcome == ReconcileOutcome::Applied {
                apply(&mut state, later + 1, CommandBody::Progress { run }).unwrap();
                assert_eq!(run_output(&state, run), Some(output));
            } else {
                assert!(state.activations[&activation].claim.is_none());
                assert_eq!(
                    state.activations[&activation].status,
                    ActivationStatus::Ready
                );
                apply(
                    &mut state,
                    later,
                    CommandBody::Claim {
                        session,
                        capacity: 8,
                    },
                )
                .unwrap();
                let retry = state.activations[&activation].claim.as_ref().unwrap();
                assert_eq!(retry.role, ExecutionRole::Forward);
                assert_eq!(retry.probes, 0);
            }
        }
    }

    #[test]
    fn not_applied_clears_claim_and_allows_forward_retry() {
        let (mut state, run, activation, session) = manual_recon_claim();
        let claim = state.activations[&activation].claim.clone().unwrap();
        assert_eq!(claim.role, ExecutionRole::Reconciliation);
        let later = crate::policy::SESSION_LEASE.as_millis() as u64 + 2;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::Reconcile {
                    run,
                    activation,
                    session,
                    generation: claim.generation,
                    revision: claim.revision,
                    outcome: ReconcileOutcome::NotApplied,
                    output: None,
                },
            },
        )
        .unwrap();
        assert!(state.activations[&activation].claim.is_none());
        assert_eq!(
            state.activations[&activation].status,
            ActivationStatus::Ready
        );
        assert!(!state.interventions.contains_key(&activation));
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let retry = state.activations[&activation].claim.clone().unwrap();
        assert_eq!(retry.role, ExecutionRole::Forward);
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later),
                body: CommandBody::ReportAssigned {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
                    session,
                    generation: retry.generation,
                    revision: retry.revision,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(later + 1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert_eq!(
            run_output(&state, run)
                .unwrap()
                .pointer("/value")
                .unwrap()
                .as_i64(),
            Some(2)
        );
    }

    fn loop_yaml(max_iterations: u32) -> String {
        format!(
            r#"
dsl: graphrun/v1
id: loop_limit
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: count
nodes:
  count:
    kind: while
    state_schema: counter/v1
    state: {{from: workflow.input}}
    condition:
      lt:
        - {{from: loop.state, path: /value}}
        - {{literal: 3}}
    max_iterations: {max_iterations}
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: increment
      nodes:
        increment:
          kind: activity
          activity: {{name: counter.increment, version: 1}}
          input: {{from: scope.input}}
          next: done
        done:
          kind: complete
          output: {{from: nodes.increment.output}}
    next: finish
  finish:
    kind: complete
    output: {{from: nodes.count.output}}
"#
        )
    }

    #[test]
    fn loop_limit_boundary() {
        let ok = drive_state(
            &loop_yaml(1),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
        );
        let run = *ok.runs.keys().next().unwrap();
        match &ok.runs.get(&run).unwrap().status {
            RunStatus::Succeeded { output } => {
                assert_eq!(output.pointer("/value").unwrap().as_i64(), Some(3));
            }
            other => panic!("false at the limit must succeed, got {other:?}"),
        }
        let failed = drive_state(
            &loop_yaml(1),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
        );
        let run = *failed.runs.keys().next().unwrap();
        match &failed.runs.get(&run).unwrap().status {
            RunStatus::Failed { error } => assert_eq!(error.code, "LoopLimitExceeded"),
            other => panic!("expected LoopLimitExceeded, got {other:?}"),
        }
    }

    #[test]
    fn nested_child_complete_does_not_complete_root() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/nested-controls.yaml"),
            Value::Array(vec![
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(4))])),
            ]),
        );
        let run = *state.runs.keys().next().unwrap();
        let events = state.history.get(&run).unwrap();
        let mut root = None;
        let mut root_completed_at = None;
        for (i, event) in events.iter().enumerate() {
            match event {
                DomainEvent::ScopeOpened {
                    scope,
                    role: ScopeRole::Root,
                    ..
                } => root = Some(*scope),
                DomainEvent::ScopeCompleted { scope, .. } if Some(*scope) == root => {
                    root_completed_at = Some(i);
                }
                DomainEvent::ScopeCompleted { .. } => {
                    if let Some(at) = root_completed_at {
                        panic!("child completed after root at {at}");
                    }
                }
                _ => {}
            }
        }
        assert!(root_completed_at.is_some());
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Succeeded { .. }
        ));
    }

    #[test]
    fn generated_nested_controls_match_oracle() {
        fn expected(values: &[i64]) -> Value {
            Value::Array(
                values
                    .iter()
                    .map(|value| {
                        let after_repeat = *value + 2;
                        Value::Array(vec![
                            Value::Object(BTreeMap::from([(
                                "value".to_owned(),
                                Value::Int(after_repeat + 1),
                            )])),
                            Value::Object(BTreeMap::from([(
                                "value".to_owned(),
                                Value::Int(after_repeat),
                            )])),
                        ])
                    })
                    .collect(),
            )
        }
        for values in [vec![], vec![1], vec![1, 4], vec![0, 2, 5]] {
            let input = Value::Array(
                values
                    .iter()
                    .map(|value| {
                        Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(*value))]))
                    })
                    .collect(),
            );
            if values.is_empty() {
                let output = drive(
                    include_str!("../../docs/specs/v1/examples/foreach.yaml"),
                    Value::Array(Vec::new()),
                );
                assert_eq!(output, Value::Array(Vec::new()));
                continue;
            }
            let output = drive(
                include_str!("../../docs/specs/v1/examples/nested-controls.yaml"),
                input,
            );
            assert_eq!(output, expected(&values), "values={values:?}");
        }
    }

    #[test]
    fn foreach_respects_concurrency_window() {
        let yaml = r#"
dsl: graphrun/v1
id: foreach_window
version: 1
input_schema: {array: counter/v1}
output_schema: {array: counter/v1}
start: increment_all
nodes:
  increment_all:
    kind: foreach
    items: {from: workflow.input}
    item_schema: counter/v1
    max_items: 10
    max_concurrency: 1
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: increment
      nodes:
        increment:
          kind: activity
          activity: {name: counter.increment, version: 1}
          input: {from: item.value}
          next: done
        done:
          kind: complete
          output: {from: nodes.increment.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.increment_all.output}
"#;
        let catalog = catalog();
        let definition = compile_yaml(yaml, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        let input = Value::Array(vec![
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(3))])),
        ]);
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input,
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        for _ in 0..8 {
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1),
                    body: CommandBody::Progress { run },
                },
            )
            .unwrap();
        }
        let open_items = state
            .scopes
            .values()
            .filter(|scope| {
                scope.run == run
                    && matches!(scope.role, ScopeRole::ForeachItem { .. })
                    && scope.status == ScopeStatus::Open
            })
            .count();
        assert!(
            open_items <= 1,
            "foreach opened {open_items} item scopes with max_concurrency 1"
        );
        assert_eq!(ready_activations(&state, run).len(), 1);
    }

    #[test]
    fn obligation_registers_with_forward_success() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                    ])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let events = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1000)),
                        (
                            "reservation_id".to_owned(),
                            Value::String("res-1".to_owned()),
                        ),
                    ])),
                },
            },
        )
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DomainEvent::LeafSucceeded { .. }))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            DomainEvent::ObligationRegistered {
                handler,
                ..
            } if handler == "inventory.release"
        )));
    }

    #[test]
    fn nested_saga_does_not_double_compensate() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/nested-saga.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        let mut counts = BTreeMap::<String, usize>::new();
        for event in state.history.get(&run).unwrap() {
            if let DomainEvent::CompensationStarted { handler, .. } = event {
                *counts.entry(handler.clone()).or_default() += 1;
            }
        }
        assert_eq!(counts.get("payment.refund").copied(), Some(1));
        assert_eq!(counts.get("inventory.release").copied(), Some(1));
    }

    #[test]
    fn captured_policy_defaults_are_stable() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        let policy = &state.runs.get(&run).unwrap().policy;
        assert_eq!(
            policy.forward_attempt_timeout_ms,
            crate::policy::FORWARD_ATTEMPT_TIMEOUT.as_millis() as u64
        );
        assert_eq!(
            policy.compensation_attempt_timeout_ms,
            crate::policy::COMPENSATION_ATTEMPT_TIMEOUT.as_millis() as u64
        );
        assert_eq!(
            policy.reconciliation_attempt_timeout_ms,
            crate::policy::RECONCILIATION_ATTEMPT_TIMEOUT.as_millis() as u64
        );
        assert_eq!(
            policy.terminal_history_days,
            crate::policy::TERMINAL_HISTORY_DAYS
        );
        assert_eq!(
            policy.terminal_summary_days,
            crate::policy::TERMINAL_SUMMARY_DAYS
        );
        assert_eq!(
            policy.command_result_hours,
            crate::policy::COMMAND_RESULT_HOURS
        );
        assert_eq!(
            policy.unreserved_event_days,
            crate::policy::UNRESERVED_EVENT_DAYS
        );
        assert_eq!(
            policy.checkpoint_event_cadence,
            crate::policy::CHECKPOINT_EVENT_CADENCE
        );
        let admitted = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .find_map(|event| match event {
                DomainEvent::RunAdmitted { policy, .. } => Some(policy.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(admitted, *policy);
        let rebuilt = reconstruct(
            state.runs.get(&run).unwrap().definition.clone(),
            state.runs.get(&run).unwrap().catalog.clone(),
            state.history.get(&run).unwrap(),
        )
        .unwrap();
        assert_eq!(rebuilt.runs.get(&run).unwrap().policy, *policy);
    }

    #[test]
    fn binding_eval_missing_null_and_no_coercion() {
        let missing = r#"
dsl: graphrun/v1
id: missing_field
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {from: workflow.input, path: /missing}
"#;
        let catalog = catalog();
        let definition = compile_yaml(missing, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        let err = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap_err();
        assert!(
            err.message.contains("missing object field"),
            "{}",
            err.message
        );

        let null_out = drive(
            r#"
dsl: graphrun/v1
id: null_ok
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {literal: null}
"#,
            Value::Null,
        );
        assert_eq!(null_out, Value::Null);

        let coerced = r#"
dsl: graphrun/v1
id: no_coerce
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: count
nodes:
  count:
    kind: while
    state_schema: counter/v1
    state: {from: workflow.input}
    condition:
      eq:
        - {from: loop.state, path: /value}
        - {literal: "3"}
    max_iterations: 1
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: increment
      nodes:
        increment:
          kind: activity
          activity: {name: counter.increment, version: 1}
          input: {from: scope.input}
          next: done
        done:
          kind: complete
          output: {from: nodes.increment.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.count.output}
"#;
        let output = drive(
            coerced,
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(3))])),
        );
        assert_eq!(
            output.pointer("/value").unwrap().as_i64(),
            Some(3),
            "string 3 must not coerce to integer 3"
        );
    }

    #[test]
    fn event_before_wait_is_consumed() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let event_id = EventId::from_bytes([7; 16]);
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::Signal {
                    run,
                    event_id,
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(true),
                    )])),
                },
            },
        )
        .unwrap();
        assert!(state.inbox.iter().any(|entry| entry.event_id == event_id));
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(3),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(4),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert_eq!(
            run_output(&state, run)
                .unwrap()
                .pointer("/approved")
                .unwrap(),
            &Value::Bool(true)
        );
        assert!(
            state
                .inbox
                .iter()
                .any(|entry| entry.event_id == event_id && entry.consumed)
        );
    }

    #[test]
    fn event_identity_and_fifo() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        let first = EventId::from_bytes([1; 16]);
        let payload = Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))]));
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Signal {
                    run,
                    event_id: first,
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: payload.clone(),
                },
            },
        )
        .unwrap();
        let dup = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::Signal {
                    run,
                    event_id: first,
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: payload.clone(),
                },
            },
        )
        .unwrap();
        assert!(dup.is_empty());
        let conflict = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(3),
                body: CommandBody::Signal {
                    run,
                    event_id: first,
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(false),
                    )])),
                },
            },
        )
        .unwrap_err();
        assert_eq!(conflict.kind, crate::error::ErrorKind::AlreadyExists);
        let second = EventId::from_bytes([2; 16]);
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(4),
                body: CommandBody::Signal {
                    run,
                    event_id: second,
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload,
                },
            },
        )
        .unwrap();
        let inbox_ids: Vec<_> = state.inbox.iter().map(|entry| entry.event_id).collect();
        assert_eq!(inbox_ids, vec![first, second]);
    }

    #[test]
    fn renewal_does_not_extend_attempt_deadline() {
        let (mut state, _run, activation, session) = {
            let catalog = catalog();
            let definition = compile_yaml(manual_yaml(), &catalog).unwrap();
            let mut state = State::default();
            let run = RunId::generate();
            start_run(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(0),
                    body: CommandBody::Start {
                        run,
                        definition: Box::new(definition.clone()),
                        input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
                        catalog: Box::new(catalog.clone()),
                    },
                },
                definition,
                catalog,
            )
            .unwrap();
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1),
                    body: CommandBody::Progress { run },
                },
            )
            .unwrap();
            let session = WorkerSessionId::generate();
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1),
                    body: CommandBody::RegisterSession {
                        session,
                        activities: vec!["*".to_owned()],
                        capacity: 8,
                    },
                },
            )
            .unwrap();
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1),
                    body: CommandBody::Claim {
                        session,
                        capacity: 8,
                    },
                },
            )
            .unwrap();
            let activation = ready_activations(&state, run)[0];
            (state, run, activation, session)
        };
        let claim = state.activations[&activation].claim.clone().unwrap();
        let deadline = claim.attempt_deadline_ms;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(5_000),
                body: CommandBody::Renew {
                    session,
                    activation,
                    generation: claim.generation,
                    revision: claim.revision,
                },
            },
        )
        .unwrap();
        let renewed = state.activations[&activation].claim.clone().unwrap();
        assert_eq!(renewed.attempt_deadline_ms, deadline);
        assert!(renewed.lease_expiry_ms > claim.lease_expiry_ms);
    }

    #[test]
    fn expired_claim_rejects_renewal() {
        let catalog = catalog();
        let definition = compile_yaml(manual_yaml(), &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(2))])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let session = WorkerSessionId::generate();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Claim {
                    session,
                    capacity: 8,
                },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let claim = state.activations[&activation].claim.clone().unwrap();
        let err = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(claim.lease_expiry_ms),
                body: CommandBody::Renew {
                    session,
                    activation,
                    generation: claim.generation,
                    revision: claim.revision,
                },
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
    }

    fn flaky_yaml() -> &'static str {
        r#"
dsl: graphrun/v1
id: flaky_loop
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: count
nodes:
  count:
    kind: while
    state_schema: counter/v1
    state: {from: workflow.input}
    condition:
      lt:
        - {from: loop.state, path: /value}
        - {literal: 2}
    max_iterations: 10
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: step
      nodes:
        step:
          kind: activity
          activity: {name: test.flaky, version: 1}
          input: {from: scope.input}
          retry:
            errors: [test.flaky]
            max_attempts: 3
          next: done
        done:
          kind: complete
          output: {from: nodes.step.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.count.output}
"#
    }

    #[test]
    fn loop_retry_is_not_another_iteration() {
        let catalog = catalog();
        let definition = compile_yaml(flaky_yaml(), &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportError {
                    run,
                    activation,
                    code: "test.flaky".to_owned(),
                    message: "transient".to_owned(),
                },
            },
        )
        .unwrap();
        assert_eq!(
            state.activations[&activation].status,
            ActivationStatus::Ready
        );
        let extra_bodies = state
            .scopes
            .values()
            .filter(|scope| matches!(scope.role, ScopeRole::LoopBody { index, .. } if index > 0))
            .count();
        assert_eq!(extra_bodies, 0, "retry must not open another iteration");
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(3),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(4),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert_eq!(state.activations[&activation].id, activation);
    }

    #[test]
    fn terminal_error_does_not_retry() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1)),
                    ])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportError {
                    run,
                    activation,
                    code: "payment.declined".to_owned(),
                    message: "no".to_owned(),
                },
            },
        )
        .unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
    }

    #[test]
    fn parallel_sibling_cancels_on_branch_failure() {
        let yaml = r#"
dsl: graphrun/v1
id: parallel_fail
version: 1
input_schema: order/v1
output_schema: {tuple: [tax/v1, shipping/v1]}
start: quotes
nodes:
  quotes:
    kind: parallel
    branches:
      - name: tax
        input: {from: workflow.input}
        body:
          input_schema: order/v1
          output_schema: tax/v1
          start: boom
          nodes:
            boom:
              kind: fail
              error: {code: tax.failed, message: tax failed}
      - name: shipping
        input: {from: workflow.input}
        body:
          input_schema: order/v1
          output_schema: shipping/v1
          start: quote
          nodes:
            quote:
              kind: activity
              activity: {name: shipping.quote, version: 1}
              input: {from: scope.input}
              next: done
            done:
              kind: complete
              output: {from: nodes.quote.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.quotes.output}
"#;
        let state = drive_state(
            yaml,
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *state.runs.keys().next().unwrap();
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
        let cancelled = state.scopes.values().any(|scope| {
            matches!(
                &scope.status,
                ScopeStatus::Failed { error } if error.code == "branch.cancelled"
            )
        });
        assert!(cancelled, "sibling should be cancelled");
    }

    #[test]
    fn parallel_join_is_idempotent() {
        let live = drive_state(
            include_str!("../../docs/specs/v1/examples/parallel.yaml"),
            Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String("o1".to_owned())),
                ("amount".to_owned(), Value::Int(1000)),
            ])),
        );
        let run = *live.runs.keys().next().unwrap();
        let joins = live
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    DomainEvent::NodeOutputRecorded { node, .. } if node == "quotes"
                )
            })
            .count();
        assert_eq!(joins, 1);
        apply_command(
            &mut drive_state(
                include_str!("../../docs/specs/v1/examples/parallel.yaml"),
                Value::Object(BTreeMap::from([
                    ("order_id".to_owned(), Value::String("o1".to_owned())),
                    ("amount".to_owned(), Value::Int(1000)),
                ])),
            ),
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(9_000_000),
                body: CommandBody::Progress { run },
            },
        )
        .ok();
        let again = live
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    DomainEvent::NodeOutputRecorded { node, .. } if node == "quotes"
                )
            })
            .count();
        assert_eq!(again, 1);
    }

    #[test]
    fn inbox_quota_is_explicit() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        for i in 0..crate::limits::MAX_EVENTS_PER_ADDRESS {
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1 + u64::from(i)),
                    body: CommandBody::Signal {
                        run,
                        event_id: EventId::from_bytes({
                            let mut bytes = [0u8; 16];
                            bytes[0] = (i % 256) as u8;
                            bytes[1] = (i / 256) as u8;
                            bytes
                        }),
                        signal: "approval".to_owned(),
                        key: "k1".to_owned(),
                        payload: Value::Object(BTreeMap::from([(
                            "approved".to_owned(),
                            Value::Bool(true),
                        )])),
                    },
                },
            )
            .unwrap();
        }
        let err = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(10_000),
                body: CommandBody::Signal {
                    run,
                    event_id: EventId::from_bytes([9; 16]),
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(true),
                    )])),
                },
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::ResourceExhausted);
        assert_eq!(
            state.inbox.len(),
            crate::limits::MAX_EVENTS_PER_ADDRESS as usize
        );
    }

    #[test]
    fn unreserved_expired_event_does_not_satisfy_wait() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Signal {
                    run,
                    event_id: EventId::from_bytes([1; 16]),
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(true),
                    )])),
                },
            },
        )
        .unwrap();
        state.inbox[0].expires_ms = 10;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(50),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert!(state.waits.values().any(|wait| wait.pending));
        assert!(!state.inbox[0].consumed);
    }

    #[test]
    fn reserved_event_survives_ttl() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(3),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert!(state.waits.values().any(|wait| wait.pending));
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(4),
                body: CommandBody::Signal {
                    run,
                    event_id: EventId::from_bytes([3; 16]),
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(true),
                    )])),
                },
            },
        )
        .unwrap();
        assert!(
            state
                .inbox
                .iter()
                .any(|entry| entry.reserved_wait.is_some())
        );
        state.inbox[0].expires_ms = 5;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(50),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        assert_eq!(
            run_output(&state, run)
                .unwrap()
                .pointer("/approved")
                .unwrap(),
            &Value::Bool(true)
        );
    }

    #[test]
    fn cancel_releases_reservation() {
        let catalog = catalog();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/events.yaml"),
            &catalog,
        )
        .unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(3),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(4),
                body: CommandBody::Signal {
                    run,
                    event_id: EventId::from_bytes([4; 16]),
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: Value::Object(BTreeMap::from([(
                        "approved".to_owned(),
                        Value::Bool(true),
                    )])),
                },
            },
        )
        .unwrap();
        let expiry = state.inbox[0].expires_ms;
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(5),
                body: CommandBody::Cancel {
                    run,
                    reason: "stop".to_owned(),
                },
            },
        )
        .unwrap();
        assert!(state.inbox[0].reserved_wait.is_none());
        assert_eq!(state.inbox[0].expires_ms, expiry);
        assert!(!state.inbox[0].consumed);
    }

    #[test]
    fn compensation_binding_failure_keeps_forward_success() {
        let yaml = r#"
dsl: graphrun/v1
id: blocked_comp
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output, path: /missing}
          next: done
        done:
          kind: complete
          output: {from: nodes.reserve.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let catalog = catalog();
        let definition = compile_yaml(yaml, &catalog).unwrap();
        let mut state = State::default();
        let run = RunId::generate();
        start_run(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(0),
                body: CommandBody::Start {
                    run,
                    definition: Box::new(definition.clone()),
                    input: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1)),
                    ])),
                    catalog: Box::new(catalog.clone()),
                },
            },
            definition,
            catalog,
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(1),
                body: CommandBody::Progress { run },
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let events = apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::ReportLeaf {
                    run,
                    activation,
                    output: Value::Object(BTreeMap::from([
                        ("order_id".to_owned(), Value::String("o1".to_owned())),
                        ("amount".to_owned(), Value::Int(1)),
                        (
                            "reservation_id".to_owned(),
                            Value::String("res-1".to_owned()),
                        ),
                    ])),
                },
            },
        )
        .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DomainEvent::LeafSucceeded { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DomainEvent::ObligationBlocked { .. }))
        );
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Blocked { .. }
        ));
    }

    #[test]
    fn claim_fairness_shares_capacity() {
        let catalog = catalog();
        let yaml = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let mut state = State::default();
        let mut runs = Vec::new();
        for i in 0..2u8 {
            let definition = compile_yaml(yaml, &catalog).unwrap();
            let run = RunId::from_bytes({
                let mut bytes = [0u8; 16];
                bytes[0] = i + 1;
                bytes
            });
            start_run(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(0),
                    body: CommandBody::Start {
                        run,
                        definition: Box::new(definition.clone()),
                        input: Value::Object(BTreeMap::from([
                            ("order_id".to_owned(), Value::String("o1".to_owned())),
                            ("amount".to_owned(), Value::Int(1)),
                        ])),
                        catalog: Box::new(catalog.clone()),
                    },
                },
                definition,
                catalog.clone(),
            )
            .unwrap();
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(1),
                    body: CommandBody::Progress { run },
                },
            )
            .unwrap();
            runs.push(run);
        }
        let session = WorkerSessionId::generate();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::RegisterSession {
                    session,
                    activities: vec!["*".to_owned()],
                    capacity: 8,
                },
            },
        )
        .unwrap();
        apply_command(
            &mut state,
            Command {
                id: CommandId::generate(),
                time: EngineTime::from_millis(2),
                body: CommandBody::Claim {
                    session,
                    capacity: 2,
                },
            },
        )
        .unwrap();
        let claimed_runs: Vec<_> = state
            .activations
            .values()
            .filter(|act| act.claim.is_some())
            .map(|act| act.run)
            .collect();
        assert_eq!(claimed_runs.len(), 2);
        assert!(claimed_runs.contains(&runs[0]));
        assert!(claimed_runs.contains(&runs[1]));
    }

    fn order_input() -> Value {
        Value::Object(BTreeMap::from([
            ("order_id".to_owned(), Value::String("o1".to_owned())),
            ("amount".to_owned(), Value::Int(1000)),
        ]))
    }

    fn approval_payload() -> Value {
        Value::Object(BTreeMap::from([("approved".to_owned(), Value::Bool(true))]))
    }

    fn pump(state: &mut State, run: RunId, time: u64) {
        apply(state, time, CommandBody::Progress { run }).unwrap();
        for activation in ready_activations(state, run) {
            let Some((name, _, input)) = activity_key(state, activation) else {
                continue;
            };
            let output = handler(&name, &input);
            apply(
                state,
                time,
                CommandBody::ReportLeaf {
                    run,
                    activation,
                    output,
                },
            )
            .unwrap();
        }
    }

    #[test]
    fn repeated_waits_and_concurrent_keys() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/events-in-foreach.yaml"),
            Value::Array(vec![
                Value::Object(BTreeMap::from([(
                    "key".to_owned(),
                    Value::String("k1".to_owned()),
                )])),
                Value::Object(BTreeMap::from([(
                    "key".to_owned(),
                    Value::String("k2".to_owned()),
                )])),
            ]),
        );
        for i in 0..8 {
            apply(&mut state, 1 + i, CommandBody::Progress { run }).unwrap();
        }
        let pending: Vec<_> = state
            .waits
            .values()
            .filter(|wait| wait.pending)
            .map(|wait| wait.key.clone())
            .collect();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"k1".to_owned()));
        assert!(pending.contains(&"k2".to_owned()));
        apply(
            &mut state,
            20,
            CommandBody::Signal {
                run,
                event_id: EventId::from_bytes([1; 16]),
                signal: "approval".to_owned(),
                key: "k1".to_owned(),
                payload: approval_payload(),
            },
        )
        .unwrap();
        apply(
            &mut state,
            21,
            CommandBody::Signal {
                run,
                event_id: EventId::from_bytes([2; 16]),
                signal: "approval".to_owned(),
                key: "k2".to_owned(),
                payload: approval_payload(),
            },
        )
        .unwrap();
        for i in 0..8 {
            apply(&mut state, 30 + i, CommandBody::Progress { run }).unwrap();
        }
        let Value::Array(items) = run_output(&state, run).unwrap() else {
            panic!("foreach output");
        };
        assert_eq!(items.len(), 2);

        let yaml = r#"
dsl: graphrun/v1
id: wait_reuse
version: 1
input_schema: counter/v1
output_schema: counter/v1
signals:
  approval: {schema: approval/v1}
start: loop
nodes:
  loop:
    kind: while
    state_schema: counter/v1
    state: {from: workflow.input}
    condition:
      lt:
        - {from: loop.state, path: /value}
        - {literal: 2}
    max_iterations: 4
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: wait
      nodes:
        wait:
          kind: wait_signal
          signal: approval
          key: {literal: "k1"}
          consume_from: buffered
          timeout: null
          next: increment
        increment:
          kind: activity
          activity: {name: counter.increment, version: 1}
          input: {from: scope.input}
          next: done
        done:
          kind: complete
          output: {from: nodes.increment.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.loop.output}
"#;
        let (mut state, run) = start_yaml(
            yaml,
            Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(0))])),
        );
        for round in 0..2u8 {
            for i in 0..6 {
                apply(
                    &mut state,
                    100 + u64::from(round) * 20 + i,
                    CommandBody::Progress { run },
                )
                .unwrap();
            }
            assert!(
                state
                    .waits
                    .values()
                    .any(|wait| wait.pending && wait.key == "k1"),
                "iteration {round} should reopen the wait"
            );
            apply(
                &mut state,
                110 + u64::from(round) * 20,
                CommandBody::Signal {
                    run,
                    event_id: EventId::from_bytes([10 + round; 16]),
                    signal: "approval".to_owned(),
                    key: "k1".to_owned(),
                    payload: approval_payload(),
                },
            )
            .unwrap();
            for i in 0..6 {
                pump(&mut state, run, 112 + u64::from(round) * 20 + i);
            }
        }
        assert_eq!(
            run_output(&state, run)
                .unwrap()
                .pointer("/value")
                .unwrap()
                .as_i64(),
            Some(2)
        );

        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/events-in-foreach.yaml"),
            Value::Array(vec![
                Value::Object(BTreeMap::from([(
                    "key".to_owned(),
                    Value::String("same".to_owned()),
                )])),
                Value::Object(BTreeMap::from([(
                    "key".to_owned(),
                    Value::String("same".to_owned()),
                )])),
            ]),
        );
        for i in 0..12 {
            apply(&mut state, 1 + i, CommandBody::Progress { run }).unwrap();
        }
        let conflict = state.history.get(&run).unwrap().iter().any(|event| {
            matches!(
                event,
                DomainEvent::ScopeFailed { error, .. } if error.code == "WaitKeyConflict"
            )
        });
        assert!(conflict, "duplicate foreach keys must fail WaitKeyConflict");
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
    }

    #[test]
    fn parallel_saga_compensates_after_join() {
        let yaml = r#"
dsl: graphrun/v1
id: parallel_saga
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: both
      nodes:
        both:
          kind: parallel
          branches:
            - name: left
              input: {from: scope.input}
              body:
                input_schema: order/v1
                output_schema: reserved_order/v1
                start: reserve
                nodes:
                  reserve:
                    kind: activity
                    activity: {name: inventory.reserve, version: 1}
                    input: {from: scope.input}
                    compensation:
                      kind: activity
                      activity: {name: inventory.release, version: 1}
                      input: {from: forward.output}
                    next: done
                  done:
                    kind: complete
                    output: {from: nodes.reserve.output}
            - name: right
              input: {from: scope.input}
              body:
                input_schema: order/v1
                output_schema: reserved_order/v1
                start: reserve
                nodes:
                  reserve:
                    kind: activity
                    activity: {name: inventory.reserve, version: 1}
                    input: {from: scope.input}
                    compensation:
                      kind: activity
                      activity: {name: inventory.release, version: 1}
                      input: {from: forward.output}
                    next: done
                  done:
                    kind: complete
                    output: {from: nodes.reserve.output}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: fail after parallel}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(yaml, order_input());
        let mut first_success = None;
        for i in 0..16 {
            apply(&mut state, 1 + i, CommandBody::Progress { run }).unwrap();
            let ready = ready_activations(&state, run);
            if ready.len() == 2 && first_success.is_none() {
                let left = ready[0];
                let right = ready[1];
                apply(
                    &mut state,
                    50,
                    CommandBody::ReportLeaf {
                        run,
                        activation: left,
                        output: handler("inventory.reserve", &order_input()),
                    },
                )
                .unwrap();
                first_success = Some(left);
                apply(&mut state, 51, CommandBody::Progress { run }).unwrap();
                assert!(
                    state
                        .history
                        .get(&run)
                        .unwrap()
                        .iter()
                        .all(|event| !matches!(event, DomainEvent::CompensationStarted { .. })),
                    "one finished branch must not start compensation"
                );
                apply(
                    &mut state,
                    52,
                    CommandBody::ReportLeaf {
                        run,
                        activation: right,
                        output: handler("inventory.reserve", &order_input()),
                    },
                )
                .unwrap();
            } else {
                for activation in ready {
                    if Some(activation) == first_success {
                        continue;
                    }
                    let Some((name, _, input)) = activity_key(&state, activation) else {
                        continue;
                    };
                    apply(
                        &mut state,
                        60 + i,
                        CommandBody::ReportLeaf {
                            run,
                            activation,
                            output: handler(&name, &input),
                        },
                    )
                    .unwrap();
                }
            }
        }
        let handlers: Vec<_> = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                DomainEvent::CompensationStarted { handler, .. } => Some(handler.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(handlers, ["inventory.release", "inventory.release"]);
        let succeeded: Vec<_> = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                DomainEvent::LeafSucceeded {
                    activation,
                    role: ExecutionRole::Forward,
                    ..
                } => Some(*activation),
                _ => None,
            })
            .collect();
        assert_eq!(succeeded.len(), 2);
        let first_comp = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .position(|event| matches!(event, DomainEvent::CompensationStarted { .. }))
            .unwrap();
        let last_success = state
            .history
            .get(&run)
            .unwrap()
            .iter()
            .rposition(|event| {
                matches!(
                    event,
                    DomainEvent::LeafSucceeded {
                        role: ExecutionRole::Forward,
                        ..
                    }
                )
            })
            .unwrap();
        assert!(
            first_comp > last_success,
            "compensation starts only after both forward successes"
        );
        assert!(
            state
                .obligations
                .iter()
                .all(|item| matches!(item.status, ObligationStatus::Compensated))
        );
    }

    #[test]
    fn compensation_reported_error_policy() {
        let yaml = r#"
dsl: graphrun/v1
id: refund_policy
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: receipt/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output}
          next: charge
        charge:
          kind: activity
          activity: {name: payment.charge, version: 1}
          input: {from: nodes.reserve.output}
          compensation:
            kind: activity
            activity: {name: payment.refund, version: 1}
            input: {from: forward.output}
            retry:
              errors: [payment.refund_unavailable]
              max_attempts: 2
              backoff: {initial: "1s", multiplier: 2, max: "30s"}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: force compensate}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(yaml, order_input());
        for i in 0..16 {
            apply(&mut state, 1 + i, CommandBody::Progress { run }).unwrap();
            for activation in ready_activations(&state, run) {
                if state.activations[&activation].role == ExecutionRole::Compensation {
                    continue;
                }
                let Some((name, _, input)) = activity_key(&state, activation) else {
                    continue;
                };
                apply(
                    &mut state,
                    1 + i,
                    CommandBody::ReportLeaf {
                        run,
                        activation,
                        output: handler(&name, &input),
                    },
                )
                .unwrap();
            }
        }
        let refund = state
            .activations
            .values()
            .find(|act| act.role == ExecutionRole::Compensation)
            .map(|act| act.id)
            .expect("compensation activation");
        apply(
            &mut state,
            20,
            CommandBody::ReportError {
                run,
                activation: refund,
                code: "payment.refund_unavailable".to_owned(),
                message: "busy".to_owned(),
            },
        )
        .unwrap();
        assert_eq!(state.activations[&refund].status, ActivationStatus::Ready);
        apply(
            &mut state,
            21,
            CommandBody::ReportError {
                run,
                activation: refund,
                code: "payment.refund_rejected".to_owned(),
                message: "no".to_owned(),
            },
        )
        .unwrap();
        assert!(matches!(
            state
                .obligations
                .iter()
                .find(|item| item.handler == "payment.refund")
                .unwrap()
                .status,
            ObligationStatus::Blocked { .. }
        ));
        assert!(!matches!(
            state
                .obligations
                .iter()
                .find(|item| item.handler == "payment.refund")
                .unwrap()
                .status,
            ObligationStatus::Compensated
        ));

        let empty = r#"
dsl: graphrun/v1
id: empty_retry
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output}
            retry:
              errors: []
              max_attempts: 3
              backoff: {initial: "1s", multiplier: 2, max: "30s"}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: force}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(empty, order_input());
        for i in 0..16 {
            apply(&mut state, 1 + i, CommandBody::Progress { run }).unwrap();
            for activation in ready_activations(&state, run) {
                if state.activations[&activation].role == ExecutionRole::Compensation {
                    continue;
                }
                let Some((name, _, input)) = activity_key(&state, activation) else {
                    continue;
                };
                apply(
                    &mut state,
                    1 + i,
                    CommandBody::ReportLeaf {
                        run,
                        activation,
                        output: handler(&name, &input),
                    },
                )
                .unwrap();
            }
        }
        let release = state
            .activations
            .values()
            .find(|act| act.role == ExecutionRole::Compensation)
            .map(|act| act.id)
            .unwrap();
        apply(
            &mut state,
            20,
            CommandBody::ReportError {
                run,
                activation: release,
                code: "inventory.release_unavailable".to_owned(),
                message: "busy".to_owned(),
            },
        )
        .unwrap();
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Blocked { .. }
        ));
    }

    #[test]
    fn irreversible_effect_fails_closed() {
        let yaml = r#"
dsl: graphrun/v1
id: irreversible_saga
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: irreversible
            reason: inventory cannot be unreserved
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: cannot undo}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let state = drive_state(yaml, order_input());
        let run = *state.runs.keys().next().unwrap();
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Irreversible { .. }
        ));
        match &state.runs.get(&run).unwrap().status {
            RunStatus::Failed { error } => {
                assert_eq!(error.code, "saga.irreversible");
            }
            other => panic!("expected irreversible failure, got {other:?}"),
        }
    }

    #[test]
    fn operator_resolves_blocked_compensation() {
        let yaml = r#"
dsl: graphrun/v1
id: resolve_blocked
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output, path: /missing}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: need undo}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(yaml, order_input());
        for i in 0..8 {
            pump(&mut state, run, 1 + i);
        }
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Blocked { .. }
        ));
        let forward = state.obligations[0].forward;
        let pinned_handler = state.obligations[0].handler.clone();
        apply(&mut state, 20, CommandBody::Progress { run }).unwrap();
        assert!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(event, DomainEvent::InterventionRequired { .. }))
        );
        let supplied = handler("inventory.reserve", &order_input());
        apply(
            &mut state,
            21,
            CommandBody::ResolveBlocked {
                run,
                forward,
                input: supplied.clone(),
            },
        )
        .unwrap();
        assert_eq!(state.obligations[0].handler, pinned_handler);
        assert_eq!(state.obligations[0].input, supplied);
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Open
        ));
        for i in 0..8 {
            pump(&mut state, run, 30 + i);
        }
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Compensated
        ));
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
        let err = apply(
            &mut state,
            40,
            CommandBody::ResolveBlocked {
                run,
                forward,
                input: supplied,
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::InvalidArgument);
    }

    #[test]
    fn operator_abandons_blocked_compensation() {
        let yaml = r#"
dsl: graphrun/v1
id: abandon_blocked
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output, path: /missing}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: need undo}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(yaml, order_input());
        for i in 0..8 {
            pump(&mut state, run, 1 + i);
        }
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Blocked { .. }
        ));
        apply(
            &mut state,
            20,
            CommandBody::AbandonCompensation {
                run,
                reason: "operator gives up".to_owned(),
            },
        )
        .unwrap();
        assert!(matches!(
            state.obligations[0].status,
            ObligationStatus::Abandoned
        ));
        match &state.runs.get(&run).unwrap().status {
            RunStatus::Failed { error } => {
                assert_eq!(error.code, "saga.abandoned");
                assert!(!error.message.contains("rollback"));
            }
            other => panic!("expected abandoned failure, got {other:?}"),
        }
        apply(
            &mut state,
            21,
            CommandBody::AbandonCompensation {
                run,
                reason: "operator gives up".to_owned(),
            },
        )
        .unwrap();
        assert_eq!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .filter(|event| matches!(event, DomainEvent::CompensationAbandoned { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn successful_saga_does_not_reopen_on_later_failure() {
        let yaml = r#"
dsl: graphrun/v1
id: later_failure
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output}
          next: done
        done:
          kind: complete
          output: {from: nodes.reserve.output}
    next: boom
  boom:
    kind: fail
    error: {code: later.failed, message: after saga success}
"#;
        let state = drive_state(yaml, order_input());
        let run = *state.runs.keys().next().unwrap();
        assert!(
            state
                .obligations
                .iter()
                .all(|item| matches!(item.status, ObligationStatus::Released))
        );
        assert!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .all(|event| !matches!(event, DomainEvent::CompensationStarted { .. }))
        );
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Failed { .. }
        ));
    }

    #[test]
    fn cancel_during_saga_stops_forward_and_compensates() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            order_input(),
        );
        apply(&mut state, 1, CommandBody::Progress { run }).unwrap();
        let activation = ready_activations(&state, run)[0];
        apply(
            &mut state,
            2,
            CommandBody::ReportLeaf {
                run,
                activation,
                output: handler("inventory.reserve", &order_input()),
            },
        )
        .unwrap();
        assert!(
            state
                .obligations
                .iter()
                .any(|item| item.handler == "inventory.release"
                    && matches!(item.status, ObligationStatus::Open))
        );
        apply(
            &mut state,
            3,
            CommandBody::Cancel {
                run,
                reason: "operator stop".to_owned(),
            },
        )
        .unwrap();
        for i in 0..16 {
            apply(&mut state, 10 + i, CommandBody::Progress { run }).unwrap();
            for activation in ready_activations(&state, run) {
                if state.activations[&activation].role != ExecutionRole::Compensation {
                    continue;
                }
                let Some((name, _, input)) = activity_key(&state, activation) else {
                    continue;
                };
                apply(
                    &mut state,
                    10 + i,
                    CommandBody::ReportLeaf {
                        run,
                        activation,
                        output: handler(&name, &input),
                    },
                )
                .unwrap();
            }
        }
        assert!(
            state
                .obligations
                .iter()
                .all(|item| matches!(item.status, ObligationStatus::Compensated))
        );
        match &state.runs.get(&run).unwrap().status {
            RunStatus::Failed { error } => assert_eq!(error.code, "run.cancelled"),
            other => panic!("expected cancel, got {other:?}"),
        }
        assert!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(
                    event,
                    DomainEvent::CompensationStarted { handler, .. } if handler == "inventory.release"
                ))
        );
    }

    #[test]
    fn run_deadline_blocks_forward_claims_not_settlement() {
        let yaml = r#"
dsl: graphrun/v1
id: deadline_saga
version: 1
input_schema: order/v1
output_schema: reserved_order/v1
run_timeout: "10ms"
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: reserved_order/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output}
          next: abort
        abort:
          kind: fail
          error: {code: fixture.failed, message: compensate after deadline}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
"#;
        let (mut state, run) = start_yaml(yaml, order_input());
        apply(&mut state, 1, CommandBody::Progress { run }).unwrap();
        let session = WorkerSessionId::generate();
        apply(
            &mut state,
            1,
            CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: 8,
            },
        )
        .unwrap();
        apply(
            &mut state,
            1,
            CommandBody::Claim {
                session,
                capacity: 8,
            },
        )
        .unwrap();
        let forward = ready_activations(&state, run)[0];
        assert_eq!(
            state.activations[&forward].claim.as_ref().unwrap().role,
            ExecutionRole::Forward
        );
        let output = handler("inventory.reserve", &order_input());
        let (generation, revision) = {
            let claim = state.activations[&forward].claim.as_ref().unwrap();
            (claim.generation, claim.revision)
        };
        apply(
            &mut state,
            1,
            CommandBody::ReportAssigned {
                run,
                activation: forward,
                output,
                session,
                generation,
                revision,
            },
        )
        .unwrap();
        for i in 0..8 {
            apply(&mut state, 2 + i, CommandBody::Progress { run }).unwrap();
        }
        let later = 10_000u64;
        apply(
            &mut state,
            later,
            CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: 8,
            },
        )
        .unwrap();
        let before = state
            .activations
            .values()
            .filter(|act| {
                act.role == ExecutionRole::Forward
                    && act.claim.as_ref().is_some_and(|claim| {
                        later < claim.lease_expiry_ms && claim.role == ExecutionRole::Forward
                    })
            })
            .count();
        apply(
            &mut state,
            later,
            CommandBody::Claim {
                session,
                capacity: 8,
            },
        )
        .unwrap();
        let forward_after = state
            .activations
            .values()
            .filter(|act| {
                act.role == ExecutionRole::Forward
                    && act
                        .claim
                        .as_ref()
                        .is_some_and(|claim| claim.role == ExecutionRole::Forward)
            })
            .count();
        assert_eq!(forward_after, before);
        let compensation = state
            .activations
            .values()
            .find(|act| act.role == ExecutionRole::Compensation)
            .map(|act| act.id)
            .expect("compensation ready after fail");
        assert_eq!(
            state.activations[&compensation]
                .claim
                .as_ref()
                .unwrap()
                .role,
            ExecutionRole::Compensation
        );
        let (generation, revision, attempt_deadline_ms) = {
            let claim = state.activations[&compensation].claim.as_ref().unwrap();
            (claim.generation, claim.revision, claim.attempt_deadline_ms)
        };
        assert!(attempt_deadline_ms > later);
        apply(
            &mut state,
            later,
            CommandBody::ReportAssigned {
                run,
                activation: compensation,
                output: Value::Null,
                session,
                generation,
                revision,
            },
        )
        .unwrap();
        for i in 0..6 {
            apply(&mut state, later + 1 + i, CommandBody::Progress { run }).unwrap();
        }
        assert!(
            state
                .obligations
                .iter()
                .all(|item| matches!(item.status, ObligationStatus::Compensated))
        );
    }

    #[test]
    fn schema_invalid_result_does_not_advance() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            order_input(),
        );
        apply(&mut state, 1, CommandBody::Progress { run }).unwrap();
        let activation = ready_activations(&state, run)[0];
        let err = apply(
            &mut state,
            2,
            CommandBody::ReportLeaf {
                run,
                activation,
                output: Value::Null,
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::InvalidArgument);
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Active
        ));
    }

    #[test]
    fn stale_assigned_result_does_not_advance() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            order_input(),
        );
        apply(&mut state, 1, CommandBody::Progress { run }).unwrap();
        let session = WorkerSessionId::generate();
        apply(
            &mut state,
            1,
            CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: 8,
            },
        )
        .unwrap();
        apply(
            &mut state,
            1,
            CommandBody::Claim {
                session,
                capacity: 8,
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let claim = state.activations[&activation].claim.clone().unwrap();
        let err = apply(
            &mut state,
            2,
            CommandBody::ReportAssigned {
                run,
                activation,
                output: handler("inventory.reserve", &order_input()),
                session,
                generation: crate::ids::OwnerGeneration::new(claim.generation.get() + 1),
                revision: claim.revision,
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        assert!(matches!(
            state.runs.get(&run).unwrap().status,
            RunStatus::Active
        ));
    }

    #[test]
    fn expired_history_range_is_unavailable() {
        let state = drive_state(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            order_input(),
        );
        let run = *state.runs.keys().next().unwrap();
        let terminal = state.runs.get(&run).unwrap().terminal_ms;
        assert!(terminal > 0);
        let events =
            history_or_unavailable(&state, run, EngineTime::from_millis(terminal)).unwrap();
        assert!(!events.is_empty());
        let rebuilt = reconstruct(
            state.runs.get(&run).unwrap().definition.clone(),
            state.runs.get(&run).unwrap().catalog.clone(),
            events,
        )
        .unwrap();
        assert_eq!(run_output(&state, run), run_output(&rebuilt, run));
        let expired = EngineTime::from_millis(terminal.saturating_add(31 * 24 * 60 * 60 * 1000));
        let err = history_or_unavailable(&state, run, expired).unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::Unavailable);
        assert!(run_output(&state, run).is_some());
    }

    #[test]
    fn unknown_blocks_undo_until_applied() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/saga.yaml"),
            order_input(),
        );
        apply(&mut state, 1, CommandBody::Progress { run }).unwrap();
        let session = WorkerSessionId::generate();
        apply(
            &mut state,
            1,
            CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: 8,
            },
        )
        .unwrap();
        apply(
            &mut state,
            1,
            CommandBody::Claim {
                session,
                capacity: 8,
            },
        )
        .unwrap();
        apply(
            &mut state,
            2,
            CommandBody::Cancel {
                run,
                reason: "stop with in-flight reserve".to_owned(),
            },
        )
        .unwrap();
        assert!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(event, DomainEvent::AbortIntent { .. }))
        );
        assert!(
            !state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(event, DomainEvent::CompensationStarted { .. }))
        );
        let later = crate::policy::SESSION_LEASE.as_millis() as u64 + 3;
        apply(
            &mut state,
            later,
            CommandBody::RegisterSession {
                session,
                activities: vec!["*".to_owned()],
                capacity: 8,
            },
        )
        .unwrap();
        apply(
            &mut state,
            later,
            CommandBody::Claim {
                session,
                capacity: 8,
            },
        )
        .unwrap();
        let activation = ready_activations(&state, run)[0];
        let claim = state.activations[&activation].claim.clone().unwrap();
        assert_eq!(claim.role, ExecutionRole::Reconciliation);
        apply(
            &mut state,
            later,
            CommandBody::Reconcile {
                run,
                activation,
                session,
                generation: claim.generation,
                revision: claim.revision,
                outcome: ReconcileOutcome::Unknown,
                output: None,
            },
        )
        .unwrap();
        assert!(
            !state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(event, DomainEvent::CompensationStarted { .. }))
        );
        apply(
            &mut state,
            later + 1,
            CommandBody::Reconcile {
                run,
                activation,
                session,
                generation: claim.generation,
                revision: claim.revision,
                outcome: ReconcileOutcome::Applied,
                output: Some(handler("inventory.reserve", &order_input())),
            },
        )
        .unwrap();
        assert!(
            state
                .obligations
                .iter()
                .any(|item| item.handler == "inventory.release")
        );
        for i in 0..12 {
            apply(&mut state, later + 2 + i, CommandBody::Progress { run }).unwrap();
            for ready in ready_activations(&state, run) {
                if state.activations[&ready].role != ExecutionRole::Compensation {
                    continue;
                }
                let Some((name, _, input)) = activity_key(&state, ready) else {
                    continue;
                };
                apply(
                    &mut state,
                    later + 2 + i,
                    CommandBody::ReportLeaf {
                        run,
                        activation: ready,
                        output: handler(&name, &input),
                    },
                )
                .unwrap();
            }
        }
        assert!(
            state
                .history
                .get(&run)
                .unwrap()
                .iter()
                .any(|event| matches!(
                    event,
                    DomainEvent::CompensationStarted { handler, .. } if handler == "inventory.release"
                ))
        );
    }

    #[test]
    fn restore_hold_blocks_start_until_ack() {
        let (mut state, run) = start_yaml(
            include_str!("../../docs/specs/v1/examples/sequence.yaml"),
            order_input(),
        );
        state.recovery = Some(RecoveryHold {
            reason: "disaster restore".to_owned(),
            authorized: false,
        });
        let err = apply(
            &mut state,
            1,
            CommandBody::Start {
                run: RunId::generate(),
                definition: Box::new(
                    compile_yaml(
                        include_str!("../../docs/specs/v1/examples/sequence.yaml"),
                        &catalog(),
                    )
                    .unwrap(),
                ),
                input: order_input(),
                catalog: Box::new(catalog()),
            },
        )
        .unwrap_err();
        assert_eq!(err.kind, crate::error::ErrorKind::FailedPrecondition);
        apply(
            &mut state,
            2,
            CommandBody::AcknowledgeRecovery {
                reason: "operator ack".to_owned(),
            },
        )
        .unwrap();
        assert!(state.recovery.as_ref().unwrap().authorized);
        let _ = run;
    }
}
