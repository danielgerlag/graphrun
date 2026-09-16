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
    },
    ObligationRegistered {
        run: RunId,
        forward: ActivationId,
        handler: String,
        handler_version: u32,
        input: Value,
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
    ClaimGranted {
        run: RunId,
        activation: ActivationId,
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
    Reconcile {
        run: RunId,
        activation: ActivationId,
        session: WorkerSessionId,
        generation: OwnerGeneration,
        revision: LeaseRevision,
        outcome: ReconcileOutcome,
        output: Option<Value>,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSession {
    pub id: WorkerSessionId,
    pub activities: Vec<String>,
    pub capacity: u32,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObligationStatus {
    Open,
    Compensating { activation: ActivationId },
    Compensated,
    Released,
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub runs: HashMap<RunId, RunState>,
    pub scopes: HashMap<ScopeId, ScopeState>,
    pub activations: HashMap<ActivationId, ActivationState>,
    pub waits: HashMap<WaitId, WaitState>,
    pub inbox: Vec<InboxEntry>,
    pub commands: HashMap<CommandId, Vec<DomainEvent>>,
    pub loop_carry: HashMap<ActivationId, (Value, u32)>,
    pub foreach_items: HashMap<ActivationId, Vec<Value>>,
    pub foreach_done: HashMap<ActivationId, Vec<(u32, Value)>>,
    pub parallel_done: HashMap<ActivationId, BTreeMap<String, Value>>,
    #[serde(default)]
    pub history: HashMap<RunId, Vec<DomainEvent>>,
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
        CommandBody::Start {
            run,
            definition,
            input,
            catalog: _,
        } => decide_start(*run, definition, input.clone(), &mut ids),
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
        CommandBody::Claim { session, capacity } => {
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
    }
}

fn decide_start(
    run: RunId,
    definition: &Definition,
    input: Value,
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
        return decide_compensation_result(state, run, activation, output);
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
    if let Some(comp) = compensation_for(state, act, &output) {
        events.push(comp);
    }
    events.extend(follow_next(state, act.scope, &act.node, &output, ids)?);
    Ok(Decision { events })
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

fn decide_claim(
    state: &State,
    session: WorkerSessionId,
    capacity: u32,
    time: EngineTime,
    ids: &mut IdGen,
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
    let cap = capacity
        .min(worker.capacity)
        .min(crate::limits::CLAIM_BATCH);
    let mut events = Vec::new();
    let mut granted = 0u32;
    let mut runs = active_runs(state);
    runs.sort();
    for run in runs {
        if granted >= cap {
            break;
        }
        let mut ready = unclaimed_ready(state, run, time);
        ready.sort();
        for activation in ready {
            if granted >= cap {
                break;
            }
            let Some(act) = state.activations.get(&activation) else {
                continue;
            };
            let lease_expiry_ms = time
                .as_millis()
                .saturating_add(crate::policy::SESSION_LEASE.as_millis() as u64);
            let role = grant_role(state, act, time);
            let timeout = match role {
                ExecutionRole::Reconciliation => crate::policy::RECONCILIATION_ATTEMPT_TIMEOUT,
                ExecutionRole::Compensation => crate::policy::COMPENSATION_ATTEMPT_TIMEOUT,
                ExecutionRole::Forward => crate::policy::FORWARD_ATTEMPT_TIMEOUT,
            };
            let attempt_deadline_ms = time.as_millis().saturating_add(timeout.as_millis() as u64);
            let effect_key = act
                .claim
                .as_ref()
                .map(|claim| claim.effect_key)
                .unwrap_or_else(|| EffectKey::from_bytes(ids.next_bytes()));
            events.push(DomainEvent::ClaimGranted {
                run,
                activation,
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
            granted += 1;
        }
    }
    Ok(Decision { events })
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
    let act = state
        .activations
        .get(&activation)
        .ok_or_else(|| Error::invalid("unknown activation"))?;
    if act.status != ActivationStatus::Ready {
        return Err(Error::invalid("activation is not awaiting a result"));
    }
    let Some(claim) = &act.claim else {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "no claim",
        ));
    };
    if claim.session != session || claim.generation != generation || claim.revision != revision {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale claim",
        ));
    }
    if time.as_millis() >= claim.lease_expiry_ms || time.as_millis() >= claim.attempt_deadline_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim expired",
        ));
    }
    if claim.role == ExecutionRole::Reconciliation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "reconciliation claim must use reconcile",
        ));
    }
    report_success(state, run, activation, output, ids)
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
    if contract.recovery == crate::catalog::Recovery::Manual && contract.reconciler.is_some() {
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
    let Some(claim) = &act.claim else {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "no claim",
        ));
    };
    if claim.session != session || claim.generation != generation || claim.revision != revision {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "stale claim",
        ));
    }
    if claim.role != ExecutionRole::Reconciliation {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim is not a reconciliation probe",
        ));
    }
    if time.as_millis() >= claim.lease_expiry_ms || time.as_millis() >= claim.attempt_deadline_ms {
        return Err(Error::new(
            crate::error::ErrorKind::FailedPrecondition,
            "claim expired",
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
            let value = output.unwrap_or(Value::Null);
            events.extend(report_success(state, run, activation, value, ids)?.events);
        }
        ReconcileOutcome::NotApplied => {
            events.push(DomainEvent::ClaimCleared { activation });
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

fn compensation_for(state: &State, act: &ActivationState, output: &Value) -> Option<DomainEvent> {
    let region = region_for_scope(state, act.scope)?;
    let Node::Activity {
        compensation:
            Some(crate::ir::Compensation::Activity {
                activity, input, ..
            }),
        ..
    } = region.nodes.get(act.node.as_str())?
    else {
        return None;
    };
    let scope = state.scopes.get(&act.scope)?;
    let mut ctx = eval_ctx(state, scope);
    ctx.forward_input = Some(eval_binding(act_input_binding(region, &act.node)?, &ctx).ok()?);
    ctx.forward_output = Some(output.clone());
    let bound = eval_binding(input, &ctx).ok()?;
    Some(DomainEvent::ObligationRegistered {
        run: act.run,
        forward: act.id,
        handler: activity.name.clone(),
        handler_version: activity.version,
        input: bound,
    })
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
    _time: EngineTime,
    ids: &mut IdGen,
) -> Result<Decision> {
    let run_state = state
        .runs
        .get(&run)
        .ok_or_else(|| Error::invalid("unknown run"))?;
    if matches!(
        run_state.status,
        RunStatus::Succeeded { .. } | RunStatus::Failed { .. }
    ) {
        return Err(Error::invalid("run is terminal"));
    }
    if !run_state.definition.signals.contains_key(signal) {
        return Err(Error::invalid(format!("unknown signal {signal}")));
    }
    if let Some(existing) = state.inbox.iter().find(|entry| entry.event_id == event_id) {
        if existing.signal == signal && existing.key == key && existing.payload == payload {
            return Ok(Decision { events: Vec::new() });
        }
        return Err(Error::new(
            crate::error::ErrorKind::AlreadyExists,
            "event id payload conflict",
        ));
    }
    let sequence = run_state.next_sequence.get();
    let mut events = vec![DomainEvent::EventAccepted {
        run,
        event_id,
        signal: signal.to_owned(),
        key: key.to_owned(),
        payload: payload.clone(),
        sequence,
    }];
    if let Some(wait) = pending_wait(state, run, signal, key) {
        events.push(DomainEvent::WaitSatisfied {
            run,
            wait: wait.id,
            event_id,
            payload: payload.clone(),
        });
        events.push(DomainEvent::NodeOutputRecorded {
            run,
            scope: state.activations.get(&wait.activation).unwrap().scope,
            node: state
                .activations
                .get(&wait.activation)
                .unwrap()
                .node
                .as_str()
                .to_owned(),
            output: payload.clone(),
        });
        let act = state.activations.get(&wait.activation).unwrap();
        events.extend(follow_next(state, act.scope, &act.node, &payload, ids)?);
    }
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
    if let Some(entry) = reserved_or_eligible(state, wait_state) {
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

fn reserved_or_eligible<'a>(state: &'a State, wait: &WaitState) -> Option<&'a InboxEntry> {
    state.inbox.iter().find(|entry| {
        !entry.consumed
            && entry.signal == wait.signal
            && entry.key == wait.key
            && match wait.consume_from {
                ConsumeFrom::Buffered => true,
                ConsumeFrom::AfterActivation => entry.sequence >= wait.opened_sequence,
            }
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
        apply_events(&mut working, &next);
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
    let mut due: Vec<&WaitState> = state
        .waits
        .values()
        .filter(|wait| {
            wait.run == run
                && wait.pending
                && wait
                    .deadline_ms
                    .is_some_and(|deadline| time.as_millis() >= deadline)
        })
        .collect();
    due.sort_by_key(|wait| wait.deadline_ms.unwrap_or(0));
    let Some(wait) = due.first().copied() else {
        return Ok(None);
    };
    if let Some(entry) = reserved_or_eligible(state, wait) {
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
    if let Some(entry) = reserved_or_eligible(state, &fake) {
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
            return fail_scope(state, act.scope, error);
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
    if state.saga_errors.contains_key(&act.id) {
        let error = state.saga_errors.get(&act.id).unwrap().clone();
        return compensate_or_fail(state, act.id, error, ids);
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
    let pending: Vec<&Obligation> = state
        .obligations
        .iter()
        .filter(|item| item.owner == saga && matches!(item.status, ObligationStatus::Open))
        .collect();
    if pending.is_empty() {
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

pub fn apply_events(state: &mut State, events: &[DomainEvent]) {
    for event in events {
        if let Some(run) = event_run(event) {
            state.history.entry(run).or_default().push(event.clone());
        }
        evolve(state, event);
    }
}

fn event_run(event: &DomainEvent) -> Option<RunId> {
    Some(match event {
        DomainEvent::RunAdmitted { run, .. }
        | DomainEvent::ScopeOpened { run, .. }
        | DomainEvent::ScopeCompleted { run, .. }
        | DomainEvent::ScopeFailed { run, .. }
        | DomainEvent::ActivationOpened { run, .. }
        | DomainEvent::NodeOutputRecorded { run, .. }
        | DomainEvent::GuardRecorded { run, .. }
        | DomainEvent::LeafSucceeded { run, .. }
        | DomainEvent::WaitOpened { run, .. }
        | DomainEvent::WaitSatisfied { run, .. }
        | DomainEvent::WaitTimedOut { run, .. }
        | DomainEvent::EventAccepted { run, .. }
        | DomainEvent::ObligationRegistered { run, .. }
        | DomainEvent::CompensationStarted { run, .. }
        | DomainEvent::RunSucceeded { run, .. }
        | DomainEvent::RunFailed { run, .. }
        | DomainEvent::ObligationReleased { run, .. }
        | DomainEvent::ClaimGranted { run, .. }
        | DomainEvent::ReconciliationRecorded { run, .. }
        | DomainEvent::InterventionRequired { run, .. } => *run,
        DomainEvent::ObligationTransferred { .. }
        | DomainEvent::SessionRegistered { .. }
        | DomainEvent::ClaimRenewed { .. }
        | DomainEvent::ClaimCleared { .. } => {
            return None;
        }
    })
}

pub fn evolve(state: &mut State, event: &DomainEvent) {
    match event {
        DomainEvent::RunAdmitted {
            run,
            input,
            root,
            policy,
            ..
        } => {
            // definition is filled by the caller via start_with
            if let Some(run_state) = state.runs.get_mut(run) {
                run_state.input = input.clone();
                run_state.root = *root;
                run_state.policy = policy.clone();
                run_state.status = RunStatus::Active;
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
                        .insert(name.clone(), output.clone());
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
            });
            if let Some(run) = state.runs.values_mut().next() {
                run.next_sequence = RunSequence::new(sequence + 1);
            }
        }
        DomainEvent::ObligationRegistered {
            run,
            forward,
            handler,
            handler_version,
            input,
        } => {
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
                },
            );
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
                    probes: 0,
                });
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
        DomainEvent::ReconciliationRecorded {
            activation, probes, ..
        } => {
            if let Some(act) = state.activations.get_mut(activation) {
                if let Some(claim) = &mut act.claim {
                    claim.probes = *probes;
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
        },
    );
    let decision = decide(state, &command)?;
    apply_events(state, &decision.events);
    state.commands.insert(command.id, decision.events.clone());
    Ok(decision.events)
}

pub fn apply_command(state: &mut State, command: Command) -> Result<Vec<DomainEvent>> {
    if let Some(events) = state.commands.get(&command.id) {
        return Ok(events.clone());
    }
    let decision = decide(state, &command)?;
    apply_events(state, &decision.events);
    state.commands.insert(command.id, decision.events.clone());
    Ok(decision.events)
}

pub fn commit_command(state: &mut State, command: Command) -> Result<Vec<DomainEvent>> {
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
                },
            );
        }
    }
    apply_command(state, command)
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

pub fn assignments_from(state: &State, events: &[DomainEvent]) -> Vec<AssignmentView> {
    events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::ClaimGranted {
                run,
                activation,
                session,
                generation,
                revision,
                lease_expiry_ms,
                attempt_deadline_ms,
                effect_key,
                role,
            } => {
                let (name, version, input) = activity_key(state, *activation)?;
                Some(AssignmentView {
                    run: *run,
                    activation: *activation,
                    activity_name: name,
                    activity_version: version,
                    input,
                    role: *role,
                    effect_key: *effect_key,
                    generation: *generation,
                    revision: *revision,
                    lease_expiry_ms: *lease_expiry_ms,
                    attempt_deadline_ms: *attempt_deadline_ms,
                    session: *session,
                })
            }
            _ => None,
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct AssignmentView {
    pub run: RunId,
    pub activation: ActivationId,
    pub activity_name: String,
    pub activity_version: u32,
    pub input: Value,
    pub role: ExecutionRole,
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
        },
    );
    apply_events(&mut state, events);
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
                let mut out = fields.clone();
                out.insert(
                    "reservation_id".to_owned(),
                    Value::String("res-1".to_owned()),
                );
                Value::Object(out)
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
        assert_eq!(
            run_output(&live, run).unwrap(),
            run_output(&rebuilt, run).unwrap()
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
                    input: Value::Object(BTreeMap::from([("value".to_owned(), Value::Int(1))])),
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
        for _ in 0..3 {
            let (generation, revision) = {
                let claim = state.activations[&activation].claim.clone().unwrap();
                (claim.generation, claim.revision)
            };
            apply_command(
                &mut state,
                Command {
                    id: CommandId::generate(),
                    time: EngineTime::from_millis(
                        crate::policy::SESSION_LEASE.as_millis() as u64 + 2,
                    ),
                    body: CommandBody::Reconcile {
                        run,
                        activation,
                        session,
                        generation,
                        revision,
                        outcome: ReconcileOutcome::Unknown,
                        output: None,
                    },
                },
            )
            .unwrap();
        }
        assert!(state.interventions.contains_key(&activation));
        assert!(state.activations[&activation].claim.is_none());
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
}
