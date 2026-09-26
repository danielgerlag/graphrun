//! Replicated publication and keyed admission authority.
use crate::catalog::Catalog;
use crate::compiler::digest_of;
use crate::domain::{self, Command, CommandBody, RunState, RunStatus, State};
use crate::error::{Error, ErrorKind, Result};
use crate::ids::{CommandId, RunId, RunSequence, ScopeId, valid_ascii_name};
use crate::ir::Definition;
use crate::policy::CapturedRunPolicy;
use crate::value::{Value, canonical_json};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const RESULT_FORMAT: &str = "graphrun.command-result/v1";
pub const CATALOG_FORMAT: &str = "graphrun.catalog-publication/v1";
pub const DEFINITION_FORMAT: &str = "graphrun.definition-publication/v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommandKey {
    pub cluster_id: String,
    pub principal_id: String,
    pub command_id: CommandId,
}

impl CommandKey {
    pub fn storage_key(&self) -> String {
        format!(
            "{}:{}:{}",
            hex::encode(self.cluster_id.as_bytes()),
            hex::encode(self.principal_id.as_bytes()),
            self.command_id
        )
    }
}

/// Only trusted transport authentication or the local owner socket may mint this
/// context. It is never decoded from an application request or metadata header.
#[derive(Clone, Debug)]
pub struct AuthContext {
    cluster_id: String,
    principal_id: String,
    admin: bool,
}

impl AuthContext {
    pub(crate) fn local_owner(cluster_id: String) -> Self {
        Self {
            cluster_id,
            principal_id: "local-owner".to_owned(),
            admin: true,
        }
    }

    pub(crate) fn verified_peer(peer: &crate::tls::VerifiedPeerIdentity) -> Self {
        Self {
            cluster_id: peer.cluster_id().as_str().to_owned(),
            principal_id: peer.principal_id().as_str().to_owned(),
            admin: peer.roles().any(|role| role == crate::tls::PeerRole::Admin),
        }
    }

    pub fn authorize(&self, operation: &PublicationOperation) -> Result<()> {
        if self.cluster_id.is_empty() || self.principal_id.is_empty() {
            return Err(Error::new(
                ErrorKind::Unauthenticated,
                "missing authenticated identity",
            ));
        }
        if !self.admin && !matches!(operation, PublicationOperation::Start { .. }) {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "admin role required to publish",
            ));
        }
        Ok(())
    }

    pub fn key(&self, command_id: CommandId) -> CommandKey {
        CommandKey {
            cluster_id: self.cluster_id.clone(),
            principal_id: self.principal_id.clone(),
            command_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum PublicationOperation {
    Catalog {
        version: u32,
        catalog: Catalog,
    },
    Definition {
        definition: Box<Definition>,
        catalog_version: u32,
    },
    Start {
        workflow: String,
        version: Option<u32>,
        start_key: String,
        input: Value,
    },
}

impl PublicationOperation {
    pub fn target(&self) -> String {
        match self {
            Self::Catalog { version, .. } => format!("catalog/v{version}"),
            Self::Definition { definition, .. } => {
                format!("{}/v{}", definition.id, definition.version)
            }
            Self::Start {
                workflow,
                start_key,
                ..
            } => format!("{workflow}/{start_key}"),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Catalog { .. } => "publish_catalog",
            Self::Definition { .. } => "publish_definition",
            Self::Start { .. } => "start",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishedCatalog {
    pub format: String,
    pub digest: String,
    pub catalog: Catalog,
}

impl PublishedCatalog {
    pub fn verify(&self) -> Result<()> {
        if self.format != CATALOG_FORMAT
            || self.digest != digest(&self.catalog, crate::limits::PUBLIC_COMMAND_ENVELOPE)?
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "retained catalog unavailable or corrupt",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishedDefinition {
    pub format: String,
    pub normalized_format_version: u32,
    pub digest: String,
    pub catalog_version: u32,
    pub definition: Definition,
}

impl PublishedDefinition {
    pub fn verify(&self) -> Result<()> {
        if self.format != DEFINITION_FORMAT
            || self.normalized_format_version != crate::ir::FORMAT_VERSION
            || self.definition.format_version != self.normalized_format_version
            || self.definition.digest.0 != self.digest
            || digest_of(&self.definition)?.0 != self.digest
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "retained definition unavailable or corrupt",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartKeyRecord {
    pub run: RunId,
    pub version: u32,
    pub input_digest: String,
    pub admitted_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PinnedPublication {
    pub definition_format_version: u32,
    pub definition_digest: String,
    pub catalog_version: u32,
    pub catalog_digest: String,
    pub policy_format: String,
}

impl PinnedPublication {
    pub fn verify(&self, definition: &Definition, catalog: &Catalog) -> Result<()> {
        if self.definition_format_version != crate::ir::FORMAT_VERSION
            || definition.format_version != self.definition_format_version
            || self.definition_digest != digest_of(definition)?.0
            || self.catalog_digest != digest(catalog, crate::limits::PUBLIC_COMMAND_ENVELOPE)?
            || self.policy_format != "graphrun.run-policy/v1"
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "pinned history artifact unavailable or corrupt",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum Disposition {
    Applied {
        run: Option<RunId>,
        digest: Option<String>,
        version: Option<u32>,
    },
    Rejected {
        code: String,
        details: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventRange {
    pub run: RunId,
    pub first: u64,
    pub last: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandResult {
    pub format: String,
    pub key: CommandKey,
    pub request_digest: String,
    pub operation: String,
    pub target: String,
    pub outcome: Disposition,
    pub event_range: Option<EventRange>,
    pub recorded_ms: u64,
}

impl CommandResult {
    pub fn applied_run(&self) -> Result<RunId> {
        self.ensure_format()?;
        match &self.outcome {
            Disposition::Applied { run: Some(run), .. } => Ok(*run),
            Disposition::Rejected { code, details } => {
                Err(Error::new(error_kind(code), details.clone()))
            }
            _ => Err(Error::new(
                ErrorKind::FailedPrecondition,
                "command did not start a run",
            )),
        }
    }

    pub fn ensure_applied(&self) -> Result<()> {
        self.ensure_format()?;
        match &self.outcome {
            Disposition::Applied { .. } => Ok(()),
            Disposition::Rejected { code, details } => {
                Err(Error::new(error_kind(code), details.clone()))
            }
        }
    }

    pub fn ensure_format(&self) -> Result<()> {
        if self.format == RESULT_FORMAT {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::FailedPrecondition,
                "unsupported committed command-result format",
            ))
        }
    }
}

fn error_kind(code: &str) -> ErrorKind {
    match code {
        "already_exists" => ErrorKind::AlreadyExists,
        "not_found" => ErrorKind::NotFound,
        "failed_precondition" => ErrorKind::FailedPrecondition,
        "resource_exhausted" => ErrorKind::ResourceExhausted,
        _ => ErrorKind::InvalidArgument,
    }
}

fn error_code(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::AlreadyExists => "already_exists",
        ErrorKind::NotFound => "not_found",
        ErrorKind::FailedPrecondition => "failed_precondition",
        ErrorKind::ResourceExhausted => "resource_exhausted",
        _ => "invalid_argument",
    }
}

fn canonical_bytes<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let json = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    let bytes = canonical_json(&json)?;
    if bytes.len() > limit {
        return Err(Error::new(
            ErrorKind::ResourceExhausted,
            format!("published payload exceeds {limit} bytes"),
        ));
    }
    Ok(bytes)
}

fn digest<T: Serialize>(value: &T, limit: usize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(canonical_bytes(value, limit)?)))
}

pub fn command(
    auth: &AuthContext,
    id: CommandId,
    time: crate::time::EngineTime,
    operation: PublicationOperation,
) -> Result<Command> {
    if id.as_bytes() == &[0; 16] {
        return Err(Error::invalid("command ID must be nonempty"));
    }
    auth.authorize(&operation)?;
    Ok(Command {
        id,
        time,
        body: CommandBody::Publication {
            key: auth.key(id),
            operation,
        },
    })
}

pub fn apply(
    state: &mut State,
    command: &Command,
    key: &CommandKey,
    operation: &PublicationOperation,
) -> CommandResult {
    let (request_digest, request_bytes) = match canonical_bytes(operation, usize::MAX) {
        Ok(bytes) => (hex::encode(Sha256::digest(&bytes)), Ok(bytes)),
        Err(err) => {
            let (bytes, rejection) = match serde_json::to_vec(operation) {
                Ok(bytes) => (bytes, err),
                Err(encode_err) => (
                    format!("{operation:?}").into_bytes(),
                    Error::invalid(format!("publication request encoding: {encode_err}")),
                ),
            };
            let mut hasher = Sha256::new();
            hasher.update(b"graphrun.uncanonical-publication-request/v1\0");
            hasher.update(bytes);
            (hex::encode(hasher.finalize()), Err(rejection))
        }
    };
    if let Some(previous) = state.command_results.get(&key.storage_key()) {
        if previous.request_digest == request_digest && previous.format == RESULT_FORMAT {
            return previous.clone();
        }
        return CommandResult {
            format: RESULT_FORMAT.to_owned(),
            key: key.clone(),
            request_digest,
            operation: operation.name().to_owned(),
            target: operation.target(),
            outcome: Disposition::Rejected {
                code: "already_exists".to_owned(),
                details: "command ID reused with a different request".to_owned(),
            },
            event_range: None,
            recorded_ms: command.time.as_millis(),
        };
    }
    let result = match request_bytes {
        Err(err) => Err(err),
        Ok(bytes) if bytes.len() > crate::limits::PUBLIC_COMMAND_ENVELOPE => Err(Error::new(
            ErrorKind::ResourceExhausted,
            "publication command exceeds envelope limit",
        )),
        Ok(_) => apply_operation(state, command, key, operation),
    };
    let (outcome, event_range) = match result {
        Ok((outcome, range)) => (outcome, range),
        Err(err) => (
            Disposition::Rejected {
                code: error_code(err.kind).to_owned(),
                details: err.message,
            },
            None,
        ),
    };
    let receipt = CommandResult {
        format: RESULT_FORMAT.to_owned(),
        key: key.clone(),
        request_digest,
        operation: operation.name().to_owned(),
        target: operation.target(),
        outcome,
        event_range,
        recorded_ms: command.time.as_millis(),
    };
    state
        .command_results
        .insert(key.storage_key(), receipt.clone());
    receipt
}

fn apply_operation(
    state: &mut State,
    command: &Command,
    key: &CommandKey,
    operation: &PublicationOperation,
) -> Result<(Disposition, Option<EventRange>)> {
    match operation {
        PublicationOperation::Catalog { version, catalog } => {
            if *version == 0 {
                return Err(Error::invalid("catalog version must be positive"));
            }
            catalog.validate_for_publication()?;
            for schema in catalog.schemas.values() {
                canonical_bytes(schema, crate::limits::MAX_SCHEMA_BYTES)?;
            }
            let digest = digest(catalog, crate::limits::PUBLIC_COMMAND_ENVELOPE)?;
            if let Some(existing) = state.published_catalogs.get(version) {
                if existing.format != CATALOG_FORMAT
                    || existing.digest != digest
                    || existing.catalog != *catalog
                {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "catalog version has different content",
                    ));
                }
            } else {
                for prior in state.published_catalogs.values() {
                    for (key, schema) in &catalog.schemas {
                        if prior
                            .catalog
                            .schemas
                            .get(key)
                            .is_some_and(|old| old != schema)
                        {
                            return Err(Error::new(
                                ErrorKind::AlreadyExists,
                                format!(
                                    "schema {key} was already published with different content"
                                ),
                            ));
                        }
                    }
                    for (key, contract) in &catalog.activities {
                        if prior
                            .catalog
                            .activities
                            .get(key)
                            .is_some_and(|old| old != contract)
                        {
                            return Err(Error::new(
                                ErrorKind::AlreadyExists,
                                format!(
                                    "activity {key} was already published with different content"
                                ),
                            ));
                        }
                    }
                    for (key, contract) in &catalog.reconcilers {
                        if prior
                            .catalog
                            .reconcilers
                            .get(key)
                            .is_some_and(|old| old != contract)
                        {
                            return Err(Error::new(
                                ErrorKind::AlreadyExists,
                                format!(
                                    "reconciler {key} was already published with different content"
                                ),
                            ));
                        }
                    }
                }
                state.published_catalogs.insert(
                    *version,
                    PublishedCatalog {
                        format: CATALOG_FORMAT.to_owned(),
                        digest: digest.clone(),
                        catalog: catalog.clone(),
                    },
                );
            }
            Ok((
                Disposition::Applied {
                    run: None,
                    digest: Some(digest),
                    version: Some(*version),
                },
                None,
            ))
        }
        PublicationOperation::Definition {
            definition,
            catalog_version,
        } => {
            let catalog = state
                .published_catalogs
                .get(catalog_version)
                .ok_or_else(|| {
                    Error::new(ErrorKind::NotFound, "published catalog version not found")
                })?;
            catalog.verify()?;
            if definition.version == 0 || !valid_ascii_name(&definition.id) {
                return Err(Error::invalid("invalid workflow name or version"));
            }
            if definition.format_version != crate::ir::FORMAT_VERSION
                || definition.digest != digest_of(definition)?
            {
                return Err(Error::new(
                    ErrorKind::FailedPrecondition,
                    "invalid normalized definition format or digest",
                ));
            }
            crate::compiler::validate_definition(definition, &catalog.catalog)?;
            let entry = state
                .published_definitions
                .entry(definition.id.clone())
                .or_default();
            if let Some(existing) = entry.get(&definition.version) {
                if existing.format != DEFINITION_FORMAT
                    || existing.digest != definition.digest.0
                    || existing.catalog_version != *catalog_version
                    || existing.definition != **definition
                {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "workflow version has different content or catalog",
                    ));
                }
            } else {
                entry.insert(
                    definition.version,
                    PublishedDefinition {
                        format: DEFINITION_FORMAT.to_owned(),
                        normalized_format_version: definition.format_version,
                        digest: definition.digest.0.clone(),
                        catalog_version: *catalog_version,
                        definition: (**definition).clone(),
                    },
                );
            }
            Ok((
                Disposition::Applied {
                    run: None,
                    digest: Some(definition.digest.0.clone()),
                    version: Some(definition.version),
                },
                None,
            ))
        }
        PublicationOperation::Start {
            workflow,
            version,
            start_key,
            input,
        } => {
            if !valid_ascii_name(workflow) || start_key.is_empty() || start_key.len() > 128 {
                return Err(Error::invalid(
                    "workflow name or start key invalid (key must be 1..128 UTF-8 bytes)",
                ));
            }
            if *version == Some(0) {
                return Err(Error::invalid("workflow version must be positive"));
            }
            let input_digest = digest(input, crate::limits::MAX_PAYLOAD_BYTES)?;
            if let Some(existing) = state
                .start_keys
                .get(workflow)
                .and_then(|keys| keys.get(start_key))
            {
                if existing.input_digest != input_digest
                    || version.is_some_and(|v| v != existing.version)
                {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "start key has different input or pinned version",
                    ));
                }
                return Ok((
                    Disposition::Applied {
                        run: Some(existing.run),
                        digest: None,
                        version: Some(existing.version),
                    },
                    None,
                ));
            }
            let versions = state
                .published_definitions
                .get(workflow)
                .ok_or_else(|| Error::new(ErrorKind::NotFound, "workflow not published"))?;
            let pinned = match version {
                Some(v) => versions.get(v),
                None => versions.last_key_value().map(|(_, d)| d),
            }
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "workflow version not published"))?;
            pinned.verify()?;
            let catalog = state
                .published_catalogs
                .get(&pinned.catalog_version)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::FailedPrecondition,
                        "retained catalog unavailable",
                    )
                })?;
            catalog.verify()?;
            catalog
                .catalog
                .validate_value(&pinned.definition.input_schema, input)?;
            if state.recovery.as_ref().is_some_and(|hold| !hold.authorized) {
                return Err(Error::new(
                    ErrorKind::FailedPrecondition,
                    "execution suspended until recovery is acknowledged",
                ));
            }
            let mut hasher = Sha256::new();
            hasher.update(key.cluster_id.as_bytes());
            hasher.update([0]);
            hasher.update(key.principal_id.as_bytes());
            hasher.update([0]);
            hasher.update(key.command_id.as_bytes());
            let hash = hasher.finalize();
            let run = RunId::from_bytes(hash[..16].try_into().expect("16 bytes"));
            if state.runs.contains_key(&run) {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "run identity collision",
                ));
            }
            let pinned = pinned.clone();
            let pinned_identity = PinnedPublication {
                definition_format_version: pinned.normalized_format_version,
                definition_digest: pinned.digest.clone(),
                catalog_version: pinned.catalog_version,
                catalog_digest: catalog.digest.clone(),
                policy_format: "graphrun.run-policy/v1".to_owned(),
            };
            let catalog = catalog.catalog.clone();
            let start = Command {
                id: CommandId::from_bytes(hash[16..32].try_into().expect("16 bytes")),
                time: command.time,
                body: CommandBody::Start {
                    run,
                    definition: Box::new(pinned.definition.clone()),
                    catalog: Box::new(catalog.clone()),
                    input: input.clone(),
                },
            };
            let mut provisional = state.clone();
            // next_sequence tracks external inputs; history tracks every per-run event.
            let first = provisional
                .history
                .get(&run)
                .map_or(1, |events| events.len() as u64 + 1);
            provisional.runs.insert(
                run,
                RunState {
                    id: run,
                    definition: pinned.definition.clone(),
                    catalog,
                    input: Value::Null,
                    policy: CapturedRunPolicy::defaults(None),
                    status: RunStatus::Active,
                    root: ScopeId::from_bytes([0; 16]),
                    next_sequence: RunSequence::new(1),
                    next_ready_order: 0,
                    admitted_ms: 0,
                    terminal_ms: 0,
                    published: Some(pinned_identity),
                },
            );
            let decision = domain::decide(&provisional, &start)?;
            domain::apply_events_with_cause(
                &mut provisional,
                &decision.events,
                Some(command.id),
                Some(&key.principal_id),
                command.time,
            )?;
            let mut progress_hasher = Sha256::new();
            progress_hasher.update(b"graphrun-start-progress/v1\0");
            progress_hasher.update(hash);
            let progress_id = progress_hasher.finalize();
            domain::apply_command(
                &mut provisional,
                Command {
                    id: CommandId::from_bytes(progress_id[..16].try_into().expect("16 bytes")),
                    time: command.time,
                    body: CommandBody::Progress { run },
                },
            )?;
            let last = provisional
                .history
                .get(&run)
                .map_or(0, |events| events.len() as u64);
            if last < first {
                return Err(Error::new(
                    ErrorKind::FailedPrecondition,
                    "start emitted no run events",
                ));
            }
            provisional
                .start_keys
                .entry(workflow.clone())
                .or_default()
                .insert(
                    start_key.clone(),
                    StartKeyRecord {
                        run,
                        version: pinned.definition.version,
                        input_digest,
                        admitted_ms: command.time.as_millis(),
                    },
                );
            *state = provisional;
            Ok((
                Disposition::Applied {
                    run: Some(run),
                    digest: Some(pinned.digest),
                    version: Some(pinned.definition.version),
                },
                Some(EventRange { run, first, last }),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::compile_yaml;
    use crate::time::EngineTime;

    #[test]
    fn tombstone_and_command_identity_survive_pruned_run() {
        let auth_a = AuthContext::local_owner("cluster-a".to_owned());
        let auth_b = AuthContext::local_owner("cluster-b".to_owned());
        let mut state = State::default();
        let commit = |state: &mut State, auth: &AuthContext, id, op| {
            let command = super::command(auth, id, EngineTime::from_millis(100), op).unwrap();
            let CommandBody::Publication { key, operation } = &command.body else {
                unreachable!()
            };
            super::apply(state, &command, key, operation)
        };
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        commit(
            &mut state,
            &auth_a,
            CommandId::generate(),
            PublicationOperation::Catalog {
                version: 1,
                catalog: catalog.clone(),
            },
        )
        .ensure_applied()
        .unwrap();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/remote.yaml"),
            &catalog,
        )
        .unwrap();
        commit(
            &mut state,
            &auth_a,
            CommandId::generate(),
            PublicationOperation::Definition {
                definition: Box::new(definition),
                catalog_version: 1,
            },
        )
        .ensure_applied()
        .unwrap();
        let id = CommandId::generate();
        let input: Value = serde_json::from_value(serde_json::json!({"value":1})).unwrap();
        let op = || PublicationOperation::Start {
            workflow: "external_worker_echo".to_owned(),
            version: None,
            start_key: "tombstone".to_owned(),
            input: input.clone(),
        };
        let first = commit(&mut state, &auth_a, id, op()).applied_run().unwrap();
        state.history.remove(&first);
        state.runs.remove(&first);
        assert_eq!(
            commit(&mut state, &auth_a, CommandId::generate(), op())
                .applied_run()
                .unwrap(),
            first
        );
        let other = commit(&mut state, &auth_b, id, op());
        assert_ne!(other.key, auth_a.key(id));
        assert_eq!(other.applied_run().unwrap(), first);
        let rejected = commit(
            &mut state,
            &auth_a,
            id,
            PublicationOperation::Start {
                workflow: "external_worker_echo".to_owned(),
                version: None,
                start_key: "tombstone".to_owned(),
                input: Value::Null,
            },
        );
        assert_eq!(
            rejected.ensure_applied().unwrap_err().kind,
            ErrorKind::AlreadyExists
        );
    }

    #[test]
    fn oversize_and_unknown_result_format_fail_closed() {
        let auth = AuthContext::local_owner("cluster".to_owned());
        assert_eq!(
            super::command(
                &auth,
                CommandId::from_bytes([0; 16]),
                EngineTime::from_millis(1),
                PublicationOperation::Catalog {
                    version: 1,
                    catalog: Catalog::default(),
                },
            )
            .unwrap_err()
            .kind,
            ErrorKind::InvalidArgument
        );
        let mut state = State::default();
        let id = CommandId::generate();
        let command = super::command(
            &auth,
            id,
            EngineTime::from_millis(1),
            PublicationOperation::Start {
                workflow: "example".to_owned(),
                version: None,
                start_key: "key".to_owned(),
                input: Value::String("x".repeat(crate::limits::MAX_PAYLOAD_BYTES + 1)),
            },
        )
        .unwrap();
        let CommandBody::Publication { key, operation } = &command.body else {
            unreachable!()
        };
        let receipt = super::apply(&mut state, &command, key, operation);
        assert_eq!(
            receipt.ensure_applied().unwrap_err().kind,
            ErrorKind::ResourceExhausted
        );
        assert!(
            state
                .command_results
                .contains_key(&auth.key(id).storage_key())
        );
        let mut corrupt = receipt;
        corrupt.format = "graphrun.command-result/v999".to_owned();
        assert_eq!(
            corrupt.ensure_applied().unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
    }

    #[test]
    fn uncanonical_requests_keep_distinct_durable_identities() {
        let auth = AuthContext::local_owner("cluster".to_owned());
        let id = CommandId::generate();
        let mut state = State::default();
        let commit = |state: &mut State, time, operation| {
            let command =
                super::command(&auth, id, EngineTime::from_millis(time), operation).unwrap();
            let CommandBody::Publication { key, operation } = &command.body else {
                unreachable!()
            };
            super::apply(state, &command, key, operation)
        };
        let bad_catalog = |multiple_of| {
            let mut catalog = Catalog::default();
            catalog.schemas.insert(
                crate::schema::SchemaKey::parse("float/v1").unwrap(),
                serde_json::json!({"type":"number","multipleOf":multiple_of}),
            );
            PublicationOperation::Catalog {
                version: 1,
                catalog,
            }
        };
        let first = commit(&mut state, 10, bad_catalog(0.5));
        assert_eq!(
            first.ensure_applied().unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        assert_eq!(first.request_digest.len(), 64);
        assert!(state.published_catalogs.is_empty());

        let retry = commit(&mut state, 20, bad_catalog(0.5));
        assert_eq!(
            serde_json::to_value(&retry).unwrap(),
            serde_json::to_value(&first).unwrap()
        );
        let conflict = commit(&mut state, 30, bad_catalog(0.25));
        assert_eq!(
            conflict.ensure_applied().unwrap_err().kind,
            ErrorKind::AlreadyExists
        );
        assert_ne!(conflict.request_digest, first.request_digest);

        let bytes = serde_json::to_vec(&state).unwrap();
        let mut restarted: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(commit(&mut restarted, 40, bad_catalog(0.5))).unwrap(),
            serde_json::to_value(&first).unwrap()
        );
        assert_eq!(
            restarted.command_results[&auth.key(id).storage_key()].request_digest,
            first.request_digest
        );
    }

    #[test]
    fn overly_deep_input_is_rejected_without_losing_receipt() {
        let auth = AuthContext::local_owner("cluster".to_owned());
        let mut input = Value::Null;
        for _ in 0..=crate::limits::MAX_DATA_DEPTH {
            input = Value::Array(vec![input]);
        }
        let operation = PublicationOperation::Start {
            workflow: "deep".to_owned(),
            version: None,
            start_key: "key".to_owned(),
            input,
        };
        assert!(serde_json::to_vec(&operation).is_ok());
        let command = super::command(
            &auth,
            CommandId::generate(),
            EngineTime::from_millis(1),
            operation,
        )
        .unwrap();
        let CommandBody::Publication { key, operation } = &command.body else {
            unreachable!()
        };
        let mut state = State::default();
        let receipt = super::apply(&mut state, &command, key, operation);
        assert_eq!(
            receipt.ensure_applied().unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            receipt.request_digest,
            super::apply(&mut state, &command, key, operation).request_digest
        );
        assert!(state.command_results.contains_key(&key.storage_key()));
    }

    #[test]
    fn retained_catalog_digest_is_verified_before_start() {
        let auth = AuthContext::local_owner("cluster".to_owned());
        let catalog = Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap();
        let mut state = State::default();
        let commit = |state: &mut State, op| {
            let command =
                super::command(&auth, CommandId::generate(), EngineTime::from_millis(1), op)
                    .unwrap();
            let CommandBody::Publication { key, operation } = &command.body else {
                unreachable!()
            };
            super::apply(state, &command, key, operation)
        };
        commit(
            &mut state,
            PublicationOperation::Catalog {
                version: 1,
                catalog: catalog.clone(),
            },
        )
        .ensure_applied()
        .unwrap();
        let definition = compile_yaml(
            include_str!("../../docs/specs/v1/examples/remote.yaml"),
            &catalog,
        )
        .unwrap();
        commit(
            &mut state,
            PublicationOperation::Definition {
                definition: Box::new(definition),
                catalog_version: 1,
            },
        )
        .ensure_applied()
        .unwrap();
        state
            .published_catalogs
            .get_mut(&1)
            .unwrap()
            .catalog
            .schemas
            .insert(
                crate::schema::SchemaKey::parse("counter/v1").unwrap(),
                serde_json::json!({"type":"string"}),
            );
        let result = commit(
            &mut state,
            PublicationOperation::Start {
                workflow: "external_worker_echo".to_owned(),
                version: None,
                start_key: "checked".to_owned(),
                input: serde_json::from_value(serde_json::json!({"value":1})).unwrap(),
            },
        );
        assert_eq!(
            result.ensure_applied().unwrap_err().kind,
            ErrorKind::FailedPrecondition
        );
        assert!(state.runs.is_empty());
    }
}
