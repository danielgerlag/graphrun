//! Independent application worker. No replica or fixture handler is opened.
use crate::catalog::{Catalog, ExecutionKind};
use crate::error::{Error, ErrorKind, Result};
use crate::generated::{
    Assignment, ClaimRequest, ReconcileRequest, RegisterRequest, RenewRequest, RenewSessionRequest,
    ReportRequest, WatchReadyRequest, worker_client::WorkerClient,
};
use crate::ids::{
    ActivationId, ActivityKey, CommandId, ExecutionRole, RunId, ScopeId, WorkerSessionId,
};
use crate::schema::DurablePayload;
use crate::tls::{ClusterId, PeerRole, TlsMaterial, verify_peer_identity};
use crate::value::Value;
use crate::worker_contract::{
    PROTOCOL_VERSION, WorkerCapability, capability_for, contract_schemas, parse_role, role_name,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Outcome>> + Send>>;
type Handler = Arc<dyn Fn(Value, HandlerContext) -> HandlerFuture + Send + Sync>;
type HandlerKey = (String, u32, &'static str);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityError {
    pub code: String,
    pub message: String,
}

impl ActivityError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observed<O> {
    Applied(O),
    NotApplied,
    Unknown,
}

#[derive(Clone, Copy)]
struct Permission {
    session_expiry_ms: u64,
    claim_expiry_ms: u64,
    attempt_deadline_ms: u64,
}

#[derive(Clone, Copy)]
struct ClockSample {
    wall_ms: u64,
    boot_ms: u64,
}

struct ClockGuard {
    last: Mutex<ClockSample>,
    safe: AtomicBool,
}

impl ClockGuard {
    fn new() -> Result<Self> {
        Ok(Self {
            last: Mutex::new(Self::sample()?),
            safe: AtomicBool::new(true),
        })
    }

    fn sample() -> Result<ClockSample> {
        Ok(ClockSample {
            wall_ms: wall_ms()?,
            boot_ms: boot_ms()?,
        })
    }

    fn check(&self) -> Result<()> {
        if !self.safe.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "worker clock unsafe",
            ));
        }
        let mut last = self.last.lock().expect("worker clock sample");
        let current = Self::sample().inspect_err(|_| {
            self.safe.store(false, Ordering::SeqCst);
        })?;
        if let Some(reason) = clock_fault(*last, current) {
            self.safe.store(false, Ordering::SeqCst);
            return Err(Error::new(ErrorKind::FailedPrecondition, reason));
        }
        *last = current;
        Ok(())
    }
}

async fn monitor_clock<F: Future>(clock: &ClockGuard, operation: F) -> Result<F::Output> {
    tokio::pin!(operation);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            result = &mut operation => return Ok(result),
            _ = tick.tick() => clock.check()?,
        }
    }
}

fn clock_fault(previous: ClockSample, current: ClockSample) -> Option<&'static str> {
    let Some(elapsed) = current.boot_ms.checked_sub(previous.boot_ms) else {
        return Some("worker boot clock reversed");
    };
    if elapsed > 2_000 {
        return Some("worker watchdog gap");
    }
    let wall_delta = i128::from(current.wall_ms) - i128::from(previous.wall_ms);
    let difference = (wall_delta - i128::from(elapsed)).unsigned_abs();
    (difference > u128::from(250 + elapsed / 1_000)).then_some("worker wall/boot clock skew")
}
#[cfg(target_os = "linux")]
fn boot_ms() -> Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) } != 0 {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            format!("CLOCK_BOOTTIME: {}", std::io::Error::last_os_error()),
        ));
    }
    let sec = u64::try_from(time.tv_sec).map_err(|_| Error::invalid("negative boot time"))?;
    let ns =
        u64::try_from(time.tv_nsec).map_err(|_| Error::invalid("negative boot nanoseconds"))?;
    Ok(sec.saturating_mul(1_000).saturating_add(ns / 1_000_000))
}

#[cfg(target_os = "macos")]
fn boot_ms() -> Result<u64> {
    #[repr(C)]
    struct MachTimebaseInfo {
        numer: u32,
        denom: u32,
    }
    unsafe extern "C" {
        fn mach_continuous_time() -> u64;
        fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
    }
    let mut info = MachTimebaseInfo { numer: 0, denom: 0 };
    if unsafe { mach_timebase_info(&mut info) } != 0 || info.denom == 0 {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            "mach timebase unavailable",
        ));
    }
    let ticks = unsafe { mach_continuous_time() };
    Ok(
        (u128::from(ticks) * u128::from(info.numer) / u128::from(info.denom) / 1_000_000)
            .try_into()
            .map_err(|_| Error::invalid("boot time exceeds u64"))?,
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn boot_ms() -> Result<u64> {
    Err(Error::new(
        ErrorKind::FailedPrecondition,
        "suspend-aware boot clock unavailable",
    ))
}

/// Identity and current permission for one claimed handler invocation.
#[derive(Clone)]
pub struct HandlerContext {
    pub run: RunId,
    pub scope: ScopeId,
    pub activation: ActivationId,
    pub attempt: u32,
    pub role: ExecutionRole,
    pub effect_key: String,
    pub attempt_deadline_ms: u64,
    cancelled: Arc<AtomicBool>,
    permission: Arc<Mutex<Permission>>,
    clock: Arc<ClockGuard>,
}

impl HandlerContext {
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Check immediately before starting another external effect.
    pub fn can_start_effect(&self) -> bool {
        let permission = *self.permission.lock().expect("worker permission");
        if self.clock.check().is_err() {
            return false;
        }
        let Ok(now) = wall_ms() else { return false };
        !self.is_cancelled()
            && now < permission.session_expiry_ms.saturating_sub(5_000)
            && now < permission.claim_expiry_ms.saturating_sub(5_000)
            && now < permission.attempt_deadline_ms
    }
}

enum Outcome {
    Success(Value),
    Failure(ActivityError),
    Reconciled(Observed<Value>),
}

struct WorkerEndpoint {
    url: String,
    server_name: String,
}

fn leader_redirect(status: &tonic::Status) -> Result<Option<WorkerEndpoint>> {
    let endpoint = status.metadata().get("graphrun-leader-endpoint");
    let server_name = status.metadata().get("graphrun-leader-server-name");
    let (Some(endpoint), Some(server_name)) = (endpoint, server_name) else {
        if endpoint.is_some() || server_name.is_some() {
            return Err(Error::invalid("incomplete leader redirect"));
        }
        return Ok(None);
    };
    let endpoint = endpoint
        .to_str()
        .map_err(|err| Error::invalid(err.to_string()))?;
    let server_name = server_name
        .to_str()
        .map_err(|err| Error::invalid(err.to_string()))?;
    if endpoint.is_empty() || server_name.is_empty() {
        return Err(Error::invalid("empty leader redirect"));
    }
    rustls::pki_types::ServerName::try_from(server_name.to_owned())
        .map_err(|err| Error::invalid(format!("leader server name: {err}")))?;
    if !endpoint.starts_with("https://") {
        return Err(Error::invalid("leader redirect requires TLS"));
    }
    let url = endpoint.to_owned();
    Endpoint::from_shared(url.clone()).map_err(|err| Error::invalid(err.to_string()))?;
    Ok(Some(WorkerEndpoint {
        url,
        server_name: server_name.to_owned(),
    }))
}

/// Register only the roles this process can actually execute.
pub struct WorkerBuilder {
    endpoints: Vec<WorkerEndpoint>,
    tls: TlsMaterial,
    catalog: Catalog,
    capacity: u32,
    handlers: BTreeMap<HandlerKey, Handler>,
    blocking_slots: Arc<Semaphore>,
}

/// Remote worker session bound to the signed worker certificate and catalog.
pub struct Worker {
    client: WorkerClient<Channel>,
    endpoints: Vec<WorkerEndpoint>,
    endpoint_index: usize,
    tls: TlsMaterial,
    session: WorkerSessionId,
    session_revision: u64,
    session_expiry_ms: u64,
    capacity: u32,
    catalog: Catalog,
    capabilities: Vec<WorkerCapability>,
    handlers: BTreeMap<HandlerKey, Handler>,
    clock: Arc<ClockGuard>,
}

impl Worker {
    pub fn builder(
        endpoint: impl Into<String>,
        tls: TlsMaterial,
        catalog: Catalog,
    ) -> WorkerBuilder {
        let server_name = tls.server_name.clone();
        WorkerBuilder {
            endpoints: vec![WorkerEndpoint {
                url: endpoint.into(),
                server_name,
            }],
            tls,
            catalog,
            capacity: crate::limits::CLAIM_BATCH,
            handlers: BTreeMap::new(),
            blocking_slots: Arc::new(Semaphore::new(
                crate::limits::BLOCKING_POOL_DEFAULT as usize,
            )),
        }
    }
}

impl WorkerBuilder {
    /// Add a member with the same TLS server name as the first endpoint.
    pub fn seed(self, endpoint: impl Into<String>) -> Self {
        let server_name = self.tls.server_name.clone();
        self.seed_with_server_name(endpoint, server_name)
    }

    /// Add a member with its own certificate DNS name.
    pub fn seed_with_server_name(
        mut self,
        endpoint: impl Into<String>,
        server_name: impl Into<String>,
    ) -> Self {
        let url = endpoint.into();
        let server_name = server_name.into();
        if let Some(existing) = self.endpoints.iter_mut().find(|item| item.url == url) {
            existing.server_name = server_name;
        } else {
            self.endpoints.push(WorkerEndpoint { url, server_name });
        }
        self
    }

    pub fn capacity(mut self, capacity: u32) -> Result<Self> {
        if capacity == 0 || capacity > crate::limits::MAX_ACTIVE_LEAVES_PER_WORKER {
            return Err(Error::invalid("worker capacity must be between 1 and 128"));
        }
        self.capacity = capacity;
        Ok(self)
    }

    #[cfg(any(test, feature = "fixture-worker"))]
    pub fn fixture_handlers(mut self) -> Self {
        for key in self.catalog.activities.keys() {
            if !crate::handlers::is_fixture(&key.name) {
                continue;
            }
            for role in [ExecutionRole::Forward, ExecutionRole::Compensation] {
                let name = key.name.clone();
                let wrapped: Handler = Arc::new(move |input, context| {
                    let name = name.clone();
                    Box::pin(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            crate::engine::dispatch_handler(
                                &name,
                                &input,
                                Some(&context.effect_key),
                            )
                        })
                        .await
                        .map_err(|err| Error::invalid(format!("fixture worker panicked: {err}")))?;
                        match result {
                            Ok(output) => Ok(Outcome::Success(output)),
                            Err(err) => Ok(Outcome::Failure(ActivityError::new(
                                "worker.fixture_error",
                                err.to_string(),
                            ))),
                        }
                    })
                });
                self.handlers
                    .insert((key.name.clone(), key.version, role_name(role)), wrapped);
            }
        }
        for key in self.catalog.reconcilers.keys() {
            if !crate::handlers::is_fixture(&key.name) {
                continue;
            }
            let name = key.name.clone();
            let wrapped: Handler = Arc::new(move |input, context| {
                let name = name.clone();
                Box::pin(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        crate::engine::builtin_reconcile(&name, &input, Some(&context.effect_key))
                    })
                    .await
                    .map_err(|err| Error::invalid(format!("fixture reconciler panicked: {err}")))?;
                    Ok(Outcome::Reconciled(match result {
                        (crate::domain::ReconcileOutcome::Applied, Some(output)) => {
                            Observed::Applied(output)
                        }
                        (crate::domain::ReconcileOutcome::Applied, None) => {
                            return Err(Error::invalid("fixture applied without output"));
                        }
                        (crate::domain::ReconcileOutcome::NotApplied, None) => Observed::NotApplied,
                        (crate::domain::ReconcileOutcome::Unknown, None) => Observed::Unknown,
                        _ => {
                            return Err(Error::invalid(
                                "fixture reconciliation outcome has unexpected output",
                            ));
                        }
                    }))
                })
            });
            self.handlers.insert(
                (
                    key.name.clone(),
                    key.version,
                    role_name(ExecutionRole::Reconciliation),
                ),
                wrapped,
            );
        }
        self
    }

    fn insert<I: DurablePayload, O: DurablePayload>(
        mut self,
        name: &str,
        version: u32,
        role: ExecutionRole,
        kind: ExecutionKind,
        handler: Handler,
    ) -> Result<Self> {
        let key = ActivityKey::new(name, version);
        let (input, output) = contract_schemas(&self.catalog, &key, role)?;
        if self.catalog.schema_json(input)? != self.catalog.schema_json(&I::schema_ref())?
            || self.catalog.schema_json(output)? != self.catalog.schema_json(&O::schema_ref())?
        {
            return Err(Error::invalid(format!(
                "Rust payload schemas differ from catalog contract {}",
                key.as_stable_name()
            )));
        }
        if role != ExecutionRole::Reconciliation && self.catalog.activity(&key)?.execution != kind {
            return Err(Error::invalid(format!(
                "handler execution kind differs from catalog {}",
                key.as_stable_name()
            )));
        }
        let handler_key = (name.to_owned(), version, role_name(role));
        if self.handlers.insert(handler_key, handler).is_some() {
            return Err(Error::invalid("worker handler role already registered"));
        }
        Ok(self)
    }

    pub fn activity<I, O, F, Fut>(self, name: &str, version: u32, handler: F) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, ActivityError>> + Send + 'static,
    {
        self.async_role(name, version, ExecutionRole::Forward, handler)
    }

    pub fn compensation<I, O, F, Fut>(self, name: &str, version: u32, handler: F) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, ActivityError>> + Send + 'static,
    {
        self.async_role(name, version, ExecutionRole::Compensation, handler)
    }

    fn async_role<I, O, F, Fut>(
        self,
        name: &str,
        version: u32,
        role: ExecutionRole,
        handler: F,
    ) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<O, ActivityError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let wrapped: Handler = Arc::new(move |value, context| {
            let handler = handler.clone();
            Box::pin(async move {
                let input: I = decode(value)?;
                match handler(input, context).await {
                    Ok(output) => Ok(Outcome::Success(encode(output)?)),
                    Err(error) => Ok(Outcome::Failure(error)),
                }
            })
        });
        self.insert::<I, O>(name, version, role, ExecutionKind::Async, wrapped)
    }

    pub fn blocking<I, O, F>(self, name: &str, version: u32, handler: F) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> std::result::Result<O, ActivityError> + Send + Sync + 'static,
    {
        self.blocking_role(name, version, ExecutionRole::Forward, handler)
    }

    pub fn blocking_compensation<I, O, F>(
        self,
        name: &str,
        version: u32,
        handler: F,
    ) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> std::result::Result<O, ActivityError> + Send + Sync + 'static,
    {
        self.blocking_role(name, version, ExecutionRole::Compensation, handler)
    }

    fn blocking_role<I, O, F>(
        self,
        name: &str,
        version: u32,
        role: ExecutionRole,
        handler: F,
    ) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> std::result::Result<O, ActivityError> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let slots = self.blocking_slots.clone();
        let wrapped: Handler = Arc::new(move |value, context| {
            let handler = handler.clone();
            let slots = slots.clone();
            Box::pin(async move {
                let input: I = decode(value)?;
                let permit = slots
                    .acquire_owned()
                    .await
                    .map_err(|err| Error::invalid(err.to_string()))?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    match handler(input, context) {
                        Ok(output) => Ok(Outcome::Success(encode(output)?)),
                        Err(error) => Ok(Outcome::Failure(error)),
                    }
                })
                .await
                .map_err(|err| Error::invalid(format!("blocking handler panicked: {err}")))?
            })
        });
        self.insert::<I, O>(name, version, role, ExecutionKind::Blocking, wrapped)
    }

    pub fn reconciler<I, O, F, Fut>(self, name: &str, version: u32, handler: F) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Observed<O>, ActivityError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let wrapped: Handler = Arc::new(move |value, context| {
            let handler = handler.clone();
            Box::pin(async move {
                let input: I = decode(value)?;
                match handler(input, context).await {
                    Ok(Observed::Applied(output)) => {
                        Ok(Outcome::Reconciled(Observed::Applied(encode(output)?)))
                    }
                    Ok(Observed::NotApplied) => Ok(Outcome::Reconciled(Observed::NotApplied)),
                    Ok(Observed::Unknown) => Ok(Outcome::Reconciled(Observed::Unknown)),
                    Err(error) => Ok(Outcome::Failure(error)),
                }
            })
        });
        self.insert::<I, O>(
            name,
            version,
            ExecutionRole::Reconciliation,
            ExecutionKind::Async,
            wrapped,
        )
    }

    pub fn blocking_reconciler<I, O, F>(self, name: &str, version: u32, handler: F) -> Result<Self>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, HandlerContext) -> std::result::Result<Observed<O>, ActivityError>
            + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        let slots = self.blocking_slots.clone();
        let wrapped: Handler = Arc::new(move |value, context| {
            let handler = handler.clone();
            let slots = slots.clone();
            Box::pin(async move {
                let input: I = decode(value)?;
                let permit = slots
                    .acquire_owned()
                    .await
                    .map_err(|err| Error::invalid(err.to_string()))?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    match handler(input, context) {
                        Ok(Observed::Applied(output)) => {
                            Ok(Outcome::Reconciled(Observed::Applied(encode(output)?)))
                        }
                        Ok(Observed::NotApplied) => Ok(Outcome::Reconciled(Observed::NotApplied)),
                        Ok(Observed::Unknown) => Ok(Outcome::Reconciled(Observed::Unknown)),
                        Err(error) => Ok(Outcome::Failure(error)),
                    }
                })
                .await
                .map_err(|err| Error::invalid(format!("blocking reconciler panicked: {err}")))?
            })
        });
        self.insert::<I, O>(
            name,
            version,
            ExecutionRole::Reconciliation,
            ExecutionKind::Blocking,
            wrapped,
        )
    }

    pub async fn open(self) -> Result<Worker> {
        self.catalog.validate_for_publication()?;
        if self.handlers.is_empty() {
            return Err(Error::invalid("worker has no handlers"));
        }
        let clock = Arc::new(ClockGuard::new()?);
        let mut capabilities = Vec::new();
        for (name, version, role) in self.handlers.keys() {
            capabilities.push(capability_for(
                &self.catalog,
                &ActivityKey::new(name, *version),
                parse_role(role)?,
            )?);
        }
        crate::tls::install_provider();
        let ca_digest = Sha256::digest(self.tls.ca_pem.as_bytes());
        let cluster = ClusterId::parse(hex::encode(&ca_digest[..16]))?;
        let chain = rustls_pemfile::certs(&mut self.tls.cert_pem.as_bytes())
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|err| Error::invalid(err.to_string()))?;
        let peer = verify_peer_identity(&self.tls.ca_pem, &cluster, &chain)?;
        peer.require_role(PeerRole::Worker)?;
        let mut session = WorkerSessionId::generate();
        let mut request = RegisterRequest {
            session_id: session.to_hex(),
            capacity: self.capacity,
            capabilities: capabilities.iter().map(WorkerCapability::to_wire).collect(),
            principal_id: peer.principal_id().as_str().to_owned(),
            protocol_min: PROTOCOL_VERSION,
            protocol_max: PROTOCOL_VERSION,
            command_id: CommandId::generate().to_hex(),
        };
        let mut last_error = Error::new(ErrorKind::Unavailable, "no worker endpoint is available");
        let mut endpoints = self.endpoints;
        let mut tried = BTreeSet::new();
        let mut index = 0usize;
        let mut backoff = Duration::from_millis(250);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5 * 60);
        loop {
            clock.check()?;
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::new(
                    ErrorKind::DeadlineExceeded,
                    format!(
                        "worker registration unknown (session={} command={}): {last_error}",
                        request.session_id, request.command_id,
                    ),
                ));
            }
            if index >= endpoints.len() {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                monitor_clock(
                    &clock,
                    tokio::time::sleep(retry_delay(backoff)?.min(remaining)),
                )
                .await?;
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
                tried.clear();
                index = 0;
                continue;
            }
            let endpoint = &endpoints[index];
            let current_index = index;
            index += 1;
            if !tried.insert((endpoint.url.clone(), endpoint.server_name.clone())) {
                continue;
            }
            match monitor_clock(
                &clock,
                tokio::time::timeout(
                    Duration::from_secs(5)
                        .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                    connect(&endpoint.url, &tls_for(&self.tls, endpoint)),
                ),
            )
            .await?
            {
                Ok(Ok(channel)) => {
                    let mut client = WorkerClient::new(channel);
                    let mut rpc = tonic::Request::new(request.clone());
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let rpc_limit = Duration::from_secs(2).min(remaining);
                    rpc.set_timeout(rpc_limit);
                    match monitor_clock(
                        &clock,
                        tokio::time::timeout(rpc_limit, client.register(rpc)),
                    )
                    .await?
                    {
                        Ok(Ok(response)) => {
                            let registered = response.into_inner();
                            if !registered.error.is_empty() {
                                return Err(Error::invalid(registered.error));
                            }
                            if registered.revision == 0 {
                                return Err(Error::invalid("invalid worker session revision"));
                            }
                            if registered.lease_expiry_ms <= wall_ms()? {
                                session = WorkerSessionId::generate();
                                request.session_id = session.to_hex();
                                request.command_id = CommandId::generate().to_hex();
                                tried.clear();
                                index = 0;
                                backoff = Duration::from_millis(250);
                                continue;
                            }
                            return Ok(Worker {
                                client,
                                endpoints,
                                endpoint_index: current_index,
                                tls: self.tls,
                                session,
                                session_revision: registered.revision,
                                session_expiry_ms: registered.lease_expiry_ms,
                                capacity: self.capacity,
                                catalog: self.catalog,
                                capabilities,
                                handlers: self.handlers,
                                clock,
                            });
                        }
                        Err(_) => {
                            last_error = Error::new(
                                ErrorKind::Unavailable,
                                format!(
                                    "worker registration response unresolved for {}",
                                    request.command_id
                                ),
                            );
                        }
                        Ok(Err(err)) if uncertain_rpc(&err) => {
                            if let Some(redirect) = leader_redirect(&err)? {
                                if !endpoints.iter().any(|item| {
                                    item.url == redirect.url
                                        && item.server_name == redirect.server_name
                                }) {
                                    if endpoints.len() >= 32 {
                                        return Err(Error::new(
                                            ErrorKind::ResourceExhausted,
                                            "too many worker seed endpoints",
                                        ));
                                    }
                                    endpoints.push(redirect);
                                }
                            }
                            last_error = rpc_error(err);
                        }
                        Ok(Err(err)) => return Err(rpc_error(err)),
                    }
                }
                Ok(Err(err)) => last_error = err,
                Err(_) => {
                    last_error = Error::new(
                        ErrorKind::Unavailable,
                        format!("worker endpoint {} did not connect", endpoint.url),
                    );
                }
            }
        }
    }
}

fn decode<I: DurablePayload>(value: Value) -> Result<I> {
    serde_json::from_value(value.to_json())
        .map_err(|err| Error::invalid(format!("worker input decode: {err}")))
}

fn encode<O: DurablePayload>(value: O) -> Result<Value> {
    Value::from_json(
        serde_json::to_value(value)
            .map_err(|err| Error::invalid(format!("worker output encode: {err}")))?,
    )
}

fn wall_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            Error::new(
                ErrorKind::FailedPrecondition,
                "worker wall clock predates Unix epoch",
            )
        })?
        .as_millis() as u64)
}

fn rpc_error(error: tonic::Status) -> Error {
    let kind = match error.code() {
        tonic::Code::FailedPrecondition => ErrorKind::FailedPrecondition,
        tonic::Code::InvalidArgument => ErrorKind::InvalidArgument,
        tonic::Code::Unauthenticated => ErrorKind::Unauthenticated,
        tonic::Code::PermissionDenied => ErrorKind::PermissionDenied,
        tonic::Code::ResourceExhausted => ErrorKind::ResourceExhausted,
        tonic::Code::DeadlineExceeded => ErrorKind::DeadlineExceeded,
        _ => ErrorKind::Unavailable,
    };
    Error::new(kind, error.to_string())
}

fn uncertain_rpc(error: &tonic::Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

fn retry_delay(base: Duration) -> Result<Duration> {
    let mut random = [0u8; 2];
    getrandom::fill(&mut random).map_err(|err| Error::invalid(err.to_string()))?;
    let jitter_ms = u64::from(u16::from_le_bytes(random)) % ((base.as_millis() / 4) as u64 + 1);
    Ok((base + Duration::from_millis(jitter_ms)).min(Duration::from_secs(5)))
}

async fn connect(endpoint: &str, tls: &TlsMaterial) -> Result<Channel> {
    Endpoint::from_shared(endpoint.to_owned())
        .map_err(|err| Error::invalid(err.to_string()))?
        .tls_config(crate::rpc::client_tls(tls)?)
        .map_err(|err| Error::invalid(err.to_string()))?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))
}

fn tls_for(tls: &TlsMaterial, endpoint: &WorkerEndpoint) -> TlsMaterial {
    let mut configured = tls.clone();
    configured.server_name = endpoint.server_name.clone();
    configured
}

struct Active {
    assignment: Assignment,
    permission: Arc<Mutex<Permission>>,
    cancelled: Arc<AtomicBool>,
    outcome: Option<Outcome>,
    pending_result: Option<PendingResult>,
    submitted: bool,
    renew_id: Option<CommandId>,
}

#[derive(Clone)]
enum PendingResult {
    Report(ReportRequest),
    Reconcile(ReconcileRequest),
}

impl PendingResult {
    fn command_id(&self) -> &str {
        match self {
            Self::Report(request) => &request.command_id,
            Self::Reconcile(request) => &request.command_id,
        }
    }
}

impl Worker {
    fn rotate_endpoint(&mut self, status: &tonic::Status) -> Result<()> {
        if let Some(redirect) = leader_redirect(status)? {
            if let Some(index) = self
                .endpoints
                .iter()
                .position(|item| item.url == redirect.url)
            {
                self.endpoints[index].server_name = redirect.server_name;
                self.endpoint_index = index;
            } else {
                if self.endpoints.len() >= 32 {
                    return Err(Error::new(
                        ErrorKind::ResourceExhausted,
                        "too many worker seed endpoints",
                    ));
                }
                self.endpoints.push(redirect);
                self.endpoint_index = self.endpoints.len() - 1;
            }
        } else {
            self.endpoint_index = (self.endpoint_index + 1) % self.endpoints.len();
        }
        let endpoint = &self.endpoints[self.endpoint_index];
        let channel = Endpoint::from_shared(endpoint.url.clone())
            .map_err(|err| Error::invalid(err.to_string()))?
            .tls_config(crate::rpc::client_tls(&tls_for(&self.tls, endpoint))?)
            .map_err(|err| Error::invalid(err.to_string()))?
            .connect_lazy();
        self.client = WorkerClient::new(channel);
        Ok(())
    }

    /// Run until the shutdown future completes, then stop claiming and drain running handlers.
    pub async fn run_until<F: Future<Output = ()>>(mut self, shutdown: F) -> Result<()> {
        let mut watcher = self.client.clone();
        let mut active: BTreeMap<String, Active> = BTreeMap::new();
        let mut tasks: JoinSet<(String, Result<Outcome>)> = JoinSet::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut clock_tick = tokio::time::interval(Duration::from_millis(250));
        clock_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut shutdown = Box::pin(shutdown);
        let mut draining = false;
        let mut generation = 0u64;
        let mut cursor = 0u64;
        let mut session_renew_id: Option<CommandId> = None;
        let mut pending_claim: Option<(CommandId, u32)> = None;
        loop {
            if draining && active.is_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = clock_tick.tick() => {
                    if let Err(err) = self.clock.check() {
                        for claim in active.values() {
                            claim.cancelled.store(true, Ordering::SeqCst);
                        }
                        return Err(err);
                    }
                }
                _ = &mut shutdown, if !draining => {
                    draining = true;
                    for claim in active.values() {
                        claim.cancelled.store(true, Ordering::SeqCst);
                    }
                }
                _ = ticker.tick() => {
                    if active.is_empty() && draining { return Ok(()); }
                    self.clock.check()?;
                    let id = *session_renew_id.get_or_insert_with(CommandId::generate);
                    let mut request = tonic::Request::new(RenewSessionRequest {
                        command_id: id.to_hex(),
                        session_id: self.session.to_hex(),
                        revision: self.session_revision,
                    });
                    request.set_timeout(Duration::from_secs(2));
                    match self.client.renew_session(request).await {
                        Ok(response) => {
                            let response = response.into_inner();
                            self.session_revision = response.revision;
                            self.session_expiry_ms = response.lease_expiry_ms;
                            session_renew_id = None;
                        }
                        Err(err) if uncertain_rpc(&err) => {
                            if wall_ms()? >= self.session_expiry_ms.saturating_sub(5_000) {
                                for claim in active.values() { claim.cancelled.store(true, Ordering::SeqCst); }
                                return Err(rpc_error(err));
                            }
                            self.rotate_endpoint(&err)?;
                            watcher = self.client.clone();
                            generation = 0;
                            cursor = 0;
                            continue;
                        }
                        Err(err) => return Err(rpc_error(err)),
                    }
                    for claim in active.values_mut() {
                        claim.permission.lock().expect("worker permission").session_expiry_ms = self.session_expiry_ms;
                        if claim.pending_result.is_some() || (claim.outcome.is_some() && claim.renew_id.is_none()) {
                            continue;
                        }
                        if wall_ms()? >= claim.assignment.attempt_deadline_ms {
                            claim.cancelled.store(true, Ordering::SeqCst);
                            return Err(Error::new(ErrorKind::FailedPrecondition,
                                format!("attempt deadline elapsed for {}", claim.assignment.activation_id)));
                        }
                        let id = *claim.renew_id.get_or_insert_with(CommandId::generate);
                        let mut request = tonic::Request::new(RenewRequest {
                            command_id: id.to_hex(),
                            session_id: self.session.to_hex(),
                            activation_id: claim.assignment.activation_id.clone(),
                            generation: claim.assignment.generation,
                            revision: claim.assignment.revision,
                        });
                        request.set_timeout(Duration::from_secs(2));
                        match self.client.renew(request).await {
                            Ok(response) => {
                                let response = response.into_inner();
                                if !response.error.is_empty() {
                                    claim.cancelled.store(true, Ordering::SeqCst);
                                    return Err(Error::new(ErrorKind::FailedPrecondition, response.error));
                                }
                                claim.assignment.revision = response.revision;
                                claim.assignment.lease_expiry_ms = response.lease_expiry_ms;
                                claim.permission.lock().expect("worker permission").claim_expiry_ms = response.lease_expiry_ms;
                                claim.renew_id = None;
                            }
                            Err(err) if uncertain_rpc(&err) => {
                                if wall_ms()? >= claim.assignment.lease_expiry_ms.saturating_sub(5_000) {
                                    claim.cancelled.store(true, Ordering::SeqCst);
                                    return Err(rpc_error(err));
                                }
                                tracing::warn!(activation = %claim.assignment.activation_id, "claim renewal uncertain; retrying same command");
                                self.rotate_endpoint(&err)?;
                                watcher = self.client.clone();
                                generation = 0;
                                cursor = 0;
                            }
                            Err(err) => return Err(rpc_error(err)),
                        }
                    }
                    let completed: Vec<_> = active.iter().filter(|(_,claim)| claim.outcome.is_some() && claim.renew_id.is_none()).map(|(id,_)| id.clone()).collect();
                    for id in completed { self.report(&id, &mut active).await?; }
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    let (id, result) = result.expect("join set nonempty").map_err(|err| Error::invalid(format!("worker handler task panicked: {err}")))?;
                    let claim = active.get_mut(&id).ok_or_else(|| Error::invalid("completed handler claim unavailable"))?;
                    claim.outcome = Some(match result {
                        Ok(outcome) => outcome,
                        Err(err) => Outcome::Failure(ActivityError::new("worker.handler_error", err.to_string())),
                    });
                    if claim.renew_id.is_none() { self.report(&id, &mut active).await?; }
                }
                claim = async {
                    let (id, capacity) = pending_claim.expect("enabled claim branch");
                    self.client.claim(ClaimRequest {
                        command_id: id.to_hex(),
                        session_id: self.session.to_hex(),
                        capacity,
                    }).await
                }, if pending_claim.is_some() && !draining => {
                    match claim {
                        Ok(response) => {
                            pending_claim = None;
                            let claimed = response.into_inner();
                            if !claimed.error.is_empty() { return Err(Error::invalid(claimed.error)); }
                            for assignment in claimed.assignments {
                                let (id, input, context, handler) = self.prepare(&assignment)?;
                                let permission = context.permission.clone();
                                let cancelled = context.cancelled.clone();
                                active.insert(id.clone(), Active { assignment, permission, cancelled, outcome: None, pending_result: None, submitted: false, renew_id: None });
                                tasks.spawn(async move { (id, handler(input, context).await) });
                            }
                        }
                        Err(err) if uncertain_rpc(&err) => {
                            self.rotate_endpoint(&err)?;
                            watcher = self.client.clone();
                            generation = 0;
                            cursor = 0;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(err) => return Err(rpc_error(err)),
                    }
                }
                watch = watcher.watch_ready(WatchReadyRequest {
                    session_id: self.session.to_hex(), generation, cursor,
                }), if !draining && pending_claim.is_none() && active.len() < self.capacity as usize => {
                    match watch {
                        Ok(response) => {
                            let response = response.into_inner();
                            generation = response.generation;
                            cursor = response.cursor;
                            if !response.ready { continue; }
                            pending_claim = Some((
                                CommandId::generate(),
                                (self.capacity as usize - active.len()).min(crate::limits::CLAIM_BATCH as usize) as u32,
                            ));
                        }
                        Err(err) if uncertain_rpc(&err) => {
                            generation = 0;
                            cursor = 0;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            self.rotate_endpoint(&err)?;
                            watcher = self.client.clone();
                        }
                        Err(err) => return Err(rpc_error(err)),
                    }
                }
            }
        }
    }

    pub async fn run(self) -> Result<()> {
        self.run_until(std::future::pending()).await
    }

    fn prepare(&self, assignment: &Assignment) -> Result<(String, Value, HandlerContext, Handler)> {
        self.clock.check()?;
        let now = wall_ms()?;
        let role = parse_role(&assignment.role)?;
        let capability = self
            .capabilities
            .iter()
            .find(|capability| {
                capability.activity_name == assignment.activity_name
                    && capability.activity_version == assignment.activity_version
                    && capability.role == role
            })
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::FailedPrecondition,
                    "unadvertised worker assignment",
                )
            })?;
        if assignment.session_id != self.session.to_hex()
            || assignment.codec_version != capability.codec_version
            || assignment.input_schema_digest != capability.input_schema_digest
            || assignment.output_schema_digest != capability.output_schema_digest
            || assignment.contract_digest != capability.contract_digest
            || assignment.attempt == 0
            || assignment.lease_expiry_ms <= now
            || assignment.attempt_deadline_ms <= now
        {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "assignment authority or pinned contract mismatch",
            ));
        }
        let input = serde_json::from_slice(&assignment.input_json)
            .map_err(|err| Error::invalid(format!("worker input codec: {err}")))?;
        let key = ActivityKey::new(&assignment.activity_name, assignment.activity_version);
        let (input_schema, _) = contract_schemas(&self.catalog, &key, role)?;
        self.catalog.validate_value(input_schema, &input)?;
        let handler = self
            .handlers
            .get(&(
                assignment.activity_name.clone(),
                assignment.activity_version,
                role_name(role),
            ))
            .ok_or_else(|| Error::invalid("advertised handler unavailable"))?
            .clone();
        let context = HandlerContext {
            run: RunId::from_hex(&assignment.run_id).map_err(Error::invalid)?,
            scope: ScopeId::from_hex(&assignment.scope_id).map_err(Error::invalid)?,
            activation: ActivationId::from_hex(&assignment.activation_id)
                .map_err(Error::invalid)?,
            attempt: assignment.attempt,
            role,
            effect_key: assignment.effect_key.clone(),
            attempt_deadline_ms: assignment.attempt_deadline_ms,
            cancelled: Arc::new(AtomicBool::new(false)),
            permission: Arc::new(Mutex::new(Permission {
                session_expiry_ms: self.session_expiry_ms,
                claim_expiry_ms: assignment.lease_expiry_ms,
                attempt_deadline_ms: assignment.attempt_deadline_ms,
            })),
            clock: self.clock.clone(),
        };
        if !context.can_start_effect() {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "assignment is inside lease stop margin",
            ));
        }
        Ok((assignment.activation_id.clone(), input, context, handler))
    }

    fn prepare_result(&self, claim: &Active) -> Result<PendingResult> {
        let outcome = claim
            .outcome
            .as_ref()
            .ok_or_else(|| Error::invalid("handler not completed"))?;
        let a = &claim.assignment;
        let command_id = CommandId::generate().to_hex();
        let role = parse_role(&a.role)?;
        let key = ActivityKey::new(&a.activity_name, a.activity_version);
        let (_, output_schema) = contract_schemas(&self.catalog, &key, role)?;
        let validated = match outcome {
            Outcome::Success(value)
                if self.catalog.validate_value(output_schema, value).is_err() =>
            {
                Outcome::Failure(ActivityError::new(
                    "worker.invalid_output",
                    "handler output does not match pinned schema",
                ))
            }
            Outcome::Reconciled(Observed::Applied(value))
                if self.catalog.validate_value(output_schema, value).is_err() =>
            {
                Outcome::Failure(ActivityError::new(
                    "worker.invalid_output",
                    "reconciled output does not match pinned schema",
                ))
            }
            Outcome::Failure(error) if error.message.is_empty() => {
                Outcome::Failure(ActivityError::new(
                    "worker.invalid_error",
                    format!("handler returned empty message for {}", error.code),
                ))
            }
            Outcome::Failure(error)
                if role != ExecutionRole::Reconciliation
                    && !error.code.starts_with("worker.")
                    && self
                        .catalog
                        .activity(&key)?
                        .known_code(&error.code)
                        .is_none() =>
            {
                Outcome::Failure(ActivityError::new(
                    "worker.unmapped_error",
                    format!("undeclared error {}: {}", error.code, error.message),
                ))
            }
            Outcome::Failure(error) if error.code.is_empty() => Outcome::Failure(
                ActivityError::new("worker.invalid_error", error.message.clone()),
            ),
            Outcome::Success(value) => Outcome::Success(value.clone()),
            Outcome::Failure(error) => Outcome::Failure(error.clone()),
            Outcome::Reconciled(Observed::Applied(value)) => {
                Outcome::Reconciled(Observed::Applied(value.clone()))
            }
            Outcome::Reconciled(Observed::NotApplied) => Outcome::Reconciled(Observed::NotApplied),
            Outcome::Reconciled(Observed::Unknown) => Outcome::Reconciled(Observed::Unknown),
        };
        let outcome = &validated;
        if role == ExecutionRole::Reconciliation {
            let (outcome, output_json, error_code, error_message) = match outcome {
                Outcome::Reconciled(Observed::Applied(value)) => (
                    "applied",
                    serde_json::to_vec(value).map_err(|err| Error::invalid(err.to_string()))?,
                    String::new(),
                    String::new(),
                ),
                Outcome::Reconciled(Observed::NotApplied) => {
                    ("not_applied", Vec::new(), String::new(), String::new())
                }
                Outcome::Reconciled(Observed::Unknown) => {
                    ("unknown", Vec::new(), String::new(), String::new())
                }
                Outcome::Failure(error) => (
                    "unknown",
                    Vec::new(),
                    error.code.clone(),
                    error.message.clone(),
                ),
                Outcome::Success(_) => {
                    return Err(Error::invalid(
                        "reconciliation handler returned activity success",
                    ));
                }
            };
            Ok(PendingResult::Reconcile(ReconcileRequest {
                command_id,
                session_id: self.session.to_hex(),
                run_id: a.run_id.clone(),
                activation_id: a.activation_id.clone(),
                outcome: outcome.to_owned(),
                output_json,
                generation: a.generation,
                revision: a.revision,
                output_schema_digest: a.output_schema_digest.clone(),
                error_code,
                error_message,
            }))
        } else {
            let (output_json, code, message) = match outcome {
                Outcome::Success(value) => (
                    serde_json::to_vec(value).map_err(|err| Error::invalid(err.to_string()))?,
                    String::new(),
                    String::new(),
                ),
                Outcome::Failure(error) => (Vec::new(), error.code.clone(), error.message.clone()),
                Outcome::Reconciled(_) => {
                    return Err(Error::invalid(
                        "activity handler returned reconciliation outcome",
                    ));
                }
            };
            Ok(PendingResult::Report(ReportRequest {
                command_id,
                session_id: self.session.to_hex(),
                run_id: a.run_id.clone(),
                activation_id: a.activation_id.clone(),
                generation: a.generation,
                revision: a.revision,
                output_json,
                error_code: code,
                error_message: message,
                output_schema_digest: a.output_schema_digest.clone(),
            }))
        }
    }

    async fn report(&mut self, id: &str, active: &mut BTreeMap<String, Active>) -> Result<()> {
        self.clock.check()?;
        let claim = active
            .get(id)
            .ok_or_else(|| Error::invalid("claim unavailable for reporting"))?;
        if claim.pending_result.is_none() && claim.renew_id.is_some() {
            return Ok(());
        }
        if claim.pending_result.is_none() {
            let request = self.prepare_result(claim)?;
            active
                .get_mut(id)
                .expect("claim still present")
                .pending_result = Some(request);
        }
        let claim = active.get_mut(id).expect("claim still present");
        let permission = *claim.permission.lock().expect("worker permission");
        let now = wall_ms()?;
        if now >= self.session_expiry_ms {
            claim.cancelled.store(true, Ordering::SeqCst);
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                format!(
                    "worker session expired with unknown result command {} for activation {id}",
                    claim
                        .pending_result
                        .as_ref()
                        .expect("prepared result")
                        .command_id(),
                ),
            ));
        }
        if !claim.submitted
            && (now >= permission.session_expiry_ms.saturating_sub(5_000)
                || now >= permission.claim_expiry_ms.saturating_sub(5_000)
                || now >= permission.attempt_deadline_ms)
        {
            claim.cancelled.store(true, Ordering::SeqCst);
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                "handler completed without live reporting permission",
            ));
        }
        let request = claim
            .pending_result
            .as_ref()
            .expect("prepared result")
            .clone();
        claim.submitted = true;
        let (command_id, response) = match request {
            PendingResult::Report(body) => {
                let command_id = body.command_id.clone();
                let mut request = tonic::Request::new(body);
                request.set_timeout(Duration::from_secs(2));
                (
                    command_id,
                    monitor_clock(
                        &self.clock,
                        tokio::time::timeout(Duration::from_secs(2), self.client.report(request)),
                    )
                    .await?,
                )
            }
            PendingResult::Reconcile(body) => {
                let command_id = body.command_id.clone();
                let mut request = tonic::Request::new(body);
                request.set_timeout(Duration::from_secs(2));
                (
                    command_id,
                    monitor_clock(
                        &self.clock,
                        tokio::time::timeout(
                            Duration::from_secs(2),
                            self.client.reconcile(request),
                        ),
                    )
                    .await?,
                )
            }
        };
        let reply = match response {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(err)) if uncertain_rpc(&err) => {
                tracing::warn!(activation = %id, command = %command_id, "worker result uncertain; retrying same command");
                self.rotate_endpoint(&err)?;
                return Ok(());
            }
            Ok(Err(err)) => {
                let error = rpc_error(err);
                return Err(Error::new(
                    error.kind,
                    format!("result command {command_id}: {}", error.message),
                ));
            }
            Err(_) => {
                let err = tonic::Status::deadline_exceeded("worker result response unresolved");
                tracing::warn!(activation = %id, command = %command_id, "worker result timed out; retrying same command");
                self.rotate_endpoint(&err)?;
                return Ok(());
            }
        };
        if !reply.error.is_empty() {
            return Err(Error::new(
                ErrorKind::FailedPrecondition,
                format!("result command {command_id}: {}", reply.error),
            ));
        }
        active.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_and_wall_faults_fence_worker_permission() {
        let start = ClockSample {
            wall_ms: 10_000,
            boot_ms: 10_000,
        };
        assert_eq!(
            clock_fault(
                start,
                ClockSample {
                    wall_ms: 10_500,
                    boot_ms: 10_500,
                },
            ),
            None
        );
        assert_eq!(
            clock_fault(
                start,
                ClockSample {
                    wall_ms: 13_000,
                    boot_ms: 13_000,
                },
            ),
            Some("worker watchdog gap")
        );
        assert_eq!(
            clock_fault(
                start,
                ClockSample {
                    wall_ms: 10_800,
                    boot_ms: 10_100,
                },
            ),
            Some("worker wall/boot clock skew")
        );
        assert_eq!(
            clock_fault(
                start,
                ClockSample {
                    wall_ms: 10_000,
                    boot_ms: 9_999,
                },
            ),
            Some("worker boot clock reversed")
        );
        let guard = ClockGuard {
            last: Mutex::new(ClockSample {
                wall_ms: u64::MAX,
                boot_ms: u64::MAX,
            }),
            safe: AtomicBool::new(true),
        };
        assert!(guard.check().is_err());
        assert!(!guard.safe.load(Ordering::SeqCst));
        assert!(guard.check().is_err());
    }

    #[test]
    fn leader_redirect_requires_https_endpoint_and_tls_name() {
        let mut status = tonic::Status::unavailable("not leader");
        status.metadata_mut().insert(
            "graphrun-leader-endpoint",
            "https://127.0.0.1:7731".parse().unwrap(),
        );
        assert!(leader_redirect(&status).is_err());
        status.metadata_mut().insert(
            "graphrun-leader-server-name",
            "node-2.graphrun.local".parse().unwrap(),
        );
        let target = leader_redirect(&status).unwrap().unwrap();
        assert_eq!(target.url, "https://127.0.0.1:7731");
        assert_eq!(target.server_name, "node-2.graphrun.local");
        status.metadata_mut().insert(
            "graphrun-leader-endpoint",
            "http://127.0.0.1:7731".parse().unwrap(),
        );
        assert!(leader_redirect(&status).is_err());
    }

    #[test]
    fn cancelled_result_timeout_has_unknown_commit_outcome() {
        assert!(uncertain_rpc(&tonic::Status::cancelled("Timeout expired")));
        assert!(uncertain_rpc(&tonic::Status::unknown(
            "transport lost response"
        )));
        assert!(!uncertain_rpc(&tonic::Status::failed_precondition(
            "stale claim"
        )));
    }
}
