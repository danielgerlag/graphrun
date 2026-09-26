//! Application activity handlers.
//!
//! Register implementations before [`crate::Engine::start`]. Unregistered
//! catalog names fail instead of echoing input.

use crate::clock::ClockFaultState;
use crate::domain::ReconcileOutcome;
use crate::error::{Error, Result};
use crate::ids::{ActivationId, ActivityKey, ExecutionRole, RunId, valid_ascii_name};
use crate::schema::DurablePayload;
use crate::value::Value;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

type BoxFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
type AsyncFn = Arc<dyn Fn(Value) -> BoxFuture + Send + Sync>;
type BlockingFn = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync>;
type ContextAsyncFn = Arc<dyn Fn(Value, LocalHandlerContext) -> BoxFuture + Send + Sync>;
type ContextBlockingFn = Arc<dyn Fn(Value, LocalHandlerContext) -> Result<Value> + Send + Sync>;
type ReconcileFn =
    Arc<dyn Fn(&Value, Option<&str>) -> (ReconcileOutcome, Option<Value>) + Send + Sync>;

#[derive(Clone)]
pub struct Handlers {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    async_handlers: HashMap<(String, u32), AsyncFn>,
    blocking: HashMap<(String, u32), BlockingFn>,
    contextual_async: HashMap<(String, u32), ContextAsyncFn>,
    contextual_blocking: HashMap<(String, u32), ContextBlockingFn>,
    reconcilers: HashMap<(String, u32), ReconcileFn>,
    fixtures: bool,
}

#[derive(Clone)]
pub struct LocalHandlerContext {
    pub run: RunId,
    pub activation: ActivationId,
    pub role: ExecutionRole,
    pub effect_key: String,
    pub attempt_deadline_ms: u64,
    session_expiry_ms: u64,
    claim_expiry_ms: u64,
    fault: watch::Receiver<ClockFaultState>,
    cancelled: watch::Sender<bool>,
    clock_sample: Arc<Mutex<(u64, u64)>>,
}

impl LocalHandlerContext {
    pub(crate) fn new(
        run: RunId,
        activation: ActivationId,
        role: ExecutionRole,
        effect_key: String,
        session_expiry_ms: u64,
        claim_expiry_ms: u64,
        attempt_deadline_ms: u64,
        fault: watch::Receiver<ClockFaultState>,
    ) -> Result<Self> {
        let wall = crate::time::wall_millis()?;
        let boot = crate::time::boot_millis()?;
        let (cancelled, _) = watch::channel(false);
        Ok(Self {
            run,
            activation,
            role,
            effect_key,
            attempt_deadline_ms,
            session_expiry_ms,
            claim_expiry_ms,
            fault,
            cancelled,
            clock_sample: Arc::new(Mutex::new((wall, boot))),
        })
    }

    pub fn is_cancelled(&self) -> bool {
        if matches!(&*self.fault.borrow(), ClockFaultState::Latched { .. }) {
            self.cancelled.send_replace(true);
        }
        *self.cancelled.borrow()
    }

    pub async fn cancelled(&self) {
        let mut fault = self.fault.clone();
        let mut cancelled = self.cancelled.subscribe();
        while !self.is_cancelled() {
            tokio::select! {
                changed = fault.changed() => {
                    if changed.is_err() {
                        self.cancelled.send_replace(true);
                    }
                }
                changed = cancelled.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    pub fn can_start_effect(&self) -> bool {
        if self.is_cancelled() {
            return false;
        }
        let (wall, boot) = match (crate::time::wall_millis(), crate::time::boot_millis()) {
            (Ok(wall), Ok(boot)) => (wall, boot),
            _ => {
                self.cancelled.send_replace(true);
                return false;
            }
        };
        let mut sample = self.clock_sample.lock().expect("local handler clock");
        if crate::time::clock_delta_fault(sample.0, sample.1, wall, boot).is_some() {
            self.cancelled.send_replace(true);
            return false;
        }
        *sample = (wall, boot);
        wall < self.session_expiry_ms.saturating_sub(5_000)
            && wall < self.claim_expiry_ms.saturating_sub(5_000)
            && wall < self.attempt_deadline_ms
    }
}

impl Default for Handlers {
    fn default() -> Self {
        Self::empty()
    }
}

impl Handlers {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                async_handlers: HashMap::new(),
                blocking: HashMap::new(),
                contextual_async: HashMap::new(),
                contextual_blocking: HashMap::new(),
                reconcilers: HashMap::new(),
                fixtures: false,
            })),
        }
    }

    pub fn fixtures() -> Self {
        let handlers = Self::empty();
        handlers.enable_fixtures();
        handlers
    }

    pub fn enable_fixtures(&self) {
        self.inner.write().expect("handlers").fixtures = true;
    }

    pub fn fixtures_enabled(&self) -> bool {
        self.inner.read().expect("handlers").fixtures
    }

    pub fn activity<I, O, F, Fut>(&self, name: &str, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.activity_version(name, 1, handler)
    }

    pub fn activity_version<I, O, F, Fut>(&self, name: &str, version: u32, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        validate_handler_key(name, version)?;
        let handler = Arc::new(handler);
        let wrapped: AsyncFn = Arc::new(move |value: Value| {
            let handler = handler.clone();
            Box::pin(async move {
                let input: I = decode(&value)?;
                let output = handler(input).await?;
                encode(&output)
            })
        });
        self.inner
            .write()
            .expect("handlers")
            .async_handlers
            .insert((name.to_owned(), version), wrapped);
        Ok(())
    }

    pub fn blocking<I, O, F>(&self, name: &str, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I) -> Result<O> + Send + Sync + 'static,
    {
        self.blocking_version(name, 1, handler)
    }

    pub(crate) fn activity_with_context<I, O, F, Fut>(&self, name: &str, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, LocalHandlerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        validate_handler_key(name, 1)?;
        let handler = Arc::new(handler);
        let wrapped: ContextAsyncFn = Arc::new(move |value, context| {
            let handler = handler.clone();
            Box::pin(async move {
                let input: I = decode(&value)?;
                encode(&handler(input, context).await?)
            })
        });
        self.inner
            .write()
            .expect("handlers")
            .contextual_async
            .insert((name.to_owned(), 1), wrapped);
        Ok(())
    }

    pub(crate) fn blocking_with_context<I, O, F>(&self, name: &str, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I, LocalHandlerContext) -> Result<O> + Send + Sync + 'static,
    {
        validate_handler_key(name, 1)?;
        let handler = Arc::new(handler);
        let wrapped: ContextBlockingFn = Arc::new(move |value, context| {
            let input: I = decode(&value)?;
            encode(&handler(input, context)?)
        });
        self.inner
            .write()
            .expect("handlers")
            .contextual_blocking
            .insert((name.to_owned(), 1), wrapped);
        Ok(())
    }

    pub fn blocking_version<I, O, F>(&self, name: &str, version: u32, handler: F) -> Result<()>
    where
        I: DurablePayload,
        O: DurablePayload,
        F: Fn(I) -> Result<O> + Send + Sync + 'static,
    {
        validate_handler_key(name, version)?;
        let handler = Arc::new(handler);
        let wrapped: BlockingFn = Arc::new(move |value: Value| {
            let input: I = decode(&value)?;
            encode(&handler(input)?)
        });
        self.inner
            .write()
            .expect("handlers")
            .blocking
            .insert((name.to_owned(), version), wrapped);
        Ok(())
    }

    pub fn reconciler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&Value, Option<&str>) -> (ReconcileOutcome, Option<Value>) + Send + Sync + 'static,
    {
        self.reconciler_version(name, 1, handler)
            .expect("valid version-1 reconciler name");
    }

    pub fn reconciler_version<F>(&self, name: &str, version: u32, handler: F) -> Result<()>
    where
        F: Fn(&Value, Option<&str>) -> (ReconcileOutcome, Option<Value>) + Send + Sync + 'static,
    {
        validate_handler_key(name, version)?;
        self.inner
            .write()
            .expect("handlers")
            .reconcilers
            .insert((name.to_owned(), version), Arc::new(handler));
        Ok(())
    }

    /// Whether a runnable activity is registered for the legacy local-engine path.
    pub fn provides(&self, key: &ActivityKey) -> bool {
        self.provides_role(key, ExecutionRole::Forward)
    }

    /// Compensation uses the same activity implementation as forward execution.
    /// Reconcilers must be registered independently.
    pub fn provides_role(&self, key: &ActivityKey, role: ExecutionRole) -> bool {
        let inner = self.inner.read().expect("handlers");
        let k = (key.name.clone(), key.version);
        match role {
            ExecutionRole::Forward | ExecutionRole::Compensation => {
                inner.async_handlers.contains_key(&k)
                    || inner.blocking.contains_key(&k)
                    || inner.contextual_async.contains_key(&k)
                    || inner.contextual_blocking.contains_key(&k)
                    || (inner.fixtures && key.version == 1 && is_fixture_activity(&key.name))
            }
            ExecutionRole::Reconciliation => {
                inner.reconcilers.contains_key(&k)
                    || (inner.fixtures && key.version == 1 && is_fixture_reconciler(&key.name))
            }
        }
    }

    /// Enumerate exact runnable (role, name/version) pairs; never advertise a wildcard.
    pub fn capabilities(&self) -> Vec<(ExecutionRole, ActivityKey)> {
        let inner = self.inner.read().expect("handlers");
        let mut activities: BTreeSet<_> = inner
            .async_handlers
            .keys()
            .chain(inner.blocking.keys())
            .chain(inner.contextual_async.keys())
            .chain(inner.contextual_blocking.keys())
            .map(|(name, version)| ActivityKey::new(name.clone(), *version))
            .collect();
        let mut reconcilers: BTreeSet<_> = inner
            .reconcilers
            .keys()
            .map(|(name, version)| ActivityKey::new(name.clone(), *version))
            .collect();
        if inner.fixtures {
            activities.extend(
                FIXTURE_ACTIVITIES
                    .iter()
                    .map(|name| ActivityKey::new(*name, 1)),
            );
            reconcilers.extend(
                FIXTURE_RECONCILERS
                    .iter()
                    .map(|name| ActivityKey::new(*name, 1)),
            );
        }
        let mut result = Vec::new();
        for key in activities {
            result.push((ExecutionRole::Forward, key.clone()));
            result.push((ExecutionRole::Compensation, key));
        }
        result.extend(
            reconcilers
                .into_iter()
                .map(|key| (ExecutionRole::Reconciliation, key)),
        );
        result
    }

    pub fn require_all(&self, keys: &[ActivityKey]) -> Result<()> {
        for key in keys {
            if !self.provides(key) {
                return Err(Error::invalid(format!(
                    "activity.unregistered: {}/v{}",
                    key.name, key.version
                )));
            }
        }
        Ok(())
    }

    pub fn run_blocking(
        &self,
        name: &str,
        version: u32,
        input: Value,
        effect_key: Option<&str>,
    ) -> Result<Value> {
        let key = (name.to_owned(), version);
        let inner = self.inner.read().expect("handlers");
        if let Some(handler) = inner.blocking.get(&key).cloned() {
            drop(inner);
            return handler(input);
        }
        let fixtures = inner.fixtures;
        drop(inner);
        if fixtures && version == 1 && is_fixture_activity(name) {
            return crate::engine::dispatch_handler(name, &input, effect_key);
        }
        Err(Error::invalid(format!(
            "activity.unregistered: {name}/v{version}"
        )))
    }

    pub(crate) fn run_blocking_with_context(
        &self,
        name: &str,
        version: u32,
        input: Value,
        context: LocalHandlerContext,
    ) -> Result<Value> {
        let handler = self
            .inner
            .read()
            .expect("handlers")
            .contextual_blocking
            .get(&(name.to_owned(), version))
            .cloned();
        if let Some(handler) = handler {
            return handler(input, context);
        }
        self.run_blocking(name, version, input, Some(&context.effect_key))
    }

    pub(crate) async fn run_with_context(
        &self,
        name: &str,
        version: u32,
        input: Value,
        context: LocalHandlerContext,
    ) -> Result<Value> {
        let handler = self
            .inner
            .read()
            .expect("handlers")
            .contextual_async
            .get(&(name.to_owned(), version))
            .cloned();
        if let Some(handler) = handler {
            return handler(input, context).await;
        }
        self.run(name, version, input, Some(&context.effect_key), false)
            .await
    }

    pub async fn run(
        &self,
        name: &str,
        version: u32,
        input: Value,
        effect_key: Option<&str>,
        blocking: bool,
    ) -> Result<Value> {
        let key = (name.to_owned(), version);
        let (async_h, blocking_h, fixtures) = {
            let inner = self.inner.read().expect("handlers");
            (
                inner.async_handlers.get(&key).cloned(),
                inner.blocking.get(&key).cloned(),
                inner.fixtures,
            )
        };
        if blocking {
            if let Some(handler) = blocking_h {
                return handler(input);
            }
        } else if let Some(handler) = async_h {
            return handler(input).await;
        } else if let Some(handler) = blocking_h {
            return handler(input);
        }
        if fixtures && version == 1 && is_fixture_activity(name) {
            return crate::engine::dispatch_handler(name, &input, effect_key);
        }
        Err(Error::invalid(format!(
            "activity.unregistered: {name}/v{version}"
        )))
    }

    pub fn reconcile(
        &self,
        name: &str,
        input: &Value,
        effect_key: Option<&str>,
    ) -> (ReconcileOutcome, Option<Value>) {
        self.reconcile_version(name, 1, input, effect_key)
    }

    pub fn reconcile_version(
        &self,
        name: &str,
        version: u32,
        input: &Value,
        effect_key: Option<&str>,
    ) -> (ReconcileOutcome, Option<Value>) {
        let inner = self.inner.read().expect("handlers");
        if let Some(handler) = inner.reconcilers.get(&(name.to_owned(), version)).cloned() {
            drop(inner);
            return handler(input, effect_key);
        }
        let fixtures = inner.fixtures;
        drop(inner);
        if fixtures && version == 1 && is_fixture_reconciler(name) {
            return crate::engine::builtin_reconcile(name, input, effect_key);
        }
        (ReconcileOutcome::Unknown, None)
    }
}

fn validate_handler_key(name: &str, version: u32) -> Result<()> {
    if !valid_ascii_name(name) {
        return Err(Error::invalid(format!("invalid handler name {name}")));
    }
    if version == 0 {
        return Err(Error::invalid("handler version must be positive"));
    }
    Ok(())
}

const FIXTURE_ACTIVITIES: &[&str] = &[
    "counter.increment",
    "inventory.reserve",
    "inventory.release",
    "payment.charge",
    "payment.refund",
    "tax.quote",
    "shipping.quote",
    "remote.echo",
    "test.gate",
    "test.manual",
    "test.block",
];

const FIXTURE_RECONCILERS: &[&str] = &["test.lookup", "inventory.lookup", "payment.lookup"];

fn is_fixture_activity(name: &str) -> bool {
    FIXTURE_ACTIVITIES.contains(&name)
}

fn is_fixture_reconciler(name: &str) -> bool {
    FIXTURE_RECONCILERS.contains(&name)
}

pub fn is_fixture(name: &str) -> bool {
    is_fixture_activity(name) || is_fixture_reconciler(name) || name == "test.flaky"
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    let json = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    serde_json::from_value(json).map_err(|err| Error::invalid(format!("handler input: {err}")))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value> {
    let json = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    Value::from_json(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_exact_and_role_specific() {
        let handlers = Handlers::empty();
        assert!(handlers.capabilities().is_empty());

        handlers
            .activity_version("shared", 1, |input: i64| async move { Ok(input + 1) })
            .unwrap();
        handlers
            .blocking_version("shared", 1, |input: i64| Ok(input + 10))
            .unwrap();
        handlers
            .blocking_version("shared", 2, |input: i64| Ok(input + 20))
            .unwrap();
        handlers
            .reconciler_version("shared", 2, |_, _| (ReconcileOutcome::NotApplied, None))
            .unwrap();

        let v1 = ActivityKey::new("shared", 1);
        let v2 = ActivityKey::new("shared", 2);
        let v3 = ActivityKey::new("shared", 3);
        assert_eq!(
            handlers.capabilities(),
            vec![
                (ExecutionRole::Forward, v1.clone()),
                (ExecutionRole::Compensation, v1.clone()),
                (ExecutionRole::Forward, v2.clone()),
                (ExecutionRole::Compensation, v2.clone()),
                (ExecutionRole::Reconciliation, v2.clone()),
            ]
        );
        assert!(handlers.provides(&v1));
        assert!(handlers.provides_role(&v2, ExecutionRole::Compensation));
        assert!(handlers.provides_role(&v2, ExecutionRole::Reconciliation));
        assert!(!handlers.provides_role(&v1, ExecutionRole::Reconciliation));
        for role in [
            ExecutionRole::Forward,
            ExecutionRole::Compensation,
            ExecutionRole::Reconciliation,
        ] {
            assert!(!handlers.provides_role(&v3, role));
        }
        assert!(!handlers.provides(&ActivityKey::new("*", 1)));
    }

    #[test]
    fn invalid_handler_identities_are_not_registered() {
        let handlers = Handlers::empty();
        assert!(
            handlers
                .activity_version("", 1, |input: i64| async move { Ok(input) })
                .is_err()
        );
        assert!(
            handlers
                .blocking_version("bad name", 1, |input: i64| Ok(input))
                .is_err()
        );
        assert!(
            handlers
                .activity_version("valid", 0, |input: i64| async move { Ok(input) })
                .is_err()
        );
        assert!(
            handlers
                .blocking_version("valid", 0, |input: i64| Ok(input))
                .is_err()
        );
        assert!(
            handlers
                .reconciler_version("recon", 0, |_, _| (ReconcileOutcome::Unknown, None))
                .is_err()
        );
        assert!(
            handlers
                .reconciler_version("bad/name", 2, |_, _| (ReconcileOutcome::Unknown, None))
                .is_err()
        );
        assert!(handlers.capabilities().is_empty());
    }

    #[tokio::test]
    async fn dispatch_respects_versions_and_existing_v1_wrappers() {
        let handlers = Handlers::empty();
        handlers
            .activity("async", |input: i64| async move { Ok(input + 1) })
            .unwrap();
        handlers
            .activity_version("async", 2, |input: i64| async move { Ok(input + 2) })
            .unwrap();
        handlers
            .blocking("blocking", |input: i64| Ok(input + 10))
            .unwrap();
        handlers
            .blocking_version("blocking", 2, |input: i64| Ok(input + 20))
            .unwrap();

        for (name, version, expected) in [
            ("async", 1, 4),
            ("async", 2, 5),
            ("blocking", 1, 13),
            ("blocking", 2, 23),
        ] {
            assert_eq!(
                handlers
                    .run(name, version, Value::Int(3), None, false)
                    .await
                    .unwrap(),
                Value::Int(expected)
            );
        }
        assert_eq!(
            handlers
                .run_blocking("blocking", 2, Value::Int(3), None)
                .unwrap(),
            Value::Int(23)
        );
        assert!(
            handlers
                .run("async", 3, Value::Int(3), None, false)
                .await
                .is_err()
        );
        assert!(
            handlers
                .run_blocking("blocking", 3, Value::Int(3), None)
                .is_err()
        );
        assert!(
            handlers
                .run_blocking("async", 1, Value::Int(3), None)
                .is_err()
        );
    }

    #[test]
    fn reconciliation_dispatch_uses_its_own_versioned_registry() {
        let handlers = Handlers::empty();
        handlers.reconciler("recon", |_, _| (ReconcileOutcome::NotApplied, None));
        handlers
            .reconciler_version("recon", 2, |input, effect_key| {
                assert_eq!(effect_key, Some("effect"));
                (ReconcileOutcome::Applied, Some(input.clone()))
            })
            .unwrap();

        let input = Value::Int(7);
        assert_eq!(
            handlers.reconcile("recon", &input, None),
            (ReconcileOutcome::NotApplied, None)
        );
        assert_eq!(
            handlers.reconcile_version("recon", 2, &input, Some("effect")),
            (ReconcileOutcome::Applied, Some(input.clone()))
        );
        assert_eq!(
            handlers.reconcile_version("recon", 3, &input, None),
            (ReconcileOutcome::Unknown, None)
        );
        assert!(!handlers.provides(&ActivityKey::new("recon", 2)));
        assert!(
            handlers.provides_role(&ActivityKey::new("recon", 2), ExecutionRole::Reconciliation)
        );
    }

    #[tokio::test]
    async fn fixtures_are_only_advertised_at_v1_for_runnable_roles() {
        let handlers = Handlers::fixtures();
        let forward = ActivityKey::new("remote.echo", 1);
        let lookup = ActivityKey::new("test.lookup", 1);
        assert!(handlers.provides_role(&forward, ExecutionRole::Forward));
        assert!(handlers.provides_role(&forward, ExecutionRole::Compensation));
        assert!(!handlers.provides_role(&forward, ExecutionRole::Reconciliation));
        assert!(handlers.provides_role(&lookup, ExecutionRole::Reconciliation));
        assert!(!handlers.provides(&lookup));
        assert!(!handlers.provides(&ActivityKey::new("remote.echo", 2)));
        assert!(!handlers.provides_role(
            &ActivityKey::new("test.lookup", 2),
            ExecutionRole::Reconciliation
        ));
        assert!(
            !handlers
                .capabilities()
                .contains(&(ExecutionRole::Forward, ActivityKey::new("test.flaky", 1)))
        );
        assert!(
            handlers
                .capabilities()
                .contains(&(ExecutionRole::Reconciliation, lookup))
        );
        assert_eq!(
            handlers
                .run("remote.echo", 1, Value::Int(8), None, false)
                .await
                .unwrap(),
            Value::Int(8)
        );
        assert!(
            handlers
                .run("remote.echo", 2, Value::Int(8), None, false)
                .await
                .is_err()
        );
        assert_eq!(
            handlers.reconcile_version("test.lookup", 1, &Value::Int(8), None),
            (ReconcileOutcome::Applied, Some(Value::Int(8)))
        );
        assert_eq!(
            handlers.reconcile_version("test.lookup", 2, &Value::Int(8), None),
            (ReconcileOutcome::Unknown, None)
        );
    }

    #[tokio::test]
    async fn contextual_handler_cancellation_stays_latched_after_acknowledgement() {
        let handlers = Handlers::empty();
        handlers
            .activity_with_context(
                "waiting",
                |input: i64, context: LocalHandlerContext| async move {
                    context.cancelled().await;
                    Ok(input)
                },
            )
            .unwrap();
        let (fault, receiver) = watch::channel(ClockFaultState::Healthy);
        let now = crate::time::wall_millis().unwrap();
        let context = LocalHandlerContext::new(
            RunId::generate(),
            ActivationId::generate(),
            ExecutionRole::Forward,
            "effect".to_owned(),
            now + 60_000,
            now + 60_000,
            now + 60_000,
            receiver,
        )
        .unwrap();
        assert!(context.can_start_effect());
        let waiting = tokio::spawn({
            let handlers = handlers.clone();
            let context = context.clone();
            async move {
                handlers
                    .run_with_context("waiting", 1, Value::Int(5), context)
                    .await
            }
        });
        fault.send_replace(ClockFaultState::Latched {
            reason: "test rollback".to_owned(),
        });
        assert_eq!(waiting.await.unwrap().unwrap(), Value::Int(5));
        fault.send_replace(ClockFaultState::Healthy);
        assert!(context.is_cancelled());
        assert!(!context.can_start_effect());
    }
}
