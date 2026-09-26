//! Application activity handlers.
//!
//! Register implementations before [`crate::Engine::start`]. Unregistered
//! catalog names fail instead of echoing input.

use crate::domain::ReconcileOutcome;
use crate::error::{Error, Result};
use crate::ids::ActivityKey;
use crate::schema::DurablePayload;
use crate::value::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

type BoxFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
type AsyncFn = Arc<dyn Fn(Value) -> BoxFuture + Send + Sync>;
type BlockingFn = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync>;
type ReconcileFn =
    Arc<dyn Fn(&Value, Option<&str>) -> (ReconcileOutcome, Option<Value>) + Send + Sync>;

#[derive(Clone)]
pub struct Handlers {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    async_handlers: HashMap<(String, u32), AsyncFn>,
    blocking: HashMap<(String, u32), BlockingFn>,
    reconcilers: HashMap<(String, u32), ReconcileFn>,
    fixtures: bool,
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
        let handler = Arc::new(handler);
        let wrapped: BlockingFn = Arc::new(move |value: Value| {
            let input: I = decode(&value)?;
            encode(&handler(input)?)
        });
        self.inner
            .write()
            .expect("handlers")
            .blocking
            .insert((name.to_owned(), 1), wrapped);
        Ok(())
    }

    pub fn reconciler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&Value, Option<&str>) -> (ReconcileOutcome, Option<Value>) + Send + Sync + 'static,
    {
        self.inner
            .write()
            .expect("handlers")
            .reconcilers
            .insert((name.to_owned(), 1), Arc::new(handler));
    }

    pub fn provides(&self, key: &ActivityKey) -> bool {
        let inner = self.inner.read().expect("handlers");
        let k = (key.name.clone(), key.version);
        inner.async_handlers.contains_key(&k)
            || inner.blocking.contains_key(&k)
            || (inner.fixtures && is_fixture(&key.name))
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
        if fixtures {
            return crate::engine::dispatch_handler(name, &input, effect_key);
        }
        Err(Error::invalid(format!(
            "activity.unregistered: {name}/v{version}"
        )))
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
        if fixtures {
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
        let inner = self.inner.read().expect("handlers");
        if let Some(handler) = inner.reconcilers.get(&(name.to_owned(), 1)).cloned() {
            drop(inner);
            return handler(input, effect_key);
        }
        let fixtures = inner.fixtures;
        drop(inner);
        if fixtures {
            return crate::engine::builtin_reconcile(name, input, effect_key);
        }
        (ReconcileOutcome::Unknown, None)
    }
}

pub fn is_fixture(name: &str) -> bool {
    matches!(
        name,
        "counter.increment"
            | "inventory.reserve"
            | "inventory.release"
            | "payment.charge"
            | "payment.refund"
            | "tax.quote"
            | "shipping.quote"
            | "remote.echo"
            | "test.gate"
            | "test.manual"
            | "test.flaky"
            | "test.block"
            | "test.lookup"
            | "inventory.lookup"
            | "payment.lookup"
    )
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    let json = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    serde_json::from_value(json).map_err(|err| Error::invalid(format!("handler input: {err}")))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value> {
    let json = serde_json::to_value(value).map_err(|err| Error::invalid(err.to_string()))?;
    Value::from_json(json)
}
