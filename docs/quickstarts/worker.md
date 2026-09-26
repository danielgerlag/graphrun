# Run an application worker

An independent Rust process needs an endpoint, a cluster-CA-signed certificate
with a `worker` URI SAN, the same versioned catalog that the member published,
and implementations for the roles it advertises. It does not open a redb file
or join the voter set. `Engine::local` still enables fixture handlers for
samples; remote workers never dispatch fixtures implicitly.

For a multi-member group with separate member DNS names, add each endpoint
with `.seed_with_server_name("https://host:port", "node-2.graphrun.local")`.
Use `.seed("https://host:port")` only when that member's certificate uses the
initial endpoint's server name. The worker retries uncertain commands under
their original IDs when leadership changes.

See [`graphrun/examples/remote_worker.rs`](../../graphrun/examples/remote_worker.rs)
for a complete worker process. A blocking handler uses a bounded blocking
pool; async handlers run on Tokio. Both receive an owned `HandlerContext`:

```rust
let reserve_provider = provider.clone();
let release_provider = provider.clone();
let lookup_provider = provider.clone();
let worker = graphrun::Worker::builder(endpoint, worker_tls, catalog)
    .activity("order.reserve", 3, move |order: Order, ctx| {
        let provider = reserve_provider.clone();
        async move {
            if !ctx.can_start_effect() {
                return Err(graphrun::ActivityError::new(
                    "worker.stopping", "claim is inside its stop margin"
                ));
            }
            let reservation = provider.reserve(order, &ctx.effect_key).await
                .map_err(|err| graphrun::ActivityError::new(
                    "inventory.unavailable", err.to_string()
                ))?;
            Ok(reservation)
        }
    })?
    .compensation("order.release", 2, move |reservation: Reservation, ctx| {
        let provider = release_provider.clone();
        async move {
            provider.release(reservation, &ctx.effect_key).await
                .map_err(|err| graphrun::ActivityError::new(
                    "inventory.release_unavailable", err.to_string()
                ))
        }
    })?
    .reconciler("order.lookup", 1, move |order: Order, ctx| {
        let provider = lookup_provider.clone();
        async move {
            // Query the provider's operation ledger by the original effect key.
            provider.lookup(order, &ctx.effect_key).await
        }
    })?
    .open().await?;
worker.run().await?;
```

The snippet assumes an `Arc`-backed provider client and `Order` and
`Reservation` types implementing `DurablePayload`.
The catalog must declare each activity, its input/output schemas, execution
kind and error codes, and link `order.lookup` to `order.reserve`. The
reconciler returns `Observed::Applied(output)`, `Observed::NotApplied`, or
`Observed::Unknown`; an error is recorded as an unknown probe with its code
and message. For a graceful drain use `worker.run_until(shutdown_signal).await`
instead of `run()`. Shutdown stops claims, asks running handlers to cooperate,
and keeps renewing their leases until they finish or their fixed deadlines
expire. A blocking call cannot be forcibly stopped.

Issue a `worker` certificate with `issue_principal`, using the cluster ID
encoded by the CA used to configure `Engine::member`. The server takes the
principal from the signed URI SAN, checks the request's principal ID against
it, and binds that identity to the live session. It checks the signed worker
role and current session on every operation, before command deduplication.
Use the [worker protocol reference](../worker-protocol.md) to implement a
worker directly against protobuf, including codec, digest, renewal, and
result rules.
