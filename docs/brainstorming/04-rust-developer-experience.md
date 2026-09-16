# A Rust developer experience

Historical API notes. [The mandatory v1 Rust builder specification](../specs/v1/05-rust-builder.md) supersedes earlier statements excluding the builder.

YAML is the v1 workflow-authoring format. Rust supplies typed async activities. A public Rust graph builder is outside v1; a later frontend must use the same persisted graph.

The [v1 baseline](10-v1-design-baseline.md) fixes the supported API roles and behavior. There is no `graphrun` implementation yet. The snippets omit application types and are not runnable examples.

## One graph, distinct authoring guarantees

| Authoring path | What survives a restart | Developer experience | Validation boundary |
|---|---|---|---|
| YAML definition with registered activities | The normalized graph, activations, outputs, waits, and pinned versions | Edit workflow structure without expressing it as a Rust type | Graph and schema checks at load time, actual payload decoding at runtime |

Use a state machine inside the engine. Do not create a second interpreter with different retries or persistence for YAML-authored workflows.

Do not promise that an arbitrary `async fn` can pause across process restarts. A native future can contain borrowed data, sockets, locks, and compiler-generated state. Rust does not supply a general durable serialization protocol for it.

Event-based reconstruction of workflow state is part of the selected design. It is not deterministic replay of arbitrary async Rust code. Native activities still execute only through the live claim and result protocol.

## Typed activities with owned inputs

An activity consumes an owned input and returns an owned output or a typed error. Persisted inputs and outputs have explicit codecs and schema versions.

Keep service clients outside persisted data. Capture an `Arc` to application services in an activity handler. Do not require a global dependency-injection container or resolve a handler from a type-name string.

One possible registration style is the following.

```rust
let reserve = activity_fn(
	"inventory.reserve",
	1,
	RecoveryPolicy::RetrySafe,
	move |ctx: ActivityContext, order: Order| {
		let inventory = Arc::clone(&inventory);
		async move {
			inventory.reserve(order, ctx.idempotency_key()).await
		}
	},
);
```

In this sketch, `reserve` maps `Order` to `ReservedOrder`. Another registration, `payment.charge` version 1, maps `ReservedOrder` to `Receipt`.

Registration exposes stable input and output schema identities and declared error codes to the activity catalog. The [YAML example](examples/fulfillment.yaml) refers to those contracts, not Rust type names.

Rust activity implementations have compiler-checked input/output types. YAML connections are checked when the definition is loaded against the catalog. Actual data is checked at execution boundaries, and neither mechanism proves external idempotency.

Recovery policy is mandatory at registration. `RetrySafe` permits bounded recovery of an ambiguous attempt; `Manual` requires intervention. The application also supplies the static error-code catalog and typed classifier, omitted from this snippet.

Function registration is the default API. The `Activity` trait supports reusable implementations with the same contract and recovery metadata.

## Async traits need deliberate bounds

For a multi-threaded Tokio runtime, activity futures must be `Send`. Registration also needs owned data and handler lifetimes that can outlive the caller's stack.

The reusable activity trait has the following shape.

```rust
pub trait Activity: Send + Sync + 'static {
	type Input: DurablePayload + Send + 'static;
	type Output: DurablePayload + Send + 'static;
	type Error: std::error::Error + Send + Sync + 'static;

	fn execute(
		&self,
		context: ActivityContext,
		input: Self::Input,
	) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
}
```

`DurablePayload` is the boundary contract for owned Serde encoding/decoding, stable schema identity, and Schemars metadata. It is not a marker that makes every arbitrary Rust value safe to persist.

YAML-facing structured bindings need machine-readable payload contracts that agree with the Rust codecs. The initial proposal uses JSON-compatible data for these bindings. Keep schema identities stable and do not infer them from Rust type names.

Do not require ordinary Rust errors to implement `Serialize`. Errors often contain an I/O error or a client-library error. An explicit classifier converts the activity error into a versioned, redacted durable failure record and a retry decision.

Preserve the source-error chain in the live process. After restart, report the durable failure record rather than pretending to reconstruct the original error object.

The trait is intended for generic registration, not as `dyn Activity`. A heterogeneous internal registry can erase payload types and box futures after registration validates the binding.

Do not force every user to manipulate `Pin<Box<dyn Future<...>>>` merely because the registry eventually needs dynamic dispatch.

The Rust team's [async-trait guidance](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/) explains the `Send` and dynamic-dispatch distinctions. The baseline owns the toolchain and dependency versions.

## Public types should encode different meanings

Use newtypes for `ClusterId`, `MemberId`, `CommandId`, `RunId`, `RunSequence`, `EventVersion`, `ActivationId`, `AttemptId`, `WorkerSessionId`, `NodeKey`, `StartKey`, `SignalId`, and the external `IdempotencyKey`.

An ownership token must not be interchangeable with an idempotency key. A workflow version must not be confused with a payload schema version.

A persistent storage member identity is not a worker session. Restart preserves the former and creates a new instance of the latter. Neither is a Raft term or an activity ownership generation.

A workflow event is identified by its run and sequence. That sequence is not a Raft log position. Public history and query types must not expose OpenRaft's internal Rust types as their compatibility contract.

`ActivityContext` exposes the stable effect key, attempt identity for diagnostics, the deadline, and cooperative cancellation. It does not expose mutable engine state or a storage transaction held across user code.

Use enums for meaningful alternatives in activity outcomes, waits, and run status. Keep fields such as a completion timestamp inside the variant that requires them.

Avoid a public outcome value that simultaneously permits sleep, completion, event subscription, and branch creation. Separate effectful activity results from engine control-node decisions.

Internal payload erasure is acceptable at a validated storage or registry boundary. Requiring application authors to downcast `Any` or navigate `serde_json::Value` for normal workflow composition is not.

## Composition follows Rust ownership

Represent input mappings as declarative YAML bindings that compile into serializable, side-effect-free graph data. An arbitrary Rust closure cannot implement a published graph operation.

Use named activities for complex transformations. Do not hide functions in a process-local registry of graph closures.

V1 executes one active path per run in a finite DAG. Parallel branches, joins, and loops are outside v1. Many independent runs still execute concurrently across workers.

Do not introduce `Arc<Mutex<WorkflowData>>` as shared workflow state. Activity inputs and outputs remain owned durable values.

Durable sleep and signal waits are graph operations. Calling `tokio::time::sleep` inside an activity is allowed Rust, but it only delays that attempt in the current process.

## Runtime ownership is explicit

The application supplies a Tokio runtime and owns its shutdown policy. The library does not create a hidden runtime or install process signal handlers.

The lifecycle API separates a clonable client from a supervised runtime. Named constructors select a local group, data-bearing member, or worker-only connection.

The default development example opens local storage and loads a YAML file at runtime. All names remain API sketches, not existing methods.

```rust
let mut builder = Engine::local("./.graphrun/dev");
builder.register_activity(reserve)?;
builder.register_activity(charge)?;

let source = tokio::fs::read_to_string("workflows/fulfillment.yaml").await?;
let definition = builder.compile_yaml("workflows/fulfillment.yaml", &source)?;
let fulfillment = builder.register_definition::<Order, Receipt>(definition)?;
let (client, runtime) = builder.build().await?;

let task = tokio::spawn(runtime.run(shutdown.clone()));

let run = client
	.start(&fulfillment, StartKey::new("order-482"), order)
	.await?;

shutdown.cancel();
task.await??;
```

`Engine::local(data_dir)` combines a durable single-member group and worker runtime. `Engine::member(member_config)` is storage-only by default, and `Engine::worker(cluster_config)` is worker-only. Co-locating activities on a member requires explicit configuration.

Worker-only processes do not need a local redb replica or voter membership. In a cluster, an admin publishes contract metadata and definitions; workers register matching Rust implementations and advertise their capabilities. Registration alone does not grant publication authority.

The generic registration method compares the YAML definition's declared input and output schemas with `Order` and `Receipt` before returning a typed handle. It is a fallible runtime binding, not a cast or compile-time proof about YAML.

Dynamic clients can use schema-validated data at an explicit API boundary. Normal Rust activity implementations still receive their declared input types.

A later process can restore access to the same persisted definition without restoring the old handle or possessing the original YAML file. It needs compatible graph, activity, and codec implementations.

Member construction opens persistent storage and validates identity, membership, and format compatibility. Bootstrap and joining are explicit operations. Reopening an existing directory does not initialize a new group.

The client acknowledges `start()` after quorum commitment and durable application at the serving leader, not after an activity starts. Local mode follows the same rule with one voter. There is no direct-redb execution shortcut.

Requests can redirect to a new leader or finish with an unknown outcome. Preserve `CommandId` and business idempotency keys across those retries. An RPC reconnect must not create another logical command accidentally.

`run()` supervises the configured storage, consensus, scheduling, and worker roles. Readiness must distinguish a recovered member from a group able to commit.

Cancelling `shutdown` drains local activities and stops the configured runtime. It does not cancel runs or remove a voter from membership. Stopping a data-bearing member can reduce quorum availability. Local restart uses the same data directory.

Dropping a Rust handle cannot synchronously guarantee a durable shutdown. A runtime task failure must be observable by the host. A background task silently disappearing is not an acceptable lifecycle API.

Bound admission channels and return meaningful overload errors. Tokio's [bounded channels](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html) provide backpressure, but a database backlog needs its own limit.

## Commands and queries have separate contracts

Commands such as admission, signal delivery, and activity completion change state through Raft. Queries return run descriptions, history, and summaries without changing execution.

Start with eager read models in the same redb apply transaction. Preserve authoritative query consistency through the leader's read protocol rather than treating CQRS as a requirement for eventual consistency.

History pagination uses a run sequence and reports the available range. Reporting models remain eager and authoritative in v1.

The engine generates lifecycle events automatically. Application developers do not have to declare an event stream in YAML or convert their own application into an event-sourced system.

Retain exact bound activity inputs, accepted outcomes, signal data, and definition references behind the declared payload contracts. Default logs should still omit payloads.

Historical reconstruction uses a pure event projector with no live command or activity capability. Do not add an `is_replay` flag to `ActivityContext` and ask every handler to avoid side effects.

## The daily local loop needs no service

After the normal Rust build, running the local example should start only the application. It should not require Docker, download database server binaries, or create a background database process.

Persist runs, outputs, inboxes, deadlines, and Raft recovery state in the selected directory. The local CLI uses the application's private Unix-domain control socket for live inspection and command operations. No extra server process is required.

Runtime YAML loading must be supported. `include_str!` may be a convenience, but must not be the only way to supply a definition. A workflow-only change using existing activities can publish without rebuilding the Rust handlers.

On YAML reload, compile and publish a new immutable version. Existing runs keep their stored graph. A conflicting edit under the same version reports an error and preserves the published definition.

Show the active role, cluster and member identities, and resolved data directory without logging workflow payloads. Report a second process opening the same member directory clearly.

Memory-only storage is explicit and separate from local development. A controlled clock and fake activities belong in the test runtime, not in hidden production behavior.

Changing constructors does not rewrite persisted membership. Expanding a local group uses an explicit membership procedure. An unavailable clustered group must never turn into standalone mode automatically.

The [backend comparison](07-durable-backends-and-local-development.md) defines which semantics are shared and which still require real clustered scenarios.

## Error handling and versioning

Separate activity errors, payload decoding errors, invalid definitions, storage failures, ownership conflicts, and runtime shutdown.

Do not erase every failure into `Box<dyn Error>` at the public boundary. Preserve structured categories and source errors. An optional convenience adapter can support applications that choose dynamic errors.

Retries belong to explicit error classifications. Avoid a blanket policy that retries every error, including a permanently incompatible payload.

Use explicit stable names for workflows and activities. `type_name::<T>()`, Rust `TypeId`, and compiler symbol names are not durable wire identifiers.

Changing Serde field names, enum representation, or a codec can break old payloads. A derive does not replace a migration policy.

Keep definitions immutable per version. Workers claim only supported graph formats, activity versions, and codecs. A local missing handler is not a reason to stop a run that a peer can execute. Rolling deployments must preserve compatible capacity for old runs.

Retained event, checkpoint, and payload formats also need compatible readers. Event evolution applies recorded decisions rather than rerunning current workflow conditions or activity code.

## Testing should not require waiting

Offer a test runtime with a controlled clock, fake activities, deterministic identifiers, and explicit stepping until quiescence.

Use memory for isolated domain tests and a durable one-member redb/OpenRaft runtime for the normal local loop. The one-member path still performs log commitment and durable application.

Changes to log storage, application, snapshots, claims, notification streams, or cluster recovery require multi-member scenarios with independent processes. A one-voter quorum cannot exercise partitions or competing leadership.

Record authoritative versioned workflow events. Reconstruct logical state from history and retained artifacts and compare it with the eagerly materialized state. Renewal-only coordination must not be mistaken for fully replayable workflow history.

Reconstruction must work without an activity registry, command sender, or scheduler. A historical `Ready` state is data, not permission to execute work.

A useful authoring scenario loads YAML for a sequential workflow and a signal with timeout and choice. Parallel work and a Rust graph builder are outside v1.

## Avoid these early commitments

Start with domain decisions and event evolution, redb storage, consensus, query models, and the worker protocol. These are responsibilities within the selected implementation, not a requirement for separate services or crates.

Do not add SQL adapters, a public backend plugin system, or a separate local executor to preserve the superseded design. Local and clustered modes differ in membership and role configuration, not workflow semantics.

YAML support is mandatory. Procedural macros, runtime reflection, arbitrary expression-string evaluation, and a plugin ABI are not. Keep initial conditions and bindings declarative and bounded.

Do not claim `no_std`, every async runtime, every database, or arbitrary workflow-code hot reload. Each adds constraints that do not follow from being an embedded library.
