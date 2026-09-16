# API, worker protocol, and operations

The public API exposes domain operations, not mutable database records or OpenRaft implementation types.

## Runtime roles and packaging

Create a Cargo workspace with:

| Package | Purpose |
|---|---|
| `graphrun` | Public library, domain/compiler/builder/runtime/storage/transport modules |
| `graphrun-cli` | Production `graphrun` executable |
| `graphrun-e2e` | Non-published verification driver and fixture subcommands |

The library uses the host's Tokio runtime and does not install process signal handlers.

`Engine::local(data_dir)` combines one durable member and worker execution. It also offers a private owner-only Unix control socket for CLI interaction.

`Engine::member(config)` is storage-only by default. Hosting activities is explicit. `Engine::worker(config)` connects without opening a data-bearing replica.

Local mode runs the real Raft commit/apply path. No database daemon, broker, container, certificate service, or runtime binary download is required.

Linux and macOS are release targets. Ordinary builds use checked-in generated protobuf Rust and do not require `protoc`.

## Protocol and authorization

Use Tonic gRPC/protobuf over HTTP/2. Cluster connections require mutual TLS with rustls and explicit seed addresses.

Bind certificates to cluster and principal identity. Member RPCs must additionally match the committed member roster. Authenticate and authorize before cache lookup or returning a deduplicated result.

| Role | Operations |
|---|---|
| Member | Raft, snapshots, and clock health for the roster |
| Worker | Required artifacts/contracts, session/capability advertisement, claim/renew/report/reconcile for its assignments |
| Client | Start, signal, cancel, inspect, list, and read history within the trust domain |
| Admin | Publish contracts/definitions, policy and membership operations, intervention, clock acknowledgement, backup and restore |

Roles combine explicitly. V1 is one trust domain, not hostile multi-tenancy.

Local control uses filesystem ownership rather than local TLS setup. Production volumes and exported backups must be protected at rest; the engine does not implement internal redb encryption.

## Commands and queries

The implementation must expose equivalent SDK and CLI behavior for these operations.

| Operation | Required semantics |
|---|---|
| Publish contracts | Immutable key/version plus schemas, execution/effect/recovery metadata, and optional reconciliation contract |
| Publish definition | Compile YAML or builder IR, enforce immutable digest/version, and commit the artifact |
| Start | Resolve explicit version or `latest` once, deduplicate by workflow/start key, and pin the definition |
| Signal | Accept a directed external event by run, declared name, correlation key, and event ID |
| Cancel | Persist cancellation intent and perform the specified scoped settlement/compensation behavior |
| Get/list | Linearizable current views with explicit status, scopes, waits, and compensation progress |
| History | Paginate by per-run event sequence and report retained range |
| Reconstruct | Pure read-only historical domain projection |
| Resolve intervention | Version-checked observed outcome, blocked compensation input, retry, safe continuation, failure/cancel, or compensation-abandonment action |
| Cluster/admin | Health, join/promote/remove, format activation, clock acknowledgement, backup/restore |

Command IDs are created once per logical operation and reused across retries/redirects. Scope deduplication by authenticated principal and command ID.

Start keys are case-sensitive nonempty UTF-8 strings, maximum 128 bytes, scoped by workflow name across versions. Look up an existing key before resolving `latest`.

Matching input and a compatible requested version return the original run. A different input or explicitly different pinned version is a conflict. Do not create another business run merely because `latest` changed.

Default RPC deadline is five seconds, renewal two seconds, and snapshot operations use their storage deadlines. The SDK retries transient transport failures for at most five minutes with 250 ms to five-second bounded jitter.

Timeout is not proof of non-commit. Return command identity and unknown-outcome detail when appropriate.

## Worker assignments

The language-neutral worker protocol carries:

| Message | Required contents |
|---|---|
| Session registration | Fresh worker session ID, authenticated principal, supported activity/codec versions, execution kinds and capacity |
| Readiness subscription | Capability filter, last observed generation/cursor, reconnect/resync response |
| Claim request | Stable command ID, session, bounded capacity request |
| Assignment | Run/scope/activation/attempt IDs, execution role, activity key/version, immutable input reference/data, effect key, claim generation/revision, deadlines |
| Renewal | Session and assignment identities/revisions; per-assignment accepted/stale/expired results; current lease expiry and fixed attempt deadline remain distinct |
| Result | Original assignment identity, stable result command ID, typed outcome and artifact/schema metadata |
| Reconciliation | A separately authorized probe assignment and observed applied/not-applied/unknown outcome |

Execution role distinguishes forward, compensation, and reconciliation work. A worker must not select a handler only by display name and ignore version or role.

The protocol must be usable by the Rust worker SDK and by another implementation using protobuf and the documented payload codec. Non-Rust SDK packages are not required.

Assignments are not authority until committed and applied. Cached or late replies must not revive an expired lease. An external worker cannot advance a run by sending an arbitrary node ID or output.

Apply the complete grant/renew/result/expiry guards in the storage specification. A fresh renewal request is not exempt from expiry checks.

A valid forward success that blocks compensation-input construction returns an accepted result receipt with the blocked obligation and intervention status. It is not a transient result-submission failure and must not trigger another forward attempt.

## Error categories

| Condition | Category |
|---|---|
| Invalid source/schema/binding or unsupported operation | `InvalidArgument` |
| Conflicting immutable version or idempotency identity | `AlreadyExists` |
| Stale claim, closed lifecycle, unsupported format/policy | `FailedPrecondition` |
| Bad identity or role | `Unauthenticated` / `PermissionDenied` |
| Missing entity outside retained metadata | `NotFound` |
| Quorum/leader unavailable | `Unavailable` |
| Admission/payload/quota exhausted | `ResourceExhausted` |
| RPC deadline with unresolved outcome | `DeadlineExceeded` plus identity/outcome detail |

Keep domain failures distinct from transport, persistence, and definition errors. No broad catch may convert failure into empty success, an unrecorded retry, or a standalone store.

## CLI contract

Provide `validate`, `publish`, `start`, `signal`, `cancel`, `inspect`, `list`, `history`, `replay`, `resolve`, `cluster`, `backup`, and `restore` command groups.

Commands support machine-readable JSON output, stable exit codes, bounded waits, and explicit endpoint/local-directory selection. Do not require parsing decorative terminal output in automation.

`validate` can consume an exported contract catalog without starting a worker. `replay` is read-only and cannot create live work.

Destructive restore, compensation abandonment, and manual retry authorization require explicit flags and an operator reason. Membership changes and command retries remain idempotent.

## Observability

Structured tracing includes cluster, principal, run, scope, activation, attempt, role, command, and event-sequence identities. Payload values and credentials are excluded by default.

Expose readiness/clock/quorum state, commit/apply lag, queued/unapplied bytes, leaf concurrency, open child scopes, loop progress, inbox depth, stale results, retry/compensation state, history range, snapshot progress, and retained bytes.

An inspection must explain why a run is not advancing, including which child, event key, reconciliation, compensation, version, resource budget, or operator action blocks it.

The host chooses metrics exporters. Do not start a telemetry service or send data to a third party automatically.

## Operational safety

Worker drain does not remove a voter. A running container scope is not an OS process and cannot be considered cancelled merely because its parent stopped requesting work.

Restore and format-upgrade operations must preserve loop cursors, frozen branch/iteration sets, obligations already compensated, and event/artifact identities. Use the storage contract's staged and version-gated procedures.

Support local restart and live local inspection without a database service. No failure path may change a production cluster into local mode or memory-only execution.
