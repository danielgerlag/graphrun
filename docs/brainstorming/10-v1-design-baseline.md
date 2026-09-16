# Superseded design baseline

This historical decision record is superseded by [the v1 implementation specifications](../specs/v1/README.md).

Loops, parallel branches, compensation, and the Rust graph builder are mandatory v1 requirements. The earlier narrow scope below records a rejected proposal, not implementation instructions. Storage and protocol decisions were carried into the new specs where still applicable.

The engine is not implemented. Capacity and latency figures below are engineering targets, not measured capabilities or unconditional service guarantees.

## Release boundary

V1 executes finite acyclic workflows. Each run has one active execution path, while many runs execute concurrently across workers.

The supported node kinds are `activity`, `delay`, `wait_signal`, `choose`, `complete`, and `fail`.

Parallel branches, joins, loops, child workflows, compensation, cron calendars, arbitrary scripts, dynamic activity binaries, and a Rust graph builder are outside v1. They are not unfinished v1 requirements. Adding them requires a later versioned design.

V1 includes definition publication, admission, directed signals, cancellation, manual intervention, status/history queries, and read-only reconstruction at a retained event sequence.

Changed-code simulation, live forks, retroactive history editing, asynchronous reporting services, and a visual editor are outside v1.

There is one replicated group and one trust domain per cluster. V1 has no hostile-tenant isolation or cross-group transactions.

## Dependency and storage baseline

Use Rust edition 2024 and a project minimum-support target of Rust 1.90.0. Pin redb to `=4.3.0` and OpenRaft to `=0.9.25` with `serde` and `storage-v2`. Do not enable OpenRaft's single-threaded, generic-snapshot-data, or follower-log-reversion features, and do not use its 0.10 prerelease line.

redb declares Rust 1.90. OpenRaft 0.9.25 does not declare a numeric MSRV. The full dependency resolution must meet the project target before release; the target is not an upstream compatibility guarantee.

The supporting selections are Tonic 0.14.6 with matching prost tooling, yaml-rust2 0.13.0, Schemars 1.2.2, and jsonschema 0.56.0. Commit the implementation's Cargo lockfile, use rustls rather than an OpenSSL runtime dependency, and keep network/file schema resolution disabled.

Open one redb database per member. Route all writes through a dedicated ordered storage thread. Use `Durability::Immediate` and quick-repair mode for correctness-relevant transactions.

For v1, append log entries durably, invoke the successful log-flush callback, then complete the append future. Votes are durable before acknowledgement. Persist committed-position metadata as well as the atomic domain/application transaction.

An error from commit is not proof of rollback. Mark the member unhealthy, reopen and inspect durable state, and recover through the normal protocol rather than replaying an unqualified write.

Support crash-stop failures, process termination, partitions, machine restart, and power loss when the storage stack honors durable flushes. Production storage is local ext4/XFS on Linux; local development also targets APFS on macOS. Network filesystems, Byzantine members, dishonest storage, and simultaneous loss of all durable copies are outside the supported failure model.

## Membership and recovery

The default clustered topology has three stable voters. Worker-only processes scale independently. A member is storage-only unless activity hosting is explicitly enabled.

Bootstrap persists one cluster UUID, never-reused member IDs, and an exact genesis manifest. One designated process initializes that manifest. Local genesis has one voter; clustered genesis has the exact three intended voters.

Restart never calls initialization for an existing member. A failed bootstrap can resume only with the identical genesis manifest and a compatible pristine Raft state.

Join new data-bearing members as learners with fresh IDs and directories. Wait for catch-up through a barrier, then change membership. Replace one voter at a time and preserve two healthy existing voters until the final uniform membership is committed.

Never repoint an old member ID at a blank process. Persist the target of a membership operation so retries finish an interrupted joint change instead of creating a competing change.

An intact member restarts with the same identity and votes. A lost directory is replaced with a fresh-ID member while quorum survives. Quorum loss requires recovery of a majority of intact original members, not automatic quorum reduction.

V1 backups are explicit admin-triggered logical application snapshots exported to an operator-selected file. They are not raw copies of an active redb database.

Disaster restore requires fencing all old members and workers first. Create a new cluster UUID and member IDs from a verified backup, preserving run IDs, history, artifacts, and business-effect identities. Old membership and applied positions are provenance, not the new group's Raft authority.

Seed the same verified application image into the fresh genesis members through an explicit offline restore procedure. Rebuild new Raft metadata rather than importing old votes. Start in a restoring mode that forbids work grants.

After fresh Raft initialization, committed restore-finalization commands fence old claims and record intervention for restored active runs. Do not edit their domain histories offline. Normal admission remains disabled until an admin acknowledges the recovery point and authorizes the restored group.

The backup recovery point is explicit. Work committed after that backup may be lost if no durable copy survives; the engine must not claim otherwise.

## Snapshot and compaction protocol

Keep transferable application records under an active-generation prefix. Member identity, votes, local Raft logs, visible log boundaries, and generation/snapshot registry metadata remain outside that prefix.

Snapshots use a versioned binary container with a protobuf manifest, bounded record frames, counts/lengths, per-frame checksums, and a SHA-256 footer. Frames carry opaque key/value bytes. Preserve event and artifact bytes rather than reserializing historical data.

Use file-backed snapshot transport. Build from one redb read transaction into a unique temporary file, finish and sync it, rename within the snapshot directory, sync that directory, and only then durably publish its metadata.

Snapshot publication is monotonic. If a newer install overtakes a builder, discard the obsolete build rather than publishing it as current.

Install by receiving and verifying a complete durable file, importing into an inactive generation in bounded transactions, and switching the active-generation pointer, applied position, membership, and current-snapshot reference in one small durable transaction.

Normal state application remains serialized behind installation. Vote/log I/O can progress between staging batches. The installing member does not serve new live domain operations until activation completes.

A crash before activation leaves the old generation authoritative. A crash after activation leaves the complete new generation authoritative. Cleanup may reclaim unreferenced files and generations only after reading durable registry pointers; filenames are not authority.

| Storage budget | V1 default |
|---|---|
| Raft replication batch | At most 4 entries |
| Raft RPC envelope | 8 MiB; public command RPCs remain limited to 1 MiB |
| Snapshot frame/network chunk | 1 MiB |
| Import or GC transaction | At most 4 MiB or 4,096 records |
| Snapshot buffers | 16 MiB per operation |
| Snapshot concurrency | One build and one inbound transfer per member |
| Snapshot size admission limit | 128 GiB, operator-configurable |
| Background snapshot I/O | 50 MiB/s initial cap |
| Snapshot trigger | 20,000 applied entries, 512 MiB applied log bytes, or 30 minutes with changes |
| Retained snapshotted log tail | 1,024 entries |
| Transfer stall timeout | 30 seconds |
| Final install timeout | 2 hours |
| Unapplied-entry admission credits | 64 MiB encoded data and 4,096 entries per member |

Set OpenRaft's count-only snapshot policy to `Never` and drive the combined trigger explicitly. Override its short default snapshot-install timeout.

Snapshot disk admission reserves the incoming file, estimated imported generation, and `max(8 GiB, 20% of current database size)` headroom on clustered members. The local-development floor is 512 MiB rather than 8 GiB. Start the import estimate at twice the encoded snapshot size and monitor actual consumption.

OpenRaft 0.9 materializes complete apply ranges and has an unbounded state-machine channel. Enforce credits across client admission, inbound replication, persisted-but-unapplied entries, and catch-up. Release credits only on apply, overwrite, or snapshot replacement, not on RPC acknowledgement. Reserve progress capacity for control entries and already admitted work.

Do not truncate a full-range log-read response to impose a memory limit. Bound what is admitted instead. Serialized-byte credits are not a proven bound on Rust heap amplification.

Logical Raft purge advances a durable visible prefix only after a published snapshot covers it. Logical truncation lowers the visible suffix. Readers ignore physical rows outside those boundaries, and bounded GC later deletes them. New append makes every newly visible index valid before extending the boundary.

Domain-history retention is separate from Raft purge. Physical redb file compaction is offline per member: drain, stop Raft/readers, compact, restart and catch up before touching another member.

## Transport and trust

Use Tonic gRPC over HTTP/2 with protobuf RPC envelopes. Versioned domain records are carried as bounded serialized data rather than exposing OpenRaft's internal Rust types as the public protocol.

Clustered connections require mutual TLS with rustls. There is no plaintext clustered mode. Certificate identities bind the cluster and principal, and member RPCs additionally bind the persistent member ID.

Use explicit seed addresses. There is no mandatory discovery server, certificate-issuing daemon, database, or broker.

Cluster operators supply a CA and certificates. CA rotation uses an overlapping trust window followed by explicit retirement of the old CA. Restarting a process does not change its stored member identity.

Authorize operations through these roles.

| Role | Allowed operations |
|---|---|
| Member | Raft, snapshot, and clock-health RPCs for members in the committed roster |
| Worker | Read required contracts/artifacts and clock health, advertise capabilities, and claim, renew, and report its own attempts |
| Client | Start, signal, cancel, inspect, and read history within this cluster's trust domain |
| Admin | Publish definitions/contracts, change policy or membership, acknowledge clock faults, resolve intervention, and manage backup/restore |

Roles can be combined explicitly. A storage-member credential is not automatically an administrator credential.

Authenticate and authorize before serving a cached command result. Scope command-result deduplication by authenticated principal and command ID; certificate rotation preserves the principal identity.

V1 does not implement encryption inside redb or snapshot files. Production deployments use encrypted volumes and protect exported backups. Mutual TLS protects transport, not data copied from disk by an authorized operator.

Local mode uses direct in-process calls and a private Unix-domain control socket for the optional CLI. The socket resides inside the member directory and is accessible only to its owner. It is not a TCP listener and does not require local certificate setup.

Linux and macOS are the v1 deployment targets. Windows support is outside the v1 support commitment.

## Public API and packaging

Use one public library crate, `graphrun`, and one CLI package, `graphrun-cli`, in a Cargo workspace. Keep storage, consensus, definition compilation, command handling, event evolution, and queries as modules rather than publishing a crate per internal trait.

The library uses the host's Tokio runtime and does not install process signal handlers. A dedicated ordered storage thread isolates blocking redb I/O. Storage-member mode is storage-only by default; activity execution is an explicit opt-in.

The entry points are `Engine::local(data_dir)`, `Engine::member(member_config)`, and `Engine::worker(cluster_config)`. Local mode combines one durable member and a worker. Worker-only mode never adds a voter or creates another authoritative store.

Activities use owned inputs and outputs and `Send` futures. `!Send` and alternative async runtimes are outside v1.

Use function registration for ordinary activities and the `Activity` trait for reusable implementations. Activity registration requires a stable key/version, payload contracts, a static error-code catalog/classifier, and an explicit recovery policy.

Generate payload schemas with Schemars and validate the supported schema contract with the `jsonschema` crate. `DurablePayload` includes owned Serde encoding/decoding, a stable schema identity/version, and schema metadata.

The CLI includes validate, publish, start, signal, cancel, inspect, history, read-only replay, intervention resolution, membership/health, and backup/restore operations. The library does not depend on the CLI.

Keep protobuf definitions and generated Rust in the repository. Ordinary application builds do not require `protoc`; maintainers regenerate protocol code with a pinned toolchain when the protocol changes.

## Request identity and errors

The SDK creates a UUID command ID once per logical operation and reuses it across retries and leader redirects. Query operations do not mutate state or consume a command identity.

Start-key uniqueness is scoped by workflow name, across definition versions. Keys are nonempty, case-sensitive UTF-8 strings of at most 128 bytes. The engine does not trim or Unicode-normalize application keys.

Look up an existing start key before resolving `latest`. Matching input returns the original run. An explicitly requested different version or different normalized input is a conflict. Publishing a new definition version must not turn a retried start into another run.

Use a five-second default RPC deadline, except renewal RPCs use two seconds and snapshot/restore operations use the storage deadlines. Retry retryable transport failures with bounded exponential jitter from 250 ms to five seconds, for at most five minutes.

| Condition | gRPC status category |
|---|---|
| Invalid definition, schema, binding, or unsupported operation | `InvalidArgument` |
| Conflicting immutable version or idempotency identity | `AlreadyExists` |
| Expired claim, closed wait, or invalid lifecycle transition | `FailedPrecondition` |
| Invalid identity or insufficient role | `Unauthenticated` or `PermissionDenied` |
| Quorum or leader unavailable | `Unavailable` |
| Capacity or payload limit | `ResourceExhausted` |
| Missing entity outside retained metadata | `NotFound` |
| Request deadline without a known outcome | `DeadlineExceeded`, including command identity and unknown-outcome detail |

A timeout is not evidence that a command failed to commit. Never convert uncertainty into a new command identity, empty success, or standalone authority.

## Time, leases, and failover

Use UTC Unix milliseconds for durable deadlines and a persisted nondecreasing engine-time watermark. The leader serializes timestamp assignment with command submission. Replicas apply temporal predicates using `max(committed_watermark, command.sampled_utc_ms)`, never their own clocks.

Require voter and worker wall clocks to stay within one second of UTC through ordinary OS time synchronization. This is an operating assumption, not something consensus proves. No engine-specific time service is required.

Durable delays are measured from their recorded originating engine timestamp and include normal process downtime. After restart, compare persisted deadlines with current trusted wall time. Do not resume a saved countdown or cap legitimate elapsed downtime.

The deadline rule is strict in engine time, not perfectly accurate UTC. Under the supported clock assumption, an absolute deadline can become eligible up to one second early in real time and a relative delay up to two seconds early. Execution may be arbitrarily late during outages or overload. V1 is not a real-time scheduler.

| Setting | V1 baseline |
|---|---|
| Activity claim and worker-session lease | 30 seconds |
| Batched renewal cadence | 5 seconds |
| Worker stop-initiating-effects margin | 5 seconds before the earlier acknowledged expiry |
| Renewal RPC timeout | 2 seconds |
| Raft heartbeat | 250 ms |
| Randomized election timeout configuration | 1,000-2,000 ms |
| Leadership recovery target on a healthy LAN | Within 10 seconds |
| Retry-safe activity recovery target | Roughly 40 seconds from the last successful renewal, with quorum available |

Library leader-lease behavior, scheduling, and catch-up contribute to failover time. Election configuration is not a hard failover bound.

Grant and renewal expiry is the recorded decision time plus the lease duration. Renewal requires the same generation and session, an active attempt, and an unexpired lease. Expiry commands compare the expected generation and lease revision, so a stale expiry cannot override a renewal.

A timed-out renewal has an unknown outcome. The worker continues to obey its last acknowledged cutoff. A late or deduplicated reply is not a fresh lease; a new renewal request is required before resuming effects.

The safety margin applies to cooperative worker and handler behavior. The engine stops dispatching activity bodies and requests cancellation, but cannot intercept arbitrary user I/O, detached tasks, or a pause between a permission check and an external request. External idempotency remains necessary.

Use a small suspend-aware `BootClock` adapter for local watchdogs: `CLOCK_BOOTTIME` on Linux and `mach_continuous_time` on macOS. Do not assume Rust `Instant` counts suspension on every platform.

Sample wall time and boot time every 250 ms and before time-dependent admission or effect dispatch. Mark the clock unsafe if the elapsed-time discrepancy exceeds 250 ms plus 1,000 ppm of the interval, if boot time reverses, or if a recovered watermark is more than two seconds ahead of wall time.

A watchdog gap over two seconds invalidates local execution permission until fresh checks succeed. On leadership acquisition, first establish and apply a current-term barrier before enabling scheduling.

In clustered mode, obtain clock-health samples from a current voting quorum. Require RTT no greater than 500 ms and a peer wall reading within the leader's send/receive interval expanded by two seconds. Refresh every five seconds and expire samples after ten seconds. These probes check clocks, not runnable storage.

On a detected clock fault, stop time-dependent proposals, new grants/renewals, and effect dispatch. Keep replication and deterministic application of already committed entries alive. Attempt transfer to a healthy voter and record the fault at the existing watermark when possible.

Resume only after explicit acknowledgement and ten seconds of healthy observations. Never decrease committed engine time. If erroneous future time was committed, wait until corrected wall time minus one second catches up before resuming.

One-member mode uses its own OS clock. A full outage followed by a common wrong clock cannot be distinguished from genuine downtime without an independent reference. Such accuracy failures are outside the supported assumption, not a reason to fabricate a consensus-clock guarantee.

## YAML and payload language

Use `yaml-rust2`'s marked parser events, not a loader that silently overwrites duplicate mapping keys. The accepted language is one YAML 1.2 document with string keys and JSON-compatible values.

Reject duplicate keys, custom tags, anchors, aliases, merge keys, unknown DSL fields, multiple documents, includes, and external reference fetching.

Workflow, activity, and signal names match `[a-z][a-z0-9_.-]{0,63}` and are case-sensitive. Node keys match `[a-z][a-z0-9_]{0,63}`. Versions are positive `u32` values. Payload schema identities use `name/vN`, where `N` is their positive version.

The v1 binding variants are `from`, `literal`, `object`, and `array`. A `from` binding may contain a JSON Pointer `path`. Bindings have exactly one root variant. There is no interpolation or implicit type coercion.

Whole-payload connections require matching schema identities. Object assembly and field projection use conservative structural type analysis. Actual values still pass their declared schema and Rust decoder at the execution boundary.

Payload contracts use JSON Schema Draft 2020-12. Permit local `$defs` references only, reject recursive references, and disable network/file retrieval. Defaults are annotations, not values injected by the engine.

V1 accepts the bounded structural subset needed for ordinary Serde models: types, properties, required fields, additional-properties rules, array items and size bounds, string size bounds, numeric bounds, enum/const, and bounded unions. The supported asserted formats are `uuid` and `date-time`; unknown formats are rejected. Regex-based keywords, dynamic references, custom validators, and arbitrary remote schema resolution are outside this contract.

For a union, projection is permitted only when the referenced path and type are unambiguous across its alternatives. Otherwise use a whole-payload binding or a named transformation activity.

Persist domain commands, events, definitions, and payloads as explicitly versioned JSON records. Object keys are sorted by UTF-8 bytes, arrays retain order, non-finite floats are rejected, payload integers retain exact signed-64-bit values, and fractional numbers use finite IEEE-754 values. Numeric kinds are not silently interchanged.

Protocol member IDs, sequence counters, and log-position integers are decimal strings in JSON envelopes rather than lossy application-language numbers. Their protobuf fields remain explicitly typed.

The engine's normalizer owns canonical bytes. Use SHA-256 over those bytes for definition/artifact identity. This is a versioned engine encoding, not a claim of compatibility with an unrelated JSON canonicalization standard.

## Exact v1 conditions

A `choose` node has an ordered nonempty `cases` list and a required `default` edge. Each case contains `when` and `next`. The first true case wins.

Conditions have exactly one operator.

| Operator | Operands and semantics |
|---|---|
| `eq`, `ne` | Two scalar bindings of the same declared kind, including null |
| `lt`, `le`, `gt`, `ge` | Two numeric bindings of the same declared numeric kind |
| `all`, `any` | A nonempty list of conditions, evaluated left to right with short-circuiting |
| `not` | One condition |
| `exists` | A `from` binding, optionally with a path; missing is false and explicit null is present |

There are no arithmetic, regex, network, or function-call operators. Arrays and objects are not comparison operands.

A missing required value is a binding error, not false or null. Invalid operand types are definition errors where statically detectable and explicit execution errors otherwise.

The compiler rejects graph cycles, unreachable nodes, missing edges, and references to outputs not available on every path to their consumer. Branches can converge only when their later bindings satisfy that rule.

## Activity failures and recovery

Activity registration must choose one recovery policy.

| Policy | Ambiguous outcome after timeout, process loss, or claim replacement |
|---|---|
| `RetrySafe` | Retry the same logical activation and input within its remaining attempt budget |
| `Manual` | Enter `NeedsIntervention`; do not invoke the activity again automatically |

`RetrySafe` is a handler guarantee, appropriate for pure work or an external operation that honors the stable effect key. YAML cannot upgrade a `Manual` handler to `RetrySafe`.

The default maximum is five total attempts, including the first. YAML may lower it or raise it up to the policy limit of ten. Reported activity errors retry only when their declared retryable code appears in the node's `retry.errors` list, which defaults to empty.

Crash recovery for a `RetrySafe` activity uses the same total-attempt budget even when the error-code list is empty. Exhausted recovery of an ambiguous attempt enters intervention. Exhausted retries for a known reported failure fail the run.

The default backoff is full jitter between zero and `min(60s, 1s * 2^(attempt - 1))`. Persist the chosen delay. Definitions can specify the initial delay, multiplier, and cap within policy limits.

The default activity timeout is five minutes, configurable from one second to 24 hours. Activity timeout is separate from claim renewal and from an optional run deadline.

There is no implicit run timeout. A definition can set `run_timeout` to a duration of at most 365 days. Its expiration uses the cancellation path with a deadline-exceeded reason. Duration parsing accepts positive integers with `ms`, `s`, `m`, `h`, or `d`, without calendars, months, or implicit time zones.

Schema/binding errors, undeclared error codes, and invalid definitions are not transient activity failures. Storage/transport retries do not consume an activity attempt unless an execution claim was actually committed.

## Cancellation and intervention

A committed cancellation request immediately prevents new ordinary claims and successors. The worker receives cooperative cancellation and has a five-second local grace period before the async future is dropped.

The run reaches `Cancelled` when its active claim has been relinquished or fenced. Record uncertainty about an abandoned external effect rather than describing cancellation as rollback.

Late results from a fenced attempt cannot advance the graph. The original attempt identity and effect key remain available for diagnosis and external reconciliation.

Worker shutdown stops new claims and drains for 30 seconds by default. It maintains claims during that interval and then requests cancellation. Stopping a voter does not remove it from membership.

An administrator resolves intervention by supplying a schema-valid observed result, authorizing another attempt on the same input/effect identity, failing, or cancelling the run. A restored or resource-paused continuation with no unresolved activity effect may also be resumed unchanged.

Each resolution is a version-checked committed command with an actor and reason. Authorizing another attempt explicitly accepts the duplicate-effect risk and grants at most ten additional attempts. It does not rewrite the old failure or history.

## Directed signal semantics

Signals target a run and a declared signal name. A definition declares the payload schema for each name. Each name can belong to at most one `wait_signal` node in the v1 DAG.

The inbox holds at most one unconsumed message per declared signal name. The same `SignalId` and payload return the original acknowledgement. Reusing that ID with different content is a conflict. A different ID for an already occupied or resolved name is rejected.

A signal can arrive before its wait. Unconsumed buffered signals expire after seven days by default and record `SignalExpired`; their historical payload remains under the workflow-history policy.

A `wait_signal` explicitly sets `timeout` to a duration or null. A duration requires `on_timeout`; null means an indefinite wait and forbids `on_timeout`.

When an active wait has a deadline, only a signal accepted before that deadline can satisfy it. Timeout wins at equality. Upstream event timestamps do not override engine ordering or revive a resolved wait.

Unknown signal names, unknown runs, terminal/cancelling runs, expired active waits, and schema-invalid payloads return explicit errors without a new accepted signal event.

V1 does not offer broadcasts, competing consumers, repeated stream subscriptions, or external event-broker adapters.

## History, deduplication, and replay

Retain complete history and required immutable artifacts for every active run. After termination, retain full history and artifacts for 30 days by default.

Retain the terminal summary and start-key/signal deduplication tombstones for 90 days after termination. A repeated start during that window returns the original run identity and an expired-history indicator if the full history has gone.

Retain command results for 24 hours. The SDK automatically retries a transport-uncertain command for at most five minutes with the same command ID. Outside the documented command-result window, clients must not assume command-level deduplication persists indefinitely.

Create a run checkpoint every 256 workflow events and at termination, retaining the latest checkpoint. A checkpoint records its included run sequence and format version. It does not delete active-run history.

Retention cleanup is a committed, deadline-driven operation over expiry indexes. It is not a periodic runnable-work scan. Artifact deletion requires that no retained run, event, checkpoint, or command result needs the data.

V1 exposes paginated history and pure reconstruction at an available run sequence. A missing range or artifact returns an explicit history-unavailable error. Replay has no command sender, activity registry, worker credentials, or scheduler capability.

Live forks, simulation of changed logic, and re-execution of external effects are outside v1. Their origin/result-reuse/effect-key policies must be specified in a later version, not guessed by the read-only replay API.

## Query and upgrade contracts

Maintain query models eagerly in the apply transaction. Authoritative reads are leader-routed and linearizable; no follower-stale read option is exposed in v1.

History cursors are run sequences, not Raft log IDs. Query responses include their domain sequence or opaque consistency receipt so clients can identify what they observed.

Workflow definitions and activity contracts are immutable per version. `latest` is supported only as an admission alias resolved once into the new run's pinned definition.

Gate new command, event, graph, and snapshot formats behind committed protocol capability activation after all configured data-bearing members can read them. Upgrade one member at a time and explicitly remove an unavailable incompatible member before activating a format it cannot read.

Keep event/checkpoint readers or deterministic representation upcasters for every retained version. There is no in-place active-run migration in v1.

## Default resource budgets

These defaults are policy, not measurements.

| Resource | V1 default |
|---|---|
| YAML source and each payload | 256 KiB |
| Normalized definition and activity schema | 256 KiB each |
| Nodes per definition | 256 |
| Data/binding/condition nesting | 32 levels |
| Condition operators per node | 256 |
| Simultaneously active runs per group | 10,000 |
| In-flight activities per worker | 128 |
| Claim batch | 16 |
| Queued client commands per process | 1,024 |
| History query page | 100 events, maximum 1,000 |
| Retained event/artifact budget per run | 64 MiB, with a separate 1 MiB control-record reserve |
| Unary RPC envelope | 1 MiB |
| Diagnostic error message | 8 KiB, with values redacted by default |

Reserve result and control-record capacity before granting work. Admission pressure must not prevent an already accepted result, cancellation, or lease renewal from being recorded. Reaching a run budget pauses it for intervention rather than deleting history.

Use oldest-ready-first scheduling with deterministic run/activation tie-breaks. Since v1 has one active path per run, a run that advances repeatedly rejoins behind older ready work.

All limits that affect replicated decisions are part of versioned replicated policy. Defaults that affect workflow semantics are expanded into the normalized definition at publication. Run retention promises are captured at admission and do not shrink retroactively. Per-process configuration can change local concurrency or caching, but cannot make replicas decide the same command differently.

## Capacity targets and explicit non-goals

The initial benchmark target is 1,000 committed engine commands per second with p95 receipt latency below 100 ms, using three same-region members with local durable SSDs, four vCPUs and 8 GiB RAM each, 1 KiB payloads, and no slow activity work in the timing.

Target local readiness within one second for an existing store with at most 10,000 events and 1 KiB payloads, and YAML compilation below 50 ms for a 256-node definition, on the same four-vCPU/8-GiB/local-SSD reference configuration. Cargo compilation is not included. These targets must be measured during implementation.

No latency target applies during quorum loss, storage failure, required operator intervention, or violation of the supported clock assumptions.

If one group misses its target, first measure and improve batching and storage scheduling. Automatic sharding and cross-group workflows are not a v1 fallback.
