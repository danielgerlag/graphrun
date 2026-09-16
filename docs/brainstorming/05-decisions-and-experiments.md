# Decisions and experiments

Historical decision register. Use [the normative verification contract](../specs/v1/08-testing-and-verification.md) and its matrix for the expanded v1 requirements.

The [v1 design baseline](10-v1-design-baseline.md) resolves architecture, defaults, and release scope. This file records the selected direction and the scenarios needed to establish it. The engine is not implemented and no performance result is claimed.

## Fixed requirements

The first release must support multiple independent worker processes sharing the same run population across hosts. A run must outlive its admitting worker, and compatible peers must be able to recover abandoned work.

Everyday local development must also run inside the application without a database service, broker, Docker, or runtime download of server binaries. Accepted state must survive a local application restart.

Workflow authors must be able to load YAML definitions and bind them to registered Rust activities. YAML must use the same durable execution model in local and clustered profiles.

The selected backend direction is redb with OpenRaft. Both local and clustered modes use the same durable command, log, and state-application path.

Lightweight CQRS and workflow-domain event sourcing are also selected. Retained versioned events and immutable artifacts are the source for logical workflow state. Live coordination and Raft bookkeeping are not entirely event-sourced.

The [backend decision](07-durable-backends-and-local-development.md) retains the earlier SQL options as alternatives considered, not planned adapters.

## Decisions worth making now

| Decision | Working position | Reason |
|---|---|---|
| Authoring | YAML is the v1 frontend; a Rust graph builder is outside v1 | Complete the required authoring path without a second public graph API |
| Definition compiler | One validated graph representation with versioned activity contracts | All authoring paths and replicas must agree on execution semantics |
| DSL evaluation | Structured bindings and bounded conditions | Arbitrary code evaluation and runtime type-name lookup are unnecessary for a useful YAML DSL |
| Durable backend | redb with OpenRaft | One embedded storage and replication implementation supports both required topologies |
| CQRS | Separate command and query models in the same engine and redb store | Read optimization does not require a broker, second database, or eventual consistency |
| Workflow state | Authoritative versioned domain events with pure event evolution | History must reconstruct logical state, not merely describe some mutations after they happen |
| Event-sourcing scope | Workflow lifecycle facts, not every coordination heartbeat | Current leases and consensus state keep their own recovery protocol |
| Shared execution | One guarded decision path that emits events, plus one pure evolution path | Local mode, replicas, and read-only reconstruction must agree on recorded facts |
| Atomicity | Quorum commitment followed by one redb event-and-state apply transaction | Events, derived state, coordination, critical indexes, results, and applied metadata cannot diverge |
| Replay boundary | Historical reconstruction has no live execution capabilities | Rebuilding a view must not issue payments, schedule activities, or grant claims |
| Ownership | Renewable claims with worker sessions and ownership generations | Peers must recover abandoned work without accepting stale writes |
| Scheduling topology | Leader-coordinated deadlines and committed worker claims | A cached follower view or an old leader's timer cannot authorize work |
| Member and worker roles | Stable data-bearing members, elastic activity workers | Worker scaling must not change voter membership implicitly |
| Local mode | One durable voter with the real command and application path | Development must not bypass persistence or consensus semantics |
| Queue identity | One durable activation per logical invocation | Queue messages and attempts must not create new logical work |
| Wakeups | Notifications are coalescible hints | Correctness cannot depend on receiving one notification for every job |
| Effects | Explicit at-least-once attempt semantics | A remote success can precede a crash before local persistence |
| Definition identity | Stable node keys and immutable versions | Source reordering must not reinterpret active work |
| Activity data | Owned input and output with versioned codecs | Avoid shared mutable workflow objects and unchecked downcasts |
| Runtime ownership | The host supplies Tokio and supervises the engine task | An embedded library should not own the application's process lifecycle |
| Extensibility | One internal storage integration and one replicated group initially | Additional providers and automatic partitioning would dilute the selected design |

## Resolved v1 policy

The baseline is the canonical source for values and detailed protocols. These are decisions, not unanswered questions.

| Area | Selection |
|---|---|
| Dependencies | redb 4.3.0, OpenRaft 0.9.25, Rust 2024 with a 1.90.0 support target |
| Topology | Three stable voters, worker-only processes, explicit bootstrap/join/replacement |
| Transport | Tonic/protobuf, clustered mTLS, static seeds, private local Unix socket |
| Time | Leader-recorded UTC milliseconds, nondecreasing watermark, bounded clock assumptions and fail-closed guard |
| Leases | 30 seconds, five-second renewal, explicit worker safety margin |
| Snapshots | File-backed framing, staged generations, atomic activation, bounded import/GC and unapplied-entry credits |
| History | Complete active history, 30-day completed history, 90-day metadata/tombstones, 24-hour command results |
| Activity recovery | Mandatory `RetrySafe` or `Manual`, bounded attempts, explicit intervention |
| Signals | One declared name per wait node, one pending message per name, explicit finite or indefinite timeout |
| YAML | Finite single-path DAG with six node kinds and bounded structured conditions/bindings |
| Replay | History and pure reconstruction in v1; simulation and live forks excluded |
| Upgrades | Reader-first member rollout and committed format activation |
| API scope | Tokio `Send` activities, one library and CLI workspace, no public Rust graph builder |

Remaining implementation work is to realize these contracts, not to silently choose another behavior when a requirement becomes difficult.

## A small first release

The first durable slice must load a YAML workflow in a durable one-member local group and in a three-voter group with independent activity workers. It must retain committed work across a member failure.

It includes the YAML parser boundary, activity catalog, graph compiler, immutable publication, ordered vote and log persistence, versioned workflow events, pure event evolution, eager query models, atomic claims, and duplicate-safe admission.

Events, derived domain state, coordination changes, critical indexes, command results, and applied metadata share one redb transaction. No mutable workflow-domain path bypasses its events.

Add the engine command and worker-notification protocol, leader-change recovery, snapshots, explicit membership lifecycle, and renewal of active claims. These are part of the selected backend, not later optional adapters.

Use typed sequential activities, stable identities, persisted outputs, immutable definition versions, and duplicate-safe admission to exercise that protocol. Do not first build a single-owner engine and add clustering later.

Add durable timers, directed signals, bounded conditions, explicit retries, and cancellation next, with YAML representations for the definition-level operations. Include status inspection and meaningful blocked reasons in that slice, not after it.

Parallel branches, joins, loops, and child workflows are outside v1. Independent runs still execute concurrently across workers.

Defer built-in compensation, a third-party or non-Rust activity-worker protocol, child-workflow orchestration, cron calendars, search-provider plugins, arbitrary scripting, and live instance migration. The embedded engine's own worker-only protocol is required.

Changed-code simulation, live forks, and a visual replay UI are outside v1. Capture the required historical data now, but do not include their execution policies in the v1 API.

Keep one redb/OpenRaft integration. Do not add SQL adapters, a public provider API, or multiple replicated groups before the selected protocol and its resource limits are established.

## Experiments that can reject the design

None of these experiments has been run against a Rust engine. No engine exists in this repository yet.

Use the smallest real implementation that exercises the relevant guarantee. OpenRaft's storage conformance suite and engine-specific crash scenarios are both needed. An in-memory simulation cannot prove disk persistence or cross-process recovery.

| ID | Experiment | Observable acceptance criterion |
|---|---|---|
| E01 | Leave each profile idle for 60 seconds with no timers or in-flight work | Zero periodic runnable-discovery queries in local mode and on every clustered worker; no busy loop |
| E02 | Send 10,000 duplicate wake hints for one activation to multiple workers | One logical activation, bounded hint storage, and no extra activity attempt caused solely by hints |
| E03 | Retry admission through different processes before and after disconnecting at commit | Same `StartKey` resolves to the same run; conflicting content returns a conflict |
| E04 | Kill local and clustered processes at admission, claim, result, and successor transaction boundaries | Every acknowledged record survives according to the configured durability profile; no committed successor is lost |
| E05 | Crash after a fake external service commits an effect but before the engine commits the output | Stable effect key prevents another logical effect, or the run enters documented intervention |
| E06 | Race signal admission with wait creation, duplicate signal delivery, and timeout | Exactly one permitted wait resolution; no accepted signal disappears outside the retention policy |
| E07 | Add an earlier timer while the scheduler sleeps, then restart with overdue timers | Earlier work is not delayed until the old deadline; overdue timers become eligible after recovery |
| E08 | Resume an old worker after a committed claim replacement | Its output cannot advance engine state; possible external overlap remains visible |
| E09 | Disconnect a readiness stream and change leaders while more commands commit | Resubscription finds eligible work without depending on notifications sent while disconnected |
| E10 | Submit competing conditional claims whose preconditions change before application | Current applied state decides the result; a committed rejection creates no additional claim |
| E11 | Saturate admission, ready work, activity slots, and the durable inbox separately | Memory remains within configured bounds; completions and cancellation still make progress |
| E12 | Race independent workers to claim the same ready activation | One current claim and one accepted result; no process-local lock is needed for correctness |
| E13 | Restart a member while workers hold valid claims, then fail the leader | Claims are not reset merely by restart or election; the new leader reconstructs expiry recovery |
| E14 | Roll workers with different supported workflow versions and codecs | Only compatible workers claim each run; a local missing handler does not globally block it; conflicting definitions are rejected |
| E15 | Submit cyclic, parallel, join, or loop definitions to v1 | The compiler rejects unsupported graph structure before publication |
| E16 | Change leaders with clocks ahead or behind, and suspend a member | Replicas make the same decisions from command time, and behavior matches the declared deadline and skew policy |
| E17 | Cancel during an external request and shut down during storage failure | No forbidden successor starts; unresolved work is reported rather than called safely cancelled |
| E18 | Isolate a leader and its worker from the surviving majority until a claim is replaced | The minority cannot commit renewal or completion; the old external effect is not assumed to have stopped |
| E19 | Drain one worker during continuous admission through other processes | Peers continue claiming work; drained attempts retain or relinquish ownership through the declared protocol |
| E20 | Kill the leader holding the earliest timer without a shutdown notification | A new leader reconstructs the deadline and resolves it through a committed command without periodic runnable scans |
| E21 | Run the local example after the normal Rust build with no auxiliary services | Only the application is required; no Docker, database daemon, or runtime server-binary download |
| E22 | Stop a local run during a durable wait and restart against the same member directory | Completed outputs, pending signals, retry deadlines, and definition versions survive; completed activations do not repeat |
| E23 | Run controlled command scenarios through one-member and multi-member groups | Both use commitment and durable application and satisfy the same domain invariants |
| E24 | Open one member directory from two processes, including through path aliases | The second owner is rejected; restart preserves the original member and cluster identities |
| E25 | Fail a member after redb application commits but before its wake or reply | Recovery preserves the applied result and reconstructs scheduling without a repair scan |
| E26 | Remove the available quorum during admission or completion | The client reports unavailability or an unknown outcome; no member bootstraps standalone or uses memory |
| E27 | Restart a local application with an incompatible active definition or payload codec | The incompatibility is visible and existing data remains intact |
| E28 | Compile and run the same YAML source and activity catalog with one member and a replicated group | The normalized graph and domain semantics agree; topology does not change DSL behavior |
| E29 | Load duplicate keys, unknown fields, missing edge targets, and unknown activity versions | Compilation fails before publication with source and field context |
| E30 | Bind an unavailable output or incompatible payload schema to an activity | The definition is rejected where statically detectable; invalid actual data fails at its boundary without invoking the activity |
| E31 | Edit or remove YAML while runs are active, then restart a worker | Runs resume from the pinned persisted graph rather than the current source file |
| E32 | Publish the same version concurrently with equal or conflicting normalized content | Equivalent publication is idempotent; conflicting content never replaces the existing graph |
| E33 | Supply aliases, custom tags, includes, multiple documents, or excessive nesting | The declared YAML profile and resource limits reject unsupported input without external I/O |
| E34 | Load a new YAML workflow version using already registered activities | Definition publication requires no Rust handler rebuild; existing runs retain their old graph |
| E35 | Attempt to publish opaque closures or unsupported graph operators | V1 accepts only the declared serializable DSL model |
| E36 | Crash around vote persistence, log flush, and quorum commitment | OpenRaft durability acknowledgements are truthful; no activity starts from an uncommitted claim |
| E37 | Crash while applying domain changes and last-applied metadata | Recovery observes a consistent applied prefix with no lost or duplicated logical transition |
| E38 | Crash during snapshot creation, installation, and log compaction | State and applied metadata agree; member-local identity and retained deduplication survive |
| E39 | Bootstrap, join a learner, promote or remove a voter, and restart members | Restart does not reinitialize membership; joining and scaling workers cannot create split authorities |
| E40 | Roll storage members across command and graph format versions | New formats remain gated until required replicas can apply identical transition rules |
| E41 | Scale worker-only processes while the storage group remains fixed | Worker capacity changes without altering voter membership or creating independent stores |
| E42 | Retry one claim command across leaders that choose different decision metadata | Request deduplication returns the original outcome, not a new claim or a false payload conflict |
| E43 | Overflow a readiness stream or fail delivery of its last change | The pending generation remains observable or the stream forces resynchronization; work does not wait for an unrelated event |
| E44 | Reconstruct logical run state from events, retained artifacts, and a checkpoint where applicable | It agrees with materialized domain state without consulting live coordination or executing activities |
| E45 | Crash during event append, evolution, index updates, or applied-metadata persistence | The full redb apply result commits or none does; no changed domain state lacks its events |
| E46 | Retry an accepted command across leader changes | Event identities and sequence ranges remain unchanged; duplicate requests do not append another history |
| E47 | Renew a claim, then expire or replace it | Renewal-only state remains operational; lifecycle consequences produce the required domain events |
| E48 | Reconstruct a historical state containing ready work, pending timers, and completed effects | No command, scheduler wake, claim, or activity invocation reaches the live runtime |
| E49 | Submit a command based on stale cached query data | Current authoritative state determines the result; old observations cannot grant work or overwrite a newer transition |
| E50 | Compact Raft logs, install a snapshot, and reconstruct retained workflow history | Retained events and artifacts remain available independently of the old replication log |
| E51 | Read retained event and checkpoint versions after an upgrade | Compatible readers or upcasters preserve recorded semantics without rerunning newer decision logic |
| E52 | Request history older than its declared retention range or with an unavailable artifact | The result identifies the unavailable range or artifact instead of returning a partial reconstruction as complete |
| E53 | Prune artifacts while retained events, checkpoints, or active runs still reference them | Required data remains available; query-model rebuilding cannot change live deduplication or lease authority |

YAML, local-loop, and cluster-specific scenarios are first-release gates. Run distributed scenarios with separate operating-system processes, including a multi-host partition scenario. Local conformance does not replace them.

Run E01 separately from active-work lease maintenance. Count health checks and lease updates separately so that the no-runnable-polling result cannot hide a repair scan.

E04 proves process-crash behavior. Power-loss claims need a different storage-level experiment. Do not present a process kill as proof of fsync behavior.

Separate safety from availability. A group without its required quorum cannot commit new work. Progress resumes when the group can commit and a compatible worker is available, without discarding accepted state.

## YAML and Rust authoring comparison

Build equivalent sketches for a purchase flow, a signal with timeout, and bounded parallel item processing.

Use YAML with typed Rust activities as the required path. A public Rust graph builder is outside v1.

Evaluate the following evidence.

| Concern | Useful observation |
|---|---|
| Type errors | Does YAML report an incompatible binding at load time with source context, while Rust activity implementations remain typed? |
| Language boundaries | Do both frontends reject unsupported operations before publishing the graph? |
| Ownership | Can handlers share application clients without exposing locks around workflow data? |
| Persistence boundary | Can an author identify exactly which operations survive restart? |
| Error policy | Are business failures distinct from runtime failures without excessive wrappers? |
| Definition evolution | Can a node be inserted without silently reinterpreting old activations? |
| Diagnostics | Can a stalled run explain its current wait without debugger access? |
| Compiler cost | Measure YAML compilation and diagnostic quality without a Rust rebuild |

Keep the v1 YAML and typed-activity interfaces small. A later Rust builder must not become a prerequisite for authoring YAML.

## Performance questions, not performance claims

Measure transitions per second, durable admission latency, ready age, deadline lateness, bytes written per transition, and recovery time.

Separate database transaction cost from user activity cost. Include both tiny workflows and workflows with large histories or many waits.

Measure command commitment and apply latency, redb write cost, snapshot size and duration, worker notification fan-out, lease-renewal traffic, and leader recovery at different worker counts.

Measure bytes retained per workflow, event append cost, eager projection cost, reconstruction time, and snapshot transfer growth. Event history is not free merely because commands were already replicated.

For local mode, measure cold startup, incremental build cost, restart recovery, file growth, and durable commit latency. State member count, storage settings, and transport conditions for every result. No such measurements exist yet.

Set production latency and throughput targets from the intended workload. There is no evidence yet for a universal target or a claim that Rust alone fixes scheduler overhead.

## What could require revisiting the design

Revisit the selected integration if its durability, clock, upgrade, or operational obligations cannot be met. Do not respond by weakening clustered execution, bypassing consensus locally, or presenting incomplete snapshots as recoverable state.

If one replicated group cannot meet the target workload after sensible batching, evaluate partitioning as a separate design. Do not introduce multiple groups before measuring the need.

Historical event reconstruction is selected. Simulation of changed logic and live re-execution remain distinct future capabilities, not reasons to reinterpret past facts or replace YAML with Rust-only orchestration.

Consider a broker only when an external worker boundary or an existing durable event stream creates enough value to justify the additional delivery protocol.

Keep one backend implementation with colocated command and query models. CQRS and event sourcing do not require a new broker or service, and neither makes external effects exactly-once.
