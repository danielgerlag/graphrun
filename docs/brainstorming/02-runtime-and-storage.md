# Runtime and replicated storage

Historical design notes. [The v1 specifications](../specs/v1/README.md) supersede scope and interface statements here, including single-path limitations.

The selected direction is redb with OpenRaft, lightweight CQRS, and event-sourced workflow-domain state. Local development uses one durable member. Clustered deployment uses replicated data-bearing members and activity workers. YAML remains a required frontend.

The [v1 baseline](10-v1-design-baseline.md) fixes the versions, defaults, protocols, and exclusions. The engine is not implemented; these obligations are not measured guarantees.

## One authority, two storage responsibilities

OpenRaft orders and commits engine commands through consensus. redb durably stores each data-bearing member's Raft metadata, log, and applied application state.

A redb transaction is local atomicity. A Raft commit is agreement by the required quorum. These are different boundaries. Neither an uncommitted log append nor a direct write to one replica authorizes workflow execution.

All mutations of workflow state go through domain events produced while applying committed engine commands, including in local mode. There is no independent persistence, queue, or distributed-lock provider to combine with this backend.

Workflow events and retained immutable artifacts are the source for logical workflow state and history-only views. Queries that include live coordination also read the corresponding operational records. Current lease deadlines, worker sessions, and other coordination records remain replicated operational state.

The Raft log orders commands. It is not the retained domain-event store. See the [CQRS and replay contract](09-cqrs-event-sourcing-and-replay.md) for the exact authority boundary.

## Members and workers are distinct roles

| Role | Durable responsibility | Lifetime |
|---|---|---|
| Data-bearing member | Owns a redb store and participates as a voter or learner | Stable member identity and data directory |
| Activity worker | Claims work and runs registered Rust activities | Elastic process session; does not have to store a Raft replica |
| Client | Publishes definitions, admits runs, and sends signals | No ownership of execution merely because it submitted work |

Roles can coexist in one application process. Local mode combines one voter, one worker runtime, and the client. Clustered applications can host data members, worker-only runtimes, or both.

```text
YAML publisher and activity workers
                |
       leader-routed engine commands
                |
       OpenRaft replicated group
       /          |          \
   member A   member B    member C
     redb       redb        redb
```

A three-voter group can commit with two available voters. Two voters cannot tolerate losing either voter while retaining a majority. One voter is durable locally but has no replica failover.

Worker autoscaling does not automatically add or remove voters. Voters need stable storage and explicit membership changes. Learners catch up before promotion.

## Local storage layout and durability

Start with one redb database per data-bearing member and separate logical records for these concerns.

| Records | Purpose |
|---|---|
| Local member identity and format metadata | Bind the directory to one member and cluster |
| Raft vote, log, and log-position metadata | Satisfy OpenRaft's persistent log contract |
| Applied-state metadata | Record the last applied log position and membership |
| Immutable definitions, contracts, and payload artifacts | Preserve the values referenced by active runs and retained history |
| Versioned workflow events and run-sequence metadata | Store authoritative workflow history independently of the Raft log |
| Materialized run and activation state, waits, and query models | Store derived views for command handling and reads |
| Current claims, lease deadlines, and worker coordination | Store operational authority not reconstructed by workflow-history replay |
| Ready, deadline, and correlation indexes | Find eligible work without inspecting every payload |
| Retained command results and business deduplication records | Resolve retries without creating new logical operations |

Append workflow events, evolve domain state, and maintain correctness-critical indexes in the same apply transaction. Coordination-only changes also update their indexes atomically. redb's typed tables do not maintain these rules automatically.

Use durable redb commits for Raft writes and applied state. Do not report a vote as saved or a log as flushed before its required disk persistence completes.

OpenRaft requires ordered write I/O across vote and log operations. A dedicated ordered storage path must preserve that ordering. A database writer lock alone does not establish the order of independently scheduled tasks.

Keep blocking storage work off Tokio's network and timer threads. Never hold a redb write transaction while awaiting network consensus or user activity code.

Use the baseline's redb 4.3.0 and OpenRaft 0.9.25 pins, immediate durable commits, quick repair, and ordered write owner. Storage errors make the member unhealthy; they do not enable a volatile fallback or prove an errored commit rolled back.

Primary contracts are OpenRaft's [log storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) and [state-machine storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html).

## Persist the compiled definition

YAML and any Rust builder feed the same compiler. A publication command records the normalized graph, format version, dependency contracts, digest, and source provenance.

Publishing a matching definition version is idempotent. Conflicting semantic content under the same version is rejected. Run admission pins one committed definition before work becomes eligible.

Workers execute that graph and registered activity handlers. They do not need the original YAML file. Data-bearing replicas apply the engine's declarative transition rules, not arbitrary application closures or activity bodies.

## The durable data model

The organizing structure is a persisted state machine for each logical node invocation, called an **activation**.

| Record | Identity | Purpose |
|---|---|---|
| Definition | Workflow name, explicit version, graph format version, and digest | Pins a normalized graph, bindings, policies, and payload contracts |
| Activity contract | Stable activity name, version, and contract digest | Describes input and output schemas, error codes, and recovery capabilities |
| Cluster and member | `ClusterId` and stable `MemberId` | Identify the replicated group and a particular data-bearing member |
| Engine command | `CommandId` plus immutable request content | Deduplicates retries after redirects or uncertain replies |
| Workflow event | `RunId` plus `RunSequence`, with an event contract version | Records a domain fact and the command that caused it |
| Run | `RunId`, with a unique caller-supplied `StartKey` in a defined namespace | Tracks one workflow execution and its lifecycle |
| Activation | `ActivationId`, unique within a run | Tracks one invocation of a stable `NodeKey` |
| Attempt | Activation identity plus an attempt number | Records a particular execution attempt |
| Worker session | `WorkerSessionId`, new for each process incarnation | Distinguishes a restarted worker from its old process |
| Claim | Activation identity plus an ownership generation, worker session, and lease deadline | Authorizes one current attempt to commit engine state |
| Wait | `WaitId`, derived from a persisted activation and wait occurrence | Represents a timer, signal subscription, or join |
| Signal | Run identity plus caller-supplied `SignalId` | Stores an accepted signal until its defined consumption or expiry |
| Run checkpoint | Run identity plus an included event sequence and checkpoint version | Shortens logical state reconstruction without replacing earlier event history |

V1 has a finite DAG and one active path per run. A node has at most one logical activation in that run, while retries have distinct attempts. Do not derive activation identity from a worker-local counter, recreated queue message, or vector position.

One durable activation represents one logical invocation across retries. Attempt numbers and ownership tokens can change without creating another logical invocation.

The lifecycle has explicit alternatives.

```text
Ready -> Running -> Completed
                 -> WaitingForRetry -> Ready
                 -> Failed
                 -> NeedsIntervention

WaitingForTimer -> Ready
WaitingForSignal -> Ready or TimedOut

Nonterminal states -> cancellation handling -> Cancelled
```

The final Rust enums can separate activity execution from control-node waits. Do not turn this diagram into a struct with independent `active`, `complete`, `sleeping`, and `cancelled` booleans.

Store typed payload envelopes with an explicit schema identifier and version. Keep status, due time, ownership, and correlation keys in indexed records. The scheduler must not deserialize every workflow payload to discover runnable work.

## Commit commands, then produce and evolve events

The organizing structure is a deterministic decision over current domain and coordination state. Commands include definition publication, admission, signal delivery, claim acquisition, renewal, activity completion, and timer resolution.

`decide` produces ordered workflow events, required coordination changes, and a command result. `evolve` derives workflow state from recorded events. There is no separate imperative path that changes workflow-domain state without an event.

Requests go to the current leader or receive a redirect. The caller retries the same command identity after an uncertain response. A different payload under an existing identity is a conflict.

Deduplication compares the original logical request, not leader-added timestamps or generated values. Once a request has a recorded outcome, a retry returns that outcome rather than choosing another activation or generating another effect identity.

The intended write path is the following.

1. The leader validates the request and supplies explicit decision inputs, including generated values and decision time where needed.
2. OpenRaft replicates the command and establishes commitment under the current membership.
3. Replicas process committed entries in order and decide their outcomes against current applied domain and coordination state.
4. They assign event sequences and evolve workflow state from the emitted facts.
5. One redb transaction records events, derived state, coordination changes, critical indexes, command results, last-applied position, and any applied membership change.
6. The client receives success only after commitment and the serving leader's durable application of the command.

Do not conflate the local log append with the later application transaction. A crash between commitment and application must recover through the log and snapshot protocol.

Revalidate domain preconditions against the state at application, not only against the leader's earlier read. A stale claim can produce a committed rejection with no workflow event. A duplicate accepted command returns its original result without another event sequence range.

Supply nondeterministic choices as command data. Replicas must not independently read clocks, generate IDs, draw jitter, or evaluate application-specific closures during apply.

`evolve` only applies recorded facts. It does not re-evaluate conditions, draw jitter, consult clocks, invoke handlers, or publish commands. This enables logical workflow reconstruction without deterministic replay of arbitrary Rust futures.

## Activities remain outside replicated apply

A worker may start an activity only after receiving an accepted committed claim and confirming that the claim is still usable. Activity input is immutable. Its output is submitted through another engine command.

Applying an activity result checks its activation, worker session, ownership generation, deadline, and run status. Acceptance emits facts describing the result and its domain consequences. Event evolution derives terminal state, successor activations, and affected waits within the atomic apply boundary.

Retain exact bound inputs, accepted outcomes, selected deadlines, and control-flow decisions as event data or immutable artifact references. An activity implementation is not called again to reconstruct its historical outcome.

Stale or conflicting results do not advance the graph. Repeated accepted commands return their recorded outcome within the declared retention window.

Unique activation identities prevent duplicate logical work. Attempts may still repeat after failures. Stable external-effect keys remain separate from changing ownership generations.

Never execute an HTTP request, payment, email, or activity body inside replicated apply. Every replica applies the command, so that would duplicate the effect by design.

Independent runs execute concurrently. Parallel branches and joins inside one run are outside v1. Replicas never share a mutable application data object with workers.

## Scheduling from applied state

The active leader coordinates deadline scheduling from committed, applied state. Workers receive coalescible readiness hints and submit bounded claim requests. A follower's cached view cannot independently grant ownership.

An applied transition that changes readiness wakes the scheduler. If a wake is lost, reconnection, leadership change, or supervised restart reconstructs readiness from the stored indexes.

Use level-triggered readiness generations rather than an untracked sequence of edge notifications. Overflow or delivery failure must preserve a pending change or fail the stream and force resynchronization. Silently dropping the last wake on an apparently healthy connection would strand work without polling.

The worker watch protocol must close the subscribe-before-read race. Establish observation before obtaining an authoritative readiness view, then drain eligible work. Repeat that sequence after connection or leader changes.

A cursor is not a promise to retain the entire Raft log forever. If its history was compacted, resynchronize against current committed state. Keep the business deduplication window independent of log retention.

After leadership changes, first establish authority and catch up applied state. Then reconstruct ready work and the earliest durable deadlines. Never rely on a timer stored only in the previous leader's memory.

V1 query models are eager and authoritative reads use a fresh quorum-confirmed read barrier, not cached leader-lease authority. There is no follower-stale or asynchronous reporting mode in v1.

There is no fixed-interval runnable-store scan. Raft heartbeats, election timers, connection health, and renewal of active claims still exist. They are not a claim that an idle cluster produces no network traffic.

## Time, leases, and fencing

Persist UTC Unix-millisecond deadlines. The leader supplies wall-clock observations in commands, and apply uses the maximum of that observation and the persisted engine-time watermark. Replicas never use their own clocks to decide temporal outcomes.

The baseline fixes the supported clock error, suspend-aware watchdog, quorum clock-health gate, explicit fault acknowledgement, and lease safety margins. A detected clock fault pauses time-dependent work, not elapsed workflow time.

Restart accounts for actual downtime rather than resuming a saved countdown. The deadline contract is in recorded engine time. Its documented real-clock error bounds are not a perfect UTC not-before guarantee.

The leader sleeps until the nearest known deadline, then proposes a time or expiry command. Applying that command resolves eligible waits and claims in bounded batches. There is no requirement for a constant stream of application clock ticks when no deadlines exist.

Claims and worker sessions use 30-second leases with five-second batched renewal. Renewal must match the current session, generation, and lease revision and cannot revive an expired lease.

Workers stop initiating effects five seconds before their earlier acknowledged expiry. A delayed or deduplicated renewal reply is not a fresh authorization. An isolated worker obeys its last acknowledged cutoff and requests cooperative cancellation.

Leadership change does not itself expire every claim. Preserve valid claims and let compatible workers route subsequent requests to the new leader. Reclaim through explicit committed transitions and the declared ambiguity policy.

An ownership generation is not a Raft term or a member ID. Fencing protects engine state after actual claim replacement. It does not stop an external operation that the old worker already issued.

Routine renewal can update operational state without appending a workflow event. Expiry, abandonment, retry, or intervention that changes a workflow's lifecycle must record that domain consequence. Workflow-history replay does not reconstruct the exact live lease state.

## Snapshots preserve more than workflow rows

Choose persistent application state. Each successful apply durably records workflow events, derived state, coordination changes, and applied metadata together. Build a snapshot from one consistent applied-state view.

A Raft snapshot includes retained workflow events, run checkpoints, referenced artifacts, materialized domain state, current claims and coordination, required indexes, command results, deduplication records, applied position, and applied membership.

A run checkpoint serves a different purpose. It caches logical workflow state through an event sequence. It does not replace a Raft snapshot or recover current coordination and membership.

Installing a Raft snapshot must not overwrite the receiving member's local identity or vote state. Copying the leader's entire redb file onto a follower is not the application snapshot protocol.

Receive and validate a durable file-backed snapshot, then import it into an inactive state generation through bounded transactions. Activate its generation, applied metadata, membership, and snapshot reference in one small transaction. A crash leaves the old or complete new generation, never a mixture.

If snapshots use external files, make a complete file durable before publishing metadata that references it. Recovery must distinguish installed snapshots from incomplete or unreferenced files.

Respect OpenRaft's compaction and installation contracts. Log truncation does not prune workflow history or referenced artifacts. Event retention, run-checkpoint retention, and business deduplication each need an explicit policy.

Rebuilding a historical view must not write to live ready indexes or grant a claim. Use a pure projector with no command or activity-dispatch capability, isolated from the live authority.

Use the baseline's snapshot framing, credits, disk headroom, trigger thresholds, and extended installation deadline. OpenRaft 0.9 has unbounded internal apply buffering; a bounded storage-owner queue alone is insufficient. Account for persisted-but-unapplied entries before admitting more work.

## Bootstrap, restart, and local development

Local construction creates a one-member group only for a fresh local directory. Restart opens existing redb and membership state. It does not initialize a new cluster or invent a new persistent member identity.

Cluster bootstrap and joining are explicit, distinct operations. Do not initialize a collection of independent single-member groups and treat matching peer addresses as a merged cluster.

Enforce exclusive ownership of each member's data directory, including path aliases. A restarted worker gets a fresh `WorkerSessionId`; its colocated storage member retains its `MemberId`.

Local mode uses the same Raft command, log, application, and durability path as the cluster. It must not optimize development by directly mutating redb. No external database process or broker is needed.

Never recover quorum loss by switching a clustered member into standalone mode. Reopening, expanding membership, replacing a lost member, and disaster recovery have different procedures.

Stopping a voter is not removal. Bootstrap an exact genesis manifest; join replacements as caught-up learners with never-reused IDs before changing voter membership. Planned changes preserve a healthy existing majority.

The baseline defines backup and disaster restore. A new recovery cluster gets new cluster/member identities and keeps execution disabled until committed recovery actions and explicit admin authorization.

## Backpressure and version compatibility

Bound command queues, worker concurrency, notification buffers, payloads, replicated backlog, and snapshot resources. Reserve progress capacity for consensus, claim renewal, cancellation, and completions.

Use one replicated group. Multiple groups, cross-group workflows, SQL adapters, and automatic sharding are outside v1.

Data-bearing members must agree on command decisions, event production and evolution, and graph versions. Deploy compatible engine code before enabling new formats for every replica that must apply them, or explicitly remove incompatible members.

Retained event, checkpoint, and payload versions need compatible readers or explicit deterministic upcasters for the advertised replay window. A projection upgrade does not authorize a live replica to reinterpret committed state independently.

Activity workers can host different activity versions. Claim routing considers their capabilities without requiring every storage voter to contain every activity implementation.
