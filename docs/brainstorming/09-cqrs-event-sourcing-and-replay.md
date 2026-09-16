# CQRS, event sourcing, and replay

Historical rationale. [The v1 domain specification](../specs/v1/01-domain-model.md) extends these rules to durable scopes, iterations, joining, and compensation.

Lightweight CQRS and event sourcing for workflow-domain state are selected parts of the design. They use the selected redb/OpenRaft architecture in both local and clustered modes. The engine is not implemented yet.

The [v1 baseline](10-v1-design-baseline.md) fixes recording, retention, and reconstruction policy. V1 includes ordered history and read-only reconstruction. Changed-code simulation, live forks, and a replay UI are outside v1.

## Separate the responsibilities

[CQRS](https://martinfowler.com/bliki/CQRS.html) separates commands from query models. Commands request changes such as admitting a run or recording an activity result. Queries return status, history, and summaries without changing execution.

[Event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) makes recorded domain facts the source for workflow state. It is stronger than updating mutable state and then writing an optional audit message.

Keep the scope explicit.

| State | Authority and recovery |
|---|---|
| Workflow lifecycle, data, outcomes, and decisions | Ordered workflow events and retained immutable artifacts, optionally starting from a versioned run checkpoint |
| Materialized workflow-domain state and history-only views | Derived from workflow history, not independently writable business state |
| Query views that include live coordination | Derived from the corresponding domain and operational records at their reported positions |
| Current lease deadlines, worker sessions, engine coordination, and deduplication bookkeeping | Replicated operational state with its own rules and retention |
| Raft votes, logs, membership, and applied positions | OpenRaft's protocol and storage contracts |

We do not event-source every heartbeat or lease renewal. A renewal that changes only coordination state need not append a workflow event. A claim expiry that changes a workflow's lifecycle must record that domain consequence.

Historical workflow replay does not recreate the complete live cluster, current ownership permissions, or every past heartbeat.

## The Raft log is not workflow history

OpenRaft replicates commands and protocol records. That log can be compacted once the storage protocol permits it.

Store workflow events as replicated application data in redb with a separate retention contract. Their identity and availability must not depend on an old Raft log entry remaining present.

The engine's commands still pass through Raft. We do not add a second broker or another replication path for domain events.

## One deterministic decision and application path

The functional split is the following.

```text
Committed command
  -> deduplicate and decide(domain state, coordination state, command)
  -> ordered workflow events + coordination changes + command result
  -> evolve(domain state, workflow events)
  -> atomically persist the application result
```

`decide` evaluates the command against current applied state. It chooses domain outcomes using explicit command inputs, including decision time and generated values where needed.

`evolve` applies recorded facts. It does not reconsider a branch, recalculate a retry policy, sample time or randomness, invoke an activity, or send a command.

Every change to logical workflow state goes through this event path. Do not retain a parallel imperative update path that can change a run without an event.

A rejected command can produce no domain events. A repeated accepted command returns its retained result without appending its events again. Deduplication uses the original logical request, not leader-added metadata.

Each redb apply transaction atomically records the following.

1. Newly emitted workflow events and their per-run sequence positions.
2. Materialized domain state obtained by applying those events.
3. Current coordination changes and correctness-critical indexes.
4. The command result and required deduplication records.
5. The Raft last-applied position and any applied membership change.

Domain preconditions must be evaluated under this consistent application boundary. Quorum commitment still precedes application, and client success follows durable application at the serving leader.

The event stream is authoritative for workflow state even though current state is stored alongside it for efficient command handling and reads.

## A versioned event envelope

The identity is the pair `RunId` and `RunSequence`. A new event does not need an unrelated random ID.

| Field | Meaning |
|---|---|
| `run_id` | The workflow instance whose history contains the event |
| `sequence` | Monotonic position in that run's event stream |
| `event_version` | The version of the serialized event contract |
| `command_id` | The command that caused this fact to be recorded |
| `principal_id` | Its authenticated or internal engine principal, completing the command's deduplication identity |
| `recorded_at` | Recorded engine time, not a clock sampled independently by each replica |
| `kind` | A typed event variant carrying its required payload |

Event versions, workflow definition versions, payload schema versions, and Raft terms are distinct concepts.

Assign event sequences during ordered application. A command that produces several events preserves their defined order. Retrying that command does not consume another sequence range.

These are the v1 event kinds. Their payloads use explicit versioned JSON records and typed outcome/reason variants.

| Fact | Information to retain |
|---|---|
| `RunStarted` | Pinned normalized definition, versioned workflow input, and origin metadata |
| `ActivityScheduled` | Node and activation identity, activity contract, and exact bound input |
| `ActivityClaimed` | Attempt identity, ownership generation, and worker session that obtained the claim |
| `ActivityAttemptAbandoned` | Attempt identity, loss/timeout/cancellation reason, and effect uncertainty |
| `ActivityResultRecorded` | Accepted success or failure, output or durable failure record, attempt identity, and worker or operator origin |
| `RetryScheduled` | Chosen deadline, attempt budget state, and the failure classification that caused the retry |
| `SignalAccepted`, `SignalConsumed`, `SignalExpired` | Signal identity, payload, and the wait or expiry decision |
| `TimerScheduled`, `TimerFired`, `WaitTimedOut` | Durable deadline and the recorded resolution |
| `BranchSelected` | The chosen case/default edge of a `choose` node |
| `RunCancellationRequested`, `RunCancelled` | Requested and completed cancellation with user/deadline/operator reason |
| `RunCompleted`, `RunFailed` | Terminal output or failure |
| `RunNeedsIntervention`, `RunInterventionResolved` | The paused cause and an authorized resolution, actor, reason, and any added attempt budget |

Names must reflect what the engine knows. A committed claim is not proof that the worker entered its activity body. A recorded result is not proof of effects beyond the activity's declared outcome.

## Retain the data needed for replay

Events contain their data or immutable references to retained artifacts. Those artifacts include normalized definitions, activity and payload contracts, exact inputs and outputs, and signal payloads.

An expiring URL, mutable application row, or reference to the latest definition is not a sufficient historical record.

Payloads may be stored once and referenced by history and materialized state. Garbage collection must respect every retained event and checkpoint that requires them.

Preserve selected deadlines and jitter, not only the policy that could calculate a new deadline. Preserve selected branches, not only the condition that a future interpreter might evaluate differently.

This is replay of the workflow engine's observations and decisions. Replaying a recorded activity outcome substitutes the whole outcome. It does not reproduce the activity's internal HTTP calls or make changed activity code deterministic.

## CQRS without extra infrastructure

Command handling, event storage, domain state, and read models remain in the same embedded engine and redb store.

Start with synchronously maintained read models. CQRS does not require eventual consistency. Authoritative queries still use the leader's linearizable-read protocol and an appropriate applied-state barrier.

Runtime scheduling, ownership, and eligibility use current authoritative domain and coordination state. Ready and deadline indexes are updated in the apply transaction. A lagging dashboard projection must never grant a claim.

Asynchronous reporting projections are outside v1. A later extension must expose its processed position and use notifications/resynchronization rather than a periodic event-table poller.

Query responses can describe workflow state and join it with current operational information. They must distinguish that current lease information from an event-sourced historical workflow view.

Keep public query and event types independent of OpenRaft's internal Rust types. A run sequence is a history cursor, not a Raft term or replication-log position.

## Replay has a read-only default

The initial replay foundation is a pure projector over retained history and immutable artifacts. It reconstructs logical workflow state without invoking the live decision path.

The projector has no activity dispatcher, worker credentials, command sender, or scheduler wake capability. Do not implement this boundary as an `is_replay` flag scattered through the live runtime.

Build projections in an isolated context or store. Reconstructing a historical `Ready` activation must not insert executable work into the live group's ready index.

Different replay capabilities need different contracts.

| Capability | Contract |
|---|---|
| Rebuild workflow query models | Fold recorded events through compatible event readers and reducers |
| Inspect historical workflow state | Stop at a retained run sequence, with its required checkpoint and artifacts |
| Simulate changed workflow logic | Outside v1; no live calls or inferred missing outcomes |
| Fork execution from a prior point | Outside v1; a later command must define new identity, provenance, and result reuse |
| Reissue an external effect | Not a replay operation; v1 permits only the explicit intervention/retry policy |

Simulation must not reuse an old result for a materially different invocation merely because the node name matches. Activity version, scope, and actual input identity matter.

If a simulation needs an outcome that history does not contain, it must stop or use an explicitly supplied simulated outcome. It must not silently call the live external service.

A later live-fork design must not rewrite the source history and must specify new run identity, provenance, output reuse, and business-effect keys. V1 exposes no implicit fork or resume-from-history command.

## Checkpoints, snapshots, and retention

These storage mechanisms have different purposes.

| Mechanism | Purpose |
|---|---|
| Run checkpoint | Cache logical workflow state through a particular event sequence to shorten reconstruction |
| Raft application snapshot | Transfer or recover replicated application state, including retained events, artifacts, current coordination state, and required metadata |
| Reporting projection snapshot | Replaceable derived data for a particular query model |

Raft compaction does not delete logical workflow history. A run checkpoint alone does not preserve the events before it.

Retaining only a checkpoint and event tail would lose earlier intermediate states. V1 instead retains complete active-run history and reports unavailable history explicitly after its completed-run retention window.

V1 retains complete active-run history. The default full-history/artifact window is 30 days after termination, with terminal summaries and start/signal tombstones retained for 90 days. Command results are retained for 24 hours.

Create checkpoints every 256 workflow events and at termination, retaining the latest checkpoint. V1 does not prune an active run's event prefix. Missing or expired history produces an explicit unavailable-range result, not a partial reconstruction presented as complete.

Retention periods are captured for a run at admission. A later policy change does not silently shorten the promise for existing runs. Deletion becomes eligible at the recorded deadline and proceeds through committed bounded cleanup commands.

Event-history retention and business deduplication retention are separate contracts. Rebuilding a query model must not recreate expired permissions, discard current deduplication records, or change which repeated requests the engine accepts.

Historical replay is not a disaster-recovery procedure for active leases or voter membership. Full member recovery still follows the Raft log and snapshot protocol.

## Versioning is a long-lived contract

Record an event version from the first release. Preserve readers for retained versions or use explicit deterministic upcasters that convert their representation.

Do not edit historical event bytes to make them resemble what a newer engine would have decided. Corrective actions append new facts. Simulation of changed logic belongs in an isolated history or a new run.

Event readers, checkpoints, payload codecs, and normalized graph versions must remain compatible for the advertised replay window.

Storage members must also agree on event production and application rules while processing the same committed commands. An offline projection upgrade does not authorize one live replica to interpret workflow events differently.

## Cost and initial scope

The first durable implementation includes versioned workflow events, pure event evolution, materialized current state, and query models in the atomic apply boundary.

Reconstruction scenarios establish that workflow history is complete. Rich history interfaces, changed-code simulation, and live forks are explicitly outside v1.

Retained history increases disk usage and snapshot-transfer cost. Event-schema evolution and artifact retention become operational obligations.

No broker, second database, general-purpose event-store product, or requirement for host applications to adopt CQRS is introduced. The selected pattern belongs to the workflow engine's domain.
