# Execution and failure semantics

Historical design notes. [The v1 specifications](../specs/v1/README.md) now require loops, parallel scopes, compensation, and the expanded activity contracts.

Queue deduplication is a resource and contention improvement. It is not an exactly-once execution guarantee.

YAML definitions, clustered workers, and durable local development are required. The domain guarantees apply to every authoring path and membership topology over redb and OpenRaft, including when a partitioned worker resumes after peer takeover.

One-member local mode uses the real durable log and state-application path. It does not establish multi-member quorum availability, partition recovery, or leader-change behavior.

The selected design separates logical work, execution attempts, and external effects. It also separates authoritative workflow events from current coordination state and the Raft replication log.

## What the engine can promise

| Concern | Proposed guarantee | Necessary condition |
|---|---|---|
| Starting a run | One committed run for a namespaced `StartKey` | A unique durable admission record and retained deduplication state |
| Retrying an engine request | The same accepted command identity resolves to its recorded result within retention | Immutable command content and replicated result deduplication |
| Creating work | One activation for each logical invocation | Stable persisted activation identity and transactional creation |
| Completing work | One accepted terminal result for the current claim | Conditional updates and atomic dependent-state changes |
| Retrying an activity | At-least-once attempts when policy permits retry | Durable attempt records and recovery |
| Waking the scheduler | Repeated hints do not create more work | Hints are not the authoritative queue |
| Reconstructing workflow state | Recorded facts can reproduce logical state within the retained range | Complete versioned history or a checkpoint and tail, required artifacts, and compatible event readers |
| External side effects | No general exactly-once promise | The external system must cooperate through idempotency or a shared transaction |
| Local restart | A restarted member resumes eligible durable work | The same durable member identity, log and application recovery, and the declared policy |
| Cluster worker failover | Another compatible worker can resume eligible work | An available quorum, a live worker, committed expiry recovery, and a policy that allows progress |

Retention is part of idempotency. Deleting a `StartKey` or `SignalId` tombstone permits an old request to be accepted again. Define the supported deduplication window.

Raft log compaction must not remove retained workflow events, required artifacts, command results, or business deduplication records. Snapshots preserve those records independently of the truncated log.

History retention and deduplication retention are different promises. Rebuilding a query model must not change current acceptance of repeated requests or recreate a live lease.

## Stable effect keys, changing attempt tokens

An idempotency key identifies the logical effect. It stays the same across crashes, activity retries, and worker replacement.

An ownership token identifies permission to commit a particular attempt's result. It changes when ownership changes.

A Raft term identifies leadership history, not activity ownership. A leader change does not by itself replace every valid claim.

Using an attempt number as the external idempotency key would tell a payment provider that every retry is a different charge. These values need distinct Rust types and distinct names.

An activity that performs several independently retryable effects needs an explicit sub-operation key for each effect, or should be split into separate activities.

The default key can identify a run activation. Deduplication across separate runs needs an application-supplied business identity, such as the identity of the order being charged.

If the remote system cannot deduplicate or report whether an effect occurred, an ambiguous attempt can require operator intervention. Automatic retries are not always the safe default.

## Failure timelines

| Failure point | Durable situation | Required behavior |
|---|---|---|
| Before run admission commits | A local log entry may exist, but no accepted run is guaranteed | Do not execute an activity or report admission success; an uncertain caller retries the same identity |
| After quorum commitment but before leader application | The committed command still needs application | Recover from the committed log and apply durably before acknowledging success |
| After admission commits but before the response | Run exists, caller may not know | A repeated request resolves to the same run |
| After durable application but before a wake | Durable ready work exists | Reconnection, new-leader catch-up, or supervised restart reconstructs scheduling state |
| After a claim commits but before user code starts | Attempt may have started from the engine's perspective | Recover or reclaim under the declared retry policy |
| After a remote effect succeeds but before output commits | Engine cannot know the remote result from its own store | Reuse the logical effect key, query the remote result, or enter intervention |
| After output and successor work commit | Activation is complete and dependents are durable | A repeated result or wake cannot repeat the transition |
| During event append and state evolution | History, domain state, coordination, and applied metadata belong to one transaction | Observe the complete application result or none of it, never changed workflow state without its events |
| After a worker loses ownership | An older computation may still be alive | Reject its engine writes and cancel it cooperatively |
| During a clustered worker network partition | Peers may reclaim an expired lease while the old activity still runs | Reject stale completion and renewal, and apply the declared external-effect policy |
| When another clustered worker starts or restarts | Existing peers can still own valid claims | Preserve their claims; reclaim only through the normal expiry protocol |
| While a timer or signal wait is pending | Wait remains durable | Reconstruct it and resolve it at most once |
| After cancellation commits but before an activity stops | The external operation may still finish | Reject disallowed graph advancement and report the uncertain external outcome |
| During snapshot installation | Application state and its applied position must agree | Recover either the old or complete new state without overwriting member-local identity |
| After quorum loss | No new state transition can be confirmed | Stop granting work and report unavailability or an unknown outcome, never create a standalone replacement authority |

No workflow engine can close the remote-effect crash window by holding a local mutex or adding a unique queue index.

Replicated apply updates state only. Executing an activity on each replica would duplicate its external effects, even when the Raft protocol itself is correct.

## Durable signals, not transient pub-sub

Start with directed signals to an existing run. Require a stable `SignalId` from the caller. Persist the signal before acknowledging it.

A signal sent before its wait exists records `SignalAccepted` and enters the derived durable inbox. Applying wait creation records any matching consumption in the same redb transaction as its derived state.

Retain the accepted signal payload in history or an immutable artifact even after it leaves the current inbox, for as long as the replay policy promises it.

Signal admission, wait creation, signal consumption, and timeout resolution are ordered engine commands. Their preconditions are evaluated against current applied state, not only against a worker's prior read.

V1 uses one signal name for one wait node in a run's finite DAG. At most one unconsumed message exists per name. Broadcasts, stream subscriptions, and multiple consumers are outside v1.

A repeated signal ID with the same envelope returns the original acknowledgement. The same ID with different content is a conflict, not a silently accepted duplicate.

Signals to unknown, terminal, or cancelling runs return explicit errors. Reject a new ID for a name that already has a pending message or resolved wait. Buffer expiry defaults to seven days; expiration is recorded without deleting the historical payload before its history deadline.

### Signal versus timeout

Treat this as one atomic choice, not two tasks that independently set flags.

An active wait accepts a signal only before its deadline. Timeout wins at equality. Installing a wait first checks its buffered eligible signal under the same serialization rule. A YAML wait explicitly chooses a finite timeout or null for an indefinite wait.

This favors a clear operational deadline over event-time interpretation. If the product needs a message that arrived at an upstream system before the deadline to count despite late delivery, it needs a different contract.

Record the winning decision durably. A losing signal cannot revive an expired activation. A signal that arrived early but remained unmatched can expire according to the inbox policy.

## Retry policies distinguish kinds of failure

Expected activity failures belong in typed errors. The static activity catalog classifies codes as retryable or terminal. YAML may retry only a declared retryable code.

Include maximum attempts or elapsed retry budget, a persisted next deadline, backoff, and jitter. Record nondeterministic decision inputs in commands and the chosen domain outcomes in events. Neither replica application nor historical replay redraws the delay.

Storage failures, leader redirects, and quorum unavailability belong to the runtime. They are not business activity errors and must not consume the activity's retry budget.

A panic, timeout, or lost worker produces an unresolved attempt, not a fabricated business failure. V1 uses unwind-enabled builds and converts a worker-task panic into abandonment when it can observe it. Process abort and termination follow the ordinary lost-worker path.

Activity registration explicitly chooses `RetrySafe` or `Manual`. The former permits bounded re-execution with the same input/effect identity; the latter pauses for intervention. The default total attempt budget is five and the maximum configurable budget is ten.

Reported failures retry only when listed in `retry.errors`. A `RetrySafe` lost attempt can recover within the same budget even when that list is empty. Unknown outcomes with an exhausted budget enter intervention.

Activity deadlines, heartbeat deadlines, retry budgets, and a run deadline are different policies. A retry must not reset the run's overall deadline.

## Cancellation is not rollback

Record a cancellation-request event before reporting acceptance. Derive the new lifecycle state, prevent new ordinary activations, notify running activities, and record the resulting wait resolutions.

Use cooperative cancellation for activity futures. Dropping a future cannot guarantee that a remote request did not happen. Blocking work may not stop merely because its async wrapper was cancelled.

Distinguish `CancelRequested`, completed cancellation, activity timeout, and business failure in status and diagnostics. Do not release ownership and start a replacement while the old owner is still permitted to commit.

Shutdown is local to a worker, not a cancellation of its runs. Stop new claims on that worker, drain or cancel its activities within a deadline, commit known results, and report unresolved attempts.

In clustered mode, peers continue. In local mode, durable waits remain pending until the application restarts. Stopping the local application does not clear its database.

The local cancellation grace is five seconds. Default worker drain time is 30 seconds. An optional run deadline uses the cancellation path with a deadline-exceeded reason; there is no implicit run timeout.

Maintain ownership while draining. Hand unresolved attempts back only through a fenced transition or normal lease expiry. Closing a local client stops that client's admission, not admission across the cluster. The host owns process signals and the Tokio runtime.

Removing a data-bearing voter is different from draining a worker. A voter shutdown leaves its membership intact. Planned removal must follow the membership protocol while the group can still commit.

Tokio's [shutdown guide](https://tokio.rs/tokio/topics/shutdown) separates notification from waiting for tasks to finish. Both are needed here.

## Definition upgrades are data migrations

Pin each run to an immutable workflow version. Preserve activity and payload decoders needed by active runs.

Compile YAML before publication and persist the normalized graph with its format version, dependency contracts, and source provenance. Editing or deleting a source file does not alter a running instance's definition.

Stable `NodeKey` values survive source reordering. Renaming a durable node, changing a branch identity rule, or changing a payload representation is an explicit compatibility change.

A definition digest can detect changed graph structure and declared schemas. It cannot prove that a function body has not changed. The application must version behavior changes, and deployment metadata should record the build that executed each attempt.

Normalize execution-relevant content before hashing. YAML comments, formatting, and mapping order do not change the graph's identity. A semantic change under an existing workflow version is a conflict, not an in-place update.

Claim eligibility must include the worker's supported graph format, activity versions, and codecs. A worker missing a required handler leaves that work for a compatible peer rather than globally failing or blocking the run. Workers load the published graph rather than requiring the original YAML or a Rust workflow type.

Expose unclaimed work with its required definition and codec versions. Do not infer that no compatible worker exists merely because one worker lacks a handler. Reject conflicting registrations for the same immutable definition version.

Initially support new runs on new versions and old runs on their original versions. A rolling deployment must retain compatible worker capacity for active versions. Defer live instance migration until there is a protocol for mapping activations, waits, payloads, and side-effect identities together.

Workflow-only YAML changes can publish new versions using already deployed activity contracts. New Rust activity code still requires deployment. Neither file reload nor compiler upgrades may silently replace an active run's graph.

Replicated command, event, and graph interpretation need coordinated engine upgrades. Enable new formats only after the replicas that must apply them are compatible. Activity-worker capability differences do not permit storage replicas to interpret the same command differently.

Preserve event and payload readers for retained history, not only for currently running workflows. Upcasters change representation explicitly. They must not rewrite historical facts to match a newer retry or branch policy.

## Historical replay is not execution

Reconstruct logical workflow state by applying recorded facts through a pure projector. Do not call the live command decision path, schedule an activation, or invoke an activity handler.

The projector receives history and retained artifacts, not worker credentials or a command sender. Its output belongs to an isolated view or projection store, never the live ready index.

Current lease deadlines and other coordination state are not fully event-sourced. Workflow replay cannot restore their live authority or replace the member-recovery protocol.

Simulation of changed logic and live forks are outside v1. Intervention permits an administrator to supply an observed result, authorize another attempt on the same input/effect identity, fail, or cancel. A restore/resource-paused continuation with no unresolved activity effect can resume unchanged. Every resolution requires the expected run version, actor, and reason.

Event sourcing does not fill the gap where an external effect succeeded but no result was committed. The existing ambiguity and intervention policy still applies.

## Compensation is outside v1

Keep the useful idea of compensating activities, but do not describe compensation as a database rollback.

A compensation can fail, retry, time out, or have an ambiguous result. Its identity must differ from the forward effect while remaining stable across its own attempts.

Persist which forward activations completed and which compensations completed. Parallel branches need a dependency-aware compensation order, not a blanket reversal of wall-clock completion order.

V1 has no automatic saga or compensation API. An application can start an explicit cleanup workflow. Any later compensation feature must implement the durable identities and failure rules above rather than reverse effects through event replay.

## Diagnostics are part of the design

A maintainer should be able to answer why a run is not advancing without reading source code or inspecting a broker.

Expose the pinned definition, current activations, wait reasons, next deadlines, attempt history, worker session, ownership generation, last committed transition, and blocked reason.

Use `tracing` spans keyed by run, activation, and attempt. Do not log activity payloads by default.

Track ready age, timer lateness, claim conflicts, stale-result rejections, admission pressure, recovery duration, and notification reconnects. Queue depth alone cannot describe a durable workflow's health.

Authoritative workflow events replace the earlier diagnostic-journal-only proposal. Capture complete logical lifecycle facts and their required data from the first durable release, with an explicit retention and reconstruction contract.

Historical workflow views must identify their event sequence and available range. Current operational details, such as renewed leases, are a separate view and must not appear to come from complete historical event replay.

Report cluster identity, member identity where applicable, worker session, leadership, commit and applied positions, snapshot progress, and claim ownership. Local mode also reports its resolved data directory.

Never fall back to memory after a storage error or bootstrap a standalone group after quorum loss. Neither restores the unavailable authority. Existing data directories retain their membership and cannot change topology merely because a constructor changed.
