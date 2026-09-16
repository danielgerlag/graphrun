# Domain model and execution

The organizing structure is a tree of durable execution scopes over a normalized graph of regions. A flat list of step IDs or one mutable workflow-data object is insufficient.

## Definitions and identities

| Type | Identity and meaning |
|---|---|
| Definition | Workflow name, explicit version, normalized graph digest, and format version |
| Region | A named graph with input/output schema contracts and its own node namespace |
| Definition node path | Stable region/branch/body names plus a local node key, never a vector offset |
| Run | `RunId`, pinned definition, admission input, and policy version |
| Scope | `ScopeId`, run, parent activation, role, and captured input/context |
| Activation | `ActivationId`, scope, definition node path, and logical invocation |
| Attempt | Activation, execution role, and monotonically allocated attempt identity |
| Claim | Attempt, worker session, owner generation, lease revision, and deadlines |
| Wait | Stable `WaitId`, owning activation, event address/deadline, and resolution state |
| Compensation obligation | Forward activation plus compensation role, handler contract, captured input/output, and state |
| Event | Run and monotonically assigned run sequence, with versioned payload and command/principal cause |

Persistent member identity, worker session, owner generation, Raft term, activation identity, and external effect identity MUST be distinct types.

Loop iterations and foreach items create new logical scopes/activations. Retrying an activity does not. Compensation uses a distinct effect role but retains stable identity across its retries.

## Regions and lexical data

A region is structurally acyclic. Repetition occurs only through the explicit loop node variants, never a `next` edge pointing backward.

An edge cannot escape its region. A region's `complete` node yields a value to its owner. Only completion of the root scope can complete the run.

Each scope captures immutable input and context when created. Local node outputs become immutable artifacts on successful completion. A child receives parent data only through explicit input bindings or the immutable root workflow input.

`nodes.x.output` always refers to a node in the current region. It cannot mean the latest similarly named node in another branch or iteration.

The compiler rejects references unavailable on any path reaching the consumer. It must also reject cross-region handles in the Rust builder unless they are captured as explicit child inputs.

## State machines

Represent alternatives as enums with required payloads. These names define semantics; implementation may split records while preserving the states.

| Entity | States |
|---|---|
| Run | Active, stopping with failure/cancellation intent, blocked with continuation, terminal success/failure/cancellation |
| Scope | Open, waiting, settling, compensating, blocked, completed, failed, cancelled |
| Leaf activation | Ready, claimed, executing/awaiting result, retry waiting, succeeded, failed, abandoned, cancelled |
| Container activation | Opening children, waiting for children, settling children, resolving, terminal |
| Wait | Pending, satisfied, timed out, cancelled |
| Inbox entry | Available with expiry, reserved to a specific wait/revision, consumed, expired |
| Compensation obligation | Registered, blocked input with captured forward artifacts, ready, claimed, retry waiting, completed, released, blocked execution, abandoned |

Do not encode these as independent `active`, `complete`, `cancelled`, and `compensated` booleans.

A scope's terminal outcome is immutable. Repeated completion observations return the existing outcome or conflict. They do not create another parent transition.

## Commands and event sourcing

Commands enter the committed Raft path. Deterministic decision logic evaluates current domain and coordination state and emits facts. Pure evolution applies those facts to materialized state.

Each apply atomically records events, domain state, coordination, critical indexes, command/dedup results, and Raft applied/membership metadata.

Capture these facts explicitly when their primitives are present.

| Event family | Required information |
|---|---|
| Run lifecycle | Pinned definition/policy, input, cancellation/failure intent, intervention, terminal result |
| Scope lifecycle | Parent, role, captured input/context, expected child set, terminal outcome |
| Activity | Bound input, handler/version/role, attempts/claims, accepted result, abandonment, reconciliation |
| Loop | Initial state, captured count/collection, guard decisions, iteration identity, carry updates, final result |
| Parallel | Frozen named branch set, child outcomes, settlement intent, one join resolution |
| External input | Event identity/address/payload/acceptance sequence, delivery reservation/consumption, expiry |
| Time | Chosen deadlines and the resolution of a specific current wait/revision |
| Saga | Registered obligations, nesting transfer, compensation start/results/retries, unresolved effects and final summary |

The exact serializer uses versioned tagged records. Event kinds must reflect observed engine facts, not claim that a body entered merely because it obtained a claim.

Current lease renewal, session health, and other coordination remain operational records. A lifecycle consequence of expiry must still emit a workflow event.

## Data required for reconstruction

Retain the normalized definition, schemas/contracts, exact bound activity inputs, accepted outputs/failures, event payloads, frozen collections, loop carry values, selected branches, chosen deadlines/jitter, and compensation inputs.

Store values inline or through immutable retained artifacts. A mutable external URL or latest-version lookup is not historical evidence.

Event replay uses recorded decisions. It does not rerun loop guards, current YAML, activities, or compensation handlers.

## Control progression

Container progress is durable work, not a long-lived future that must survive a process restart.

When a child completes, the parent decision validates the parent state and expected child identity, records the observation once, and decides whether to open another child or resolve the container.

Frozen child sets and loop cursors are source facts. Absence of currently running children does not prove a container is finished.

Bound automatic control advancement to 32 transitions per committed progression command. If more remains, persist another progression obligation and wake from applied state. Do not spin recursively through unbounded nested controls.

Activity execution permits belong only to leaf attempts. Waiting containers, child scopes, timers, and event subscriptions do not hold them.

## Failure and cancellation propagation

A leaf failure first resolves its scope. A scope owner handles it according to its primitive: a loop stops, parallel begins sibling settlement, and a saga begins abort/compensation processing.

A child `fail` does not directly mark the entire run terminal while parents still have cleanup obligations.

Claim eligibility includes every ancestor scope's stop/block state. The same committed failure/cancellation records the relevant stop barrier before another ordinary claim can succeed. Deferred cleanup bookkeeping must not leave a window for new sibling effects.

Run cancellation closes ordinary forward dispatch. Existing authorized results may settle in-flight work but cannot schedule new forward effects. Waiting controls are cancelled or settled deterministically.

Any unresolved manual effect can block the run. While blocked, accept durable external input and settle already authorized outcomes, but do not grant new ordinary forward work. A blocked state retains its continuation explicitly.

After intervention, resume only the recorded continuation permitted by that resolution. Do not reconstruct a new execution path from current source files.

## Operator actions

Intervention commands require the expected run/scope version, target attempt/obligation, principal, and reason.

Supported actions include supplying a validated observed result, authorizing another attempt on the same logical input/effect identity, resuming a continuation with no unresolved effect, failing/cancelling, and explicitly abandoning compensation.

Abandoning compensation produces a terminal outcome that identifies unresolved obligations. It must never report successful rollback or complete compensation.

## Invariants

1. Every logical workflow change has a corresponding authoritative event.
2. One command retry does not produce another event range, child scope, or effect identity.
3. One logical activation can have many attempts but at most one accepted success.
4. Parent completion occurs once and only after its required child/cleanup outcomes.
5. A completed loop iteration never reruns merely because the leader changed.
6. Branch and iteration outputs cannot overwrite one another.
7. Compensation completion remains durable across retries, snapshots, and nesting.
8. No activity, reconciliation, or compensation body runs inside replicated apply or historical projection.
9. Runtime ownership decisions use current committed coordination, never a reporting projection or historical replay.
10. Quota exhaustion cannot erase obligations or prevent already admitted work from settling.
