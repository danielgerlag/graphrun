# Language, schemas, and basic primitives

The YAML and Rust frontends must compile into the same versioned normalized IR. The IR contains data and registered handler references, not executable host-language closures.

## Workflow and region grammar

A workflow contains `dsl: graphrun/v1`, `id`, positive `version`, `input_schema`, `output_schema`, `start`, `nodes`, and optional `signals` and `run_timeout`.

A nested region contains `input_schema`, `output_schema`, `start`, and `nodes`. Container nodes own regions through `body` or named parallel branches.

Node keys match `[a-z][a-z0-9_]{0,63}` within a region. Workflow, activity, event, and branch names are case-sensitive ASCII names up to 64 characters. Stable identity uses names and scope ancestry, never map iteration order.

Every node has one `kind`. Reject unknown fields and fields belonging to another variant. Edges are local region node keys.

Root `complete` completes the run. Nested `complete` returns the region's result to its owner. `fail` fails the current region and lets its owner perform settlement/compensation.

## Schema references

Support these schema-reference constructors in YAML and IR.

```yaml
input_schema: order/v1
output_schema:
  tuple:
    - tax/v1
    - shipping/v1
```

`SchemaRef` is `Named(name/version)`, `Array(element)`, or `Tuple(ordered elements)`. Composite identity includes its constructor and child nominal identities.

Rust `Vec<T>` corresponds to `Array(T)` and supported Rust tuples correspond to `Tuple(...)`. Provide built-in scalar/unit schema references and tuple support through 16 elements.

Named schemas do not become interchangeable merely because their JSON representations resemble each other. Whole-value bindings require matching nominal/composite identities.

Named payloads use JSON Schema Draft 2020-12 and stable registered keys. Support bounded objects, arrays, fixed tuples, unions, local `$defs`, required/nullability, primitive constraints, and asserted `uuid`/`date-time` formats.

Do not fetch schemas over the network or from files during compilation. Reject recursive/external/dynamic references, regex-based schema operations, unknown formats, and custom executable validators.

Field projection and object assembly use conservative structural analysis, not a claim to solve arbitrary schema subtyping. Actual data must also pass its schema and Rust decoder.

## Binding grammar

A binding is exactly one of:

| Variant | Meaning |
|---|---|
| `{from: reference}` | Whole immutable value |
| `{from: reference, path: "/field"}` | JSON Pointer projection |
| `{literal: value}` | Constant JSON-compatible value |
| `{object: {field: binding, ...}}` | Construct an object |
| `{array: [binding, ...]}` | Construct an ordered array/tuple |

References are:

| Reference | Availability |
|---|---|
| `workflow.input` | Immutable original run input |
| `scope.input` | Current region's captured input |
| `scope.id` | Current durable scope identity as a string, useful as an event correlation key |
| `nodes.<key>.output` | Successfully completed local node available on every incoming path |
| `loop.state`, `loop.index` | Captured nearest loop frame, with the guard/body semantics in the loop spec |
| `item.value`, `item.index` | Captured nearest foreach item frame |
| `forward.input`, `forward.output` | Compensation-input binding only |

Child scopes do not implicitly read arbitrary parent-local node outputs. Capture them in the child's declared input.

Values are immutable. There is no assignment to shared workflow data, implicit string interpolation, reflection, SQL, shell, JavaScript, or Rust evaluation.

Missing required data is an error. Explicit null is a value. Numeric kinds are not silently coerced. All resource limits apply before storing a definition or launching an activity.

## Conditions

Conditions are typed bounded trees with exactly one operator:

| Operator | Rule |
|---|---|
| `eq`, `ne` | Two scalar bindings of the same declared kind |
| `lt`, `le`, `gt`, `ge` | Two numeric bindings of the same numeric kind |
| `all`, `any` | Nonempty conditions, left-to-right short-circuit |
| `not` | One condition |
| `exists` | Reference binding; missing is false, explicit null is present |

No arithmetic, regex, network, or arbitrary function calls are evaluated by the control engine. Use a named activity to calculate complex next state.

Record the selected condition outcome in history. Replay must not reevaluate a historical guard under changed code.

## Activity

Required fields are `activity: {name, version}`, `input`, and `next`. Optional fields are `timeout`, `retry`, and saga compensation metadata.

The activity contract determines input/output schemas and execution kind. On activation, bind and record exact input, create one logical activation, and make its leaf work eligible.

Accepted success exposes the recorded output and follows `next`. Error, timeout, loss, cancellation, and reconciliation use the activity contract and owning scope.

An activity's retry is another attempt of the same activation, input, and effect identity. It is not another graph node or loop iteration.

## Exclusive branch

A `choose` node requires `input`, a nonempty ordered `cases` list, a `default` region, and `next`. Each case contains a stable unique `name`, `when`, and `body` region.

All case/default regions accept the captured input schema and declare the same output schema. Evaluate conditions in the parent context, select the first true case or default, and commit that choice and one child scope.

The node exposes the selected child's successful output and then follows `next`. A child failure is propagated through normal scope ownership. Unselected regions create no activations or effects.

This structured choice avoids implicit cross-branch output variables. It is the same construct used by the typed Rust builder, not a second Rust-only branch operator.

## Relative and absolute time

`delay` requires `duration` and `next`. Duration is a positive unit string or a binding producing positive integer milliseconds within policy limits.

Capture the duration and due time once when activated. The node completes only through a committed current timer resolution. It exposes no output.

`wait_until` requires `at` and `next`. `at` is a binding/literal producing UTC Unix milliseconds. Past deadlines become immediately eligible.

Restart, snapshots, and leadership changes preserve the deadline. They must not restart a relative countdown. Superseded/cancelled timer revisions cannot advance the graph.

The clock/lease accuracy and fault assumptions in the storage specification apply. These primitives are not real-time guarantees.

## External events through `wait_signal`

`wait_signal` is the durable external-event wait primitive. It is not a Tokio notification and is not the engine's internal workflow-event history.

Root `signals` declares event names and payload schemas. A wait requires `signal`, a string-valued `key` binding, `timeout`, and `next`. Finite timeout requires `on_timeout`; null is an explicit indefinite wait and forbids that edge.

An event address is `(RunId, declared signal name, correlation key)`. Repeated loop waits can use the same name. Concurrent waits must use distinct keys.

At most one unresolved wait owns an address at a time. A conflicting activation fails with `WaitKeyConflict` and normal parent settlement. It must not deliver nondeterministically to one of several waiters.

The producer supplies an event ID, address, and payload. Acceptance validates the run/name/schema/quota and persists the event before acknowledgement.

The same event ID with identical address/payload returns its original receipt. The same ID with different content conflicts. Different event IDs are distinct messages, even if their values are equal.

Inbox order is committed acceptance sequence, not an upstream event timestamp. Unreserved buffered events expire according to the seven-day default in [policies and defaults](10-policies-and-defaults.md).

The wait's `consume_from` is `buffered` by default or `after_activation`. The latter excludes events accepted before this wait occurrence was created.

An inbox entry is available, reserved to one specific wait, consumed, or expired. At event acceptance and wait creation, atomically reserve the earliest available, unexpired eligible event for the active wait. A new event cannot bypass an older eligible entry.

Reservation requires matching address/selection bounds and, for a finite wait, acceptance time strictly before its deadline. Reservation records both event ID and wait ID/revision. It may be consumed immediately or completed by later control progression.

Once reserved, the event cannot be removed by inbox-TTL cleanup while that wait is pending. Resolve the reservation by recording consumption and wait success in one transaction, even if processing resumes after the original inbox TTL or wait deadline.

Timeout resolution first honors an existing valid reservation, then attempts reservation of eligible available input, and times out only if neither exists. An event accepted at or after the deadline cannot revive that wait.

An unreserved event can expire before a waiter exists. Acceptance before a future deadline alone does not promise indefinite availability. This differs from a message already reserved to an active wait.

Cancelling a pending wait atomically releases its reservation. Return the event to available with its original acceptance sequence and expiry, or record expiration if that expiry has passed. Release must not reset TTL or create a second accepted event. Cancelling an already satisfied wait never releases its consumed message.

Late events can remain buffered for a later occurrence at the same address. Producers that mean a particular occurrence must use a unique correlation key. Do not silently reinterpret a queue address as a wait-generation identifier.

Signals to unknown/terminal/cancelling runs, unknown names, or invalid schemas are rejected. While a run is blocked for intervention, input may be durably accepted and matched, but new ordinary activity dispatch remains disabled.

On success, the node output is the declared signal payload. On timeout/cancellation there is no successful output. Availability analysis must be edge-sensitive: the wait node dominates both edges structurally, but its output exists only on its success edge.

V1 has directed correlated events, not a global broadcast broker. An application can resolve its business reference to a run and signal it; this lookup is not a separate queue provider.

## Complete and fail

`complete` requires an `output` binding matching its region output schema. It records one terminal successful region result.

`fail` requires `error.code` and a literal bounded `error.message`. It records a terminal region failure, not a successful payload.

For `unit` output, bind literal null. A nested terminal node must notify its owning loop, parallel, or saga controller rather than directly terminating the run.

## Structured containers

The mandatory container node kinds are `while`, `do_while`, `repeat`, `foreach`, `parallel`, and `saga`. Their fields and state machines are defined in the companion specifications.

Each body is an explicit region. Limits apply to the whole compiled definition, nesting, materialized scopes, payloads, history, and execution concurrency.

There are no unstructured jumps or separate break/continue nodes. Early loop exit is expressed through carry state and the next guard, or through failure/cancellation. Use a conditional loop rather than `repeat` when early successful exit is required.

## Compiler and publication contract

Use marked YAML parser events. Reject duplicate keys before ordinary map construction, multiple documents, tags, anchors, aliases, merges, and includes.

Resolve names, schemas, local edges, region boundaries, binding availability, control types, limits, and compensation references before publication.

The normalizer expands defaults, assigns stable definition paths, normalizes composite schemas and expressions, and computes a SHA-256 digest over versioned canonical bytes. Source formatting and map order do not change identity; ordered cases and branches do.

Rust and YAML definitions that mean the same graph must normalize identically. Source spans are diagnostic metadata, not execution identity.

Publication is immutable and idempotent for equal name/version/digest. Different semantics under an existing version conflict. Every run pins one published definition, including all child regions and handler contracts.

Retained history keeps required definition/schema/artifact data after the source file changes or disappears.
