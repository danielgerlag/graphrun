# Activity types, reconciliation, and compensation

Activities are effectful or pure leaf work executed outside consensus application. Workflow controls are durable engine primitives, not activity callbacks that return arbitrary execution flags.

## Registered contract

Each activity registration declares:

| Property | Contract |
|---|---|
| Key/version | Stable identity, unrelated to Rust type names |
| Input/output schemas | Owned codec contracts, including composite schema references |
| Execution kind | Async or blocking handler; a publisher may register a contract without a local implementation |
| Effects | Pure or external; purity is an application promise, not a compiler proof |
| Recovery | Mandatory `RetrySafe` or `Manual` |
| Error catalog | Stable code/classification and typed mapper |
| Reconciler | Optional versioned outcome-probe contract |
| Timeout/budget | Values from the normative policies/defaults document, expanded and captured at the specified boundary |

The registry binds implementations to contracts. A worker may advertise only contracts it can execute. A publisher can validate a definition even when a matching worker is temporarily absent.

## Async activity

Use owned input, typed output/error, and a `Send` future. The context supplies run/scope/activation/attempt identity, execution role, stable effect key, cancellation, deadline, and diagnostics.

Captured application clients remain runtime values and are not serialized. Inputs and accepted outputs are recorded as immutable artifacts.

The engine requests cancellation but cannot guarantee that a remote operation stopped. A handler must cooperate before starting additional effects after its permission is lost.

## Blocking activity

A separate blocking contract returns a typed `Result` synchronously. Run it on a bounded blocking pool, never on a Raft/network/timer executor thread or the redb writer.

Blocking functions cannot be forcibly cancelled safely. Cancelling their wrapper or losing a lease fences state commits, not the thread or external effect.

The default blocking pool capacity is 16 per worker, included in the worker's total leaf limit. A blocked handler cannot consume consensus or completion-report capacity.

Do not start conflicting compensation merely because a blocking wrapper was dropped. Settlement follows the reconciliation rules.

## External worker activity

A definition can reference a contract without any handler in the admitting process. A worker in another process claims and executes it through the public worker protocol.

External execution is a deployment/binding mode, not a handler that recursively dispatches itself. The claiming worker executes its matching async or blocking implementation once.

Remote results carry the exact assignment identity, role, schema, and stable result-command ID. They are accepted only against current ownership/lifecycle.

The Rust worker SDK is mandatory. The protobuf/JSON worker contract must be documented and exercised directly, but separate non-Rust SDK packages are not required.

## Retry and ambiguity

Default forward budget is five attempts, maximum ten unless an explicit operator action grants an extension. A committed claim consumes an attempt even if the process dies before entering the body.

Reported forward errors retry only if their declared retryable code appears in the node policy. Compensation has its own selection rule below. The exact timeout, backoff, and retention values are in [policies and defaults](10-policies-and-defaults.md).

For an ambiguous active forward attempt, `RetrySafe` permits repeating the same logical input and effect key within budget. `Manual` requires reconciliation or intervention.

While aborting/cancelling a saga, do not repeat forward work merely to discover its outcome. A retry-safe create operation could create an effect that had not happened before cancellation.

## Reconciliation handler

A reconciler is an async, blocking, or external handler with an explicit outcome-probe role. It receives the original contract/input, effect identity, and attempt evidence.

Its result is one of:

| Result | Meaning |
|---|---|
| `Applied(output)` | The original effect conclusively occurred and the output satisfies its original schema |
| `NotApplied` | The original effect conclusively did not occur and cannot later take effect from that attempt |
| `Unknown` | Available evidence cannot decide safely |

An HTTP 404 while a delayed request may still apply is `Unknown`, not `NotApplied`.

The reconciler must not create the original effect. Querying an external operation ledger is appropriate. Reissuing the original charge is not.

Default reconciliation policy is three probes, 30 seconds apart, each with a 30-second timeout. Probe failure/unknown exhaustion enters intervention.

Reconciliation is separately claimed and fenced. A stale original worker does not gain authority merely by sending a late result. Results arriving under an existing still-valid settling claim can be accepted as settlement evidence without scheduling forward successors.

## Saga primitive

A `saga` node wraps one body region. Success returns the body's output. Failure or cancellation aborts forward progress and compensates the configured completed effects.

Required node fields are `input`, `body`, and `next`. The body input must match the captured binding; the saga output is the body's declared output schema.

For each external activity inside a saga, the definition must specify either a compensator or an explicit irreversible declaration with a reason. Pure activities need no compensator.

A compensator is a registered activity contract. Its input binding can use immutable `forward.input` and `forward.output` values. It cannot depend on mutable current application state.

The node field is `compensation: {kind: activity, activity: {name, version}, input: binding}` with optional `timeout` and `retry`, or `compensation: {kind: irreversible, reason: string}`. Retry accepts `errors`, `max_attempts`, and `backoff`, as defined by the policy document. Omission is allowed only for pure activities inside a saga. Compensation metadata outside a saga is a definition error.

Explicit irreversible effects remain visible in the saga outcome. Their presence prevents a claim of full reversal, even if every configured compensator succeeds.

## Obligation registration

When a schema-valid forward success is accepted, atomically record its result and its compensation obligation or irreversible marker. Compensation-input derivation must not turn that valid success into a rejected or unrecorded result.

If projection, schema validation, or size validation of compensation input fails, record a `BlockedInput` obligation instead. It retains the original binding reference, pinned handler contract, immutable forward input/output artifacts, and the binding error.

The same transaction records the successful forward result, blocked obligation, intervention state, and forward-dispatch stop barrier. Return an accepted result receipt identifying the block, not an error that suggests the forward activity should be attempted again.

A duplicate result command returns the original receipt and obligation identity without repeating events. A new result command for the same already-succeeded attempt follows the same outcome/conflict rule. The forward activation remains succeeded.

The obligation is identified by the forward activation and compensation role. Successfully materialized compensation input is immutable. A blocked obligation has no fabricated ready input.

An administrator may supply a schema-valid concrete compensation input to the pinned handler through a version-checked command with actor and reason. Record the supplied artifact and its provenance without editing the original binding or success history.

Resolving that block resumes only the recorded continuation permitted by the current saga/ancestor phase. It must not clear other blockers or advance ordinary work after failure/cancellation intent. Retries use the resolved input, not the broken binding.

No obligation exists merely because a forward attempt was claimed. An external success lost before result persistence remains ambiguous and must be settled before compensation can assume it happened.

## Saga abort state machine

```text
Forward
  -> ClosingForward
  -> SettlingForward
  -> Compensating
  -> Failed or Cancelled, with compensation summary

Forward, SettlingForward, or Compensating -> NeedsIntervention -> recorded continuation
```

Closing forward prevents new ordinary claims and successors in the saga subtree. Cancel waits/timers and request cancellation of running leaves.

Existing authorized results may settle forward attempts and register their obligations. Once a claim is fenced, use a separately authorized reconciler or operator resolution, not the stale result path.

Compensation MUST NOT start while an in-flight forward effect might still apply and its outcome is unresolved. The saga blocks for reconciliation/intervention instead.

## Compensation order and execution

Compensate serially by default. Use reverse committed obligation-registration order, which is a reverse linearization of the engine's causal forward dependencies. Independent branch ties therefore have deterministic order.

Compensation gets its own stable effect identity and attempt budget. It must never reuse the forward effect key as though undo were another forward retry.

The default key is a versioned stable encoding of run ID, the logical forward activation ID, execution role, and any explicit sub-operation key. It does not include the current worker, attempt, Raft term, or cluster ID. Restoring a group or retrying an obligation must not generate another business effect key.

Default compensation budget is ten attempts. Omitted `compensation.retry.errors` selects all retryable codes from the pinned compensator catalog; an explicit list selects only those codes, and an empty list selects none.

Terminal, unselected, or budget-exhausted reported failures block the obligation and saga. They do not silently skip undo or allow later compensation to bypass a failed causal dependency.

Compensation uses its own recorded backoff and timeout from the policy document. `RetrySafe` can recover ambiguous undo attempts within budget; `Manual` requires reconciliation/intervention regardless of the reported-error list.

Persist each accepted compensation completion before selecting the next obligation. Crash, duplicate result, leader change, or snapshot restore must not rerun a recorded completed obligation.

A compensation handler does not register compensation for itself automatically.

## Nested sagas

An inner saga owns its obligations while it runs.

On successful inner completion, attach its obligations to the nearest enclosing saga as one nested group, preserving internal order and identities. A later outer failure compensates that group in its proper outer position.

If the inner saga aborts and compensates itself, its completed obligations must not be compensated again by the outer saga. Propagate its failure/compensation summary.

Snapshots and replay preserve group ownership, transfer completion, and each obligation's terminal state.

On success with no enclosing saga, release the saga's obligations from automatic compensation. A later failure or cancellation outside that completed boundary does not reopen it.

A saga inside a loop therefore commits each successful iteration independently unless an outer saga encloses the loop. An outer saga enclosing a loop retains the successful iterations' obligations until that outer boundary completes.

## Intervention and terminal truth

An administrator can supply a validated observed outcome or blocked compensation input, authorize another attempt with the same effect identity, conclusively mark a forward attempt not applied, resume a safe continuation, fail/cancel, or explicitly abandon compensation.

Each action requires expected versions, actor, and reason. Repeating it is idempotent.

Abandonment reports unresolved effects/obligations and terminal failure or cancellation. It is not successful compensation or rollback.

No automatic state repair, replay, or operator convenience may claim that money was refunded or an effect was absent without a recorded accepted outcome or explicit operator attestation.

## Required evidence

Exercise every execution kind and role, cooperative/noncooperative cancellation, lost results, retries, schema failures, remote ownership conflicts, forward ambiguity, a delayed external effect, compensation retries, nested transfer, irreversible markers, operator resolution, and crash after each obligation completion.

The external-provider fixture must count physical requests separately from logical effects and prove that retries use the correct forward, compensation, and reconciliation identities.
