# Policies and defaults

This is the authoritative domain-policy table for v1. Do not recover defaults from the superseded brainstorming documents. Storage, clock, and resource limits remain owned by the storage specification.

## Timeouts and deadlines

| Policy | Default | Allowed configuration and meaning |
|---|---|---|
| Forward activity attempt timeout | 5 minutes | Node `timeout`, from 1 second through 24 hours |
| Compensation attempt timeout | 5 minutes | `compensation.timeout`, from 1 second through 24 hours |
| Reconciliation attempt timeout | 30 seconds | Registered probe policy, from 1 second through 5 minutes |
| Whole-run timeout | None | Optional root `run_timeout`, positive duration through 365 days |
| Signal wait timeout | No implicit value | YAML must specify a positive duration through 365 days or null |
| Relative workflow delay | No implicit value | Positive duration through 365 days |
| Worker cancellation grace | 5 seconds | Request cancellation, then drop the async wrapper without claiming the effect stopped |
| Worker drain timeout | 30 seconds | Stop new claims and maintain existing authority during drain |

Duration strings use a positive integer followed by `ms`, `s`, `m`, `h`, or `d`. There are no calendar months or implicit time zones.

At each accepted attempt grant, record `attempt_deadline = grant_engine_time + effective_attempt_timeout`. It is fixed for that attempt. Queue waiting before the grant is not execution time. A retry creates a new attempt and its own deadline.

Lease renewal never changes the attempt deadline, run deadline, or workflow timer. The recorded grant time, rather than RPC receipt time, starts the attempt.

A run timeout starts at admission. It closes ordinary forward execution and uses the cancellation/settlement path. It does not prevent authorized reconciliation or compensation from running under their own attempt deadlines.

## Retry policies

| Policy | Forward execution | Compensation |
|---|---|---|
| Total attempt budget | 5, including the first | 10, including the first |
| Configured maximum | 10 before an explicit operator extension | 10 before an explicit operator extension |
| Reported-error selection when omitted | Empty `retry.errors` list | All retryable codes in the pinned compensator catalog |
| Initial backoff | 1 second | 1 second |
| Multiplier | 2 | 2 |
| Backoff cap | 60 seconds | 5 minutes |
| Jitter | Full jitter | Full jitter |

Backoff after failed attempt number `a`, starting at one, is sampled once from zero through `min(cap, initial * multiplier^(a - 1))`. Persist the selected delay. The multiplier must be finite and between one and ten; initial/cap durations must be positive, ordered, and no greater than one hour.

Forward `retry` and `compensation.retry` accept `errors`, `max_attempts`, and `backoff: {initial, multiplier, max}`. An explicit empty compensation error list disables retries for reported errors.

Only codes classified as retryable by the pinned handler contract may appear in these lists. Listing a terminal or unknown code is a definition error.

A retryable reported error is the handler's declaration that this known failure may be retried. It is not a way to disguise an unknown external outcome.

Ambiguous attempts follow the handler's mandatory `RetrySafe` or `Manual` policy, independently of the reported-error list but within the same attempt budget. Aborting a saga does not authorize forward retries to discover an effect.

Exhausted known forward failures fail their scope. Exhausted ambiguous forward outcomes enter intervention. Terminal, excluded, or exhausted compensation errors block the obligation and saga for intervention; they do not skip to the next undo or declare compensation complete.

Reconciliation defaults to three probes, 30 seconds apart. Unknown or failed probe exhaustion enters intervention.

Operator-authorized retry extensions add at most ten attempts and record actor, reason, and expected versions. They preserve the logical input and effect identity.

## Retention and reconstruction

| Record | Default retention |
|---|---|
| Active-run events and required artifacts | Entire active lifetime, including blocked or compensating runs |
| Full terminal-run history and required artifacts | 30 days after the actual terminal transition |
| Terminal summary and start/signal deduplication tombstones | 90 days after termination |
| Command-result deduplication | 24 hours after the result is recorded |
| Unreserved buffered external event | 7 days after acceptance |
| Event reserved to a pending wait | Until that wait resolves or releases the reservation |
| Run checkpoint | Trigger after 256 new workflow events and at termination; retain the latest checkpoint while its history is retained |

Retention promises are captured in run policy at admission. A later default change cannot silently shorten existing runs' promises. Workflow-semantic defaults are expanded into the normalized definition before publication.

Checkpoint creation uses a complete apply boundary and records its actual included run sequence. A multi-event transaction crossing the cadence must not label later state as though it represented an earlier event prefix.

Expiry makes data eligible for committed bounded cleanup. It is not a guarantee of physical deletion at that instant during quorum loss or storage failure.

An input event's original TTL is not reset by reservation or release. Reserved-event handling follows the language specification; historical payload retention is separate from inbox eligibility.

Do not delete artifacts referenced by an active run, retained event, checkpoint, or retained command result. Raft log compaction is not workflow-history pruning.

After full history expires, queries may return the retained terminal summary with an explicit unavailable-history marker. They must not manufacture a typed output or partial replay. Start-key retries within the tombstone window return the original run identity rather than creating another run.

Do not assume command-level deduplication beyond its declared window. The SDK's automatic transport retry budget remains five minutes.

## Configuration and consistency

All settings that affect replicated decisions belong to versioned replicated policy, not independent per-member environment defaults.

Definition-level overrides are validated and captured at publication. Fixed attempt deadlines are captured at grant. Retention is captured at admission. Retry delays are captured when selected.

Local concurrency/cache choices may change resource availability but cannot make replicas interpret an accepted command differently.
