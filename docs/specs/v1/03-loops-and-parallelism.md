# Loops, branches, and joining

All primitives in this document are mandatory v1 behavior in both YAML and the Rust builder.

## Common loop model

A loop activation owns durable iteration scopes. Its node definition is not duplicated or renumbered on each iteration.

Persist loop mode, initial/captured input, current carry state, completed-iteration count, next iteration identity, active child scope, and terminal outcome.

Before starting a body, commit its iteration identity and input. Body retries retain that identity. The next iteration is created only after accepting the previous body's terminal output.

Loop guards use the recorded current carry state and completed count. Historical replay applies the recorded guard result rather than reevaluating it.

Within a body, `loop.state` is the immutable carry value captured for that body and `loop.index` is its zero-based index. At a parent guard evaluation, `loop.index` is the completed-iteration count, which is also the next body's index.

Every loop has an explicit positive `max_iterations`, at most 1,000 by default. The limit does not turn a still-true condition into successful completion.

## While

Required fields are `state`, `state_schema`, `condition`, `max_iterations`, `body`, and `next`.

The body region accepts and returns `state_schema`. Evaluate the condition before each body.

If initially false, return the initial state without creating an iteration. If true and the limit allows it, create one body scope. Its successful output becomes the next carry state.

After each body, evaluate the next guard. False completes the loop with current carry. True at the limit fails with `LoopLimitExceeded` before another body starts.

A body failure/cancellation stops forward iteration and propagates through its parent scope, including any saga cleanup. No later iteration may start while failure settlement is unresolved.

## Do-while

Fields match `while`. Execute the first body before evaluating the condition.

After accepting a body result, increment completed count and evaluate the guard against the new carry. False completes. True starts another iteration only if below the limit.

The minimum execution count is one. A configured limit below one is invalid. A limit reached while the continuation guard is true is a failure, not success.

## Repeat

Required fields are `count`, `state`, `state_schema`, `max_iterations`, `body`, and `next`.

Evaluate and capture count once when the activation opens. It must be a nonnegative integer no greater than the declared limit.

Count zero returns the initial state with no body. Otherwise execute bodies sequentially, carrying each accepted output to the next input. Return the final carry after exactly count successful iterations.

The body and input/count bindings are not reevaluated to discover a new count after restart.

## Foreach

Required fields are `items`, `item_schema`, `max_items`, `max_concurrency`, `body`, and `next`.

Capture the ordered collection once. It must be an array, fit the payload budget, and contain at most `max_items`, with policy maximum 1,000.

The body input schema is `item_schema`. The node output schema is `Array(body.output_schema)`.

Within a body, `scope.input` and `item.value` are its captured item. `item.index` is the zero-based original index. Equal values at different positions are different logical iterations.

The default concurrency is eight child scopes and maximum is 32. Concurrency counts open body scopes, including bodies waiting on signals, not only executing activities.

Do not materialize every child eagerly. Persist the frozen collection reference, expected count, scheduling cursor, active set, and indexed outcomes. Open a bounded window and refill it as scopes become terminal.

Output order is input order, independent of completion order. Empty input returns an empty array exactly once.

On the first committed failure, stop opening new items and begin cancellation/settlement of open item scopes. Preserve accepted outcomes and compensation obligations. The foreach fails only after required settlement; uncertainty blocks rather than disappearing.

## Parallel

A `parallel` node contains an ordered, nonempty `branches` list, at most 16 entries, and `next`.

Each entry has a unique stable `name`, an `input` binding evaluated in the parent context, and a body region. Branch names identify definition paths; list order defines result order.

Commit the complete expected branch set and captured inputs before dispatching branch work. Branch scopes have independent outputs and lifecycle.

The default output is a tuple represented as a JSON array in declared branch order. Its schema is `Tuple(branch.output_schema...)`.

The only v1 join policy is `all`. Every branch must succeed. A zero-branch node is rejected. Race, quorum, and first-success joins are not v1 primitives.

## All-join success

The join records each expected branch outcome once. It completes only when every expected branch succeeded and there is no unsettled cleanup or blocked child.

Output validation, parent activation completion, and successor creation occur atomically with the join-resolution event.

Repeated child results, reordered notifications, simultaneous last-child completions, and leader replacement cannot produce a second join resolution or successor.

## All-join failure

The first committed child failure becomes the primary error. Preserve later secondary failures in committed order.

Stop new forward work in sibling scopes and request cancellation of running attempts. Already authorized results can settle them, but cannot advance ordinary forward successors.

Do not equate a cancelled future with an absent external effect. Use the activity reconciliation/intervention rules for uncertainty.

After all open branches are settled, propagate failure to the owning scope. If that owner is a saga, its obligations determine compensation. A join must not bypass the saga by terminating the run directly.

## Nested controls

Any loop body or parallel branch can contain another loop, parallel node, event wait, or saga, subject to depth and resource limits.

Context bindings resolve to the nearest enclosing relevant frame. Parent-local node outputs must be passed through explicit child input; similarly named nodes in another region are never implicitly selected.

A child `complete` returns from that child region. It cannot finish its parent loop, parallel node, saga, or root run prematurely.

Per-run leaf concurrency remains capped even when nested controls request more work. A parent waiting on children must not retain leaf permits and deadlock its descendants.

## Required evidence

The verification suite must cover zero/one/many iterations, false initial guards, do-while's first body, zero repeat, limit failures, empty foreach, duplicate item values, reversed completion order, concurrency windows, nested scopes, one failing parallel child, duplicate last-child results, and leader loss between every creation/completion boundary.

Snapshots and historical reconstruction must preserve frozen collections, cursors, carry values, branch sets, outcomes, and unresolved saga obligations. No body may repeat because only an in-memory loop counter was lost.
