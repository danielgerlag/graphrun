# Executable fixture requirements

These are normative input fixtures for the implementation and verification driver. They are not currently executable because no engine exists yet.

The [catalog](activity-catalog.json) declares their payload and activity contracts. The implementation must provide deterministic fixture handlers and equivalent Rust-built definitions.

| Definition | Input/example setup | Required result |
|---|---|---|
| [Sequence](sequence.yaml) | `{"order_id":"o1","amount":1000}` | One reservation and charge, a matching receipt |
| [While](while.yaml) | `{"value":0}` and `{"value":5}` | Values 3 and 5; zero body calls for the second input |
| [Do-while](do-while.yaml) | `{"value":0}` and `{"value":5}` | Values 3 and 6; at least one body call |
| [Repeat](repeat.yaml) | `{"count":3,"counter":{"value":1}}` | Value 4 after exactly three bodies; count zero returns the original carry |
| [Foreach](foreach.yaml) | `[{"value":3},{"value":1},{"value":3}]` | `[{"value":4},{"value":2},{"value":4}]`, even if completion order reverses |
| [Parallel](parallel.yaml) | An order with amount 1000 | `[{"cents":100},{"cents":500}]` in branch order |
| [Events](events.yaml) | Key `"a"`; hold `test.gate`, send approval, then release the gate | The pre-wait event is consumed once |
| [Timeout recovery](timeout-recovery.yaml) | Counter input with and without an approval event | Both paths return the original counter; only timeout executes the recovery activity, and both share `finish` |
| [Saga](saga.yaml) | Order with `fail_after_payment:true` | Run fails after refund then release; provider shows no remaining compensated effect |
| [Nested saga](nested-saga.yaml) | An order with amount 1000 | Inner success transfers its obligation; outer failure refunds then releases exactly once |
| [Timers](timers.yaml) | Null input | Past absolute wait resolves, relative timer records its deadline, output is null |
| [Remote worker](remote.yaml) | `{"value":7}` with no publisher-local handler | An independent worker returns `{"value":7}` |
| [Events in foreach](events-in-foreach.yaml) | Distinct keys, events sent in reverse order | Results stay in input order and matching never crosses item keys |
| [Nested controls](nested-controls.yaml) | `[{"value":1},{"value":4}]` | `[[{"value":4},{"value":3}],[{"value":7},{"value":6}]]` |

`counter.increment` adds one. `remote.echo` returns its input. `tax.quote` returns integer `amount / 10`. `shipping.quote` is a blocking handler returning 500 cents.

Inventory/payment handlers call the independent provider ledger. Their outputs preserve order data and add deterministic operation IDs. Undo handlers release/refund using the forward output and return null.

`test.gate` is a verification activity controlled by the driver, not an engine state shortcut. It returns its input only after the driver releases it.

The driver must also generate variants for limits, duplicate item values/keys, failed branches, nested saga transfer, uncertain effects, cancellation, and every matrix case not represented by a single static file.

Do not implement special engine behavior for these workflow IDs, node names, or expected numbers.
