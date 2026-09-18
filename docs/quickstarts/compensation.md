# Compensate or abandon

Reserve, charge, then fail after payment. Compensation refunds, then releases, in reverse order.

## Definition

[`samples/08-saga/workflow.yaml`](../../samples/08-saga/workflow.yaml)

```yaml
dsl: graphrun/v1
id: compensating_fulfillment
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: fulfill
nodes:
  fulfill:
    kind: saga
    input: {from: workflow.input}
    body:
      input_schema: order/v1
      output_schema: receipt/v1
      start: reserve
      nodes:
        reserve:
          kind: activity
          activity: {name: inventory.reserve, version: 1}
          input: {from: scope.input}
          compensation:
            kind: activity
            activity: {name: inventory.release, version: 1}
            input: {from: forward.output}
          next: charge
        charge:
          kind: activity
          activity: {name: payment.charge, version: 1}
          input: {from: nodes.reserve.output}
          compensation:
            kind: activity
            activity: {name: payment.refund, version: 1}
            input: {from: forward.output}
          next: decide
        decide:
          kind: choose
          input: {from: nodes.charge.output}
          cases:
            - name: force_failure
              when:
                all:
                  - exists: {from: workflow.input, path: /fail_after_payment}
                  - eq:
                      - {from: workflow.input, path: /fail_after_payment}
                      - {literal: true}
              body:
                input_schema: receipt/v1
                output_schema: receipt/v1
                start: abort
                nodes:
                  abort:
                    kind: fail
                    error: {code: fixture.failed, message: Failure after recorded payment.}
          default:
            input_schema: receipt/v1
            output_schema: receipt/v1
            start: accept
            nodes:
              accept:
                kind: complete
                output: {from: scope.input}
          next: done
        done:
          kind: complete
          output: {from: nodes.decide.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.fulfill.output}
```

Catalog: [`samples/08-saga/catalog.json`](../../samples/08-saga/catalog.json). Builder: [`builder.rs`](../../samples/08-saga/builder.rs). Input `fail_after_payment: true`.

```sh
cargo run -p graphrun-samples --bin 08-saga
```

Inspect `obligations`. Status values are `open`, `compensating`, `compensated`, `blocked`, `irreversible`, or `abandoned`.

If a forward success cannot build compensation input, the obligation is `blocked`. Supply input or abandon. Both require `--reason`:

```sh
graphrun resolve \
	--run <run-id> \
	--forward <activation-id> \
	--blocked-input release-input.json \
	--reason "operator supplied release payload" \
	--local-dir ./graphrun-data

graphrun resolve \
	--run <run-id> \
	--abandon \
	--confirm \
	--reason "cannot undo; leaving unresolved" \
	--local-dir ./graphrun-data
```

Abandonment fails the run with code `saga.abandoned`. It does not report rollback success.
