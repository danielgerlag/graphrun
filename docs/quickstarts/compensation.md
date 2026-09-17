# Compensate, intervene, or abandon

`saga.yaml` with `fail_after_payment: true` charges, then fails, then refunds and releases in reverse order.

```sh
echo '{"order_id":"o1","amount":1000,"fail_after_payment":true}' > /tmp/fail-order.json
./target/debug/graphrun start \
	--definition docs/specs/v1/examples/saga.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json \
	--input /tmp/fail-order.json \
	--local-dir /tmp/graphrun-local
```

Inspect `obligations`. Status values are `open`, `compensating`, `compensated`, `blocked`, `irreversible`, or `abandoned`.

## Blocked compensation input

If a forward success cannot build compensation input, the run stays accepted and the obligation is `blocked`. Supply input or abandon. Both require `--reason`.

```sh
./target/debug/graphrun resolve \
	--run <run-id> \
	--forward <activation-id> \
	--blocked-input /tmp/release-input.json \
	--reason "operator supplied release payload" \
	--local-dir /tmp/graphrun-local
```

```sh
./target/debug/graphrun resolve \
	--run <run-id> \
	--abandon \
	--confirm \
	--reason "cannot undo; leaving unresolved" \
	--local-dir /tmp/graphrun-local
```

Abandonment fails the run with code `saga.abandoned`. It does not report rollback success. Unresolved obligation ids stay in the inspect output.
