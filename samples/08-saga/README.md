# 08 Saga

Reserve, then charge, each with a compensating undo. When `fail_after_payment` is true, the saga fails after the charge and refunds, then releases, in reverse order.

This is the Graphrun equivalent of WorkflowCore Sample17. Compensation is declared on the forward activity; it is not a host callback.

YAML and the typed builder compile the same control flow. Compensation retry defaults differ slightly between the YAML compiler and `attach_compensation`, so this bin does not assert IR equality. Both runs fail and leave compensated obligations.

## Run

```sh
cargo run -p graphrun-samples --bin 08-saga
```
