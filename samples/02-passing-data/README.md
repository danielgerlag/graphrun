# 02 Passing Data

An order flows through typed activity outputs: `inventory.reserve` adds `reservation_id`, then `payment.charge` adds `payment_id`. The builder checks those payload types against the catalog at compile time.

This is the Graphrun equivalent of WorkflowCore Sample03. Data is immutable bindings, not a mutable workflow data bag.

## Run

```sh
cargo run -p graphrun-samples --bin 02-passing-data
```

The bin compiles this YAML, builds the same graph with `RegionBuilder`, checks IR parity, and runs both on `Engine::local`.
