# 02 Passing Data

An order goes through `inventory.reserve` (adds `reservation_id`) then `payment.charge` (adds `payment_id`). Those names are built-in fixtures. `activity_ref` checks the Rust payload types against this folder’s `catalog.json`.

```sh
cargo run -p graphrun-samples --bin 02-passing-data
```

Next: [03 Events](../03-events).
