# 08 Saga

Reserve, then charge, each with an undo. When `fail_after_payment` is true, the saga fails after the charge and refunds, then releases, in reverse order.

```sh
cargo run -p graphrun-samples --bin 08-saga
```
