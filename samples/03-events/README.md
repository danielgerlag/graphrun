# 03 Events

The run parks on `wait_signal` until the host calls `Engine::signal` with name `approval` and key `order-1`. Output is `{"approved":true}`.

```sh
cargo run -p graphrun-samples --bin 03-events
```

Then pick from [04–10](../README.md).
