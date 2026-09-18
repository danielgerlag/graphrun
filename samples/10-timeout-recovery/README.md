# 10 Timeout recovery

A timed wait: if the approval event arrives in time, finish with the original input. If it times out, run `counter.increment` on the recovery path, then still finish with the original input (the complete node binds `workflow.input`).

```sh
cargo run -p graphrun-samples --bin 10-timeout-recovery
```
