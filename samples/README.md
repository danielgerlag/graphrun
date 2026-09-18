# Graphrun samples

Each sample is a small in-process program. It compiles a sibling `workflow.yaml`, builds the same graph with the typed Rust builder, and runs both through [`Engine::local`](https://docs.rs/graphrun/latest/graphrun/engine/struct.Engine.html). There is no `graphrun serve` step and no custom activity plugin API. Local mode executes the catalog names that the library already handles (`counter.increment`, `inventory.reserve`, `payment.charge`, and the rest).

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```

| Sample | Shows | Run |
|---|---|---|
| [01 Hello World](01-hello-world) | Sequence of activities, then complete | `cargo run -p graphrun-samples --bin 01-hello-world` |
| [02 Passing Data](02-passing-data) | Typed payloads: order → reserve → charge | `cargo run -p graphrun-samples --bin 02-passing-data` |
| [03 Events](03-events) | `wait_signal` + `Engine::signal` | `cargo run -p graphrun-samples --bin 03-events` |
| [04 While](04-while) | `while_loop` until a condition fails | `cargo run -p graphrun-samples --bin 04-while` |
| [05 Foreach](05-foreach) | Map a body over an array | `cargo run -p graphrun-samples --bin 05-foreach` |
| [06 Choice / If](06-choice) | `choose` with `when` / `eq` | `cargo run -p graphrun-samples --bin 06-choice` |
| [07 Parallel](07-parallel) | Two branches, all-join tuple | `cargo run -p graphrun-samples --bin 07-parallel` |
| [08 Saga](08-saga) | Compensation after `fail_after_payment` | `cargo run -p graphrun-samples --bin 08-saga` |
| [09 Repeat](09-repeat) | Fixed iteration count | `cargo run -p graphrun-samples --bin 09-repeat` |
| [10 Timeout Recovery](10-timeout-recovery) | Timed wait success vs timeout ports | `cargo run -p graphrun-samples --bin 10-timeout-recovery` |

These follow the WorkflowCore `src/samples` set that Graphrun v1 actually has. Human/user workflows, a REST host, DI containers, recurring `IHostedService`, and Mongo persistence providers are out of scope.

The catalog is [`catalog.json`](catalog.json). Payload types used by several bins live in `src/lib.rs`.
