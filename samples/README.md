# Graphrun samples

Each folder is one graph: `workflow.yaml`, `catalog.json`, payload types, and a typed builder. Run it with `Engine::local`. There is no `graphrun serve` step.

**You cannot register custom activity handlers.** Local mode runs built-in fixtures for names such as `counter.increment` and `inventory.reserve`. Unknown names echo their input.

Start here, in order:

1. [01 Hello World](01-hello-world) — two increments
2. [02 Passing Data](02-passing-data) — order → reserve → charge
3. [03 Events](03-events) — `wait_signal` + `Engine::signal`

Then pick one:

| Sample | Shows |
|---|---|
| [04 While](04-while) | Loop until a condition fails |
| [05 Foreach](05-foreach) | Map a body over an array |
| [06 Choice](06-choice) | `choose` with `when` / `eq` |
| [07 Parallel](07-parallel) | Two branches, all-join |
| [08 Saga](08-saga) | Compensation after payment fails |
| [09 Repeat](09-repeat) | Fixed iteration count |
| [10 Timeout recovery](10-timeout-recovery) | Timed wait success vs timeout |

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```
