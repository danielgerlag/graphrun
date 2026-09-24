# Graphrun for agents

Embedded durable workflow engine for Rust. Not a server. Not Workflow Core.

## Use this crate

```toml
graphrun = "0.1"
```

Open `Engine::local(dir)` in the host Tokio process. Graphs are YAML or `workflow::<T>("id")`. Both compile to one IR. Persistence is Raft + redb in `dir`.

## Hard limits of this release

- You cannot register custom activity handlers.
- Catalog names such as `counter.increment` and `inventory.reserve` run built-in fixtures.
- Unknown activity names echo their input and succeed.
- Do not treat a custom catalog name as user code that ran.

## Where to read

1. `README.md` — paste-and-run hello world
2. `graphrun/examples/hello_world.rs` and `graphrun/examples/order.rs` (in the published crate)
3. `samples/01-hello-world` … `samples/10-timeout-recovery` (this repo only)
4. `docs/quickstarts/` — each how-to includes the YAML

Do **not** start from `docs/specs/v1/` or `docs/brainstorming/`. Those are the implementation contract, not a tutorial.

## Run in this repo

```sh
cargo run -p graphrun --example hello_world
cargo run -p graphrun --example order
cargo run -p graphrun-samples --bin 01-hello-world
```

## Public entry points

- `Engine::local`, `start`, `start_yaml`, `wait_terminal`, `signal`, `cancel`, `inspect`, `shutdown`
- `Catalog::from_json`, `activity_v1`
- `workflow`, `region`, `payload!`
- Optional binary crate `graphrun-cli` for inspect/signal/backup against `--local-dir`
