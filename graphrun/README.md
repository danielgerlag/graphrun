# graphrun

Durable workflow engine. YAML and a typed Rust builder compile to one IR. Local mode is one-member Raft; a cluster uses the same write path.

```toml
[dependencies]
graphrun = "0.1"
```

The `graphrun` binary is the [`graphrun-cli`](https://crates.io/crates/graphrun-cli) crate:

```sh
cargo install graphrun-cli --locked
```

Specs, quickstarts, and examples: <https://github.com/danielgerlag/graphrun>
