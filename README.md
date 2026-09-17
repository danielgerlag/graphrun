# Graphrun

Durable workflow engine. Specification lives in `docs/specs/v1/`.

## Commands

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo build --release --locked -p graphrun-cli
cargo run --locked -p graphrun-e2e -- verify \
	--cli target/release/graphrun \
	--matrix docs/specs/v1/verification-matrix.tsv \
	--artifacts target/e2e-artifacts
```

Minimum toolchain is Rust 1.90.0. Current development uses stable.

CI on GitHub Actions runs fmt, clippy, tests on Linux and macOS, the 1.90.0 toolchain, and the e2e matrix against a release `graphrun` binary. PERF-001 and PERF-002 stay BLOCKED unless the job runs on the spec's three 4-vCPU members.

```sh
rustup run 1.90.0 cargo test --workspace --locked
```

## Local YAML check

```sh
cargo run -p graphrun-cli -- validate \
	--definition docs/specs/v1/examples/sequence.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json
```

## Quickstarts

- [Run a local YAML workflow](docs/quickstarts/local-yaml.md)
- [Build a workflow in Rust](docs/quickstarts/rust-builder.md)
- [Run a three-member cluster](docs/quickstarts/cluster.md)
- [Deliver an external event](docs/quickstarts/events.md)
- [Compensate, intervene, or abandon](docs/quickstarts/compensation.md)
- [Back up and restore a member](docs/quickstarts/backup-restore.md)
- [Diagnose a stalled run](docs/quickstarts/stalled-run.md)
- [Limits and failure assumptions](docs/limits.md)
