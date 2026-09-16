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

```sh
rustup run 1.90.0 cargo test --workspace --locked
```

## Local YAML check

```sh
cargo run -p graphrun-cli -- validate \
	--definition docs/specs/v1/examples/sequence.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json
```
