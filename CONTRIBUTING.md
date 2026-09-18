# Contributing

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked -- --skip snapshot_controller_fires
cargo build --release --locked -p graphrun-cli
cargo run --locked -p graphrun-e2e -- verify \
	--cli target/release/graphrun \
	--matrix docs/specs/v1/verification-matrix.tsv \
	--artifacts target/e2e-artifacts
```

The v1 implementation contract is `docs/specs/v1`. That directory is not a getting-started guide.
