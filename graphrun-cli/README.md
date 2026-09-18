# graphrun-cli

Optional operator CLI for the [graphrun](https://crates.io/crates/graphrun) library. Your application depends on `graphrun` and opens `Engine::local` itself. This binary inspects, signals, and administers a data directory that engine already uses.

```sh
cargo install graphrun-cli --locked
```

Rust 1.90 or newer. Linux and macOS.

## Inspect a local data directory

```sh
graphrun inspect --run <run-id> --local-dir ./graphrun-data
graphrun validate --definition sequence.yaml --catalog catalog.json
```

`start` waits for a terminal status. Pass `--no-wait` to return the run id immediately.

```sh
graphrun start \
	--definition sequence.yaml \
	--catalog catalog.json \
	--input order.json \
	--local-dir ./graphrun-data
```

`graphrun serve --local-dir ./graphrun-data` opens a local engine for ops when your app is not running. Prefer embedding `Engine::local` in the application.

## Cluster member

Replace `--local-dir` with mTLS flags:

```sh
graphrun start \
	--definition sequence.yaml \
	--catalog catalog.json \
	--input order.json \
	--endpoint https://127.0.0.1:PORT \
	--ca ca.pem \
	--cert client.cert.pem \
	--tls-key client.key.pem \
	--server-name <issued-name>
```

`--key` on `signal` is the wait correlation key. Certificate keys are `--tls-key`.

## Commands

| Command | Purpose |
|---|---|
| `validate` | Compile YAML against a catalog. |
| `start` | Start a run. |
| `signal` | Deliver an event to a wait. |
| `cancel` | Cancel an active run. |
| `inspect` | Run status, output, `blocked_reason`, waits, scopes. |
| `list` | Runs on the member. |
| `history` | Recorded events. |
| `replay` | Reconstruct state from history (read-only). |
| `resolve` | Supply blocked compensation input, or `--abandon --confirm`. |
| `serve` | Open a local engine on `--local-dir` (ops only). |
| `backup` / `restore` | Logical backup. Restore assigns a new identity. |
| `cluster join\|promote\|remove` | Membership through the leader. |

Samples and how-tos: <https://github.com/danielgerlag/graphrun>
