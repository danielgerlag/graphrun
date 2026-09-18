# graphrun-cli

The `graphrun` binary for the [graphrun](https://crates.io/crates/graphrun) engine.

```sh
cargo install graphrun-cli --locked
```

Rust 1.90 or newer. Linux and macOS.

## Local engine

```sh
graphrun serve --local-dir /tmp/graphrun-demo &

graphrun validate --definition sequence.yaml --catalog catalog.json

graphrun start \
	--definition sequence.yaml \
	--catalog catalog.json \
	--input order.json \
	--local-dir /tmp/graphrun-demo

graphrun inspect --run <run-id> --local-dir /tmp/graphrun-demo
```

`start` waits for a terminal status. Pass `--no-wait` to return the run id immediately.

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
| `validate` | Compile YAML against a catalog. Prints a digest. |
| `start` | Start a run. |
| `signal` | Deliver an event to a wait. |
| `cancel` | Cancel an active run. |
| `inspect` | Run status, output, `blocked_reason`, waits, scopes. |
| `list` | Runs on the member. |
| `history` | Recorded events. |
| `replay` | Reconstruct state from history (read-only). |
| `resolve` | Supply blocked compensation input, or `--abandon --confirm`. |
| `serve` | Local engine on `--local-dir`. |
| `backup` / `restore` | Logical backup. Restore assigns a new identity. |
| `cluster join\|promote\|remove` | Membership through the leader's control socket. |

Graphs, catalogs, and how-tos: <https://github.com/danielgerlag/graphrun>
