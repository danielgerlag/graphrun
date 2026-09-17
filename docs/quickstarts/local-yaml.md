# Run a local YAML workflow

This how-to starts a one-member engine on disk and runs `sequence.yaml`. You do not install a database, broker, or container.

## Prerequisites

- Rust 1.90 or newer
- This repository as the working directory

## Build the CLI

```sh
cargo build -p graphrun-cli
```

The binary is `target/debug/graphrun`.

## Validate the definition

```sh
./target/debug/graphrun validate \
	--definition docs/specs/v1/examples/sequence.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json
```

You should see JSON with `"status":"ok"` and a digest.

## Start the engine and the run

```sh
mkdir -p /tmp/graphrun-local
echo '{"order_id":"o1","amount":1000}' > /tmp/order.json

./target/debug/graphrun serve --local-dir /tmp/graphrun-local &
sleep 1

./target/debug/graphrun start \
	--definition docs/specs/v1/examples/sequence.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json \
	--input /tmp/order.json \
	--local-dir /tmp/graphrun-local
```

The start command prints a 32-character run id. Inspect it:

```sh
./target/debug/graphrun inspect --run <run-id> --local-dir /tmp/graphrun-local
```

The run status is `succeeded` and the output includes `"payment_id":"pay-1"`.

Stop the engine with SIGTERM. The same `--local-dir` reopens the store on the next `serve`.
