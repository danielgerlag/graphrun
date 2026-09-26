# Run a three-member cluster

Do this after a local `Engine::local` run works. A cluster is the same write path with more Raft voters.

Start the same graph you already ran locally, for example [`samples/02-passing-data/workflow.yaml`](../../samples/02-passing-data/workflow.yaml):

```yaml
dsl: graphrun/v1
id: passing_data
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: reserve
nodes:
  reserve:
    kind: activity
    activity: {name: inventory.reserve, version: 1}
    input: {from: workflow.input}
    next: charge
  charge:
    kind: activity
    activity: {name: payment.charge, version: 1}
    input: {from: nodes.reserve.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.charge.output}
```

- `Engine::member` — storage replica. Activity execution is opt-in.
- `Worker::builder(endpoint, worker_tls, catalog)` — claims exact versioned roles over gRPC/mTLS. Does not open a data directory. See [the worker example](worker.md).
- Certificates: `graphrun::generate_ca` and `graphrun::tls::issue_principal` with a `worker` URI SAN for workers. `issue_node` creates multi-role fixture certificates; use explicit roles for applications.
- Membership changes go through the leader (`cluster join` / `promote` / `remove`).

Join a new id as a learner first. Wait until it has caught up. Then promote it. Remove one voter at a time so two healthy voters remain.

```sh
graphrun cluster join \
	--local-dir /path/to/leader \
	--node-id 4 \
	--addr 127.0.0.1:PORT \
	--peer-ca ca.pem \
	--peer-cert n4.cert.pem \
	--peer-tls-key n4.key.pem \
	--peer-server-name <issued-name>

graphrun cluster promote \
	--local-dir /path/to/leader \
	--node-id 4

graphrun start \
	--definition samples/02-passing-data/workflow.yaml \
	--catalog samples/02-passing-data/catalog.json \
	--input order.json \
	--endpoint https://127.0.0.1:PORT \
	--ca ca.pem \
	--cert client.cert.pem \
	--tls-key client.key.pem \
	--server-name <issued-name>
```

Workers register over gRPC. They do not change the voter set. `--key` on `signal` is the wait correlation key; certificate keys are `--tls-key` / `--peer-tls-key`.

Each certificate must have a CA-signed URI SAN
`spiffe://graphrun/<cluster-id>/<role>/<principal-id>`. Assign `member` to
voters and learners, `worker` to workers, `client` to starts and queries, and
`admin` to publication and clock acknowledgement. Members must also have the
same numeric principal ID and endpoint as their committed roster entry. Use
the cluster ID from `identity.json`. `issue_node` grants all four roles for
fixtures; use `issue_principal` with only the needed roles in production.

The owner-only `--local-dir` control interface does not use mTLS. A remote
client presenting `--cert` uses that certificate's signed roles for every RPC,
including inline `--definition` starts. `GrpcClient` follows a leader redirect
without changing its command ID. Reads require a live leader and voting quorum;
a disconnected follower returns `Unavailable` instead of a stale view.

If wall/boot divergence, a watchdog gap, or a future committed watermark
stops dispatch, correct the clock and wait for ten seconds of healthy samples.
The faulted member also requests cancellation of its active local and
connected remote handlers. It stops heartbeats and, when a healthy candidate
has a fresh voting quorum and a caught-up log, asks that voter to start an
OpenRaft election. This is an election request, not a guaranteed transfer.
With no healthy voter quorum or no caught-up voter, the member stays fenced;
already committed records still apply. A returning old member remains fenced
until acknowledgement. A handler or external operation may still be running,
so reconcile uncertain effects before starting conflicting compensation.
Then acknowledge the fault with an admin certificate or the owner-only socket:

```sh
graphrun cluster acknowledge-clock \
  --local-dir /path/to/member \
  --reason "host clock repaired"
```

Acknowledgement never lowers the committed engine-time watermark. A cluster
also needs fresh bounded clock-health samples from a voting quorum before
scheduling work.

# Check live mTLS processes

Build the CLI and verification driver, then run a focused process smoke test
that starts three separate members and two separate workers:

```sh
cargo build -p graphrun-cli -p graphrun-e2e
cargo run -p graphrun-e2e -- smoke-cluster \
  --cli target/debug/graphrun \
  --artifacts /tmp/graphrun-cluster-smoke
```

This checks the real gRPC path but does not certify the full verification
matrix. Use `graphrun-e2e verify` for that gate.
