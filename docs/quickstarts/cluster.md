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
- `Engine::run_worker` — claims ready activities over gRPC/mTLS. Does not open a data directory.
- Certificates: `graphrun::generate_ca` and `graphrun::issue_node`.
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

For immutable catalog/definition publication and keyed starts, use the
owner-only `--local-dir` control interface shown in the README, or present a
CA-signed URI SAN principal on `--endpoint`: `admin` for publishing and
`client` for starting. Use the cluster ID from the member's `identity.json`
when issuing the principal certificate. `issue_node` certificates are
roleless and do not authorize these operations; `--cert` is the client's
principal certificate. The legacy inline `--definition` start above remains
available.
