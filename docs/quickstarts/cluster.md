# Run a three-member cluster

This how-to starts three voters and two workers on loopback with generated certificates.

## Generate certificates

The e2e driver already does this. For a manual run, use `graphrun::generate_ca` and `graphrun::issue_node` from a small helper, or run:

```sh
cargo build -p graphrun-e2e -p graphrun-cli
./target/debug/graphrun-e2e verify \
	--cli ./target/debug/graphrun \
	--matrix docs/specs/v1/verification-matrix.tsv \
	--artifacts target/e2e-artifacts
```

`CLUSTER-001` in that report starts three `fixture-member` processes and two `fixture-worker` processes, then starts `sequence.yaml` through the production CLI over mTLS.

## Join, promote, and remove

Membership changes go through the leader's Unix control socket.

```sh
./target/debug/graphrun cluster join \
	--local-dir /path/to/leader \
	--node-id 4 \
	--addr 127.0.0.1:PORT \
	--peer-ca ca.pem \
	--peer-cert n4.cert.pem \
	--peer-tls-key n4.key.pem \
	--peer-server-name <issued-name>

./target/debug/graphrun cluster promote \
	--local-dir /path/to/leader \
	--node-id 4

./target/debug/graphrun cluster remove \
	--local-dir /path/to/leader \
	--node-id 1
```

Join the new id as a learner first. Wait until it has caught up. Then promote it. Remove one voter at a time so two healthy voters remain.

Workers register over gRPC. They do not change the voter set.

```sh
./target/debug/graphrun-e2e fixture-worker \
	--endpoint https://127.0.0.1:PORT \
	--ca ca.pem \
	--cert worker.cert.pem \
	--key worker.key.pem \
	--server-name <member-name>
```
