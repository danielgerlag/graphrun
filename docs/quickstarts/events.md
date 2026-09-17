# Deliver an external event

This how-to starts `events.yaml`, waits for the approval wait, then signals it.

```sh
mkdir -p /tmp/graphrun-events
echo '{"key":"k1"}' > /tmp/event-input.json
echo '{"approved":true}' > /tmp/approval.json

./target/debug/graphrun serve --local-dir /tmp/graphrun-events &
sleep 1

./target/debug/graphrun start \
	--definition docs/specs/v1/examples/events.yaml \
	--catalog docs/specs/v1/examples/activity-catalog.json \
	--input /tmp/event-input.json \
	--local-dir /tmp/graphrun-events \
	--no-wait
```

Inspect until `pending_waits` includes `"signal":"approval"`. Then:

```sh
./target/debug/graphrun signal \
	--run <run-id> \
	--name approval \
	--key k1 \
	--event-id aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
	--payload /tmp/approval.json \
	--local-dir /tmp/graphrun-events
```

`--key` is the wait correlation key. TLS material uses `--tls-key`.

The inspect output then shows `"status":"succeeded"` and `"approved":true`.
