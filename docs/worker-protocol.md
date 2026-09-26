# Worker protocol reference

[`proto/graphrun.proto`](../proto/graphrun.proto) defines the gRPC messages.
Ordinary builds use the checked-in generated Rust. Protocol version 1 uses
payload codec version 1: `input_json` and `output_json` contain JSON bytes of
an untagged `Value`. Values are null, boolean, signed 64-bit integer, string,
array, or object. Empty bytes are **not** JSON null.

## Session and readiness

| RPC | Request | Response |
|---|---|---|
| `Register` | New `session_id`, stable `command_id`, signed `principal_id`, `protocol_min`, `protocol_max`, `capacity`, and exact `capabilities` | `revision` and `lease_expiry_ms` |
| `RenewSession` | Original `session_id`, expected `revision`, and stable `command_id` | New `revision` and `lease_expiry_ms` |
| `WatchReady` | `session_id`, last `generation`, and last `cursor` | Current `generation`, `cursor`, `ready`, and `resync` |
| `Claim` | `session_id`, stable `command_id`, and requested `capacity` | At most 16 assignments, subject to the session's remaining capacity |

The signed worker URI SAN supplies the authenticated principal. The member
checks that `RegisterRequest.principal_id` matches it and stores the signed
principal. Each later worker RPC checks the signed role, current leader, and
live session owner before looking up a command ID.

Each capability contains `activity_name`, `activity_version`, `role`,
`codec_version`, `input_schema_digest`, `output_schema_digest`, and
`contract_digest`. Roles are `forward`, `compensation`, and `reconciliation`.
An assignment must match the whole tuple in its run's pinned catalog and
live worker session. A worker cannot claim by name alone or advertise `*`.
The session's protocol range must include version 1.

`WatchReady` is level-triggered. A generation change or an unobserved cursor
returns an authoritative view with `resync=true`; a disconnected worker sends
`generation=0` and `cursor=0` before claiming again. The cursor represents
committed apply progress, not a sequence of retained notifications. A worker
with no free capacity is not ready.
On a signed follower redirect, read both
`graphrun-leader-endpoint: https://host:port` and
`graphrun-leader-server-name`. Keep the worker CA, certificate, and key.
Connect to that HTTPS endpoint with its declared TLS DNS name; reject
partial, malformed, or plaintext redirects.

## Assignment and renewal

An assignment contains `run_id`, `scope_id`, `activation_id`, `attempt`,
`session_id`, activity name and version, role, `input_json`, `effect_key`,
claim `generation` and `revision`, `lease_expiry_ms`,
`attempt_deadline_ms`, codec version, and the three capability digests.
The member records the handler and input in the committed claim event. A
later projection cannot change them. The result and renewal must send the
same run, activation, session, generation, and current revision.

Renew the session and active claims every five seconds. `Renew` takes a
`command_id`, `session_id`, `activation_id`, claim `generation`, and expected
`revision`; it returns the new revision and lease expiry. A renewal never
extends `attempt_deadline_ms`. Stop starting effects five seconds before the
earlier acknowledged session or claim lease expiry, or at the attempt
deadline. Never use a late or cached renewal reply to revive a fenced claim.
The Rust worker samples wall and suspend-aware boot time every 250 ms and
before another external effect. Linux uses `CLOCK_BOOTTIME`; macOS uses
`mach_continuous_time`. A watchdog gap over two seconds, reverse boot time,
or wall and boot deltas differing by more than 250 ms plus 1,000 ppm stops
the worker and cancels its local execution permission. Restart it only
after investigating the clock fault.

## Results and retries

For `Report`, send either nonempty `output_json` and
`output_schema_digest`, or `error_code` and `error_message` with no output.
The member checks the pinned output schema, current assignment, and handler
error catalog before accepting the result. A worker reserves `worker.*`
codes for its own decoding, encoding, and handler faults. For
`Reconcile`, send `applied` with schema-valid output, or `not_applied` or
`unknown` without output. A failed probe sends `unknown` plus
`error_code` and `error_message`; the member records both the failure and
the unknown outcome. A reconciler queries the provider's operation ledger
using the original effect key. It must not create the original effect.

Generate one nonempty command ID for each logical registration, claim,
renewal, result, or probe. Reuse that ID after transport uncertainty, a
disconnect, or a leader change. The member records a digest of each worker
request in the same transaction as its effects. A retry with different
content conflicts; an equal retry returns the original receipt without
repeating events. A valid session and signed principal are required even
for a duplicate. If the old claim expires before a result is committed,
reconcile it or retry according to its pinned recovery policy. The provider
must deduplicate the assignment's stable `effect_key`.

When the first result request leaves the worker, freeze its command ID,
serialized body, claim generation, and revision. If the response is lost,
renew the session but do not renew that claim while resolving the result.
Retry the same request even if the original claim's stop margin has since
passed; an uncommitted result still fails the member's lease guard. Report
and reconciliation RPCs each have a two-second client deadline.

If registration cannot reach a leader, the Rust worker retries its original
session and registration command for at most five minutes, rotating seed
endpoints with 250 ms to five-second jittered backoff. A confirmed expired
registration is fenced before the worker creates a new session and command
identity.

## Contract digests

For each digest, hash the UTF-8 domain string, one NUL byte, and canonical
JSON using SHA-256; encode the digest as lowercase hex. Canonical JSON sorts
object keys and admits only integer numbers. Schema digests use the domain
`graphrun.worker-schema/v1` and the resolved schema JSON. Contract digests
use `graphrun.worker-contract/v1` and the activity contract JSON. A
reconciler hashes an object with `activity` and `reconciler` members,
containing the pinned forward activity and reconciler contracts. A contract
version alone cannot stand in for any of these digests.

Serialize an activity contract as a JSON object with `key` (`name/vN`),
`input_schema`, `output_schema`, `execution` (`async` or `blocking`),
`effects` (`pure` or `external`), `recovery` (`RetrySafe` or `Manual`),
`error_codes` (ordered code and retryable pairs), and `reconciler` (a
`name/vN` string or null). A named `SchemaRef` is
`{"ctor":"named","key":"name/vN"}`. A reconciler contract contains `key`
and `forward` as `name/vN` strings. Both sides hash the same serialized
contract and resolved schemas, not the catalog source formatting.
