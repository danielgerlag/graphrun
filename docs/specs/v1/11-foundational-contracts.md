# Versioned authority and verification contracts

This document fixes the interfaces between the v1 delivery units. It specifies
the required final behavior, not a claim that the 0.1.x runtime already exposes
these records or APIs. See the domain, storage, API, and verification
specifications for their owning state machines. Implementations may choose Rust
type names but MUST preserve these wire and persistence semantics.

## Committed command result

A logical command is identified by `(cluster_id, authenticated_principal_id,
command_id)`. The caller generates a nonempty command ID once and reuses it
across retries, redirects, and leader changes. Persist the deduplication key,
canonical request digest, and `graphrun.command-result/v1` record in the same
apply transaction as its emitted events, indexes, and last-applied position.
The record contains the operation and target identity, an `Applied` or
`Rejected` disposition, a stable response or domain error code and typed
details, and the inclusive per-run event sequence range when events were
emitted. A committed rejection may have no event range. Reusing a key with a
different request digest is a conflict; an equal retry returns the recorded
result without creating another run, event range, or effect identity.

Transport timeout, quorum loss, or uncertain storage commit is `Unknown`,
**not** a committed result and never an inferred `Rejected`. Return the
command identity and unknown-outcome details so the caller can retry/query
the same identity. Authentication and authorization precede dedup lookup:
neither a cached result nor a guessed principal may bypass permission checks.
An accepted forward result with a blocked compensation-input obligation is
`Applied` with the intervention state, not a failed submission to be retried.
Unknown tagged versions fail closed; roll out readers before writers.

## Immutable published and retained artifact identity

A published definition has identity `(workflow_name, explicit_version,
normalized_format_version, normalized_digest)`. Publication of the same
name/version with the same canonical normalized bytes and digest is idempotent;
different bytes or semantics under that version conflict. A run pins that
exact definition, expanded policy, and referenced contract/schema versions at
admission; `latest` is resolved only once, after start-key deduplication.

Each payload artifact is addressed by `(cluster_id, artifact_format_version,
sha256)` where the digest is SHA-256 of the exact UTF-8 bytes
`"graphrun-artifact/v1\0"`, followed by the canonical schema reference,
one NUL byte, and the canonical versioned payload bytes. Verify the digest on
read/import and reject a same-identity/different-byte collision. References
include format, schema, byte length, and digest; storage location, filename,
mutable URL, or a latest-version alias is not identity. Published artifacts
and accepted input/output/event payloads are immutable. Retain transitive
definition, schema, contract, and payload references while an active run,
retained event, checkpoint, or command result needs them. A missing or
unsupported retained artifact is an explicit unavailable-history error, not
an empty value or a fresh compilation of today's source.

## Store format and upgrade boundary

Every member directory owns an immutable genesis `(cluster_id, member_id)` and
a durable store-format manifest with `reader_floor`, `writer_format`,
`active_generation`, and enabled domain/command/event/checkpoint format
versions. The manifest and generation registry are member-local authority;
application data are generation-scoped. A process opens a directory only if
it can read every retained required record and its writer format is enabled
for that member by committed cluster policy. Unknown versions or missing
required readers reject startup/admission with `FailedPrecondition`, not
automatic reinitialization or a read-only-looking success.

Use reader-first rolling upgrades across every configured data-bearing
member before activating a new writer version. Activation is committed,
version-gated, and monotonic; an older binary cannot silently write after
activation. Keep old event/checkpoint readers through their promised
retention window. Snapshot import validates framing/checksums and all
referenced record versions before atomically activating a complete generation;
never choose the largest filename or fall back to an earlier generation after
corruption. Restore assigns new cluster/member authority but preserves
domain/event/artifact identities and fences old execution.

## Worker capability and signed peer identity

A worker session advertises `(worker_session_id, authenticated_principal_id,
protocol_version_range, capacity, capabilities)` after mTLS authentication.
A capability is an exact tuple of activity name and version, execution role
(`Forward`, `Compensation`, `Reconciliation`), payload codec version, input
and output schema digests, and required contract digest. Match assignments
against the pinned published contract and the live session capability; a
display name, compatible-looking JSON, or a stale prior session is not a
match. The advertised protocol range must include the selected wire version.
Claim generation/revision, deadlines, result identity, and fencing remain
mandatory even when capability matches. Worker readiness is not membership.

Cluster certificates are signed by the configured cluster CA and carry
canonical URI subject alternative names
`spiffe://graphrun/<cluster-id>/<role>/<principal-id>`. A principal may have
multiple role URIs in one certificate, but all must name the same cluster and
principal. Parse and validate the signed SAN, not an unauthenticated request
field, subject CN, or a caller-supplied metadata header. Reject absent,
duplicate-conflicting, malformed, or cross-cluster identities. The `member`
role also requires the certificate principal's member ID and endpoint to
match committed roster authority. Authorize every operation against the
authenticated role **before** cache lookup or deduplication; worker results
additionally require current session/assignment authority. Local owner-only
control uses filesystem ownership instead of a fabricated TLS principal.

## History, indexes, and retention

The authoritative event key is `(run_id, monotonically increasing u64
run_sequence)`; an event stores its record version, command/principal cause,
payload artifact reference, and recorded decisions. A committed command's
inclusive event range and eager domain state share the apply transaction.
Checkpoint keys are `(run_id, through_run_sequence)` with a versioned
projection and the exact pinned definition/reader dependency. History pages
are ordered by sequence and return `retained_from`, `retained_through`, and a
next cursor; a request before the retained floor reports an unavailable range
instead of silently beginning later. A projection folds recorded facts only:
it cannot dispatch an activity, probe a provider, or call a live command API.

Correctness indexes for ready work, fair scheduling, deadlines, wait
correlation, outstanding children, obligations, and retained artifact
references are updated atomically with their source facts. Index entries
name the source identity/revision and generation so stale notifications or
timers cannot recreate work. Rebuild an index only from the authoritative
retained state under a controlled versioned migration, never by guessing from
current source files. Raft log compaction does not prune domain history;
retention cannot remove any dependency of an active or still-retained run.

## Verification status and certification

Every matrix ID has exactly one fresh case record tied to a unique invocation
ID, current source/binary SHA-256 fingerprints, command/configuration,
duration, expected and observed outcomes, and in-run artifact paths. Its
status is exactly `PASS`, `FAIL`, or `BLOCKED`. `PASS` requires a successful
current execution and independently validated observations; a prior file,
matching text in a failed test process, ignored/filtered test, missing
binary, or absent artifact cannot satisfy it. `FAIL` includes scenario
failure, missing/invalid evidence, stale identity, unsupported case, and
non-performance blockers. A per-case record is not sufficient until final
matrix coverage validates every expected ID and rejects duplicates.

Only `PERF-*` may be `BLOCKED`, and only when reference hardware is unavailable
after actual measurements, hardware details, and the reason are recorded.
Normal verification may exit successfully with such an honest blocker but
sets `release_certified=false`; strict release certification exits nonzero
unless **every** matrix ID passes, including performance. Missing measurements
or failing scenarios are `FAIL`, never a performance hardware exception.
Specifying this gate does not attest that any as-yet-unimplemented contract
has passed.
