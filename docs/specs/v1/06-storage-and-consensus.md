# Storage, consensus, and time

This specification carries the durability obligations for every primitive. A loop or saga must not have a separate persistence shortcut.

## Implementation baseline

Use Rust edition 2024, redb `=4.3.0`, and OpenRaft `=0.9.25` with `serde` and `storage-v2`. The project minimum-toolchain target is Rust 1.90.0.

redb declares that minimum. OpenRaft does not declare a numeric MSRV. The coding agent must resolve and commit the complete lockfile and establish compatibility before claiming support. Do not switch to OpenRaft 0.10 prereleases or bypass incompatible APIs with an in-memory replica.

Do not enable follower-log-reversion, single-threaded, or generic-snapshot-data escape features. Keep the domain protocol independent of OpenRaft's internal serialized Rust types.

The supporting baseline is Tonic 0.14.6/prost, yaml-rust2 0.13.0, Schemars 1.2.2, jsonschema 0.56.0, rustls, Serde, and versioned canonical JSON domain records.

## Authority boundary

The write path is:

```text
authorized command with stable identity
  -> leader validation and explicit decision inputs
  -> durable OpenRaft quorum commitment
  -> deterministic decide over current applied state
  -> events plus coordination changes
  -> pure event evolution
  -> one durable redb apply transaction
```

The apply transaction contains emitted events and sequence positions, derived run/scope/activation state, coordination, correctness indexes, command results, deduplication, last-applied position, and applied membership.

Preconditions are checked at application, not only when the leader first receives a request. Duplicate commands return their prior result without another sequence range or another child scope.

No user handler, network request, current-clock read, random draw, or opaque callback executes inside replicated decision/evolution. Nondeterministic choices arrive as command data.

## Ordered storage owner

Open one redb database per data-bearing member. All mutations pass through one ordered storage thread using owned requests and response channels.

Use `Durability::Immediate` and quick-repair mode for correctness-relevant transactions. Persist votes before acknowledgement. Persist appends, report successful log flush, then return from append. Persist committed-position metadata as well as applied metadata.

Preserve OpenRaft's vote/log write ordering. Do not infer request ordering from whichever task first acquires redb's writer lock.

No write transaction may span a network await or user activity. Maintenance submits bounded batches rather than occupying the entire owner queue.

A commit error is an unknown persistence outcome, not proof of rollback. Mark the member unhealthy and reopen/recover before further writes. No volatile fallback is permitted.

## Records and indexes

Separate member-local metadata from generation-scoped replicated application records.

| Member-local | Generation-scoped application records |
|---|---|
| Cluster/member identity and genesis manifest | Immutable definitions, schema contracts, payload artifacts |
| Votes and local Raft log | Workflow events, run checkpoints, materialized domain state |
| Visible log boundaries and committed position | Scopes, child sets, loop cursors, join state, saga obligations |
| Active generation and snapshot registry | Worker/claim coordination, inboxes, deadlines, command results |
| Staging/cleanup manifests | Ready/fairness/correlation indexes and replicated policy |

An application snapshot MUST NOT replace the receiving member's identity, votes, or local log with the sender's.

Index updates are atomic with their source records. Queries and commands must not scan an entire history to find current ready work, outstanding children, matching waits, or uncompensated obligations.

## Memory and admission

OpenRaft 0.9 can materialize complete apply ranges and has an unbounded internal state-machine channel. A bounded redb queue alone is not a memory bound.

Use end-to-end admission credits of 64 MiB encoded unapplied entries and 4,096 unapplied entries per member. Count persisted-but-unapplied records and release credits only on apply, overwrite, or snapshot replacement.

Apply equivalent admission before handing inbound replication to OpenRaft. Reserve progress capacity for protocol/control traffic and already accepted work. Never truncate a requested full-range log-read result to enforce a limit.

Use at most four entries per replication batch, an 8 MiB Raft RPC envelope, and a 1 MiB public command envelope. Bound decoding concurrency as well as encoded message size.

Default application limits are:

| Limit | Value |
|---|---|
| YAML, one payload, one schema, normalized definition | 256 KiB each |
| Definition nodes across all regions | 512 |
| Region nesting | 8 |
| Data/binding/condition depth | 32 |
| Condition operators per node | 256 |
| Loop iterations or foreach items | 1,000 per node |
| Parallel branches | 16 |
| Active leaf attempts per run | 32 |
| Active leaf attempts per worker | 128 |
| Foreach child-scope concurrency | Default 8, maximum 32 |
| Materialized activations/scopes per run | 10,000 |
| Active runs per group | 10,000 |
| Queued commands per process | 1,024 |
| Claim batch | 16 |
| Per-run retained event/artifact bytes | 64 MiB plus 1 MiB reserved control records |
| Buffered external events | 1,000 per run, 100 per correlation address, 8 MiB total payloads |

Reserve capacity for in-flight results before starting effects. Limit exhaustion records a scoped resource failure or intervention without deleting history or stranding child settlement.

Containers do not hold leaf execution permits while waiting for children. Foreach concurrency controls child scopes, which is a different budget from running leaf activities.

Use deterministic fair rotation across ready runs, then FIFO readiness within a run. A run with many ready branches must not consume every worker slot while other compatible runs wait.

All decision-affecting quotas are replicated, versioned policy. Defaults affecting workflow meaning are expanded into the published definition. Capture retention policy at run admission.

Domain timeout, retry, retention, and checkpoint defaults are defined by [policies and defaults](10-policies-and-defaults.md), not the historical brainstorming baseline.

## Time and lease contract

Store UTC Unix-millisecond deadlines. The leader supplies its observed wall time in commands; apply uses the maximum of that observation and the persisted engine-time watermark.

Durable delays include ordinary downtime. Restart compares persisted deadlines with current trusted wall time rather than resuming a saved countdown.

The supported operating assumption is voter and worker clocks within one second of UTC. Under that assumption, an absolute deadline may become eligible up to one second early in real time and a relative delay up to two seconds early. V1 is not a real-time scheduler.

| Setting | Value |
|---|---|
| Session and activity lease | 30 seconds |
| Batched renewal | Every 5 seconds |
| Worker stop-dispatch margin | 5 seconds before the earlier acknowledged expiry |
| Renewal RPC timeout | 2 seconds |
| Raft heartbeat | 250 ms |
| Election timeout configuration | Randomized 1,000-2,000 ms |
| Healthy-LAN leader recovery target | Within 10 seconds |
| Retry-safe work recovery target | Approximately 40 seconds after last successful renewal |

Let `T` be the authoritative decision time used during apply. Each attempt records an immutable execution deadline at grant. Renewal never extends it or a run/workflow deadline.

| Operation | Required guards and effects |
|---|---|
| New attempt grant | Authorized live session with `T < session_expiry`; eligible activation and execution role; remaining budget; no conflicting current claim. Forward grants also require unexpired run deadline and no ancestor stop/block barrier. Record the fixed attempt deadline. |
| Session renewal | Expected session and revision, not retired, and `T < current_session_expiry`. Expired sessions require a new session, not revival. |
| Claim renewal | Matching attempt/session/generation/revision; live session; `T < session_expiry`, `T < claim_expiry`, and `T < attempt_deadline`; lifecycle permits continued execution or settlement for this role. Only the lease expiry/revision changes. |
| Result acceptance | Matching current authority and schema; `T` is strictly before session expiry, claim expiry, and attempt deadline. Active forward work may advance; stopping forward work may only settle. Cleanup roles follow their authorized phase. |
| Expiry or attempt timeout | Matching target and timer revision, followed by evaluation of current deadline values. Expire at `T >= deadline`. Stale lease timers after renewal do nothing. The immutable attempt-timeout timer is independent of lease renewal. |

An expired run deadline closes forward execution but does not prohibit authorized compensation/reconciliation or settlement of an existing attempt under its own deadline.

Worker dispatch stops at the earliest of session expiry minus five seconds, claim expiry minus five seconds, and the attempt deadline. Do not subtract the lease safety margin from a short activity timeout.

A timed-out renewal has an unknown outcome. Its original command may be retried for reconciliation, but a cached or late response is not a newly extended lease. Refresh the current assignment and issue a fresh guarded renewal before assuming new permission.

Use a suspend-aware boot clock: `CLOCK_BOOTTIME` on Linux and `mach_continuous_time` on macOS. Do not assume portable suspension behavior from Rust `Instant`.

Sample wall/boot clocks every 250 ms and before time-dependent proposals or dispatch. Clock-unsafe conditions include a delta discrepancy above 250 ms plus 1,000 ppm of elapsed time, reverse boot time, and a recovered watermark over two seconds ahead of wall time.

A watchdog gap over two seconds invalidates local execution permission pending fresh checks. Before clustered scheduling, obtain clock-health samples from a voting quorum with RTT at most 500 ms and wall readings inside the leader's send/receive interval expanded by two seconds. Refresh every five seconds; expire samples after ten.

On fault, stop time-dependent proposals and effect dispatch, preserve application of already committed records, request cooperative cancellation, and attempt transfer to a healthy voter. Resume only after explicit acknowledgement and ten seconds of healthy observations. Never lower the watermark.

If bad future time was committed, corrected wall time minus one second must catch up before resumption. One-member mode cannot detect a common wrong clock after a full outage without an independent reference.

The safety margin does not stop arbitrary user code, detached threads, or a pause between a permission check and I/O. Fencing and external idempotency remain required.

## Notifications and temporal work

Maintain ready, deadline, inbox, and child-completion indexes during apply. Use level-triggered generations for notifications.

Subscribe before obtaining an authoritative initial view. On disconnect, leadership change, overflow, or compacted cursor, resynchronize from committed state. Never silently discard the final change on a healthy-looking stream.

The leader sleeps until the next known deadline and commits bounded expiry work. Loop/parallel progression is driven by committed events and indexes, not periodic runnable-store scans.

Renewal, consensus heartbeats, clock-health probes, and deadline-driven retention are allowed. They must not hide an interval-based scan of all runs or events.

## Snapshots and log compaction

Build a snapshot from one consistent redb read transaction. Stream into a uniquely named file, finish counts/checksums, sync, rename, sync the directory, then publish metadata durably.

The container has a framing version, protobuf manifest, bounded opaque record frames, per-frame checksums, and a SHA-256 footer. Preserve historical event bytes.

Include retained events/artifacts, loop state/cursors, frozen child sets, incomplete joins, pending signals, current claims, saga obligations and completion state, deduplication, applied position, and membership.

Receive a complete verified durable file, import into an inactive generation in transactions of at most 4 MiB or 4,096 records, then atomically activate the generation with applied/membership/snapshot metadata.

Normal state application remains serialized behind installation. Vote/log I/O can progress between batches. A crash before activation leaves the old generation authoritative; a crash after activation leaves the complete new one authoritative.

Use file-backed transport, 1 MiB chunks, 16 MiB operation buffers, one build and one inbound transfer per member, a 30-second stall limit, and a two-hour final-install limit.

Trigger snapshots after 20,000 applied entries, 512 MiB applied log bytes, or 30 minutes with changes. Disable OpenRaft's count-only policy and implement this combined controller explicitly. Retain a 1,024-entry snapshotted tail.

Default snapshot admission limit is 128 GiB. Reserve incoming-file space, estimated imported-generation space, and `max(8 GiB, 20% of DB size)` cluster headroom. The local-development floor is 512 MiB. Start import estimates at twice encoded size and monitor actual usage.

Logical log purge/truncation first updates durable visible boundaries. Bounded GC later removes invisible bytes. Readers must not resurrect old truncated rows. Physical redb compaction is offline per member.

Snapshot registry pointers are authority, not the highest filename. Never fall back to an older applied state after corruption.

## Membership, upgrades, and restore

Bootstrap an exact genesis manifest on pristine directories. Local genesis has one voter; clustered genesis has three. Restart does not reinitialize.

Join fresh never-reused IDs as learners, catch up through a barrier, then change membership. Preserve a healthy existing majority through replacement. Worker registration never changes the voter set.

Gate new command/event/graph/snapshot behavior after reader-first rollout to every configured data-bearing member. A changed binary must not silently reinterpret an old-version command during rollout.

Retain event/checkpoint readers for the promised history window. There is no in-place workflow-instance migration.

Backups are logical application snapshots, not raw live database copies. Disaster restore fences the source cluster, uses new cluster/member identities, preserves domain identities/history, and starts with execution disabled.

Committed recovery actions fence old permissions and place restored active scopes into intervention. Preserve event history instead of editing domain state offline. Admin acknowledgement of the recovery point is required before normal admission or continuation.

Compensating and settling sagas must restore their exact obligations and phase. Restore must not restart compensation already recorded as complete.

Supported failures are crash-stop, partitions, machine restart, and power loss when the storage stack honors flushes. Byzantine members, network filesystems, dishonest storage, and loss of all durable copies are outside the model.
