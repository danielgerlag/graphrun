# The embedded backend and local development loop

Historical rationale. [The v1 specifications](../specs/v1/README.md) preserve the selected storage direction and supersede earlier feature exclusions.

The selected backend is redb with OpenRaft. The [v1 baseline](10-v1-design-baseline.md) fixes versions, role defaults, protocols, and limits for one-member local and multi-member clustered operation.

This replaces the earlier SQLite-local and PostgreSQL-clustered recommendation. Those remain alternatives considered, not planned adapters. YAML definitions and typed Rust activities remain unchanged by the storage choice.

## Why this direction fits

[redb](https://docs.rs/redb/latest/redb/) supplies embedded transactional storage in Rust. OpenRaft supplies consensus over engine commands. Together they allow the same log, application, and recovery path to run inside the application locally and across data-bearing members in a cluster.

The design avoids a separate database product, broker, and database-specific scheduler implementation. It does not avoid networking, replicated-state recovery, or storage operations.

OpenRaft's [getting-started guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html) makes the remaining responsibilities explicit. We provide storage, the application state machine, and transport.

## Alternatives considered

| Option | Benefit | Reason it is not the selected direction |
|---|---|---|
| SQLite locally and PostgreSQL for the cluster | Mature storage systems and a service-free local loop | Separate storage implementations and backend-specific recovery paths |
| Automatically managed PostgreSQL | Removes manual local setup while retaining PostgreSQL semantics | Still starts a database server and manages its binaries |
| Memory-only local execution | Fast isolated authoring tests | Does not exercise durable waits or restart recovery |
| redb without replication | Small embedded durable implementation | Cannot alone provide the required multi-host shared authority |

The dual-adapter design was viable. Choosing redb and OpenRaft trades that adapter work for ownership of a replicated storage integration. It is not a claim that SQLite cannot persist workflows or that PostgreSQL cannot coordinate workers.

## One-member local mode is the real engine

A local application owns a data directory, one redb-backed Raft member, an activity worker, and a client. No database daemon, Docker container, or downloaded server binary is needed.

Commands still pass through OpenRaft commitment and durable state application. Do not create a direct-redb shortcut just because the quorum contains one member.

Workflow events, materialized state, query models, and coordination records live in the same redb implementation. CQRS does not add a database process, broker, or projection service to the local loop.

The local runtime preserves retained history and artifacts as well as current runs, inboxes, deadlines, claims, and deduplication records across restart.

A developer can run a YAML workflow, stop during a wait, edit or publish another definition version, and reopen the same data directory. Active runs retain their original normalized graph.

The [OpenRaft FAQ](https://docs.rs/openraft/latest/openraft/docs/faq/index.html) supports single-member operation and membership expansion. V1 uses exact genesis manifests, explicit joining, and identity-preserving restart as defined by the baseline.

Show the resolved data directory, cluster identity, member identity, and active role at startup without logging payloads or credentials. Reject a second process opening the same member directory.

Restart retains storage identity and membership. It creates a new worker session. It must not bootstrap an existing cluster again, clear data after a compatibility error, or silently change clustered membership to one voter.

## Worker scaling is not storage scaling

An activity worker is an elastic execution role. A voter holds durable data and participates in a storage quorum. A learner receives replicated state without voting.

These roles can coexist, but they are not interchangeable. A typical production group has three stable voters for one-voter failure tolerance and a separately sized worker population.

Worker-only applications connect to the group without keeping a complete redb replica or joining the voter set. Their command and readiness connections belong to the embedded engine protocol, not an external queue provider.

Data-bearing members need stable volumes and explicit membership management. Starting more ordinary workers must not automatically repartition or enlarge the quorum.

OpenRaft provides [membership operations](https://docs.rs/openraft/latest/openraft/docs/cluster_control/dynamic_membership/index.html). We still own peer identity, secure transport, joining, promotion, planned removal, and recovery procedures.

## What this choice does not solve automatically

| Concern | Required engine work |
|---|---|
| Durable log integration | Persist votes and logs with the ordering and flush guarantees OpenRaft requires |
| Atomic application | Commit workflow events, derived state, coordination, indexes, and applied metadata together |
| Snapshot recovery | Preserve retained history and artifacts as well as current state, without overwriting member-local identity |
| History compatibility | Retain versioned events and readers for the declared replay window |
| Time and leases | Define decision time across leader changes, renewal rules, and stale-owner rejection |
| Worker notification | Reconnect and resynchronize after leader changes or compacted history |
| Version evolution | Keep replicated command interpretation compatible while allowing different activity-worker capabilities |
| External effects | Retain stable effect keys and intervention for ambiguous non-idempotent operations |

Activity code never runs inside replicated apply. A replica applying a log entry records engine state, not another payment or email.

Raft commitment does not guarantee an external effect occurs once. A worker can still lose its response after the effect happened, and an old worker can outlive its claim.

The [runtime proposal](02-runtime-and-storage.md) describes commitment, application, scheduling, and snapshots. The [event-sourcing contract](09-cqrs-event-sourcing-and-replay.md) defines event authority and read-only reconstruction.

## Local parity has a limit

Local and clustered modes share command serialization, event production and evolution, redb durability, definition compilation, and recovery logic.

A single-member run has no quorum loss, network partition, or competing leader. It therefore cannot establish those behaviors merely by sharing the implementation.

Use memory for isolated domain tests and one durable member for the ordinary local loop. Distributed scenarios need independent processes with persistent stores and controlled failures.

Compare legal outcomes and invariants, not identical event ordering or elapsed time. A more direct local request path must not change the commitment rule.

## Keep the daily loop predictable

YAML loads at runtime and publishes immutable versions. Reusing existing activity versions requires no Rust handler rebuild. An application's existing watcher can restart against the same directory.

Use fake side-effecting services in local examples. Embedded storage does not sandbox activity code or stop it calling real external APIs.

Historical reconstruction is different from running that local engine again. Its pure projector has no activity dispatcher and cannot enqueue historical ready work into the live runtime.

A failed disk write is not permission to use memory. Loss of a production quorum is not permission to create a fresh local cluster.

Expanding a local group is an explicit membership operation, not copying its database file to several machines and starting independent members. Restarting an existing group is not a new bootstrap.

Do not promise that the initial API automatically promotes a development directory into production. Preserving cluster identity, membership, and effect identities requires an explicit procedure.

## Resolved v1 boundary

The baseline selects redb 4.3.0, OpenRaft 0.9.25, Tonic/protobuf transport, staged-generation snapshots, explicit clock/lease rules, versioned event formats, and retention/resource defaults.

Start with one replicated group. Defer a public backend plugin system, multiple storage adapters, and automatic multi-group partitioning.

Implementation must establish one-member restart and multi-member recovery through the same YAML workflow and command path. Library benchmark figures are not workflow-engine performance results.
