# An embedded Rust workflow engine

This folder is historical brainstorming for a Rust reimagining of [Workflow Core](https://github.com/danielgerlag/workflow-core). The [implementation specifications](../specs/v1/README.md) are now authoritative.

The latest requirements make loops, parallel branches, compensation, and a Rust graph builder mandatory for v1. Earlier exclusions and single-active-path assumptions below are superseded. Do not implement this historical baseline instead of the specification set.

Clustered workers are a hard requirement for the first release. Multiple application processes, including processes on different hosts, must share and execute the same durable work.

The local development loop must also run without extra services. Local runs need real persistence across restarts, not just an in-memory substitute.

YAML DSL workflow definitions are also a hard requirement. Authors must be able to define workflows in YAML and bind their activities to ordinary Rust handlers.

For application developers, the goal is YAML workflow authoring with typed Rust activities and inspectable execution history in both a local application and a cluster. For maintainers, the selected design combines one definition compiler, replicated command path, event-sourced workflow state, and embedded store.

## The selected direction

Use **redb for durable storage and OpenRaft for consensus**. Local mode is a durable single-member group. Clustered mode uses multiple data-bearing members and independently sized activity workers.

Every mutation of authoritative engine state follows the same command, commit, and apply path in both modes. Do not independently plug together persistence, queues, and locks or bypass consensus with direct local writes.

Use lightweight CQRS and event sourcing for workflow-domain state. Committed commands produce versioned domain events, and those facts determine materialized workflow state and workflow-domain query models.

The Raft log is not permanent workflow history. Store the event streams and their required immutable artifacts in redb. Keep live lease and consensus bookkeeping as separate operational state rather than recording every heartbeat as a workflow event.

Applying a committed result atomically records events, derived workflow state, coordination changes, critical indexes, command results, and Raft applied metadata. Notifications wake workers but are not the only record that work exists.

The [CQRS and replay design](09-cqrs-event-sourcing-and-replay.md) makes historical reconstruction read-only. Replaying recorded facts does not rerun activities, grant live claims, or restore a whole cluster from a per-run history.

Compile YAML into a validated, versioned finite DAG with stable activity bindings. Each run has one active path, while many runs execute concurrently. V1 has no Rust graph builder, loops, parallel branches, compensation, or scripting.

The [YAML design](08-yaml-workflow-definitions.md) includes an [illustrative definition](examples/fulfillment.yaml), a validation contract, and the distinction between load-time YAML checks and Rust compile-time types.

This replaces the earlier SQLite-local and PostgreSQL-clustered recommendation. The [backend decision](07-durable-backends-and-local-development.md) records the alternatives and the operational responsibilities we accept.

## Local development and clustered execution

| Design choice | What it gives us | Cost |
|---|---|---|
| One redb-backed member inside the local application | Durable runs and waits without another service process | No replica failover |
| A replicated group of stable data-bearing members | Committed state shared across hosts | Quorum, membership, snapshots, and storage operations |
| Activity workers separate from voter membership | Worker scaling without changing the storage quorum | Leader-routed commands and resubscribable readiness notifications |
| Committed claims and indexed deadlines | Fencing and scheduling without periodic runnable-store scans | Explicit time, renewal, and recovery protocols |

A typical three-voter group tolerates one unavailable voter. Workers can run on those members or in worker-only application processes. Local mode uses the same durable path with a quorum of one.

Embedded means that host application processes run the engine and, where configured, its storage member. No external database or broker is part of the selected architecture.

The backend, CQRS, and workflow event-sourcing direction are selected. Transport, clock and lease policy, snapshots, retention, DSL grammar, dependency baselines, and release exclusions are resolved in the baseline.

## Queue duplicates are only one kind of duplication

A unique activation record can prevent duplicate logical work. It cannot guarantee that an external side effect happens once.

If a payment succeeds and the process stops before recording its result, another attempt may be necessary. The engine needs a stable external idempotency key or an explicit intervention state. A new attempt number must not become a new payment key.

Likewise, removing periodic polling does not remove startup recovery, reconnect reconciliation, or work at a known timer deadline. The [runtime proposal](02-runtime-and-storage.md) states the boundary explicitly.

Raft heartbeats and lease renewal still exist. Activity side effects never execute inside replicated state application.

## Where the current architecture is weakest

The [source-backed critique](01-workflow-core-lessons.md) identifies these priorities.

1. Separate persistence and queue operations make recovery depend on rediscovery.
2. Queue and lock contracts cannot express delivery ownership and stale-writer rejection.
3. Workflow locks span user I/O, while progress is persisted after an execution round.
4. Overlapping state fields and runtime-typed payloads weaken state and upgrade contracts.
5. Integer graph positions and accumulated execution history complicate long-lived definitions and incremental persistence.

Workflow Core already has useful mitigations, versioned definitions, and recovery for events that arrive before a wait. The proposal should preserve those capabilities rather than dismiss them.

## Documents

| Document | Purpose |
|---|---|
| [V1 design baseline](10-v1-design-baseline.md) | Resolved scope, protocols, defaults, resource budgets, and explicit non-goals |
| [Workflow Core lessons](01-workflow-core-lessons.md) | Current execution flow, architectural findings, qualifications, and pinned source links |
| [YAML workflow definitions](08-yaml-workflow-definitions.md) | A first-class DSL, stable activity contracts, shared graph compilation, and immutable publication |
| [CQRS, event sourcing, and replay](09-cqrs-event-sourcing-and-replay.md) | Workflow event authority, derived views, operational state, and safe replay boundaries |
| [Runtime and storage](02-runtime-and-storage.md) | redb and OpenRaft boundaries, member and worker roles, commands, snapshots, claims, and timers |
| [Embedded backend and local development](07-durable-backends-and-local-development.md) | The selected storage direction, alternatives considered, and one-member local operation |
| [Execution and failure semantics](03-execution-and-failure-semantics.md) | Duplicate handling, external effects, signals, retries, cancellation, upgrades, and compensation |
| [Rust developer experience](04-rust-developer-experience.md) | Typed activity registration, YAML loading, client/member/worker roles, and lifecycle contracts |
| [Decisions and experiments](05-decisions-and-experiments.md) | Selected decisions and implementation acceptance scenarios |
| [Sources and research scope](06-sources.md) | Source revision, primary references, limitations, and the source-inspection helper |

## Design principles applied

| Principle | Specific choice it changed |
|---|---|
| Model the Domain | One persisted activation state machine replaces a queue of ambiguous repeated IDs and overlapping lifecycle flags |
| Make Operations Idempotent | Admission and signal IDs deduplicate caller retries; stable effect keys remain separate from changing attempt and ownership tokens |
| Type System Discipline | Rust activities remain typed; YAML binds through validated schemas rather than pretending to have Rust compile-time guarantees |
| Build the Lever | `inspect-reference.sh` retrieves cited lines from the pinned Git object rather than a moving checkout |
| Prove It Works | Source claims point to inspected code; unimplemented runtime guarantees remain proposals with explicit acceptance experiments |
| Redesign from First Principles | YAML, local durability, and clustered execution shape the first release together rather than becoming later extensions |
| Foundational Thinking | Record versioned workflow events and required artifacts from the first durable release so later replay does not depend on missing history |
| Experience First | The default development example persists state inside the application without Docker, a database daemon, or runtime binary downloads |
| Boundary Discipline | YAML is parsed and compiled at publication; runtime code consumes a validated graph and decodes actual activity payloads at their boundaries |
| Laziness Protocol | Keep commands, events, and eager query models in one redb/OpenRaft implementation rather than adding a broker or separate CQRS services |
| Outcome-Oriented Execution | Replace unresolved alternatives with one v1 baseline and mark unsupported features out of scope rather than preserving competing designs |

The baseline separates decisions from evidence still needed during implementation. Capacity figures are targets, and clock/failure assumptions are explicit. Changes to this baseline are deliberate design revisions, not unresolved defaults.
