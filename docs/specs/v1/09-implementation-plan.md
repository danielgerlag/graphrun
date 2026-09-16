# End-to-end implementation plan

The coding agent owns delivery of the complete specification, not a scaffold or one milestone. Temporary private stubs are acceptable only within an unfinished phase and must not survive the final gate.

## Phase 1. Workspace, contracts, and executable gates

Create the library, production CLI, and non-published verification-driver packages. Pin dependencies, commit the lockfile, and establish formatting, clippy, unit, compile, and integration commands.

Implement the requirement/matrix registry so missing or skipped mandatory cases fail verification. Initially unimplemented cases must report failure or blocked, not pass.

Create the public API type prototype and compile-pass/fail cases for typed activity edges, loops, parallel tuples, region returns, and compensation mappings.

Include finite external-event waits with success/timeout ports, nonterminal timeout handling, and a shared continuation. Do not postpone this control-flow API to an incidental signature adjustment.

Confirm that ordinary release builds exclude test-only fault controls and do not need a database daemon, Docker, or `protoc`.

Gate: workspace/toolchain resolution and real compile-test evidence for a feasible API shape.

## Phase 2. Common definition compiler and pure domain

Implement named/composite schemas, marked YAML parsing, scoped typed IR, normalized identity, bindings, conditions, and diagnostics.

Implement the Rust graph builder through the same compiler. Construct every normative example both ways and compare execution-relevant normalized artifacts.

Implement commands, events, pure decisions/evolution, scopes, leaves, loops, foreach, parallel all-join, external-event matching, and saga obligation state machines.

Implement the authoritative policy/default schema, the complete lease/deadline guards, reserved-event TTL precedence, and successful-forward/blocked-compensation-input handling in the pure-domain milestone.

Gate: all primitive unit/property cases and YAML/Rust parity. A class or enum stub for a control does not satisfy this phase.

## Phase 3. Durable local authority

Implement the real redb/OpenRaft adapter, ordered I/O owner, atomic event/domain/coordination/index apply, durable command results, and query barriers.

Implement one-member bootstrap/restart, the local control socket, typed activity registry, and real async/blocking execution.

Run every basic/control example through the durable local engine and restart at meaningful intermediate states.

Gate: real local persistence, upstream storage conformance, and no alternate in-memory runtime masquerading as durability.

## Phase 4. Worker protocol and effects

Implement sessions, capabilities, readiness generations, bounded claiming, renewal, results, cancellation, and reconciliation over the production protocol.

Implement the durable external-provider fixture and external worker process. Exercise stable forward/compensation keys, delayed effects, lost results, and probes.

Gate: async, blocking, remote, compensation, and reconciliation paths all have actual process/network evidence.

## Phase 5. Clustered execution

Implement mTLS identities/roles, leader routing, fresh read barriers, clock safety, explicit membership, learner catch-up, and worker-only scaling.

Run the production CLI with three real members and independent workers. Execute YAML and Rust-built loops, parallel branches, correlated events, and sagas.

Gate: supported failures retain accepted state, stale owners cannot advance it, and quorum loss never creates standalone authority.

## Phase 6. Snapshots, recovery, and lifecycle completion

Implement file-backed snapshots, staged generations, admission credits, cleanup, logical purge, physical compaction procedure, and controlled backup/restore.

Exercise open iterations, incomplete joins, buffered events, and in-progress compensation through snapshots and member replacement.

Complete cancellation, intervention, irreversible-effect summaries, nested saga transfer, and compensation abandonment.

Gate: all storage/fault and saga settlement scenarios, not only happy-path recovery.

## Phase 7. History, queries, and read-only reconstruction

Implement eager queries, sequence pagination, retained artifacts, checkpoints, retention, and versioned readers.

Implement pure reconstruction without access to live dispatch or command capabilities. Prove zero external effects while reconstructing historical ready and compensating states.

Gate: event/materialized-state agreement and explicit handling of expired or incompatible history.

## Phase 8. Full verification, operations, and documentation

Run the complete [verification contract](08-testing-and-verification.md), including every matrix ID, production-artifact E2E, platform checks, and reference-workload measurements.

Write quickstarts for local YAML, the Rust builder, all activity kinds, three-member deployment, external event delivery, compensation/intervention, backup/restore, and diagnosing a stalled run.

Document actual limits and failure assumptions. Keep generated protocol/schema artifacts synchronized with their sources.

Gate: no missing mandatory implementation, no ignored mandatory test, no fabricated evidence, and no unsupported scope reduction.

## Progress and blockers

Track each requirement as unstarted, implementing, implemented, verified, or blocked, with source and evidence paths.

Record material design deviations and their reason. Incidental API changes can improve Rust feasibility, but must update examples and preserve all mandatory behavior.

A blocked prerequisite must identify the exact missing environment, dependency, contradiction, or external access. Attempt reasonable local alternatives. Never replace the required runtime behavior with a mock just to clear a gate.

If session limits prevent completion, preserve a precise resumable state and report incomplete work. Do not label a partial implementation end-to-end complete.
