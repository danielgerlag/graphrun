# Coding-agent implementation prompt

Copy the instructions below into a coding-agent session in this repository.

---

Implement Graphrun v1 end to end. This repository currently contains design material rather than a completed engine. Your deliverable is working Rust code, reproducible verification tooling, extensive tests, usable examples, and an honest evidence-based handoff.

## Authoritative specification

Read every document in `docs/specs/v1/`, starting with `README.md`, `requirements.tsv`, and `verification-matrix.tsv`.

The normative documents are:

- `01-domain-model.md`
- `02-language-and-primitives.md`
- `03-loops-and-parallelism.md`
- `04-activities-and-compensation.md`
- `05-rust-builder.md`
- `06-storage-and-consensus.md`
- `07-api-and-operations.md`
- `08-testing-and-verification.md`
- `09-implementation-plan.md`
- `10-policies-and-defaults.md`
- `11-foundational-contracts.md`

Use `docs/specs/v1/examples/` as required fixtures and build equivalent Rust definitions.

`docs/brainstorming/` is historical background. Its earlier exclusions of loops, parallel branches, compensation, and a Rust graph builder are superseded. Do not implement the old reduced scope.

## Non-negotiable capabilities

Implement all of these for v1:

1. YAML definitions and a typed Rust graph builder normalizing to one versioned IR.
2. Sequence, exclusive choice, relative/absolute waits, correlated external events, and scoped complete/fail.
3. Durable while, do-while, repeat, and foreach, including nested bodies, bounded iteration, stable identities, and ordered results.
4. Concurrent parallel branches with all-join, duplicate-safe resolution, output isolation, fair capacity, and failure settlement.
5. Saga compensation, including nested saga transfer, deterministic reverse causal order, durable obligations, reconciliation of uncertain forward effects, retries, and explicit incomplete-compensation outcomes.
6. Async, blocking, external-worker, compensation, and reconciliation handler contracts.
7. Genuine durable one-member local operation inside the application without a DB daemon, broker, Docker, or runtime server download.
8. Real redb/OpenRaft clustering with three voters, independent workers, mTLS, clock/lease safeguards, snapshots, membership, and recovery.
9. Lightweight CQRS, authoritative workflow events, eager current views, retained artifacts, and pure read-only historical reconstruction.
10. The entire testing and E2E contract, not only unit tests or a successful build.

Do not defer or feature-flag away loops, parallelism, compensation, or the Rust builder. They are hard v1 requirements.

## Engineering boundaries

Preserve existing user changes. Inspect the worktree before editing. Do not deploy, force-push, erase unrelated data, or use real payment/email/customer services.

Use one code path for local and clustered execution. Local mode still commits/applies through a durable one-member group. Do not substitute a fake Raft implementation or direct-redb fast path.

All logical workflow changes arise from domain events during committed command application. Persist events, derived state, operational coordination, critical indexes, dedup/results, and applied metadata atomically.

Do not run activities, reconciliation probes, compensators, clocks, random generators, or opaque application closures in replicated evolution or historical replay.

Activities own inputs/outputs. Nested scopes capture immutable data. No shared mutable workflow object, unchecked public `Any`, or unsafe cast may be used to bypass the model.

Keep worker sessions, storage-member identity, Raft terms, scope/activation identity, attempts, and effect keys distinct.

Never treat dropping a future as proof an external effect stopped. Settlement and reconciliation must precede compensation where an effect could still apply.

Preserve a schema-valid forward success even if compensation input cannot be constructed. Record a blocked-input obligation and an accepted receipt, then require the explicit intervention path. Never retry the successful forward activity to repair a compensation binding.

Implement the full lease predicate table, including strict expiry and fixed attempt deadlines. A fresh renewal cannot revive an expired session/claim or extend execution time.

Implement durable event reservation so TTL cleanup cannot remove input owed to a pending wait. The Rust builder must represent both finite-wait outcomes and shared continuations, with edge-sensitive payload availability.

Use the normative policy/default document. Do not invent timeout/retention values or recover them from superseded brainstorming.

Use versioned explicit protocols and schemas. No silent fallback to memory, standalone mode, an older snapshot, empty success, or guessed data is permitted.

## Work sequence

Follow `09-implementation-plan.md`. Track work by requirement and matrix IDs with source/evidence locations.

Begin with the workspace, API compile prototype, common IR/compiler, and pure domain tests. Then implement real persistence and execution, clustering, failure recovery, and full E2E.

The API examples are specification sketches, not proven Rust. Make them compile early. You may improve incidental names/lifetimes where necessary, but update the specs/examples and preserve typed whole-value edges, honest field-path errors, scope safety, all primitives, and YAML parity.

Temporary stubs may exist only within an unfinished implementation phase. They must not survive the final gate or be counted as completed requirements.

Use focused subagents only for genuinely independent files or investigations. Do not let multiple writers own the same module. Review their artifacts yourself.

Do not spend the session only producing another plan or researching already selected architecture. Implement in bounded units, run the relevant evidence after each unit, and continue until the full contract is met or a concrete blocker remains.

## Required verification tooling

Implement the `graphrun-e2e` driver described in the verification spec. It must launch and clean up owned processes, coordinate fixtures through explicit barriers, and produce a machine-readable report for every matrix ID.

Provide deterministic application handlers and a durable external-provider ledger. Count physical requests separately from logical effects and compensation.

Use the production release CLI for ordinary E2E. Instrumented fixtures may provide exact crash points but must use the same engine/storage/transport code. They do not replace release-artifact verification.

Generate ephemeral test certificates locally. All fixture traffic stays on owned local/test endpoints. Test-only fault controls must not appear in the shipping build.

Every E2E scenario needs independently expected outputs and observable evidence, such as run/history responses, provider balances/effect counts, member progress, snapshot metadata, or process outcomes. Do not fabricate state or hard-code workflow IDs to obtain expected results.

Coordinate through readiness/events and bounded waits. Do not rely on arbitrary sleeps or kill processes by name. Track exact owned child IDs and reap them.

## Commands to deliver and execute

The repository must support the following after implementation:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --doc --locked
cargo build --release --locked -p graphrun-cli
cargo run --locked -p graphrun-e2e -- verify \
	--cli target/release/graphrun \
	--matrix docs/specs/v1/verification-matrix.tsv \
	--artifacts target/e2e-artifacts
```

Run the selected minimum toolchain as well as the supported current toolchain. Establish the full dependency resolution and commit its lockfile. If compatibility is genuinely impossible, document the exact evidence and smallest justified adjustment rather than claiming support.

Keep the release build free of verification-only features, including through Cargo feature unification. Confirm that it does not expose fault hooks.

The full matrix must cover all primitives, activity kinds, nested interactions, local restart, multi-process execution, failures, snapshots, history, and replay. Merely defining test names or marking them ignored is not coverage.

## Completion predicate

You may report end-to-end completion only when:

- Every hard requirement has a real implementation path.
- Every mandatory matrix ID has passing evidence or is explicitly reported as blocked, in which case overall completion is not claimed.
- All YAML fixtures and equivalent Rust graphs compile and execute with expected semantics.
- Real local and multi-member/worker runs demonstrate the behavior.
- Compensation and reconciliation handle uncertain effects without inventing rollback guarantees.
- Historical reconstruction produces no live commands or effects.
- No mandatory feature remains stubbed, skipped, mocked away, or replaced by a success-shaped fallback.
- The implementation is documented and another developer can rerun the same commands.

Performance figures in the specification are targets. Record actual hardware, workload, and results. If the reference environment is unavailable, report that gate blocked; do not pretend a weaker run meets it.

## Final handoff

Report implemented capabilities, material deviations, exact commands run, matrix/evidence locations, and remaining failures or blockers.

Include instructions for running the local example, the Rust builder examples, and a three-member cluster with workers.

If you cannot finish within the session, preserve a resumable implementation state and say what is incomplete. Do not claim success based on compilation, partial tests, screenshots, or an agent's self-report.

---
