# Testing and end-to-end verification

This is a release gate, not a suggested test list. Implement the executable verification tooling and every mandatory scenario in [the matrix](verification-matrix.tsv).

Compilation, mocked execution, matching two projections produced by the same bug, or screenshots alone do not establish completion.

<!-- TODO(CONTRACT-001, final verifier): Run a fresh three-voter, authenticated
     integration case that concurrently republishes equal and changed catalog
     and definition bytes, retries one command ID across leader loss and restart,
     checks Applied/Rejected v1 receipts and inclusive event ranges against
     independently read history, and verifies every retained definition,
     catalog, and payload artifact digest after snapshot import. The
     publication-start tests cover domain/redb/Raft, local CLI, and signed
     gRPC principal behavior; do not report PASS until immutable payload
     artifact retention is integrated and the complete case actually
     executes. -->

## Test layers

| Layer | Required coverage |
|---|---|
| Unit | Definition parsing/normalization, schemas, bindings, conditions, every state transition, errors and quotas |
| Property | Generated bounded graphs, event-fold equivalence, duplicate commands, interleavings, identity and ordering invariants |
| Compile | Valid Rust builder usage and compile-fail cases for incompatible typed edges and returns |
| Storage | Real redb transactions, upstream OpenRaft storage suite, crash cuts, generation installation and corruption handling |
| Protocol | Real encoding, mTLS roles, command dedup, stale ownership, reconnect and flow control |
| Local integration | Real durable one-member engine, every primitive, restart, local control, history and replay |
| Cluster integration | Independent member/worker processes, real transport, quorum and leader failures, snapshots and membership |
| End to end | Production CLI/SDK artifacts, YAML and Rust definitions, external-worker/provider processes, independently observed outcomes |
| Performance | Measured command, compile, recovery, history and snapshot costs under declared workload/hardware |

Use Cargo tests, proptest for bounded property cases, trybuild for compile-pass/fail coverage, Tokio's controlled time where appropriate, and a Rust end-to-end driver. Add only tooling serving these obligations.

## Unit and property requirements

Every primitive must have tests for valid entry, successful exit, invalid input, duplicate commands/results, cancellation, parent propagation, and recovery of its durable intermediate states.

Generate small nested regions with bounded loops and branches. Preserve failing seeds and shrunk traces. The oracle must not merely call the implementation twice.

Cover every allowed parent/child pair among choose, while, do-while, repeat, foreach, parallel, and saga, using bounded generated or explicit domain cases. Add representative real local/clustered cases rather than claiming that generated domain tests alone cover integration.

For domain reconstruction, fold retained events with the pure projector and compare all logical state against the eagerly materialized state. Separately assert externally specified values and ordering so a shared reducer bug cannot make two wrong views pass.

Generate invalid graphs: missing edges, cycles outside loop nodes, duplicate keys, invalid schemas, cross-region handles, future-output references, wrong loop carry types, and conflicting wait keys.

Record exact activity inputs/outputs and scope identities in assertions. Do not assert only that a run reached some terminal state.

## Rust API compile evidence

Compile examples for every primitive and nested combinations. YAML-equivalent Rust definitions must produce equal execution-relevant normalized IR.

Compile-fail cases must include wrong activity input type, wrong loop-body carry type, wrong region output, invalid compensation input type for the typed direct form, and incompatible parallel tuple use.

String JSON pointers and dynamic bindings must fail through a clear build-time error where static Rust cannot prove them. Do not claim such cases are compile-time checked.

Before implementing the whole engine, build a compilable API prototype sufficient to compile these cases. Correct incidental signatures if necessary without dropping capabilities or using public `Any`, unchecked casts, or runtime workflow closures.

Include the finite-wait shared-tail fixture from the builder specification. Both success and nonterminal timeout paths must be constructible. Using the wait's success-only payload on its timeout path must fail build-time availability analysis.

Domain and persistence coverage must include valid forward success with failed compensation-input materialization, reserved-event TTL races, fresh renewal after expiry before timer processing, and exact policy-default expansion. A happy-path saga or simple wait cannot substitute for those cases.

## Storage and deterministic fault injection

Run OpenRaft's own storage conformance suite against the real adapter.

Add deterministic fault points around vote/log commit, flush acknowledgement, event append, domain/index update, applied metadata, snapshot file publication, generation activation, and cleanup.

Instrumented fault controls exist only in verification binaries/features. The production release artifact must not expose a fault RPC or environment variable that permits arbitrary state damage.

Crash tests terminate exact owned child PIDs and reopen their actual directories. They must not emulate a crash only by constructing another in-memory object.

Assert complete old or new transactions, correct applied prefix, stable event/command identities, preserved compensation progress, and truthful unknown outcomes.

Test both immediate restart and a peer taking over before the old process returns.

## Production artifact versus instrumented fixtures

Build the production CLI separately in release mode without verification features.

Use that artifact for ordinary local and clustered end-to-end scenarios. Instrumented member/worker fixture processes may be used for exact crash cuts, but their result is not a substitute for exercising the shipping artifact.

Keep the verification driver non-published. It may spawn its own executable in fixture-member, fixture-worker, and fixture-provider modes to avoid extra manually managed services.

Fixtures must use the real library/transport/storage adapter. They may provide deterministic application activities and fault controls, not an alternative workflow executor or fake consensus layer.

## External side-effect observation

The provider fixture exposes real local RPC/HTTP calls and keeps a durable operation ledger independent from engine state.

It records physical request count, logical effect key, applied effect, output, compensation, and reconciliation responses.

An idempotent retry scenario should show multiple physical requests when faults require them but one logical forward effect. Compensation uses its own key and must produce one logical undo.

The provider can hold requests behind explicit barriers and later report pending, applied, conclusively not applied, or unknown. This is required to prove that compensation does not race an unsettled forward effect.

Use fixed expected balances, arrays, counters, and histories. Do not infer external success solely from a workflow's own `Completed` status.

## Mandatory end-to-end program

The driver must perform the following using real artifacts and isolated temporary directories.

1. Start a durable local engine with no database daemon, broker, Docker, or runtime binary download.
2. Register fixture activities, publish YAML, and execute every primitive/activity kind.
3. Run equivalent Rust-built definitions and compare normalized IR and observed outcomes.
4. Stop during a loop body, event wait, join, and compensation, then restart the same store.
5. Start three voting members and at least two independent worker processes with generated test certificates.
6. Publish/start/signal/query through the production API and CLI.
7. Force out-of-order and duplicate results, fail a leader, partition an old owner, and remove quorum.
8. Restore quorum, perform learner catch-up and voter replacement, and verify accepted state survived.
9. Build/install a snapshot while runs contain iterations, pending child scopes, events, and compensation obligations.
10. Reconstruct histories in a context with no activity or command capability and assert zero provider effects.
11. Exercise cancellation and intervention, including explicit unresolved-compensation abandonment.
12. Shut down all owned children, inspect their exit results, and retain evidence.

Use readiness and fixture barriers, not arbitrary sleeps, to coordinate races. All waits have deadlines and useful failure diagnostics.

The quick local loop and full cluster verification are distinct modes. The full driver may start owned fixture processes automatically; users must not need to prepare external infrastructure.

## No-polling proof

After a cluster is quiescent with no runnable work or business deadlines, observe it for 60 seconds.

Instrument all ready-index discovery reads and scheduler wake causes. Assert that no recurring ready-store scan occurs during the interval. Counting an unused `poll_total` variable is not sufficient.

Consensus heartbeats, clock-health probes, active lease renewal, and known retention deadlines are separate categories. They must not re-enumerate workflow readiness as a side effect.

## Required commands

The implementation must provide these commands or update this document and the implementation prompt together with an equally explicit replacement.

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

For final release signoff add `--release-certification` to `verify`. The
default verification mode still rejects any functional `FAIL`, `BLOCKED`, or
missing case; it allows only measured `PERF-*` hardware blockers and reports
`release_certified=false`. Strict release certification also exits nonzero on
any blocked performance case or a substituted/incomplete matrix.

Also run the workspace on the selected minimum toolchain. If the pinned dependencies cannot meet it, resolve and document the compatibility change instead of asserting untested support.

The release build command must not enable test-only features through workspace feature unification. Assert that the resulting binary exposes no test fault controls.

## Evidence format

The driver writes a machine-readable report with one result per matrix ID in
a fresh invocation-specific directory under `--artifacts`, plus a latest
`report.json` at the requested artifacts root. Reusing the same artifacts
root must never reuse old case evidence. The report records unique run ID,
source and binary fingerprints, certification flag, and per-case status.

Each result records status, command/configuration, binary/version identifiers, duration, expected and actual observations, and artifact paths.

The final `verify` command must be self-contained. It runs the required test targets or collects their per-case evidence within the same fresh verification run. It must not infer unit/property/compile coverage from an unrelated earlier Cargo exit code.

Give each test case its own evidence file and aggregate afterward. Bind
evidence to a fresh run ID and current source/binary fingerprints. Require
successful test processes and executed named tests, not matching log text
alone. Stale, missing, ignored, filtered, or mismatched evidence fails the
coverage gate. Preserve every result even when a different case fails.

Retain node and worker logs, provider effect records, queried run/history JSON, snapshot metadata, process exit statuses, and failing seeds/traces.

The report must distinguish `PASS`, `FAIL`, and `BLOCKED`. Missing,
filtered-out, ignored, or silently skipped mandatory cases are not pass.
Only `PERF-*` may be `BLOCKED`, and only for unavailable reference hardware
after actual measurements and observed hardware details are retained.
Missing measurements or a failed performance scenario are `FAIL`, not
`BLOCKED`. A report with any `BLOCKED` result cannot be release-certified.

A nonzero scenario outcome produces a nonzero driver exit status. The driver validates matrix coverage, not merely the scenarios its author remembered to register.

## CI and completion

Fast pull-request checks include formatting, clippy, unit/property/compile/storage tests, and a representative release-artifact local/cluster path. Linux and macOS must both run the relevant platform checks.

The full matrix and fault campaign are required before declaring end-to-end implementation complete, even if scheduled separately for CI efficiency.

Performance targets use the declared reference configuration. If that configuration is unavailable, report the performance gate blocked and record actual hardware. Do not relabel a weaker measurement as the target.

The reference target is 1,000 committed engine commands/second with p95 receipt latency below 100 ms on three same-region four-vCPU/8-GiB members with local durable SSDs and 1 KiB payloads. Exclude slow application activity time from command latency.

Target local readiness below one second for an existing store with at most 10,000 events and 1 KiB payloads. Target compilation below 100 ms for a 512-node definition on the same reference machine class. Also report snapshot, replay, retained-history, and nested-control resource costs.

These are targets, not existing results or guarantees during quorum loss, clock faults, storage failure, or intervention.

No mandatory primitive may remain `todo!`, `unimplemented!`, ignored, mocked away, or represented by a successful empty fallback.

The final handoff must separate implemented capabilities, executed evidence, and any genuine blocked gate. It must not claim full completion while a required gate is absent.
