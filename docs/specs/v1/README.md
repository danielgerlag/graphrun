# Graphrun v1 implementation specification

This directory is the normative implementation contract. It supersedes `docs/brainstorming/`, especially the earlier exclusions of loops, parallel branches, compensation, and a Rust graph builder.

The engine has not been implemented. Specification/example checks are not evidence that the runtime works.

## Mandatory delivery

V1 must provide all of the following.

| Requirement | Required result |
|---|---|
| YAML authoring | Every supported primitive compiles from YAML to the common IR |
| Rust graph builder | The same primitives can be composed through a typed Rust API |
| Loops | Durable `while`, `do_while`, `repeat`, and `foreach`, including nested bodies |
| Branching | Exclusive `choose` and concurrent `parallel` with an all-join |
| Compensation | Durable saga compensation, including nested and parallel work |
| External events | Correlated durable input, early arrival, repeated waits, deduplication, and timeout races |
| Activity kinds | Async, blocking, remote-worker, compensation, and reconciliation handlers |
| Local development | A real durable one-member engine inside the application, with no database daemon or broker |
| Clustering | redb/OpenRaft members with independent workers, fencing, membership, and recovery |
| History | CQRS and event-sourced workflow state, with read-only historical reconstruction |
| Verification | Extensive automated tests and real release-artifact, multi-process end-to-end exercises |

Dropping one of these requirements is not an acceptable way to simplify implementation.

## Specification map

| File | Owns |
|---|---|
| [Domain model](01-domain-model.md) | Identities, scopes, lifecycle, commands, events, and invariants |
| [Language and primitives](02-language-and-primitives.md) | YAML/IR shape, bindings, basic controls, external events, and schemas |
| [Loops and parallelism](03-loops-and-parallelism.md) | Iteration, frozen collections, child scopes, joining, and failure settlement |
| [Activities and compensation](04-activities-and-compensation.md) | Handler kinds, recovery, reconciliation, saga obligations, and operators |
| [Rust builder](05-rust-builder.md) | Typed construction, schema composition, compile-time boundaries, and YAML parity |
| [Storage and consensus](06-storage-and-consensus.md) | redb/OpenRaft integration, time, snapshots, resources, and restore |
| [API and operations](07-api-and-operations.md) | RPC/CLI contracts, authentication, local loop, deployment, and observability |
| [Policies and defaults](10-policies-and-defaults.md) | Attempt/run timeouts, retry selection, compensation defaults, retention, and checkpoints |
| [Testing and verification](08-testing-and-verification.md) | Required suites, executable commands, scenario matrix, and evidence |
| [Implementation plan](09-implementation-plan.md) | Ordered delivery units and completion gates |
| [Coding-agent prompt](IMPLEMENTATION_PROMPT.md) | Copy-ready end-to-end implementation instructions |

The [examples](examples/README.md), [requirement map](requirements.tsv), and [verification matrix](verification-matrix.tsv) accompany these documents.

Run `python3 docs/specs/v1/check-specs.py` to check documentation links, requirement/test traceability, catalog references, and example-kind coverage. This checks specification structure, not engine behavior.

## Interpretation and precedence

`MUST` and `MUST NOT` describe release requirements. Defaults may change only through explicit, versioned policy where this specification allows configuration.

Detailed ownership is given by the table above. Resolve a genuine contradiction by recording it and preserving the stricter safety requirement. Do not silently substitute the old brainstorming scope.

The Rust API spelling may be adjusted to satisfy Rust's type system, but capabilities, typed boundaries, normalized-IR equivalence, and compile-test obligations cannot be weakened.

A release is complete only when the implementation plan's final gate and the full verification contract are satisfied. A build, mocked test, screenshot, or manually edited database is not an end-to-end result.

## Explicit non-goals

V1 does not include arbitrary workflow scripts, dynamically loaded activity binaries, cron calendars, a separately versioned child-workflow primitive, race/quorum joins, automatic multi-group sharding, Byzantine consensus, hostile-tenant isolation, retroactive event editing, changed-code simulation, or live history forks.

Compensation is not rollback. External effects are not generally exactly-once. Historical reconstruction must never dispatch work.

Independent runs, loop iterations, and parallel branches may overlap. No primitive may introduce a shared mutable application data object.
