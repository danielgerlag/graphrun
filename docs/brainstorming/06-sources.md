# Sources and research scope

These sources informed historical brainstorming. [The v1 implementation specifications](../specs/v1/README.md) are the current contract.

The Workflow Core critique refers to one source snapshot.

| Field | Value |
|---|---|
| Repository | [danielgerlag/workflow-core](https://github.com/danielgerlag/workflow-core) |
| Commit | [`f64d503a7217b1f314a23379da1f4c6bec70c267`](https://github.com/danielgerlag/workflow-core/commit/f64d503a7217b1f314a23379da1f4c6bec70c267) |
| Commit date | 2026-09-14 |
| Inspection date | 2026-09-15 |
| Method | Source inspection of scheduling and execution paths, plus primary backend and Rust documentation |

Source links in the [critique](01-workflow-core-lessons.md) use the commit hash rather than a moving branch.

The critique distinguishes observed code from architectural consequences. It does not claim production incident frequency, measured performance, or the original authors' motivations.

No Workflow Core runtime failure was reproduced during this research. Failure timelines describe what the inspected ordering permits or what a proposed design must handle.

## Research boundaries

The scheduling investigation covers producers, background consumers, queue and persistence contracts, locks, recovery polling, and representative providers.

The execution investigation covers the graph, execution pointers, results, builders, definition registry, DSL, retries, and compensation.

These are separate source investigations, not independent reviews of every conclusion. The proposals combine their findings with the documented backend semantics.

| Research area | Result | Remaining limit |
|---|---|---|
| Scheduling and delivery | ISSUES. Source establishes split persistence and queue operations, polling, and provider-specific delivery behavior | Not an exhaustive provider comparison |
| Execution and authoring | ISSUES. Source establishes mutable pointer state, integer step identities, execution-round persistence, and runtime type coupling | No measured performance results or complete DSL audit |
| Backend and Rust alternatives | Primary semantics support the tradeoff analysis | No engine prototype or authoring comparison has run |

Provider observations do not establish identical behavior across every provider or release. This is an architectural critique, not a security audit or a production-readiness verdict.

The API examples illustrate the resolved v1 contracts and are not runnable implementations. The scenarios are an implementation acceptance plan, not reported results.

Clustered workers and service-free durable local development were confirmed as product requirements on 2026-09-15. The selected direction is now redb with OpenRaft, replacing the earlier PostgreSQL-only and SQLite-local/PostgreSQL-clustered recommendations.

The embedded-replication design uses primary OpenRaft and redb documentation. SQL and managed-database references below document alternatives considered. No replicated backend has been implemented or benchmarked.

YAML DSL workflow definitions were also confirmed as a hard requirement on 2026-09-15. The earlier suggestion to defer the DSL is superseded. The YAML syntax and compiler contract are proposals informed by the language specification and the inspected Workflow Core loader.

The illustrative YAML files are not runtime implementations. V1 selects yaml-rust2, Schemars, and jsonschema; the actual parser integration and compiler remain unimplemented.

Lightweight CQRS and workflow-domain event sourcing were selected on 2026-09-15. This supersedes the diagnostic-journal-only proposal. The architecture retains a separate operational and Raft recovery model, and it does not claim that replay tooling or live forks have been implemented.

The user delegated remaining choices to engineering judgment. The [v1 baseline](10-v1-design-baseline.md) now fixes scope, defaults, and protocols. Focused clock and storage research informed those choices; numerical budgets are selected targets, not upstream defaults or measurements.

## Baseline-specific evidence

| Reference | Evidence used |
|---|---|
| [redb 4.3.0 manifest](https://raw.githubusercontent.com/cberner/redb/v4.3.0/Cargo.toml) | Version 4.3.0, Rust 2024, and declared Rust 1.90 requirement |
| [OpenRaft 0.9.25 manifest](https://raw.githubusercontent.com/databendlabs/openraft/v0.9.25/openraft/Cargo.toml) | Selected stable line and `serde`/`storage-v2` features; storage-v2 remains an upstream unstable API |
| [Tonic 0.14.6](https://docs.rs/crate/tonic/0.14.6/source/Cargo.toml) | Published gRPC implementation baseline |
| [yaml-rust2 0.13.0 parser](https://docs.rs/yaml-rust2/0.13.0/yaml_rust2/parser/index.html) | Marked parser-event integration rather than duplicate-overwriting loading |
| [Schemars 1.2.2 manifest](https://raw.githubusercontent.com/GREsau/schemars/v1.2.2/schemars/Cargo.toml) | Schema exporter and declared Rust 1.74 minimum |
| [jsonschema 0.56.0](https://docs.rs/jsonschema/0.56.0/jsonschema/) | Draft-specific validators and configurable format/reference behavior |
| [Rust SystemTime](https://doc.rust-lang.org/std/time/struct.SystemTime.html) | Wall time is not monotonic |
| [Rust Instant](https://doc.rust-lang.org/std/time/struct.Instant.html) | Suspension behavior is not a portable guarantee |

The Rust 1.90 project floor is a compatibility target for the eventual locked dependency graph, not a verified upstream OpenRaft MSRV.

## Repeatable source inspection

[`inspect-reference.sh`](inspect-reference.sh) reads a path and line range from the pinned Git object. It ignores local working-tree edits and does not change the reference checkout.

Its arguments are a local Workflow Core checkout, a repository-relative source path, a first line, and a last line.

```sh
bash docs/brainstorming/inspect-reference.sh \
	/path/to/workflow-core \
	README.md 6 6
```

The checkout must contain the pinned commit. If it does not, a fetch can obtain that object without changing the checked-out branch.

```sh
git -C /path/to/workflow-core fetch origin \
	f64d503a7217b1f314a23379da1f4c6bec70c267
```

The helper prints the immutable source URL and numbered lines. It fails for an absent commit, missing path, malformed range, or a range beyond the file.

## Primary backend references

The selected backend uses the redb and OpenRaft contracts. PostgreSQL, SQLite, rusqlite, and managed PostgreSQL remain reference material for the superseded alternatives.

| Reference | Claim it supports |
|---|---|
| [PostgreSQL NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) | Notifications follow transaction commit, can coalesce, have limited payloads, and are distinct from table data |
| [PostgreSQL LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) | Registration belongs to the session; startup must commit `LISTEN` before inspecting state |
| [PostgreSQL row locking](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) | `SKIP LOCKED` skips locked rows and is suitable for queue-like consumers, not a complete consistent view |
| [SQLite isolation](https://www.sqlite.org/isolation.html) | Writes serialize and separate connections observe committed state |
| [SQLite WAL](https://www.sqlite.org/wal.html) | WAL permits concurrent readers with one writer, requires same-host shared memory, and has durability-profile tradeoffs |
| [rusqlite README](https://raw.githubusercontent.com/rusqlite/rusqlite/master/README.md) | The bundled feature compiles and links SQLite, with no database service at runtime |
| [redb overview](https://docs.rs/redb/latest/redb/) | redb is a pure-Rust embedded transactional store, not a replication or workflow system |
| [OpenRaft getting started](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html) | Applications provide durable storage, state-machine logic, and networking |
| [OpenRaft log storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftLogStorage.html) | Vote and log I/O have ordering, durability, and flush-callback obligations |
| [OpenRaft state-machine storage](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html) | Applied state, membership, and snapshots have explicit persistence and recovery obligations |
| [OpenRaft membership](https://docs.rs/openraft/latest/openraft/docs/cluster_control/dynamic_membership/index.html) | Learners, voters, and membership changes are distinct operations |
| [OpenRaft FAQ](https://docs.rs/openraft/latest/openraft/docs/faq/index.html) | Single-member operation is supported; membership persists across restarts |
| [Managed PostgreSQL](https://docs.rs/postgresql_embedded/latest/postgresql_embedded/) | Automating local setup still installs and runs PostgreSQL binaries |

## Primary Rust references

| Reference | Claim it supports |
|---|---|
| [Rust async functions in traits](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/) | Future `Send` bounds and dynamic dispatch require deliberate API design |
| [Tokio Notify](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html) | A notification permit does not count queued jobs, and multi-consumer wake ordering needs care |
| [Tokio bounded channels](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html) | Bounded channels provide backpressure and have explicit shutdown behavior |
| [Tokio sleep_until](https://docs.rs/tokio/latest/tokio/time/fn.sleep_until.html) | A deadline wait does no work while asleep and depends on the runtime's timer |
| [Tokio shutdown](https://tokio.rs/tokio/topics/shutdown) | Requesting shutdown and waiting for tasks to finish are separate responsibilities |

## Definition-language references

| Reference | Claim it supports |
|---|---|
| [YAML 1.2.2 specification](https://yaml.org/spec/1.2.2/) | YAML is a data serialization language; the workflow DSL must define its accepted profile and semantics |
| [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) | A standard syntax can address payload fields without evaluating arbitrary source code |

## Command and history references

| Reference | Claim it supports |
|---|---|
| [CQRS](https://martinfowler.com/bliki/CQRS.html) | Command and query models can be separate while sharing storage; distributed services and eventual consistency are not mandatory |
| [Event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) | Recorded events can reconstruct domain state, while replay involving external effects needs a separate boundary |

These external documentation pages were read on the inspection date. Unlike the Workflow Core source links, `current` and `latest` documentation can change.
