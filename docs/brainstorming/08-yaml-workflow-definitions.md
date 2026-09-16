# YAML workflow definitions

Historical DSL proposal. [The current language specification](../specs/v1/02-language-and-primitives.md) includes scoped choices, loops, parallel regions, and sagas. Use its examples rather than the earlier reduced grammar here.

YAML workflow definitions are a hard requirement for the first release, alongside clustered workers and service-free durable local development.

YAML is the v1 workflow-authoring format. It compiles to one validated graph and execution engine. A public Rust graph builder is outside v1.

The [v1 baseline](10-v1-design-baseline.md) fixes the scope, grammar, policies, and limits. The examples are design artifacts, not workflows that this repository can execute yet.

## Compile definitions before admitting runs

```text
YAML source -> bounded parser -> definition AST -----+
                                                    |
                                shared graph compiler
                                  + activity catalog
                                                    |
                                     validated definition
                                                    |
                                  PublishDefinition command
                                                    |
                                  OpenRaft commit and apply
                                                    |
                            immutable definition artifact in redb
                                                    |
                              run admission and execution commands
                                                    |
                                    workflow domain events
                                                    |
                            derived state and query models in redb
```

The definition AST represents source syntax and retains source locations. The compiled graph resolves node references, activity versions, payload contracts, bindings, and control-node semantics.

Persist the normalized graph, its format version, its digest, and its activity dependencies. Retain source provenance for diagnostics. Workers execute that artifact rather than parsing a YAML file on every wake.

Any later Rust builder must use this graph rather than introduce opaque closures or a second execution model.

## An initial definition shape

The example defines a sequential fulfillment workflow.

```yaml
dsl: graphrun/v1
id: fulfillment
version: 1
input_schema: order/v1
output_schema: receipt/v1
start: reserve_stock
nodes:
  reserve_stock:
    kind: activity
    activity:
      name: inventory.reserve
      version: 1
    input:
      from: workflow.input
    retry:
      errors: [inventory.unavailable]
      max_attempts: 4
      backoff:
        initial: "1s"
        multiplier: 2
        max: "30s"
    next: charge
  charge:
    kind: activity
    activity:
      name: payment.charge
      version: 1
    input:
      from: nodes.reserve_stock.output
    next: finish
  finish:
    kind: complete
    output:
      from: nodes.charge.output
```

`dsl` versions the language. `version` identifies an immutable workflow definition. Activity versions and payload schema versions are separate contracts.

Node keys such as `reserve_stock` are stable identities. Map order does not define execution order. `start` and explicit edges do.

Node keys match `[a-z][a-z0-9_]{0,63}`. This keeps binding references unambiguous. Workflow and activity names use the baseline's separate case-sensitive ASCII identifier rule.

`max_attempts` includes the initial execution. The retry policy applies only to declared activity error codes. Storage transaction retries do not consume this budget.

The [approval example](examples/approval.yaml) adds a declared signal, timeout edge, typed condition, durable delay, and explicit failure endpoint.

That example's `approval/v1` schema is an object with a required boolean `approved` property. Both examples assume that the registered inventory and payment handlers declare `RetrySafe` and honor the supplied effect key.

The example assumes the following registered contracts.

| Activity | Version | Input schema | Output schema |
|---|---|---|---|
| `inventory.reserve` | 1 | `order/v1` | `reserved-order/v1` |
| `payment.charge` | 1 | `reserved-order/v1` | `receipt/v1` |

The application registers real Rust handlers for these contracts. YAML names the contracts, not Rust types, constructors, or executable code. Service credentials stay in host configuration.

## Bind YAML to typed Rust activities

An activity catalog associates a stable name and version with input and output contracts, supported error codes, and the implementation's declared recovery policy.

The local catalog can come from typed Rust activity registrations. A clustered publisher can use trusted persisted contract descriptors. Compilation should not depend on which workers happen to be online.

An unknown catalog reference is a definition error. A known activity with no currently compatible worker is a capacity or deployment problem, not malformed YAML.

The compiler compares bindings against declared contracts. At execution boundaries, the registered adapter decodes an activity's actual input and validates its output before committing it.

YAML cannot provide Rust compile-time type safety. It can provide precise load-time graph and schema errors, followed by runtime validation of actual data. Do not disguise that distinction with unchecked type casts.

Ordinary Rust errors remain ordinary Rust errors. Registration maps them to stable error codes such as `inventory.unavailable` and a durable failure record. YAML retry rules use those codes rather than matching display strings.

The YAML author cannot make a non-idempotent external API safe by setting a flag. Retry and ambiguity policies must remain consistent with the registered activity's capabilities.

## Bindings are data, not source code

The v1 binding forms are the following.

| Form | Meaning |
|---|---|
| `from` | Read the workflow input or an available node output |
| `from` with `path` | Select a declared field using a JSON Pointer |
| `literal` | Supply a constant value |
| `object` | Assemble an input object from named child bindings |
| `array` | Assemble an ordered input array from child bindings |

Each binding has exactly one root form. The compiler rejects combinations of `from`, `literal`, `object`, and `array`.

An optional `path` uses [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html), not a programming language. A path such as `/order_id` selects data. It does not call a method or execute a query.

Use versioned JSON-compatible payload records, Schemars schema export, and the `jsonschema` validator with the supported Draft 2020-12 subset. Only local nonrecursive `$defs` references are allowed. Network/file retrieval and schema-injected defaults are disabled.

Integers retain exact signed-64-bit values, floats are finite, and the engine does not silently coerce numeric kinds. The baseline fixes canonical encoding and resource bounds.

For whole-payload connections, compare explicit schema identities rather than guessing structural compatibility. For projections and object assembly, validate the supported structural schema rules. Reject unsupported conversions instead of coercing values silently.

Actual values can still violate a declared contract. Missing required data is a reported binding or decoding error, not an implicit `null` or a retried business failure.

A node output must exist on every path that reaches its consumer. Referencing a skipped branch, a future node, or an arbitrary previous loop iteration is not a valid binding.

V1 rejects graph cycles, parallel execution, and loops. There is one active path per run. A later extension needs its own activation-scope and output-binding rules.

Complex transformations belong in named activities initially. Do not embed Rust, JavaScript, shell commands, SQL, or a general-purpose expression evaluator in a string.

## A small language that covers the engine

| V1 node kind | Required fields and durable meaning |
|---|---|
| `activity` | `activity`, `input`, and `next`; claim the named activity/version and expose its successful output |
| `delay` | `duration` and `next`; persist a deadline, expose no data output, and resume when eligible |
| `wait_signal` | `signal`, `timeout`, and `next`; expose the declared signal payload on success; finite timeout also requires `on_timeout` |
| `choose` | Ordered nonempty `cases` and required `default`; choose one edge and expose no data output |
| `complete` | `output`; validate and commit the workflow result |
| `fail` | `error.code` and literal `error.message`; fail the workflow without a successful output |

A `wait_signal.timeout` is a duration or null. Null is an explicit indefinite wait and forbids `on_timeout`. A root `signals` map declares each signal name's payload schema. One signal name may be used by only one wait node in the v1 definition.

An activity can set a `timeout` and `retry` policy within the baseline bounds. Unspecified defaults are expanded into the normalized graph at publication. Activity errors do not introduce implicit graph edges.

A `choose` case contains `when` and `next`. Conditions have one of `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `all`, `any`, `not`, or `exists`.

Comparison operators take two bindings. Equality requires the same scalar kind. Ordered comparison requires the same numeric kind. `all` and `any` take nonempty condition lists and short-circuit left to right. `not` takes one condition. `exists` takes a reference binding and distinguishes missing from explicit null.

The first true case wins. `default` is always required. There is no arithmetic, regex, arbitrary function evaluation, or string interpolation.

V1 has no parallel/fan-out, join, loop, child-workflow, compensation, or cron nodes. Unsupported kinds are errors, not reserved placeholders interpreted at runtime.

Reject unknown or unavailable node kinds at load time. Do not persist a definition that will only discover an unsupported operation after a run starts.

## Validation belongs at definition boundaries

Use `yaml-rust2` marked parser events for the data-only [YAML 1.2](https://yaml.org/spec/1.2.2/) profile. Require one document, string mapping keys, finite JSON-compatible scalar values, and the baseline's limits.

Reject duplicate keys before constructing an ordinary map. A parser that silently keeps the last value could change a node or retry policy before graph validation sees it.

For the initial profile, reject custom tags, anchors, aliases, merge keys, and implicit include directives. Do not fetch schemas, files, or URLs while compiling a definition. Catalog lookup is explicit and controlled by the host.

Limit source to 256 KiB, definitions to 256 nodes, and data/binding/condition nesting to 32 levels. Each condition tree has at most 256 operators. Reject unknown fields so a misspelled retry option does not silently use a default.

Compilation resolves the following obligations.

1. The language version, workflow identity, and payload contracts are supported.
2. The start node, activity versions, and every edge target exist.
3. Required fields match each node kind and binding variant.
4. Referenced outputs are in scope and available before their consumers.
5. Payload bindings and declared error policies are compatible.
6. Durations, retry budgets, and control-flow limits are valid.
7. The graph is acyclic, every node is reachable, and only one path executes at a time.

After compilation, internal code uses typed node and binding variants. Loading a persisted graph still validates its serialized envelope and format version at the storage boundary. Actual activity data is a separate boundary and still needs decoding.

Keep filename, line, column, node key, and field path in definition errors. A binding directly from `workflow.input` to the example's `charge` activity should report the expected `reserved-order/v1` and actual `order/v1`.

Generate editor schema and completion metadata from the same versioned definition model where possible. Do not maintain an unrelated editor schema that accepts fields the runtime ignores.

## Publication is immutable and topology-neutral

Publishing a definition commits an OpenRaft command that records the validated graph and dependency contracts in replicated engine state. redb stores that state on each data-bearing member. Publication does not execute an activity or require a separate definition server.

Publishing the same workflow ID, version, and normalized digest is idempotent. Different content under an existing version is a conflict.

The digest covers execution-relevant graph structure, bindings, policies, and dependency contracts. Formatting, comments, and mapping order do not change execution identity. Ordered condition branches remain ordered.

Record the compiled graph format version separately from the YAML language version. A compiler upgrade must not silently reinterpret active runs.

Storage replicas must support the interpretation of a newly published graph and command format. Gate format upgrades across the required members. A normalized graph must not require arbitrary application closures to execute during replicated apply.

Every run pins one published definition. Admission supports `latest` only as an alias resolved once during admission. Never resolve it again at each step.

`RunStarted` records that immutable definition reference. Workflow events retain exact activity inputs and outcomes, signal payloads, and selected control-flow decisions as data or immutable artifact references.

YAML authors do not declare the engine's lifecycle events. The command decision path generates them, and event evolution derives workflow state. CQRS does not add a second YAML language or require host applications to become event-sourced.

Workers need the persisted graph format and the relevant activity handlers and codecs. They do not need a copy of the original YAML file or a Rust workflow-definition type.

Workflow-only changes that reuse deployed activity versions can publish without recompiling those handlers. A new activity implementation still requires deployment. Editing YAML is not hot-reloading Rust code.

Do not patch an active run's graph when its source file changes. New semantic content uses a new workflow version. Live instance migration remains a separate feature.

## Local development stays service-free

The default example loads YAML from a local file, registers ordinary Rust activities, and uses a durable one-member redb/OpenRaft group.

An application can reload a file through the same compile-and-publish API or use its existing watcher to restart. There is no requirement to embed YAML with `include_str!` or rebuild Rust for every definition edit.

After restart, active runs resume from their persisted graph snapshot. Deleting or editing the source file does not replace that snapshot. Missing activity code or an unsupported graph format remains an explicit compatibility problem.

Retain normalized definitions and payload artifacts for the promised history window, not only while runs are active. Historical reconstruction applies recorded facts and does not run the current YAML conditions or activity handlers again.

Simulation against changed YAML and live forks remain separate future capabilities. They cannot silently reuse an old result for different activity input or execute missing outcomes against a real external service.

A reload with changed semantics under the same version returns a conflict and preserves the prior definition and runs. Bumping the version affects only new runs.

The same YAML source and activity contracts must compile to the same normalized graph in one-member and multi-member groups. Membership topology must not introduce DSL-specific behavior.

## V1 boundaries

The parser, schema tooling, grammar, and resource limits are fixed by the baseline. The implementation still needs to establish the declared behavior; no parser or compiler runtime exists in this repository yet.

Publishing uses the authenticated admin API or the private local control interface. There is no unauthenticated YAML upload endpoint, visual editor, scripting engine, automatic include mechanism, or dynamic activity-binary loading in v1.
