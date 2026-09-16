# Typed Rust graph builder

The Rust graph builder is mandatory v1, with every primitive available through the same IR as YAML.

The selected shape combines typed handles with reusable typed regions. A builder closure can construct graph data once. No closure remains in the published graph as executable decision logic.

## Public model

The following shapes are a contract sketch, not compiled library code. The implementation's first milestone must make representative usages compile and add compile-fail cases before committing to incidental lifetime signatures.

```rust
pub struct RegionBuilder<I> { /* private draft graph */ }
pub struct RegionGraphBuilder<I> { /* explicit wiring over the same draft model */ }
pub struct Region<I, O> { /* owned serializable draft and schema witnesses */ }
pub struct WorkflowBuilder<I, O> { /* name, version, root region */ }
pub struct ActivityRef<I, O> { /* validated catalog key and schema witnesses */ }
pub struct NodeRef<O> { /* private node identity and output witness */ }
pub struct ValueRef<T> { /* private build-scope identity and expression */ }
pub struct InputMapping<T> { /* schema-checked declarative binding */ }
pub struct Branch<I, O> { /* name, captured input expression, body region */ }
pub struct TimedWaitRef<T> { /* success-only value and distinct control ports */ }
pub struct TerminalRef<O> { /* terminal node without an outgoing port */ }
pub struct EntryPort { /* private build-region and target node */ }
pub struct ExitPort { /* private build-region, source node, and outcome */ }
```

Use private constructors for typed handles. Looking up an activity with arbitrary generic arguments must validate them against its catalog contract rather than manufacture a type witness.

Cloning a `ValueRef<T>` clones a reference to graph data, not an application value, and must not require `T: Clone`.

## Caller usage

A sequential definition should look approximately like this.

```rust
let mut root = RegionBuilder::<Order>::new();
let reserved = root.activity("reserve", &reserve, root.input())?;
let charged = root.activity("charge", &charge, reserved.output())?;
let root = root.complete("finish", charged.output())?;
let definition = WorkflowBuilder::new("fulfillment", 1, root).build(&catalog)?;
```

`reserve` is `ActivityRef<Order, ReservedOrder>` and `charge` is `ActivityRef<ReservedOrder, Receipt>`. Passing the original `Order` directly to `charge` must fail compilation.

Appending in `RegionBuilder` links its current tail. For nonlinear regions, `RegionGraphBuilder` declares detached nodes and connects explicit control ports. Both use the same draft and compiler, not different runtimes.

Explicit node keys define durable identity. Constructor position may define sequence order in the convenience builder but must not become a node ID.

A reusable loop body has its own explicit input and output.

```rust
let mut body = RegionBuilder::<Counter>::new();
let bumped = body.activity("increment", &increment, body.input())?;
let body = body.complete("iteration_done", bumped.output())?;

let mut root = RegionBuilder::<Counter>::new();
let count = root.literal(3_i64);
let repeated = root.repeat("repeat", count, root.input(), body, 10)?;
let root = root.complete("finish", repeated.output())?;
```

The loop result is `Counter`, not a vector of every intermediate carry. `foreach` is the array-producing primitive.

## Required operation signatures

Equivalent names/signatures are permitted only if the implementation updates examples and preserves these typing obligations.

| Operation | Required type relation |
|---|---|
| `input()` | `ValueRef<I>` for the current region |
| `literal<T>(value)` | Owned serializable typed literal |
| `activity(key, ActivityRef<A,B>, ValueRef<A>)` | Produces `NodeRef<B>` |
| `map_input<T>(binding)` | Fallibly checks a declarative binding against T and returns `InputMapping<T>` |
| Mapped activity input | Accepts `InputMapping<A>`, never an unchecked cast to `ValueRef<A>` |
| `choose(key, input, cases, default)` | Every case/default is `Region<A,B>`, result `NodeRef<B>` |
| `while_loop` / `do_while` | Initial `S`, guarded `Region<S,S>`, result `S` |
| `repeat` | Captured integer count, initial `S`, `Region<S,S>`, result `S` |
| `foreach` | `Vec<A>`, body `Region<A,B>`, result `Vec<B>` |
| `parallel` | Ordered named heterogeneous branches, result tuple in that order |
| `saga` | Captured A and body `Region<A,B>`, result B |
| `delay` / `wait_until` | Control node without data output |
| Linear `wait_signal<T>` | Explicit indefinite wait, linked success continuation and result `NodeRef<T>` |
| `declare_timed_wait<T>` | Finite wait with distinct success/timeout ports and a success-only `ValueRef<T>` |
| Explicit `declare_*` operations | Detached forms for every primitive, with typed outputs and declared control ports |
| `start_at(entry)` / `connect(exit, entry)` | Wire nodes in one region without moving the implicit tail |
| Explicit `declare_complete<O>` / `declare_fail<O>` | Create terminal nodes without consuming the graph builder |
| Explicit `finish<O>()` | Validate all paths and terminal contracts, then return `Region<I,O>` |
| `complete(key, ValueRef<O>)` | Consumes the builder and returns `Region<I,O>` |
| `fail<O>(key, error)` | Terminal failing region with declared O and no fabricated successful value |

Provide typed tuple branch composition through 16 branches. Internal macro-generated tuple implementations are acceptable. A public `Vec<Any>` substitute is not.

## Finite waits and shared continuations

The explicit builder must express both wait outcomes, including nonterminal timeout paths and a shared suffix. A finite wait must not silently fail on timeout merely because the convenience API returns a successful payload type.

This sketch corresponds to [the timeout-recovery fixture](examples/timeout-recovery.yaml). `approval` is a validated signal reference and `increment` is `ActivityRef<Counter, Counter>`.

```rust
let mut graph = RegionGraphBuilder::<Counter>::new();
let input = graph.input();
let key = graph.literal("approval".to_owned());
let wait = graph.declare_timed_wait(
	"approval", &approval, key, Duration::from_secs(1),
)?;
let recovery = graph.declare_activity("recover", &increment, input.clone())?;
let finish = graph.declare_complete("finish", input)?;
graph.start_at(wait.entry())?;
graph.connect(wait.success_port(), finish.entry())?;
graph.connect(wait.timeout_port(), recovery.entry())?;
graph.connect(recovery.exit(), finish.entry())?;
let region = graph.finish::<Counter>()?;
```

Every port and value carries its private build-region identity. Reject cross-region links, duplicate outgoing connections, missing required edges, unsupported cycles, and unreachable nodes.

`connect` accepts an exit port and an entry port, not a payload reference. Ordinary nodes expose one success exit, timed waits expose success and timeout exits, and terminals expose no exit.

The successful wait payload may be used only where its success edge guarantees availability. Connecting a timeout path to a consumer of that payload is a build error, even though the wait node itself dominates both paths.

Terminal declaration does not finalize an explicit graph. This permits several terminal paths and shared continuations without fabricating output values. The implementation must prove this example and a negative timeout-output case in the API milestone.

## Schema composition

`DurablePayload` provides owned Serde encode/decode, Schemars-compatible metadata, and a stable schema reference.

Named application types declare their stable name/version explicitly. Do not derive persistent names from `type_name`, Rust `TypeId`, module paths, or compiler symbols.

Provide blanket supported constructors for `Vec<T>` and tuples using `SchemaRef::Array` and `SchemaRef::Tuple`. Their identities preserve child nominal identities and tuple order.

Support built-in scalar and unit types. The serialized representation must match YAML, including fixed tuple length and item schemas.

## Expressions and scope honesty

Whole-value references carry Rust type witnesses. String JSON Pointer projection is not statically proven by Rust.

Use declarative expression/binding constructors and a fallible schema-binding API for string paths and object assembly. Do not expose `field::<T>` as an unchecked assertion about an arbitrary pointer.

Loop/item/compensation context references are provided only to their corresponding region-construction context or rejected by the shared compiler when used outside it.

Every handle records its private build scope. Finishing a region validates lexical ownership and availability. A child handle cannot silently refer to the most recent output with the same node name in a sibling or another iteration.

The implementation may strengthen scope checks with generative lifetimes, but must not claim compile-time scope safety without compile-fail evidence. Honest build-time scope errors are required regardless.

Build-time scope IDs are not serialized durable identities. Normalization derives definition paths from explicit names and records runtime scope identities through committed execution.

## Compensation construction

Attach compensation to an activity node inside a saga body.

Provide a typed direct form when the compensator input is the forward output, and a schema-checked mapping form for `forward.input`/`forward.output` assembly.

A wrong direct compensator input type must fail compilation. A wrong dynamic mapping must fail build-time schema validation.

The annotation contains a handler key/version and binding IR, not the handler closure. The runtime creates obligations only when forward success is accepted.

Both direct and mapped forms accept compensation timeout/retry options from the policy contract. A runtime compensation-input failure produces a blocked-input obligation without undoing the accepted forward result.

Pure/irreversible declarations, nested saga transfer, reconciliation, and blocking/remote compensators must be expressible without bypassing the shared definition compiler.

## Normalization and equivalence

The builder produces a draft definition and invokes the same validation/normalization code as YAML.

Defaults, composite schemas, node paths, control modes, region input/output, handler versions, compensation mappings, and branch order must match in the normalized artifact.

Create paired YAML/Rust fixtures for every primitive and nested combination. Compare normalized bytes or an explicitly execution-relevant canonical representation, not only final output.

Do not build a second runtime for the Rust API or use function pointers hidden in a process-local graph registry.

## Design synthesis

Three independent sketches explored combinators, typed handles, and region-first composition. A separate cross-judge preferred typed handles.

The base is typed handles. The accepted graft is reusable `Region<I,O>` values. Explicit stable keys are mandatory on every node/branch.

Rejected ideas include schema names derived from Rust type names, identity derived from constructor position, homogeneous vectors as a substitute for heterogeneous parallel tuples, and `repeat` returning a vector instead of carry state.

The judge's confidence about compile-time scope enforcement was not accepted as proof. The first implementation milestone must compile the API examples and negative cases.

This preserves interface depth: callers specify workflow intent and typed dataflow while the builder owns edge linking, scopes, schemas, normalization, and diagnostics.
