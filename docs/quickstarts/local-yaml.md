# Run YAML from your process

Open `Engine::local` and call `start_yaml`. You do not install a database or run `graphrun serve`.

## Definition

[`samples/01-hello-world/workflow.yaml`](../../samples/01-hello-world/workflow.yaml)

```yaml
dsl: graphrun/v1
id: hello_world
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: hello
nodes:
  hello:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: workflow.input}
    next: goodbye
  goodbye:
    kind: activity
    activity: {name: counter.increment, version: 1}
    input: {from: nodes.hello.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.goodbye.output}
```

Contracts: [`samples/01-hello-world/catalog.json`](../../samples/01-hello-world/catalog.json). `counter.increment` is a built-in fixture (`{value: n}` → `{value: n+1}`). Input `{value: 0}` finishes at `{value: 2}`.

## Run it

```rust
let catalog = Catalog::from_json(include_bytes!("catalog.json"))?;
let engine = Engine::local("./graphrun-data").await?;
let input = Value::Object(BTreeMap::from([("value".into(), Value::Int(0))]));
let run = engine.start_yaml(include_str!("workflow.yaml"), &catalog, input).await?;
let output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
```

Put `workflow.yaml` and `catalog.json` next to the source, as the sample does. From this repository:

```sh
cargo run -p graphrun-samples --bin 01-hello-world
```

A longer graph (reserve, then charge) is [`samples/02-passing-data/workflow.yaml`](../../samples/02-passing-data/workflow.yaml). Restart the process on the same data directory; the run is still there.
