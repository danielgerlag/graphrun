use graphrun::binding::{Binding, Condition, Reference};
use graphrun::builder::{
    ActivityRef, Branch, Case, NodeRef, Region, RegionBuilder, RegionGraphBuilder, SignalRef,
    WorkflowBuilder,
};
use graphrun::catalog::Catalog;
use graphrun::compile_yaml;
use graphrun::ir::Node;
use graphrun::schema::{DurablePayload, SchemaRef};
use graphrun::{region, workflow};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
struct Counter {
    value: i64,
}

impl DurablePayload for Counter {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("counter", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
}

impl DurablePayload for Order {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("order", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder {
    order_id: String,
    amount: i64,
    reservation_id: String,
}

impl DurablePayload for ReservedOrder {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("reserved_order", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    amount: i64,
    payment_id: String,
}

impl DurablePayload for Receipt {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("receipt", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Approval {
    approved: bool,
}

impl DurablePayload for Approval {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("approval", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Tax {
    cents: i64,
}

impl DurablePayload for Tax {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("tax", 1).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Shipping {
    cents: i64,
}

impl DurablePayload for Shipping {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("shipping", 1).unwrap()
    }
}

fn catalog() -> Catalog {
    Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap()
}

fn quotes_catalog() -> Catalog {
    Catalog::from_json(include_bytes!("../../samples/07-parallel/catalog.json")).unwrap()
}

fn quote<T: DurablePayload>(activity: &ActivityRef<Order, T>) -> Region<Order, T> {
    region::<Order>()
        .activity("quote", activity)
        .unwrap()
        .finish()
        .unwrap()
}

fn assert_node_output<T: DurablePayload>(_: &NodeRef<T>) {}

fn parallel_branches<'a>(
    definition: &'a graphrun::Definition,
    key: &str,
) -> &'a [graphrun::ir::ParallelBranch] {
    match &definition.root.nodes[key] {
        Node::Parallel { branches, .. } => branches,
        _ => panic!("{key} is not parallel"),
    }
}

#[test]
fn sequence_builder_compiles() {
    let catalog = catalog();
    let reserve = catalog
        .activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)
        .unwrap();
    let charge = catalog
        .activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)
        .unwrap();
    let mut root = RegionBuilder::<Order>::new();
    let reserved = root.activity("reserve", &reserve, root.input()).unwrap();
    let charged = root.activity("charge", &charge, reserved.output()).unwrap();
    let root = root.complete("finish", charged.output()).unwrap();
    let definition = WorkflowBuilder::new("sequence", 1, root)
        .build(&catalog)
        .unwrap();
    assert_eq!(definition.id, "sequence");
}

#[test]
fn fluent_sequence_compiles() {
    let catalog = catalog();
    let reserve = catalog
        .activity_v1::<Order, ReservedOrder>("inventory.reserve")
        .unwrap();
    let charge = catalog
        .activity_v1::<ReservedOrder, Receipt>("payment.charge")
        .unwrap();
    let definition = workflow::<Order>("passing_data")
        .activity("reserve", &reserve)
        .unwrap()
        .activity("charge", &charge)
        .unwrap()
        .finish(&catalog)
        .unwrap();
    assert_eq!(definition.id, "passing_data");
}

#[test]
fn repeat_builder_compiles() {
    let catalog = catalog();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body
        .activity("increment", &increment, body.input())
        .unwrap();
    let body = body.complete("iteration_done", bumped.output()).unwrap();
    let mut root = RegionBuilder::<Counter>::new();
    let count = root.literal(3_i64).unwrap();
    let repeated = root
        .repeat("repeat", count, root.input(), body, 10)
        .unwrap();
    let root = root.complete("finish", repeated.output()).unwrap();
    WorkflowBuilder::new("repeat_counter", 1, root)
        .build(&catalog)
        .unwrap();
}

#[test]
fn timed_wait_shared_tail_compiles() {
    let catalog = catalog();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let approval = SignalRef::<Approval>::new("approval").unwrap();
    let mut graph = RegionGraphBuilder::<Counter>::new();
    let input = graph.input();
    let key = graph.literal("approval".to_owned()).unwrap();
    let wait = graph
        .declare_timed_wait("approval", &approval, key, Duration::from_secs(1))
        .unwrap();
    let recovery = graph
        .declare_activity("recover", &increment, input.clone())
        .unwrap();
    let finish = graph.declare_complete("finish", input).unwrap();
    graph.start_at(wait.entry()).unwrap();
    graph.connect(wait.success_port(), finish.entry()).unwrap();
    graph
        .connect(wait.timeout_port(), recovery.entry())
        .unwrap();
    graph.connect(recovery.exit(), finish.entry()).unwrap();
    let region = graph.finish::<Counter>().unwrap();
    WorkflowBuilder::new("timeout_recovery", 1, region)
        .build(&catalog)
        .unwrap();
}

#[test]
fn yaml_and_builder_sequence_have_digests() {
    let catalog = catalog();
    let yaml = compile_yaml(
        include_str!("../../docs/specs/v1/examples/sequence.yaml"),
        &catalog,
    )
    .unwrap();
    assert!(!yaml.digest.0.is_empty());
}

fn execution_ir(definition: &graphrun::Definition) -> serde_json::Value {
    let mut value = serde_json::to_value(definition).unwrap();
    fn strip(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.remove("digest");
                if map
                    .get("path")
                    .is_some_and(|value| value.is_array() || value.is_object())
                {
                    map.remove("path");
                }
                for child in map.values_mut() {
                    strip(child);
                }
            }
            serde_json::Value::Array(items) => {
                for child in items {
                    strip(child);
                }
            }
            _ => {}
        }
    }
    strip(&mut value);
    value
}

#[test]
fn yaml_and_builder_while_match() {
    let catalog = catalog();
    let yaml = compile_yaml(
        include_str!("../../docs/specs/v1/examples/while.yaml"),
        &catalog,
    )
    .unwrap();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body
        .activity("increment", &increment, body.input())
        .unwrap();
    let body = body.complete("done", bumped.output()).unwrap();
    let mut root = RegionBuilder::<Counter>::new();
    let condition = Condition::Lt {
        left: Binding::from_path(Reference::LoopState, "/value"),
        right: Binding::literal(graphrun::value::Value::Int(3)),
    };
    let looped = root
        .while_loop("count", root.workflow_input(), condition, body, 10)
        .unwrap();
    let root = root.complete("finish", looped.output()).unwrap();
    let built = WorkflowBuilder::new("while_counter", 1, root)
        .build(&catalog)
        .unwrap();
    assert_eq!(execution_ir(&yaml), execution_ir(&built));
}

#[test]
fn field_path_and_cross_scope_errors() {
    let graph = RegionGraphBuilder::<Counter>::new();
    let err =
        match graph.map_input::<Counter>(Binding::from_path(Reference::WorkflowInput, "value")) {
            Ok(_) => panic!("expected JSON Pointer error"),
            Err(err) => err,
        };
    assert!(err.to_string().contains("JSON Pointer"), "{}", err);
    let outer = graph.input();
    let mut other = RegionGraphBuilder::<Counter>::new();
    let err = match other.declare_complete("finish", outer) {
        Ok(_) => panic!("expected cross-region error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("cross-region"), "{}", err);
}

#[test]
fn timed_wait_yaml_and_builder_and_invalid_edge() {
    let catalog = catalog();
    let yaml = compile_yaml(
        include_str!("../../docs/specs/v1/examples/timeout-recovery.yaml"),
        &catalog,
    )
    .unwrap();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let approval = SignalRef::<Approval>::new("approval").unwrap();
    let mut graph = RegionGraphBuilder::<Counter>::new();
    let input = graph.workflow_input();
    let key = graph.literal("approval".to_owned()).unwrap();
    let wait = graph
        .declare_timed_wait("approval", &approval, key, Duration::from_secs(1))
        .unwrap();
    let recovery = graph
        .declare_activity("recover", &increment, input.clone())
        .unwrap();
    let finish = graph.declare_complete("finish", input).unwrap();
    graph.start_at(wait.entry()).unwrap();
    graph.connect(wait.success_port(), finish.entry()).unwrap();
    graph
        .connect(wait.timeout_port(), recovery.entry())
        .unwrap();
    graph.connect(recovery.exit(), finish.entry()).unwrap();
    let region = graph.finish::<Counter>().unwrap();
    let built = WorkflowBuilder::new("timeout_recovery", 1, region)
        .build(&catalog)
        .unwrap();
    assert_eq!(execution_ir(&yaml), execution_ir(&built));

    let mut bad = RegionGraphBuilder::<Counter>::new();
    let approval = SignalRef::<Approval>::new("approval").unwrap();
    let key = bad.literal("approval".to_owned()).unwrap();
    let wait = bad
        .declare_timed_wait("approval", &approval, key, Duration::from_secs(1))
        .unwrap();
    let finish = bad.declare_complete("finish", wait.output()).unwrap();
    bad.start_at(wait.entry()).unwrap();
    bad.connect(wait.success_port(), finish.entry()).unwrap();
    bad.connect(wait.timeout_port(), finish.entry()).unwrap();
    let region = bad.finish::<Approval>().unwrap();
    let err = WorkflowBuilder::new("bad_timeout_payload", 1, region)
        .build(&catalog)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("not available on every incoming path"),
        "{}",
        err
    );
}

#[test]
fn while_condition_builder_compiles() {
    let catalog = catalog();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body
        .activity("increment", &increment, body.input())
        .unwrap();
    let body = body.complete("done", bumped.output()).unwrap();
    let mut root = RegionBuilder::<Counter>::new();
    let condition = Condition::Lt {
        left: Binding::from_path(Reference::LoopState, "/value"),
        right: Binding::literal(graphrun::value::Value::Int(3)),
    };
    let looped = root
        .while_loop("count", root.input(), condition, body, 10)
        .unwrap();
    let root = root.complete("finish", looped.output()).unwrap();
    WorkflowBuilder::new("while_counter", 1, root)
        .build(&catalog)
        .unwrap();
}

#[test]
fn yaml_and_builder_choose_match() {
    let catalog = catalog();
    let yaml = compile_yaml(
        include_str!("../../samples/06-choice/workflow.yaml"),
        &catalog,
    )
    .unwrap();
    let increment = catalog
        .activity_ref::<Counter, Counter>("counter.increment", 1)
        .unwrap();
    let echo = catalog
        .activity_ref::<Counter, Counter>("remote.echo", 1)
        .unwrap();
    let mut bump = RegionBuilder::<Counter>::new();
    let bumped = bump
        .activity("increment", &increment, bump.input())
        .unwrap();
    let bump = bump.complete("done", bumped.output()).unwrap();
    let mut keep = RegionBuilder::<Counter>::new();
    let echoed = keep.activity("echo", &echo, keep.input()).unwrap();
    let keep = keep.complete("done", echoed.output()).unwrap();
    let mut root = RegionBuilder::<Counter>::new();
    let decided = root
        .choose(
            "decide",
            root.workflow_input(),
            vec![Case {
                name: "bump".to_owned(),
                when: Condition::Eq {
                    left: Binding::from_path(Reference::WorkflowInput, "/value"),
                    right: Binding::literal(graphrun::value::Value::Int(1)),
                },
                body: bump,
            }],
            keep,
        )
        .unwrap();
    let root = root.complete("finish", decided.output()).unwrap();
    let built = WorkflowBuilder::new("choice", 1, root)
        .build(&catalog)
        .unwrap();
    assert_eq!(execution_ir(&yaml), execution_ir(&built));
}

#[test]
fn parallel_two_preserves_fluent_chain_and_matches_yaml() {
    let catalog = quotes_catalog();
    let yaml = compile_yaml(
        include_str!("../../samples/07-parallel/workflow.yaml"),
        &catalog,
    )
    .unwrap();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let shipping = catalog
        .activity_v1::<Order, Shipping>("shipping.quote")
        .unwrap();
    let old = workflow::<Order>("parallel_quotes")
        .parallel("quotes")
        .branch("tax", quote(&tax))
        .branch("shipping", quote(&shipping))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    let tuple = workflow::<Order>("parallel_quotes")
        .parallel("quotes")
        .branches((("tax", quote(&tax)), ("shipping", quote(&shipping))))
        .unwrap()
        .finish(&catalog)
        .unwrap();

    let mut graph = RegionGraphBuilder::<Order>::new();
    let input = graph.workflow_input().binding().clone();
    let node = graph
        .declare_parallel(
            "quotes",
            (
                Branch {
                    name: "tax".into(),
                    input: input.clone(),
                    body: quote(&tax),
                },
                Branch {
                    name: "shipping".into(),
                    input,
                    body: quote(&shipping),
                },
            ),
        )
        .unwrap();
    assert_node_output::<(Tax, Shipping)>(&node);
    let finish = graph.declare_complete("finish", node.output()).unwrap();
    graph.start_at(node.entry()).unwrap();
    graph.connect(node.exit(), finish.entry()).unwrap();
    let explicit = WorkflowBuilder::new(
        "parallel_quotes",
        1,
        graph.finish::<(Tax, Shipping)>().unwrap(),
    )
    .build(&catalog)
    .unwrap();

    let mut legacy = RegionBuilder::<Order>::new();
    let input = legacy.workflow_input().binding().clone();
    let node = legacy
        .parallel2(
            "quotes",
            Branch {
                name: "tax".into(),
                input: input.clone(),
                body: quote(&tax),
            },
            Branch {
                name: "shipping".into(),
                input,
                body: quote(&shipping),
            },
        )
        .unwrap();
    let legacy = WorkflowBuilder::new(
        "parallel_quotes",
        1,
        legacy.complete("finish", node.output()).unwrap(),
    )
    .build(&catalog)
    .unwrap();

    assert_eq!(execution_ir(&yaml), execution_ir(&old));
    assert_eq!(execution_ir(&yaml), execution_ir(&tuple));
    assert_eq!(execution_ir(&yaml), execution_ir(&explicit));
    assert_eq!(execution_ir(&yaml), execution_ir(&legacy));
}

#[test]
fn parallel_one_and_three_preserve_tuple_order() {
    let catalog = quotes_catalog();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let shipping = catalog
        .activity_v1::<Order, Shipping>("shipping.quote")
        .unwrap();

    let mut root = RegionBuilder::<Order>::new();
    let only = root
        .parallel(
            "quotes",
            (Branch {
                name: "tax".into(),
                input: root.workflow_input().binding().clone(),
                body: quote(&tax),
            },),
        )
        .unwrap();
    assert_node_output::<(Tax,)>(&only);
    let one = WorkflowBuilder::new(
        "one_quote",
        1,
        root.complete("finish", only.output()).unwrap(),
    )
    .build(&catalog)
    .unwrap();
    assert_eq!(one.output_schema, <(Tax,)>::schema_ref());
    assert_eq!(parallel_branches(&one, "quotes")[0].name, "tax");

    let fluent_one = workflow::<Order>("one_quote")
        .parallel("quotes")
        .branches((("tax", quote(&tax)),))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    assert_eq!(execution_ir(&one), execution_ir(&fluent_one));

    let mut root = RegionBuilder::<Order>::new();
    let input = root.workflow_input().binding().clone();
    let three = root
        .parallel(
            "quotes",
            (
                Branch {
                    name: "tax".into(),
                    input: input.clone(),
                    body: quote(&tax),
                },
                Branch {
                    name: "shipping".into(),
                    input: input.clone(),
                    body: quote(&shipping),
                },
                Branch {
                    name: "original".into(),
                    input,
                    body: region::<Order>().finish().unwrap(),
                },
            ),
        )
        .unwrap();
    assert_node_output::<(Tax, Shipping, Order)>(&three);
    let three = WorkflowBuilder::new(
        "three_quotes",
        1,
        root.complete("finish", three.output()).unwrap(),
    )
    .build(&catalog)
    .unwrap();
    let fluent_three = workflow::<Order>("three_quotes")
        .parallel("quotes")
        .branches((
            ("tax", quote(&tax)),
            ("shipping", quote(&shipping)),
            ("original", region::<Order>().finish().unwrap()),
        ))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    assert_eq!(execution_ir(&three), execution_ir(&fluent_three));
    assert_eq!(three.output_schema, <(Tax, Shipping, Order)>::schema_ref());
    assert_eq!(
        parallel_branches(&three, "quotes")
            .iter()
            .map(|branch| branch.name.as_str())
            .collect::<Vec<_>>(),
        ["tax", "shipping", "original"]
    );

    let yaml = include_str!("../../samples/07-parallel/workflow.yaml")
        .replace("\r\n", "\n")
        .replacen("id: parallel_quotes", "id: three_quotes", 1)
        .replacen(
            "output_schema: {tuple: [tax/v1, shipping/v1]}",
            "output_schema: {tuple: [tax/v1, shipping/v1, order/v1]}",
            1,
        )
        .replacen(
            "    next: finish\n  finish:",
            "      - name: original\n        input: {from: workflow.input}\n        body:\n          input_schema: order/v1\n          output_schema: order/v1\n          start: done\n          nodes:\n            done:\n              kind: complete\n              output: {from: scope.input}\n    next: finish\n  finish:",
            1,
        );
    assert_eq!(
        execution_ir(&compile_yaml(&yaml, &catalog).unwrap()),
        execution_ir(&three)
    );
}

#[test]
fn parallel_sixteen_is_heterogeneous_on_both_surfaces() {
    type Wide = (
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
        Tax,
        Shipping,
    );
    let catalog = quotes_catalog();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let shipping = catalog
        .activity_v1::<Order, Shipping>("shipping.quote")
        .unwrap();
    let mut root = RegionGraphBuilder::<Order>::new();
    let input = root.workflow_input().binding().clone();
    macro_rules! branch {
        ($name:literal, $activity:expr) => {
            Branch {
                name: $name.into(),
                input: input.clone(),
                body: quote($activity),
            }
        };
    }
    let node = root
        .declare_parallel(
            "quotes",
            (
                branch!("b00", &tax),
                branch!("b01", &shipping),
                branch!("b02", &tax),
                branch!("b03", &shipping),
                branch!("b04", &tax),
                branch!("b05", &shipping),
                branch!("b06", &tax),
                branch!("b07", &shipping),
                branch!("b08", &tax),
                branch!("b09", &shipping),
                branch!("b10", &tax),
                branch!("b11", &shipping),
                branch!("b12", &tax),
                branch!("b13", &shipping),
                branch!("b14", &tax),
                branch!("b15", &shipping),
            ),
        )
        .unwrap();
    assert_node_output::<Wide>(&node);
    let finish = root.declare_complete("finish", node.output()).unwrap();
    root.start_at(node.entry()).unwrap();
    root.connect(node.exit(), finish.entry()).unwrap();
    let explicit = WorkflowBuilder::new("wide_quotes", 1, root.finish::<Wide>().unwrap())
        .build(&catalog)
        .unwrap();
    let fluent = workflow::<Order>("wide_quotes")
        .parallel("quotes")
        .branches((
            ("b00", quote(&tax)),
            ("b01", quote(&shipping)),
            ("b02", quote(&tax)),
            ("b03", quote(&shipping)),
            ("b04", quote(&tax)),
            ("b05", quote(&shipping)),
            ("b06", quote(&tax)),
            ("b07", quote(&shipping)),
            ("b08", quote(&tax)),
            ("b09", quote(&shipping)),
            ("b10", quote(&tax)),
            ("b11", quote(&shipping)),
            ("b12", quote(&tax)),
            ("b13", quote(&shipping)),
            ("b14", quote(&tax)),
            ("b15", quote(&shipping)),
        ))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    assert_eq!(explicit.output_schema, Wide::schema_ref());
    assert_eq!(execution_ir(&explicit), execution_ir(&fluent));
    assert_eq!(
        parallel_branches(&explicit, "quotes")
            .iter()
            .map(|branch| branch.name.as_str())
            .collect::<Vec<_>>(),
        (0..16).map(|i| format!("b{i:02}")).collect::<Vec<_>>()
    );
    match &explicit.output_schema {
        SchemaRef::Tuple { elements } => {
            assert_eq!(elements.len(), 16);
            for (index, schema) in elements.iter().enumerate() {
                assert_eq!(
                    schema,
                    &if index % 2 == 0 {
                        Tax::schema_ref()
                    } else {
                        Shipping::schema_ref()
                    }
                );
            }
        }
        _ => panic!("expected tuple schema"),
    }
}

#[test]
fn parallel_regions_nest_and_reject_duplicate_names() {
    let catalog = quotes_catalog();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let inner = region::<Order>()
        .parallel("inside")
        .branches((("original", region::<Order>().finish().unwrap()),))
        .unwrap()
        .finish_region()
        .unwrap();
    let mut outer = RegionBuilder::<Order>::new();
    let binding = outer.workflow_input().binding().clone();
    let nested = outer
        .parallel(
            "outside",
            (
                Branch {
                    name: "nested".into(),
                    input: binding.clone(),
                    body: inner,
                },
                Branch {
                    name: "tax".into(),
                    input: binding,
                    body: quote(&tax),
                },
            ),
        )
        .unwrap();
    assert_node_output::<((Order,), Tax)>(&nested);
    let nested = WorkflowBuilder::new(
        "nested_quotes",
        1,
        outer.complete("finish", nested.output()).unwrap(),
    )
    .build(&catalog)
    .unwrap();
    assert_eq!(nested.output_schema, <((Order,), Tax)>::schema_ref());
    assert_eq!(
        parallel_branches(&nested, "outside")[0].body.output_schema,
        <(Order,)>::schema_ref()
    );

    let mut invalid = RegionGraphBuilder::<Order>::new();
    let input = invalid.workflow_input().binding().clone();
    let err = invalid
        .declare_parallel(
            "quotes",
            (
                Branch {
                    name: "same".into(),
                    input: input.clone(),
                    body: quote(&tax),
                },
                Branch {
                    name: "same".into(),
                    input,
                    body: quote(&tax),
                },
            ),
        )
        .err()
        .expect("duplicate branch name must fail");
    assert!(err.to_string().contains("duplicate branch name"), "{err}");
}

#[test]
fn parallel_explicit_branches_can_have_different_input_types() {
    let catalog = quotes_catalog();
    let mut root = RegionBuilder::<Order>::new();
    let number = root.literal(17_i64).unwrap();
    let node = root
        .parallel(
            "mixed",
            (
                Branch {
                    name: "original".into(),
                    input: root.workflow_input().binding().clone(),
                    body: region::<Order>().finish().unwrap(),
                },
                Branch {
                    name: "number".into(),
                    input: number.binding().clone(),
                    body: region::<i64>().finish().unwrap(),
                },
            ),
        )
        .unwrap();
    assert_node_output::<(Order, i64)>(&node);
    let definition = WorkflowBuilder::new(
        "mixed_inputs",
        1,
        root.complete("finish", node.output()).unwrap(),
    )
    .build(&catalog)
    .unwrap();
    assert_eq!(definition.output_schema, <(Order, i64)>::schema_ref());
}

#[tokio::test]
async fn parallel_outputs_follow_declaration_not_name_order() {
    let catalog = quotes_catalog();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let shipping = catalog
        .activity_v1::<Order, Shipping>("shipping.quote")
        .unwrap();
    let one = workflow::<Order>("one_ordered_quote")
        .parallel("quotes")
        .branches((("z_shipping", quote(&shipping)),))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    let three = workflow::<Order>("three_ordered_quotes")
        .parallel("quotes")
        .branches((
            ("z_shipping", quote(&shipping)),
            ("a_tax", quote(&tax)),
            ("m_shipping", quote(&shipping)),
        ))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    let wide = workflow::<Order>("sixteen_ordered_quotes")
        .parallel("quotes")
        .branches((
            ("z15", quote(&shipping)),
            ("y14", quote(&tax)),
            ("x13", quote(&shipping)),
            ("w12", quote(&tax)),
            ("v11", quote(&shipping)),
            ("u10", quote(&tax)),
            ("t09", quote(&shipping)),
            ("s08", quote(&tax)),
            ("r07", quote(&shipping)),
            ("q06", quote(&tax)),
            ("p05", quote(&shipping)),
            ("o04", quote(&tax)),
            ("n03", quote(&shipping)),
            ("m02", quote(&tax)),
            ("l01", quote(&shipping)),
            ("a00", quote(&tax)),
        ))
        .unwrap()
        .finish(&catalog)
        .unwrap();
    let data_dir = std::path::PathBuf::from(format!("p{}", std::process::id()));
    std::fs::create_dir(&data_dir).unwrap();
    let engine = graphrun::Engine::local(&data_dir).await.unwrap();
    let input = graphrun::Value::from_json(serde_json::json!({
        "order_id": "o1", "amount": 1000
    }))
    .unwrap();
    let wide_expected = serde_json::Value::Array(
        (0..16)
            .map(|index| {
                if index % 2 == 0 {
                    serde_json::json!({"cents": 500})
                } else {
                    serde_json::json!({"cents": 100})
                }
            })
            .collect(),
    );
    for (definition, expected) in [
        (one, serde_json::json!([{"cents": 500}])),
        (
            three,
            serde_json::json!([{"cents": 500}, {"cents": 100}, {"cents": 500}]),
        ),
        (wide, wide_expected),
    ] {
        let run = engine
            .start(definition, catalog.clone(), input.clone())
            .await
            .unwrap();
        let output = engine
            .wait_terminal(run, Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(output.to_json(), expected);
    }
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(data_dir).unwrap();
}
