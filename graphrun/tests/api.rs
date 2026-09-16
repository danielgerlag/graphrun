use graphrun::binding::{Binding, Condition, Reference};
use graphrun::builder::{RegionBuilder, RegionGraphBuilder, SignalRef, WorkflowBuilder};
use graphrun::catalog::Catalog;
use graphrun::compile_yaml;
use graphrun::schema::{DurablePayload, SchemaRef};
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

fn catalog() -> Catalog {
    Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap()
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
