//! 500 Engine::local scenarios from identity graphs to nested sagas.
//! `cargo run -p graphrun-samples --bin battle`

use graphrun::binding::Condition;
use graphrun::builder::{RegionBuilder, SignalRef, WorkflowBuilder};
use graphrun::compile_yaml;
use graphrun::{ActivityRef, Catalog, Engine, EventId, Value, payload, region, workflow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const CATALOG: &[u8] = include_bytes!("../../docs/specs/v1/examples/activity-catalog.json");

#[derive(Clone, Serialize, Deserialize)]
struct Counter {
    value: i64,
}
payload!(Counter, "counter");

#[derive(Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fail_after_payment: Option<bool>,
}
payload!(Order, "order");

#[derive(Clone, Serialize, Deserialize)]
struct ReservedOrder {
    order_id: String,
    amount: i64,
    reservation_id: String,
}
payload!(ReservedOrder, "reserved_order");

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    amount: i64,
    payment_id: String,
}
payload!(Receipt, "receipt");

#[derive(Clone, Serialize, Deserialize)]
struct Tax {
    cents: i64,
}
payload!(Tax, "tax");

#[derive(Clone, Serialize, Deserialize)]
struct Shipping {
    cents: i64,
}
payload!(Shipping, "shipping");

#[derive(Clone, Serialize, Deserialize)]
struct Approval {
    approved: bool,
}
payload!(Approval, "approval");

#[derive(Clone, Serialize, Deserialize)]
struct EventRequest {
    key: String,
}
payload!(EventRequest, "event_request");

fn obj_int(field: &str, n: i64) -> Value {
    Value::Object(BTreeMap::from([(field.to_owned(), Value::Int(n))]))
}

fn counter(n: i64) -> Value {
    obj_int("value", n)
}

fn order(id: &str, amount: i64, fail: Option<bool>) -> Value {
    let mut map = BTreeMap::from([
        ("order_id".to_owned(), Value::String(id.to_owned())),
        ("amount".to_owned(), Value::Int(amount)),
    ]);
    if let Some(flag) = fail {
        map.insert("fail_after_payment".to_owned(), Value::Bool(flag));
    }
    Value::Object(map)
}

fn event_id(n: u64) -> EventId {
    EventId::from_hex(&format!("{n:032x}")).expect("event id")
}

enum Expect {
    Output(Value),
    FailContains(&'static str),
    CompileFail,
}

enum AfterStart {
    None,
    Signal {
        name: &'static str,
        key: String,
        payload: Value,
        event: EventId,
    },
    Cancel,
}

struct Case {
    id: String,
    family: &'static str,
    complexity: u8,
    definition: graphrun::Result<graphrun::Definition>,
    input: Value,
    expect: Expect,
    after: AfterStart,
}

fn push(
    out: &mut Vec<Case>,
    id: String,
    family: &'static str,
    complexity: u8,
    definition: graphrun::Result<graphrun::Definition>,
    input: Value,
    expect: Expect,
) {
    out.push(Case {
        id,
        family,
        complexity,
        definition,
        input,
        expect,
        after: AfterStart::None,
    });
}

fn build_cases(catalog: &Catalog) -> Vec<Case> {
    let inc = catalog
        .activity_v1::<Counter, Counter>("counter.increment")
        .unwrap();
    let echo = catalog
        .activity_v1::<Counter, Counter>("remote.echo")
        .unwrap();
    let reserve = catalog
        .activity_v1::<Order, ReservedOrder>("inventory.reserve")
        .unwrap();
    let release = catalog
        .activity_v1::<ReservedOrder, ()>("inventory.release")
        .unwrap();
    let charge = catalog
        .activity_v1::<ReservedOrder, Receipt>("payment.charge")
        .unwrap();
    let refund = catalog
        .activity_v1::<Receipt, ()>("payment.refund")
        .unwrap();
    let tax = catalog.activity_v1::<Order, Tax>("tax.quote").unwrap();
    let shipping = catalog
        .activity_v1::<Order, Shipping>("shipping.quote")
        .unwrap();
    let approval = SignalRef::<Approval>::new("approval").unwrap();

    let mut out = Vec::new();

    for start in 0..10 {
        let id = format!("identity-{start}");
        push(
            &mut out,
            id.clone(),
            "identity",
            1,
            workflow::<Counter>(id).finish(catalog),
            counter(start),
            Expect::Output(counter(start)),
        );
    }

    for start in 0..10 {
        for len in 1..=12 {
            let id = format!("seq-{start}-{len}");
            let mut w = workflow::<Counter>(id.clone());
            let built = (|| {
                for i in 0..len {
                    w = w.activity(&format!("s{i}"), &inc)?;
                }
                w.finish(catalog)
            })();
            push(
                &mut out,
                id,
                "sequence",
                if len <= 3 { 1 } else { 2 },
                built,
                counter(start),
                Expect::Output(counter(start + len)),
            );
        }
    }

    for start in 0..10 {
        for limit in [1_i64, 2, 3, 5, 8] {
            let id = format!("while-{start}-{limit}");
            let expected = start.max(limit);
            let built = (|| {
                let body = region::<Counter>().activity("inc", &inc)?.finish()?;
                workflow::<Counter>(id.clone())
                    .while_lt("w", "/value", limit, body, 50)?
                    .finish(catalog)
            })();
            push(
                &mut out,
                id,
                "while",
                2,
                built,
                counter(start),
                Expect::Output(counter(expected)),
            );
        }
    }

    for start in 0..5 {
        for count in 0..10 {
            let id = format!("repeat-{start}-{count}");
            let built = (|| {
                let body = region::<Counter>().activity("inc", &inc)?.finish()?;
                workflow::<Counter>(id.clone())
                    .repeat("r", count, body, 50)?
                    .finish(catalog)
            })();
            push(
                &mut out,
                id,
                "repeat",
                2,
                built,
                counter(start),
                Expect::Output(counter(start + count)),
            );
        }
    }

    for len in 0..=8 {
        for base in 0..4 {
            let id = format!("foreach-{len}-{base}");
            let items: Vec<Value> = (0..len).map(|i| counter(base + i)).collect();
            let expected: Vec<Value> = (0..len).map(|i| counter(base + i + 1)).collect();
            let built = (|| {
                let body = region::<Counter>().activity("inc", &inc)?.finish()?;
                workflow::<Vec<Counter>>(id.clone())
                    .foreach("each", body, 100, 8)?
                    .finish(catalog)
            })();
            push(
                &mut out,
                id,
                "foreach",
                2,
                built,
                Value::Array(items),
                Expect::Output(Value::Array(expected)),
            );
        }
    }

    for value in -2_i64..15 {
        let id = format!("choose-{value}");
        let expected = if value == 1 { 2 } else { value };
        let built = (|| {
            let bump = region::<Counter>().activity("inc", &inc)?.finish()?;
            let keep = region::<Counter>().activity("echo", &echo)?.finish()?;
            workflow::<Counter>(id.clone())
                .choose("decide")
                .when_eq("bump", "/value", 1, bump)
                .otherwise(keep)?
                .finish(catalog)
        })();
        // when_eq always uses literal 1; expected is bump only when value==1
        let _ = value;
        push(
            &mut out,
            id,
            "choose",
            2,
            built,
            counter(value),
            Expect::Output(counter(expected)),
        );
    }

    for amount in [0_i64, 1, 9, 10, 50, 99, 100, 250, 999, 1000, 5000] {
        let id = format!("parallel-{amount}");
        let built = (|| {
            let tax_body = region::<Order>().activity("q", &tax)?.finish()?;
            let ship_body = region::<Order>().activity("q", &shipping)?.finish()?;
            workflow::<Order>(id.clone())
                .parallel("quotes")
                .branch("tax", tax_body)
                .branch("shipping", ship_body)?
                .finish(catalog)
        })();
        let expected = Value::Array(vec![obj_int("cents", amount / 10), obj_int("cents", 500)]);
        push(
            &mut out,
            id,
            "parallel",
            3,
            built,
            order("o1", amount, None),
            Expect::Output(expected),
        );
    }

    for n in 0..8 {
        let id = format!("saga-ok-{n}");
        let built = saga_def(catalog, &reserve, &release, &charge, &refund, &id);
        push(
            &mut out,
            id,
            "saga-ok",
            3,
            built,
            order(&format!("o{n}"), 100 * n, None),
            Expect::Output(Value::Object(BTreeMap::from([
                ("order_id".to_owned(), Value::String(format!("o{n}"))),
                ("amount".to_owned(), Value::Int(100 * n)),
                ("payment_id".to_owned(), Value::String("pay-1".to_owned())),
            ]))),
        );
    }

    for n in 0..8 {
        let id = format!("saga-fail-{n}");
        let built = saga_def(catalog, &reserve, &release, &charge, &refund, &id);
        push(
            &mut out,
            id,
            "saga-fail",
            4,
            built,
            order(&format!("f{n}"), 50, Some(true)),
            Expect::FailContains("fixture.failed"),
        );
    }

    for n in 0..15 {
        let id = format!("wait-{n}");
        let key = format!("k{n}");
        let built = workflow::<EventRequest>(id.clone())
            .wait_signal("approval", &approval, &key)
            .and_then(|w| w.finish(catalog));
        out.push(Case {
            id,
            family: "wait",
            complexity: 3,
            definition: built,
            input: Value::Object(BTreeMap::from([(
                "key".to_owned(),
                Value::String(key.clone()),
            )])),
            expect: Expect::Output(Value::Object(BTreeMap::from([(
                "approved".to_owned(),
                Value::Bool(true),
            )]))),
            after: AfterStart::Signal {
                name: "approval",
                key,
                payload: Value::Object(BTreeMap::from([(
                    "approved".to_owned(),
                    Value::Bool(true),
                )])),
                event: event_id(1000 + n),
            },
        });
    }

    for n in 0..6 {
        let id = format!("timed-ok-{n}");
        let key = format!("t{n}");
        let built = workflow::<Counter>(id.clone())
            .timed_wait("approval", &approval, &key, Duration::from_millis(800))
            .and_then(|w| w.on_timeout("recover", &inc))
            .and_then(|w| w.finish(catalog));
        out.push(Case {
            id,
            family: "timed-ok",
            complexity: 4,
            definition: built,
            input: counter(n),
            expect: Expect::Output(counter(n)),
            after: AfterStart::Signal {
                name: "approval",
                key,
                payload: Value::Object(BTreeMap::from([(
                    "approved".to_owned(),
                    Value::Bool(true),
                )])),
                event: event_id(2000 + n as u64),
            },
        });
    }

    for n in 0..6 {
        let id = format!("timed-out-{n}");
        let built = workflow::<Counter>(id.clone())
            .timed_wait(
                "approval",
                &approval,
                &format!("miss{n}"),
                Duration::from_millis(20),
            )
            .and_then(|w| w.on_timeout("recover", &inc))
            .and_then(|w| w.finish(catalog));
        // timeout path runs recover then complete with original workflow input
        push(
            &mut out,
            id,
            "timed-out",
            4,
            built,
            counter(n),
            Expect::Output(counter(n)),
        );
    }

    for depth in 2..=7 {
        for start in 0..3 {
            let id = format!("nest-while-{depth}-{start}");
            let built = nest_whiles(catalog, &inc, depth, &id);
            let expected = start.max(2);
            push(
                &mut out,
                id,
                "nest-while",
                5,
                built,
                counter(start),
                Expect::Output(counter(expected)),
            );
        }
    }

    for len in 1..=6 {
        let id = format!("foreach-while-{len}");
        let items: Vec<Value> = (0..len).map(counter).collect();
        let expected: Vec<Value> = (0..len).map(|i| counter(i.max(2))).collect();
        let built = (|| {
            let inner = region::<Counter>().activity("inc", &inc)?.finish()?;
            let body = region::<Counter>()
                .while_lt("w", "/value", 2, inner, 20)?
                .finish()?;
            workflow::<Vec<Counter>>(id.clone())
                .foreach("each", body, 100, 8)?
                .finish(catalog)
        })();
        push(
            &mut out,
            id,
            "foreach-while",
            5,
            built,
            Value::Array(items),
            Expect::Output(Value::Array(expected)),
        );
    }

    for start in 0..8 {
        let id = format!("while-choose-{start}");
        let built = (|| {
            let bump = region::<Counter>().activity("inc", &inc)?.finish()?;
            let keep = region::<Counter>().activity("echo", &echo)?.finish()?;
            let body = region::<Counter>()
                .choose("c")
                .when_eq("bump", "/value", 0, bump)
                .otherwise(keep)?
                .finish_region()?;
            // choose in while body uses workflow.input for when_eq — may not match loop state
            workflow::<Counter>(id.clone())
                .while_lt("w", "/value", 3, body, 20)?
                .finish(catalog)
        })();
        push(
            &mut out,
            id,
            "while-choose",
            5,
            built,
            counter(start),
            Expect::Output(counter(start.max(3))),
        );
    }

    for start in 0..6 {
        for count in 1..=4 {
            let id = format!("seq-repeat-{start}-{count}");
            let built = (|| {
                let body = region::<Counter>().activity("inc", &inc)?.finish()?;
                let mut w = workflow::<Counter>(id.clone()).repeat("r", count, body, 20)?;
                w = w.activity("after", &inc)?;
                w.finish(catalog)
            })();
            push(
                &mut out,
                id,
                "seq-repeat",
                3,
                built,
                counter(start),
                Expect::Output(counter(start + count + 1)),
            );
        }
    }

    for n in 0..5 {
        let id = format!("do-while-{n}");
        let built = do_while_def(catalog, &inc, &id, 3);
        let expected = if n < 3 { 3 } else { n + 1 };
        push(
            &mut out,
            id,
            "do-while",
            3,
            built,
            counter(n),
            Expect::Output(counter(expected)),
        );
    }

    for n in 0..4 {
        let id = format!("fail-{n}");
        let built = region::<Counter>()
            .fail::<Counter>("abort", "battle.fail", "intentional")
            .and_then(|r| {
                // fail as root: wrap via workflow? region fail returns Region
                WorkflowBuilder::new(id.clone(), 1, r).build(catalog)
            });
        push(
            &mut out,
            id,
            "fail",
            1,
            built,
            counter(n),
            Expect::FailContains("battle.fail"),
        );
    }

    for n in 0..8 {
        let id = format!("echo-{n}");
        let built = workflow::<Counter>(id.clone())
            .activity("e", &echo)
            .and_then(|w| w.finish(catalog));
        push(
            &mut out,
            id,
            "echo",
            1,
            built,
            counter(n * 7),
            Expect::Output(counter(n * 7)),
        );
    }

    for n in 0..5 {
        let id = format!("cancel-wait-{n}");
        let key = format!("cxl{n}");
        let built = workflow::<EventRequest>(id.clone())
            .wait_signal("approval", &approval, &key)
            .and_then(|w| w.finish(catalog));
        out.push(Case {
            id,
            family: "cancel",
            complexity: 3,
            definition: built,
            input: Value::Object(BTreeMap::from([("key".to_owned(), Value::String(key))])),
            expect: Expect::FailContains("cancel"),
            after: AfterStart::Cancel,
        });
    }

    for yaml in [
        "dsl: nope\nid: x\nversion: 1\ninput_schema: counter/v1\noutput_schema: counter/v1\nstart: f\nnodes:\n  f:\n    kind: complete\n    output: {from: workflow.input}\n",
        "dsl: graphrun/v1\nid: x\nversion: 1\ninput_schema: counter/v1\noutput_schema: counter/v1\nstart: missing\nnodes:\n  f:\n    kind: complete\n    output: {from: workflow.input}\n",
        "dsl: graphrun/v1\nid: x\nversion: 1\ninput_schema: counter/v1\noutput_schema: counter/v1\nstart: f\nnodes:\n  f:\n    kind: activity\n    activity: {name: does.not.exist, version: 1}\n    input: {from: workflow.input}\n    next: d\n  d:\n    kind: complete\n    output: {from: nodes.f.output}\n",
    ] {
        let id = format!("yaml-reject-{}", out.len());
        out.push(Case {
            id,
            family: "yaml-reject",
            complexity: 1,
            definition: compile_yaml(yaml, catalog),
            input: counter(0),
            expect: Expect::CompileFail,
            after: AfterStart::None,
        });
    }

    // YAML hello-world as compiler path
    let hello_yaml = include_str!("../01-hello-world/workflow.yaml");
    for n in 0..5 {
        let id = format!("yaml-hello-{n}");
        out.push(Case {
            id,
            family: "yaml-hello",
            complexity: 1,
            definition: compile_yaml(hello_yaml, catalog),
            input: counter(n),
            expect: Expect::Output(counter(n + 2)),
            after: AfterStart::None,
        });
    }

    // while max_iterations
    for start in 0..3 {
        let id = format!("while-maxiter-{start}");
        let built = (|| {
            let body = region::<Counter>().activity("inc", &inc)?.finish()?;
            workflow::<Counter>(id.clone())
                .while_lt("w", "/value", 100, body, 3)?
                .finish(catalog)
        })();
        push(
            &mut out,
            id,
            "while-maxiter",
            3,
            built,
            counter(start),
            Expect::FailContains("max"),
        );
    }

    // Fill remaining slots with sequence variants so we have 500.
    let mut extra = 0;
    while out.len() < 500 {
        let start = (extra % 17) as i64;
        let len = 1 + (extra % 9);
        let id = format!("pad-seq-{extra}");
        let mut w = workflow::<Counter>(id.clone());
        let built = (|| {
            for i in 0..len {
                w = w.activity(&format!("p{i}"), &inc)?;
            }
            w.finish(catalog)
        })();
        push(
            &mut out,
            id,
            "sequence",
            1,
            built,
            counter(start),
            Expect::Output(counter(start + len)),
        );
        extra += 1;
        if extra > 1000 {
            break;
        }
    }

    out.truncate(500);
    out
}

fn saga_def(
    catalog: &Catalog,
    reserve: &ActivityRef<Order, ReservedOrder>,
    release: &ActivityRef<ReservedOrder, ()>,
    charge: &ActivityRef<ReservedOrder, Receipt>,
    refund: &ActivityRef<Receipt, ()>,
    id: &str,
) -> graphrun::Result<graphrun::Definition> {
    let abort =
        region::<Receipt>().fail("abort", "fixture.failed", "Failure after recorded payment.")?;
    let accept = region::<Receipt>().complete("accept")?;
    let body = region::<Order>()
        .activity("reserve", reserve)?
        .compensate(release)?
        .activity("charge", charge)?
        .compensate(refund)?
        .choose("decide")
        .when_true("force_failure", "/fail_after_payment", abort)
        .otherwise(accept)?
        .complete("done")?;
    workflow::<Order>(id).saga("fulfill", body)?.finish(catalog)
}

fn nest_whiles(
    catalog: &Catalog,
    inc: &ActivityRef<Counter, Counter>,
    depth: usize,
    id: &str,
) -> graphrun::Result<graphrun::Definition> {
    let mut body = region::<Counter>().activity("inc", inc)?.finish()?;
    for d in 1..depth {
        body = region::<Counter>()
            .while_lt(&format!("w{d}"), "/value", 2, body, 30)?
            .finish()?;
    }
    workflow::<Counter>(id.to_owned())
        .while_lt("w0", "/value", 2, body, 30)?
        .finish(catalog)
}

fn do_while_def(
    catalog: &Catalog,
    inc: &ActivityRef<Counter, Counter>,
    id: &str,
    limit: i64,
) -> graphrun::Result<graphrun::Definition> {
    let mut body = RegionBuilder::<Counter>::new();
    let bumped = body.activity("inc", inc, body.input())?;
    let body = body.complete("done", bumped.output())?;
    let mut root = RegionBuilder::<Counter>::new();
    let looped = root.do_while(
        "dw",
        root.workflow_input(),
        Condition::lt_loop("/value", limit),
        body,
        40,
    )?;
    let root = root.complete("finish", looped.output())?;
    WorkflowBuilder::new(id.to_owned(), 1, root).build(catalog)
}

async fn run_case(engine: &Engine, catalog: &Catalog, case: &Case) -> Result<(), String> {
    match &case.expect {
        Expect::CompileFail => match &case.definition {
            Err(_) => Ok(()),
            Ok(_) => Err("expected compile failure, definition built".into()),
        },
        _ => {
            let def = case
                .definition
                .as_ref()
                .map_err(|err| format!("compile: {err}"))?
                .clone();
            let run = engine
                .start(def, catalog.clone(), case.input.clone())
                .await
                .map_err(|err| format!("start: {err}"))?;
            match &case.after {
                AfterStart::None => {}
                AfterStart::Signal {
                    name,
                    key,
                    payload,
                    event,
                } => {
                    engine
                        .signal(run, *event, name, key, payload.clone())
                        .await
                        .map_err(|err| format!("signal: {err}"))?;
                }
                AfterStart::Cancel => {
                    engine
                        .cancel(run, "battle cancel")
                        .await
                        .map_err(|err| format!("cancel: {err}"))?;
                }
            }
            let result = engine.wait_terminal(run, Duration::from_secs(2)).await;
            match (&case.expect, result) {
                (Expect::Output(want), Ok(got)) => {
                    if &got == want {
                        Ok(())
                    } else {
                        Err(format!("output {got:?} want {want:?}"))
                    }
                }
                (Expect::FailContains(needle), Err(err)) => {
                    let text = err.to_string();
                    if text.contains(needle) {
                        Ok(())
                    } else {
                        Err(format!("error {text:?} missing {needle:?}"))
                    }
                }
                (Expect::FailContains(needle), Ok(got)) => {
                    Err(format!("expected fail {needle:?}, got {got:?}"))
                }
                (Expect::Output(_), Err(err)) => Err(format!("run: {err}")),
                (Expect::CompileFail, _) => unreachable!(),
            }
        }
    }
}

#[tokio::main]
async fn main() -> graphrun::Result<()> {
    let catalog = Catalog::from_json(CATALOG)?;
    let cases = build_cases(&catalog);
    println!("built {} scenarios", cases.len());
    let started = Instant::now();
    let mut pass = 0u32;
    let mut fail = Vec::new();
    let mut compile_fail_ok = 0u32;
    for (i, case) in cases.iter().enumerate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Engine::local(dir.path()).await?;
        let outcome = run_case(&engine, &catalog, case).await;
        let _ = engine.shutdown().await;
        match outcome {
            Ok(()) => {
                pass += 1;
                if matches!(case.expect, Expect::CompileFail) {
                    compile_fail_ok += 1;
                }
            }
            Err(err) => {
                eprintln!("FAIL {}/{}: {}", case.family, case.id, err);
                fail.push((case.id.clone(), case.family, case.complexity, err));
            }
        }
        if (i + 1) % 25 == 0 {
            eprintln!(
                "... {}/{} pass={} fail={}",
                i + 1,
                cases.len(),
                pass,
                fail.len()
            );
        }
    }
    let elapsed = started.elapsed();

    let mut by_family: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
    for case in &cases {
        let entry = by_family.entry(case.family).or_insert((0, 0));
        entry.0 += 1;
    }
    for (id, family, _, _) in &fail {
        by_family.entry(*family).or_insert((0, 0)).1 += 1;
        let _ = id;
    }

    println!();
    println!("=== battle report ===");
    println!(
        "total={} pass={} fail={} compile_reject_ok={} elapsed={:.1}s",
        cases.len(),
        pass,
        fail.len(),
        compile_fail_ok,
        elapsed.as_secs_f64()
    );
    println!("by family (count/fail):");
    for (family, (n, f)) in &by_family {
        println!("  {family}: {n} runs, {f} fail");
    }
    if fail.is_empty() {
        println!("no failures");
    } else {
        println!("failures:");
        for (id, family, complexity, err) in &fail {
            println!("  [{complexity}] {family}/{id}: {err}");
        }
    }
    if !fail.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
