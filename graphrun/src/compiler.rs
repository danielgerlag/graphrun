use crate::binding::{Binding, Reference};
use crate::catalog::{ActivityContract, Catalog, EffectKind};
use crate::error::{Error, Result};
use crate::ids::{ActivityKey, NodeKey, valid_ascii_name};
use crate::ir::{
    ChooseCase, Compensation, ConsumeFrom, DSL, Definition, Digest, FORMAT_VERSION, FailError,
    Node, ParallelBranch, Region, RegionPath, SignalDecl,
};
use crate::limits::{
    MAX_CONDITION_OPS, MAX_DATA_DEPTH, MAX_FOREACH_CONCURRENCY, MAX_LOOP_ITERATIONS, MAX_NESTING,
    MAX_NODES, MAX_NORMALIZED_BYTES, MAX_PARALLEL_BRANCHES,
};
use crate::policy::{
    COMPENSATION_ATTEMPT_TIMEOUT, FORWARD_ATTEMPT_TIMEOUT, MAX_RUN_TIMEOUT, RetryPolicy,
    validate_attempt_timeout,
};
use crate::schema::SchemaRef;
use crate::time::{duration_to_millis, parse_duration};
use crate::value::canonical_json;
use crate::yaml::{
    Doc, Span, Spanned, parse_binding, parse_condition, parse_node_key, parse_schema_ref,
    parse_yaml, reject_unknown, required,
};
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

const WORKFLOW_FIELDS: &[&str] = &[
    "dsl",
    "id",
    "version",
    "input_schema",
    "output_schema",
    "start",
    "nodes",
    "signals",
    "run_timeout",
];
const REGION_FIELDS: &[&str] = &["input_schema", "output_schema", "start", "nodes"];

pub fn compile_yaml(text: &str, catalog: &Catalog) -> Result<Definition> {
    let root = parse_yaml(text)?;
    compile_spanned(&root, catalog)
}

pub fn compile_spanned(root: &Spanned, catalog: &Catalog) -> Result<Definition> {
    let map = root.as_map()?;
    reject_unknown(map, WORKFLOW_FIELDS, &root.span)?;
    let dsl = required(map, "dsl", &root.span)?.as_str()?;
    if dsl != DSL {
        return Err(Error::invalid(format!("unsupported dsl {dsl}"))
            .at_span(root.span.line, root.span.column));
    }
    let id = required(map, "id", &root.span)?.as_str()?.to_owned();
    if !valid_ascii_name(&id) {
        return Err(Error::invalid(format!("invalid workflow id {id}")));
    }
    let version = required(map, "version", &root.span)?.as_u32()?;
    if version == 0 {
        return Err(Error::invalid("workflow version must be positive"));
    }
    let input_schema = parse_schema_ref(required(map, "input_schema", &root.span)?)?;
    let output_schema = parse_schema_ref(required(map, "output_schema", &root.span)?)?;
    catalog.require_schema(&input_schema)?;
    catalog.require_schema(&output_schema)?;
    let run_timeout_ms = match map.get("run_timeout") {
        Some(node) => Some(duration_to_millis(parse_duration(node.as_str()?)?)?),
        None => None,
    };
    if let Some(ms) = run_timeout_ms {
        if Duration::from_millis(ms) > MAX_RUN_TIMEOUT {
            return Err(Error::invalid("run_timeout exceeds 365 days"));
        }
    }
    let mut signals = BTreeMap::new();
    if let Some(node) = map.get("signals") {
        let signal_map = node.as_map()?;
        for (name, spec) in signal_map {
            if !valid_ascii_name(name) {
                return Err(Error::invalid(format!("invalid signal name {name}")));
            }
            let spec_map = spec.as_map()?;
            reject_unknown(spec_map, &["schema"], &spec.span)?;
            let schema = parse_schema_ref(required(spec_map, "schema", &spec.span)?)?;
            catalog.require_schema(&schema)?;
            signals.insert(name.clone(), SignalDecl { schema });
        }
    }
    let mut counts = Counts::default();
    let region = compile_region(
        map,
        &root.span,
        catalog,
        &signals,
        RegionPath::root(),
        RegionCtx {
            input_schema: input_schema.clone(),
            output_schema: output_schema.clone(),
            in_loop: false,
            in_foreach: false,
            in_saga: false,
            in_compensation: false,
        },
        1,
        &mut counts,
        true,
    )?;
    if counts.nodes > MAX_NODES {
        return Err(Error::invalid("definition exceeds 512 nodes"));
    }
    let mut definition = Definition {
        format_version: FORMAT_VERSION,
        dsl: DSL.to_owned(),
        id,
        version,
        input_schema,
        output_schema,
        signals,
        run_timeout_ms,
        root: region,
        digest: Digest(String::new()),
    };
    definition.digest = digest_of(&definition)?;
    Ok(definition)
}

pub fn validate_definition(definition: &Definition, catalog: &Catalog) -> Result<()> {
    let ctx = RegionCtx {
        input_schema: definition.input_schema.clone(),
        output_schema: definition.output_schema.clone(),
        in_loop: false,
        in_foreach: false,
        in_saga: false,
        in_compensation: false,
    };
    analyze_tree(&definition.root, &ctx, catalog)
}

fn analyze_tree(region: &Region, ctx: &RegionCtx, catalog: &Catalog) -> Result<()> {
    analyze_region(region, ctx, catalog)?;
    for node in region.nodes.values() {
        match node {
            Node::While { body, .. } | Node::DoWhile { body, .. } | Node::Repeat { body, .. } => {
                let mut child = ctx.clone();
                child.in_loop = true;
                child.input_schema = body.input_schema.clone();
                child.output_schema = body.output_schema.clone();
                analyze_tree(body, &child, catalog)?;
            }
            Node::Foreach { body, .. } => {
                let mut child = ctx.clone();
                child.in_foreach = true;
                child.input_schema = body.input_schema.clone();
                child.output_schema = body.output_schema.clone();
                analyze_tree(body, &child, catalog)?;
            }
            Node::Saga { body, .. } => {
                let mut child = ctx.clone();
                child.in_saga = true;
                child.input_schema = body.input_schema.clone();
                child.output_schema = body.output_schema.clone();
                analyze_tree(body, &child, catalog)?;
            }
            Node::Parallel { branches, .. } => {
                for branch in branches {
                    let mut child = ctx.clone();
                    child.input_schema = branch.body.input_schema.clone();
                    child.output_schema = branch.body.output_schema.clone();
                    analyze_tree(&branch.body, &child, catalog)?;
                }
            }
            Node::Choose { cases, default, .. } => {
                for case in cases {
                    let mut child = ctx.clone();
                    child.input_schema = case.body.input_schema.clone();
                    child.output_schema = case.body.output_schema.clone();
                    analyze_tree(&case.body, &child, catalog)?;
                }
                let mut child = ctx.clone();
                child.input_schema = default.input_schema.clone();
                child.output_schema = default.output_schema.clone();
                analyze_tree(default, &child, catalog)?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Default)]
struct Counts {
    nodes: usize,
}

#[derive(Clone)]
struct RegionCtx {
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    in_loop: bool,
    in_foreach: bool,
    in_saga: bool,
    in_compensation: bool,
}

fn compile_region(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: RegionCtx,
    depth: usize,
    counts: &mut Counts,
    is_root: bool,
) -> Result<Region> {
    if depth > MAX_NESTING {
        return Err(Error::invalid("region nesting exceeds 8").at_span(span.line, span.column));
    }
    if is_root {
        reject_unknown(map, WORKFLOW_FIELDS, span)?;
    } else {
        reject_unknown(map, REGION_FIELDS, span)?;
    }
    let start = parse_node_key(required(map, "start", span)?)?;
    let nodes_node = required(map, "nodes", span)?;
    let nodes_map = nodes_node.as_map()?;
    let mut nodes = BTreeMap::new();
    for (key, node) in nodes_map {
        let parsed_key = NodeKey::parse(key).map_err(Error::invalid)?;
        counts.nodes += 1;
        let compiled = compile_node(
            node,
            catalog,
            signals,
            path.child(crate::ir::PathSeg::Node(key.clone())),
            &ctx,
            depth,
            counts,
        )?;
        nodes.insert(parsed_key.as_str().to_owned(), compiled);
    }
    if !nodes.contains_key(start.as_str()) {
        return Err(
            Error::invalid(format!("start node {} is missing", start.as_str()))
                .at_span(span.line, span.column),
        );
    }
    let region = Region {
        path,
        input_schema: ctx.input_schema.clone(),
        output_schema: ctx.output_schema.clone(),
        start,
        nodes,
    };
    analyze_region(&region, &ctx, catalog)?;
    Ok(region)
}

fn compile_node(
    node: &Spanned,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
) -> Result<Node> {
    let map = node.as_map()?;
    let kind = required(map, "kind", &node.span)?.as_str()?;
    match kind {
        "activity" => compile_activity(map, &node.span, catalog, ctx),
        "choose" => compile_choose(map, &node.span, catalog, signals, path, ctx, depth, counts),
        "delay" => compile_delay(map, &node.span),
        "wait_until" => compile_wait_until(map, &node.span),
        "wait_signal" => compile_wait_signal(map, &node.span, signals),
        "complete" => compile_complete(map, &node.span),
        "fail" => compile_fail(map, &node.span),
        "while" => compile_loop(
            map,
            &node.span,
            catalog,
            signals,
            path,
            ctx,
            depth,
            counts,
            LoopKind::While,
        ),
        "do_while" => compile_loop(
            map,
            &node.span,
            catalog,
            signals,
            path,
            ctx,
            depth,
            counts,
            LoopKind::DoWhile,
        ),
        "repeat" => compile_loop(
            map,
            &node.span,
            catalog,
            signals,
            path,
            ctx,
            depth,
            counts,
            LoopKind::Repeat,
        ),
        "foreach" => compile_foreach(map, &node.span, catalog, signals, path, ctx, depth, counts),
        "parallel" => compile_parallel(map, &node.span, catalog, signals, path, ctx, depth, counts),
        "saga" => compile_saga(map, &node.span, catalog, signals, path, ctx, depth, counts),
        other => Err(Error::invalid(format!("unknown node kind {other}"))
            .at_span(node.span.line, node.span.column)),
    }
}

fn compile_activity(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    ctx: &RegionCtx,
) -> Result<Node> {
    reject_unknown(
        map,
        &[
            "kind",
            "activity",
            "input",
            "retry",
            "timeout",
            "compensation",
            "next",
        ],
        span,
    )?;
    let activity = parse_activity_key(required(map, "activity", span)?)?;
    let contract = catalog.activity(&activity)?;
    let input = parse_binding(required(map, "input", span)?)?;
    let next = parse_node_key(required(map, "next", span)?)?;
    let timeout_ms = parse_optional_timeout(map, "timeout", FORWARD_ATTEMPT_TIMEOUT)?;
    let retry = parse_retry(map, contract, false)?;
    let compensation = match map.get("compensation") {
        Some(node) => {
            if !ctx.in_saga {
                return Err(Error::invalid("compensation is only valid inside a saga")
                    .at_span(node.span.line, node.span.column));
            }
            Some(parse_compensation(node, catalog, contract)?)
        }
        None => {
            if ctx.in_saga && contract.effects == EffectKind::External {
                return Err(Error::invalid(
                    "external activity inside a saga requires compensation or irreversible",
                )
                .at_span(span.line, span.column));
            }
            None
        }
    };
    Ok(Node::Activity {
        activity,
        input,
        timeout_ms,
        retry,
        compensation,
        next,
    })
}

fn parse_activity_key(node: &Spanned) -> Result<ActivityKey> {
    let map = node.as_map()?;
    reject_unknown(map, &["name", "version"], &node.span)?;
    Ok(ActivityKey::new(
        required(map, "name", &node.span)?.as_str()?,
        required(map, "version", &node.span)?.as_u32()?,
    ))
}

fn parse_optional_timeout(
    map: &BTreeMap<String, Spanned>,
    field: &str,
    default: Duration,
) -> Result<Option<u64>> {
    match map.get(field) {
        Some(node) => {
            let duration = parse_duration(node.as_str()?)?;
            validate_attempt_timeout(duration)?;
            Ok(Some(duration_to_millis(duration)?))
        }
        None => Ok(Some(duration_to_millis(default)?)),
    }
}

fn parse_retry(
    map: &BTreeMap<String, Spanned>,
    contract: &ActivityContract,
    compensation: bool,
) -> Result<RetryPolicy> {
    let mut policy = if compensation {
        RetryPolicy::compensation_default(contract.retryable_codes())
    } else {
        RetryPolicy::forward_default()
    };
    if let Some(node) = map.get("retry") {
        let retry_map = node.as_map()?;
        reject_unknown(
            retry_map,
            &["errors", "max_attempts", "backoff"],
            &node.span,
        )?;
        if let Some(errors) = retry_map.get("errors") {
            let Doc::Array(items) = &errors.doc else {
                return Err(Error::invalid("retry.errors must be an array")
                    .at_span(errors.span.line, errors.span.column));
            };
            policy.errors = items
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect::<Result<Vec<_>>>()?;
            for code in &policy.errors {
                match contract.known_code(code) {
                    Some(spec) if spec.retryable => {}
                    Some(_) => {
                        return Err(Error::invalid(format!(
                            "retry lists terminal or unknown code {code}"
                        )));
                    }
                    None => {
                        return Err(Error::invalid(format!(
                            "retry lists terminal or unknown code {code}"
                        )));
                    }
                }
            }
        }
        if let Some(max) = retry_map.get("max_attempts") {
            policy.max_attempts = max.as_u32()?;
        }
        if let Some(backoff) = retry_map.get("backoff") {
            let backoff_map = backoff.as_map()?;
            reject_unknown(
                backoff_map,
                &["initial", "multiplier", "max"],
                &backoff.span,
            )?;
            policy.backoff.initial_ms = duration_to_millis(parse_duration(
                required(backoff_map, "initial", &backoff.span)?.as_str()?,
            )?)?;
            let multiplier = required(backoff_map, "multiplier", &backoff.span)?.as_i64()?;
            if !(1..=10).contains(&multiplier) {
                return Err(Error::invalid(
                    "backoff multiplier must be between 1 and 10",
                ));
            }
            policy.backoff.multiplier_millis = (multiplier as u32) * 1_000;
            policy.backoff.max_ms = duration_to_millis(parse_duration(
                required(backoff_map, "max", &backoff.span)?.as_str()?,
            )?)?;
        }
    }
    policy.validate()?;
    Ok(policy)
}

fn parse_compensation(
    node: &Spanned,
    catalog: &Catalog,
    forward: &ActivityContract,
) -> Result<Compensation> {
    let map = node.as_map()?;
    let kind = required(map, "kind", &node.span)?.as_str()?;
    match kind {
        "irreversible" => {
            reject_unknown(map, &["kind", "reason"], &node.span)?;
            Ok(Compensation::Irreversible {
                reason: required(map, "reason", &node.span)?.as_str()?.to_owned(),
            })
        }
        "activity" => {
            reject_unknown(
                map,
                &["kind", "activity", "input", "timeout", "retry"],
                &node.span,
            )?;
            let activity = parse_activity_key(required(map, "activity", &node.span)?)?;
            let contract = catalog.activity(&activity)?;
            let input = parse_binding(required(map, "input", &node.span)?)?;
            for reference in input.references() {
                if !matches!(
                    reference,
                    Reference::ForwardInput | Reference::ForwardOutput
                ) {
                    return Err(Error::invalid(
                        "compensation input may only use forward.input or forward.output",
                    )
                    .at_span(node.span.line, node.span.column));
                }
            }
            let timeout_ms = match map.get("timeout") {
                Some(timeout) => {
                    let duration = parse_duration(timeout.as_str()?)?;
                    validate_attempt_timeout(duration)?;
                    Some(duration_to_millis(duration)?)
                }
                None => Some(duration_to_millis(COMPENSATION_ATTEMPT_TIMEOUT)?),
            };
            let retry = parse_retry(map, contract, true)?;
            let _ = forward;
            Ok(Compensation::Activity {
                activity,
                input,
                timeout_ms,
                retry: Some(retry),
            })
        }
        other => Err(Error::invalid(format!("unknown compensation kind {other}"))
            .at_span(node.span.line, node.span.column)),
    }
}

fn compile_delay(map: &BTreeMap<String, Spanned>, span: &Span) -> Result<Node> {
    reject_unknown(map, &["kind", "duration", "next"], span)?;
    let duration_node = required(map, "duration", span)?;
    let duration = match &duration_node.doc {
        Doc::String(_) => Binding::literal(duration_node.to_value()?),
        Doc::Map(_) => parse_binding(duration_node)?,
        _ => {
            return Err(Error::invalid("duration must be a string or binding")
                .at_span(duration_node.span.line, duration_node.span.column));
        }
    };
    Ok(Node::Delay {
        duration,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

fn compile_wait_until(map: &BTreeMap<String, Spanned>, span: &Span) -> Result<Node> {
    reject_unknown(map, &["kind", "at", "next"], span)?;
    Ok(Node::WaitUntil {
        at: parse_binding(required(map, "at", span)?)?,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

fn compile_wait_signal(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    signals: &BTreeMap<String, SignalDecl>,
) -> Result<Node> {
    reject_unknown(
        map,
        &[
            "kind",
            "signal",
            "key",
            "timeout",
            "consume_from",
            "next",
            "on_timeout",
        ],
        span,
    )?;
    let signal = required(map, "signal", span)?.as_str()?.to_owned();
    if !signals.contains_key(&signal) {
        return Err(
            Error::invalid(format!("unknown signal {signal}")).at_span(span.line, span.column)
        );
    }
    let timeout_node = required(map, "timeout", span)?;
    let timeout_ms = match &timeout_node.doc {
        Doc::Null => None,
        Doc::String(_) => Some(duration_to_millis(parse_duration(timeout_node.as_str()?)?)?),
        _ => {
            return Err(Error::invalid("timeout must be a duration string or null")
                .at_span(timeout_node.span.line, timeout_node.span.column));
        }
    };
    let on_timeout = map.get("on_timeout").map(parse_node_key).transpose()?;
    match (timeout_ms.is_some(), on_timeout.is_some()) {
        (true, false) => {
            return Err(Error::invalid("finite wait_signal requires on_timeout")
                .at_span(span.line, span.column));
        }
        (false, true) => {
            return Err(Error::invalid("indefinite wait_signal forbids on_timeout")
                .at_span(span.line, span.column));
        }
        _ => {}
    }
    let consume_from = match map.get("consume_from") {
        Some(node) => match node.as_str()? {
            "buffered" => ConsumeFrom::Buffered,
            "after_activation" => ConsumeFrom::AfterActivation,
            other => {
                return Err(Error::invalid(format!("unknown consume_from {other}"))
                    .at_span(node.span.line, node.span.column));
            }
        },
        None => ConsumeFrom::Buffered,
    };
    Ok(Node::WaitSignal {
        signal,
        key: parse_binding(required(map, "key", span)?)?,
        timeout_ms,
        consume_from,
        next: parse_node_key(required(map, "next", span)?)?,
        on_timeout,
    })
}

fn compile_complete(map: &BTreeMap<String, Spanned>, span: &Span) -> Result<Node> {
    reject_unknown(map, &["kind", "output"], span)?;
    Ok(Node::Complete {
        output: parse_binding(required(map, "output", span)?)?,
    })
}

fn compile_fail(map: &BTreeMap<String, Spanned>, span: &Span) -> Result<Node> {
    reject_unknown(map, &["kind", "error"], span)?;
    let error = required(map, "error", span)?;
    let error_map = error.as_map()?;
    reject_unknown(error_map, &["code", "message"], &error.span)?;
    Ok(Node::Fail {
        error: FailError {
            code: required(error_map, "code", &error.span)?
                .as_str()?
                .to_owned(),
            message: required(error_map, "message", &error.span)?
                .as_str()?
                .to_owned(),
        },
    })
}

#[derive(Clone, Copy)]
enum LoopKind {
    While,
    DoWhile,
    Repeat,
}

fn compile_loop(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
    kind: LoopKind,
) -> Result<Node> {
    let mut allowed = vec![
        "kind",
        "state",
        "state_schema",
        "condition",
        "max_iterations",
        "body",
        "next",
    ];
    if matches!(kind, LoopKind::Repeat) {
        allowed.push("count");
    }
    reject_unknown(map, &allowed, span)?;
    let state_schema = parse_schema_ref(required(map, "state_schema", span)?)?;
    catalog.require_schema(&state_schema)?;
    let max_iterations = required(map, "max_iterations", span)?.as_u32()?;
    if max_iterations == 0 || max_iterations > MAX_LOOP_ITERATIONS {
        return Err(
            Error::invalid("max_iterations is out of range").at_span(span.line, span.column)
        );
    }
    if matches!(kind, LoopKind::DoWhile) && max_iterations < 1 {
        return Err(Error::invalid("do_while max_iterations must be at least 1"));
    }
    let body_node = required(map, "body", span)?;
    let body_map = body_node.as_map()?;
    let body_input = parse_schema_ref(required(body_map, "input_schema", &body_node.span)?)?;
    let body_output = parse_schema_ref(required(body_map, "output_schema", &body_node.span)?)?;
    if body_input != state_schema || body_output != state_schema {
        return Err(Error::invalid("loop body schemas must match state_schema")
            .at_span(body_node.span.line, body_node.span.column));
    }
    let mut body_ctx = ctx.clone();
    body_ctx.input_schema = body_input.clone();
    body_ctx.output_schema = body_output.clone();
    body_ctx.in_loop = true;
    let body = compile_region(
        body_map,
        &body_node.span,
        catalog,
        signals,
        path.child(crate::ir::PathSeg::Body),
        body_ctx,
        depth + 1,
        counts,
        false,
    )?;
    let next = parse_node_key(required(map, "next", span)?)?;
    let state = parse_binding(required(map, "state", span)?)?;
    Ok(match kind {
        LoopKind::While => Node::While {
            state,
            state_schema,
            condition: parse_condition(required(map, "condition", span)?)?,
            max_iterations,
            body,
            next,
        },
        LoopKind::DoWhile => Node::DoWhile {
            state,
            state_schema,
            condition: parse_condition(required(map, "condition", span)?)?,
            max_iterations,
            body,
            next,
        },
        LoopKind::Repeat => Node::Repeat {
            count: parse_binding(required(map, "count", span)?)?,
            state,
            state_schema,
            max_iterations,
            body,
            next,
        },
    })
}

fn compile_foreach(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
) -> Result<Node> {
    reject_unknown(
        map,
        &[
            "kind",
            "items",
            "item_schema",
            "max_items",
            "max_concurrency",
            "body",
            "next",
        ],
        span,
    )?;
    let item_schema = parse_schema_ref(required(map, "item_schema", span)?)?;
    catalog.require_schema(&item_schema)?;
    let max_items = required(map, "max_items", span)?.as_u32()?;
    if max_items == 0 || max_items > MAX_LOOP_ITERATIONS {
        return Err(Error::invalid("max_items is out of range").at_span(span.line, span.column));
    }
    let max_concurrency = required(map, "max_concurrency", span)?.as_u32()?;
    if max_concurrency == 0 || max_concurrency > MAX_FOREACH_CONCURRENCY {
        return Err(
            Error::invalid("max_concurrency is out of range").at_span(span.line, span.column)
        );
    }
    let body_node = required(map, "body", span)?;
    let body_map = body_node.as_map()?;
    let body_input = parse_schema_ref(required(body_map, "input_schema", &body_node.span)?)?;
    if body_input != item_schema {
        return Err(Error::invalid(
            "foreach body input_schema must match item_schema",
        ));
    }
    let body_output = parse_schema_ref(required(body_map, "output_schema", &body_node.span)?)?;
    catalog.require_schema(&body_output)?;
    let mut body_ctx = ctx.clone();
    body_ctx.input_schema = body_input;
    body_ctx.output_schema = body_output;
    body_ctx.in_foreach = true;
    let body = compile_region(
        body_map,
        &body_node.span,
        catalog,
        signals,
        path.child(crate::ir::PathSeg::Body),
        body_ctx,
        depth + 1,
        counts,
        false,
    )?;
    Ok(Node::Foreach {
        items: parse_binding(required(map, "items", span)?)?,
        item_schema,
        max_items,
        max_concurrency,
        body,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

fn compile_parallel(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
) -> Result<Node> {
    reject_unknown(map, &["kind", "branches", "next"], span)?;
    let branches_node = required(map, "branches", span)?;
    let Doc::Array(items) = &branches_node.doc else {
        return Err(Error::invalid("branches must be a sequence")
            .at_span(branches_node.span.line, branches_node.span.column));
    };
    if items.is_empty() || items.len() > MAX_PARALLEL_BRANCHES {
        return Err(
            Error::invalid("parallel requires 1 to 16 branches").at_span(span.line, span.column)
        );
    }
    let mut branches = Vec::new();
    let mut names = BTreeSet::new();
    for item in items {
        let branch_map = item.as_map()?;
        reject_unknown(branch_map, &["name", "input", "body"], &item.span)?;
        let name = required(branch_map, "name", &item.span)?
            .as_str()?
            .to_owned();
        if !valid_ascii_name(&name) || !names.insert(name.clone()) {
            return Err(
                Error::invalid(format!("invalid or duplicate branch name {name}"))
                    .at_span(item.span.line, item.span.column),
            );
        }
        let body_node = required(branch_map, "body", &item.span)?;
        let body_map = body_node.as_map()?;
        let mut body_ctx = ctx.clone();
        body_ctx.input_schema =
            parse_schema_ref(required(body_map, "input_schema", &body_node.span)?)?;
        body_ctx.output_schema =
            parse_schema_ref(required(body_map, "output_schema", &body_node.span)?)?;
        catalog.require_schema(&body_ctx.input_schema)?;
        catalog.require_schema(&body_ctx.output_schema)?;
        let body = compile_region(
            body_map,
            &body_node.span,
            catalog,
            signals,
            path.child(crate::ir::PathSeg::Branch(name.clone())),
            body_ctx,
            depth + 1,
            counts,
            false,
        )?;
        branches.push(ParallelBranch {
            name,
            input: parse_binding(required(branch_map, "input", &item.span)?)?,
            body,
        });
    }
    Ok(Node::Parallel {
        branches,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

fn compile_choose(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
) -> Result<Node> {
    reject_unknown(map, &["kind", "input", "cases", "default", "next"], span)?;
    let cases_node = required(map, "cases", span)?;
    let Doc::Array(items) = &cases_node.doc else {
        return Err(Error::invalid("cases must be a sequence")
            .at_span(cases_node.span.line, cases_node.span.column));
    };
    if items.is_empty() {
        return Err(
            Error::invalid("choose requires a nonempty cases list").at_span(span.line, span.column)
        );
    }
    let mut cases = Vec::new();
    let mut names = BTreeSet::new();
    let mut case_output: Option<SchemaRef> = None;
    for item in items {
        let case_map = item.as_map()?;
        reject_unknown(case_map, &["name", "when", "body"], &item.span)?;
        let name = required(case_map, "name", &item.span)?.as_str()?.to_owned();
        if !valid_ascii_name(&name) || !names.insert(name.clone()) {
            return Err(Error::invalid(format!(
                "invalid or duplicate case name {name}"
            )));
        }
        let body_node = required(case_map, "body", &item.span)?;
        let body_map = body_node.as_map()?;
        let mut body_ctx = ctx.clone();
        body_ctx.input_schema =
            parse_schema_ref(required(body_map, "input_schema", &body_node.span)?)?;
        body_ctx.output_schema =
            parse_schema_ref(required(body_map, "output_schema", &body_node.span)?)?;
        catalog.require_schema(&body_ctx.input_schema)?;
        catalog.require_schema(&body_ctx.output_schema)?;
        match &case_output {
            None => case_output = Some(body_ctx.output_schema.clone()),
            Some(expected) if expected != &body_ctx.output_schema => {
                return Err(Error::invalid("choose cases must share an output schema"));
            }
            _ => {}
        }
        let body = compile_region(
            body_map,
            &body_node.span,
            catalog,
            signals,
            path.child(crate::ir::PathSeg::Case(name.clone())),
            body_ctx,
            depth + 1,
            counts,
            false,
        )?;
        cases.push(ChooseCase {
            name,
            when: parse_condition(required(case_map, "when", &item.span)?)?,
            body,
        });
    }
    let default_node = required(map, "default", span)?;
    let default_map = default_node.as_map()?;
    let mut default_ctx = ctx.clone();
    default_ctx.input_schema =
        parse_schema_ref(required(default_map, "input_schema", &default_node.span)?)?;
    default_ctx.output_schema =
        parse_schema_ref(required(default_map, "output_schema", &default_node.span)?)?;
    if Some(&default_ctx.output_schema) != case_output.as_ref() {
        return Err(Error::invalid(
            "choose default output schema must match cases",
        ));
    }
    let default = compile_region(
        default_map,
        &default_node.span,
        catalog,
        signals,
        path.child(crate::ir::PathSeg::Default),
        default_ctx,
        depth + 1,
        counts,
        false,
    )?;
    Ok(Node::Choose {
        input: parse_binding(required(map, "input", span)?)?,
        cases,
        default,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

fn compile_saga(
    map: &BTreeMap<String, Spanned>,
    span: &Span,
    catalog: &Catalog,
    signals: &BTreeMap<String, SignalDecl>,
    path: RegionPath,
    ctx: &RegionCtx,
    depth: usize,
    counts: &mut Counts,
) -> Result<Node> {
    reject_unknown(map, &["kind", "input", "body", "next"], span)?;
    let body_node = required(map, "body", span)?;
    let body_map = body_node.as_map()?;
    let mut body_ctx = ctx.clone();
    body_ctx.input_schema = parse_schema_ref(required(body_map, "input_schema", &body_node.span)?)?;
    body_ctx.output_schema =
        parse_schema_ref(required(body_map, "output_schema", &body_node.span)?)?;
    body_ctx.in_saga = true;
    catalog.require_schema(&body_ctx.input_schema)?;
    catalog.require_schema(&body_ctx.output_schema)?;
    let body = compile_region(
        body_map,
        &body_node.span,
        catalog,
        signals,
        path.child(crate::ir::PathSeg::Body),
        body_ctx,
        depth + 1,
        counts,
        false,
    )?;
    Ok(Node::Saga {
        input: parse_binding(required(map, "input", span)?)?,
        body,
        next: parse_node_key(required(map, "next", span)?)?,
    })
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Edge {
    Next,
    Timeout,
}

fn analyze_region(region: &Region, ctx: &RegionCtx, catalog: &Catalog) -> Result<()> {
    let mut incoming: HashMap<String, Vec<(String, Edge)>> = HashMap::new();
    let mut outgoing: HashMap<String, Vec<(Edge, String)>> = HashMap::new();
    for (key, node) in &region.nodes {
        for (edge_name, target) in node.successors() {
            if !region.nodes.contains_key(target.as_str()) {
                return Err(Error::invalid(format!(
                    "node {key} points at missing target {}",
                    target.as_str()
                )));
            }
            let edge = if edge_name == "on_timeout" {
                Edge::Timeout
            } else {
                Edge::Next
            };
            outgoing
                .entry(key.clone())
                .or_default()
                .push((edge, target.as_str().to_owned()));
            incoming
                .entry(target.as_str().to_owned())
                .or_default()
                .push((key.clone(), edge));
        }
        if let Node::WaitSignal { key: wait_key, .. } = node {
            if wait_key.depth() > MAX_DATA_DEPTH {
                return Err(Error::invalid("binding exceeds maximum depth"));
            }
        }
        for (_, child) in node.child_regions() {
            let _ = child;
        }
        match node {
            Node::While { condition, .. } | Node::DoWhile { condition, .. } => {
                if condition.operator_count() > MAX_CONDITION_OPS {
                    return Err(Error::invalid("condition exceeds operator limit"));
                }
            }
            Node::Choose { cases, .. } => {
                for case in cases {
                    if case.when.operator_count() > MAX_CONDITION_OPS {
                        return Err(Error::invalid("condition exceeds operator limit"));
                    }
                }
            }
            _ => {}
        }
    }
    detect_cycle(region.start.as_str(), &outgoing)?;
    let mut available: HashMap<(String, Edge), BTreeSet<Reference>> = HashMap::new();
    let mut at_node: HashMap<String, BTreeSet<Reference>> = HashMap::new();
    let mut base = BTreeSet::from([
        Reference::WorkflowInput,
        Reference::ScopeInput,
        Reference::ScopeId,
    ]);
    if ctx.in_loop {
        base.insert(Reference::LoopState);
        base.insert(Reference::LoopIndex);
    }
    if ctx.in_foreach {
        base.insert(Reference::ItemValue);
        base.insert(Reference::ItemIndex);
    }
    if ctx.in_compensation {
        base.insert(Reference::ForwardInput);
        base.insert(Reference::ForwardOutput);
    }
    let mut pending = vec![region.start.as_str().to_owned()];
    let mut seen = HashSet::new();
    at_node.insert(region.start.as_str().to_owned(), base.clone());
    while let Some(key) = pending.pop() {
        if !seen.insert(key.clone()) {
            continue;
        }
        let node = region.nodes.get(&key).expect("node exists");
        check_bindings(node, at_node.get(&key).unwrap_or(&base), ctx)?;
        if let Some(edges) = outgoing.get(&key) {
            for (edge, target) in edges {
                let mut next_avail = at_node.get(&key).cloned().unwrap_or_else(|| base.clone());
                if *edge == Edge::Next {
                    if let Some(output_ref) = success_output(&key, node) {
                        next_avail.insert(output_ref);
                    }
                }
                available.insert((key.clone(), *edge), next_avail.clone());
                let entry = at_node
                    .entry(target.clone())
                    .or_insert_with(|| next_avail.clone());
                *entry = entry.intersection(&next_avail).cloned().collect();
                pending.push(target.clone());
            }
        }
    }
    if seen.len() != region.nodes.len() {
        return Err(Error::invalid("region contains unreachable nodes"));
    }
    let _ = catalog;
    let _ = available;
    Ok(())
}

fn success_output(key: &str, node: &Node) -> Option<Reference> {
    match node {
        Node::Fail { .. } | Node::Delay { .. } | Node::WaitUntil { .. } => None,
        Node::Complete { .. } => None,
        _ => Some(Reference::NodeOutput {
            node: NodeKey(key.to_owned()),
        }),
    }
}

fn check_bindings(node: &Node, available: &BTreeSet<Reference>, ctx: &RegionCtx) -> Result<()> {
    let mut bindings = Vec::new();
    match node {
        Node::Activity {
            input,
            compensation,
            ..
        } => {
            bindings.push(input);
            if let Some(Compensation::Activity { input, .. }) = compensation {
                bindings.push(input);
            }
        }
        Node::Choose { input, cases, .. } => {
            bindings.push(input);
            for case in cases {
                for binding in case.when.bindings() {
                    bindings.push(binding);
                }
            }
        }
        Node::Delay { duration, .. } => bindings.push(duration),
        Node::WaitUntil { at, .. } => bindings.push(at),
        Node::WaitSignal { key, .. } => bindings.push(key),
        Node::Complete { output } => bindings.push(output),
        Node::While {
            state, condition, ..
        }
        | Node::DoWhile {
            state, condition, ..
        } => {
            bindings.push(state);
            bindings.extend(condition.bindings());
        }
        Node::Repeat { count, state, .. } => {
            bindings.push(count);
            bindings.push(state);
        }
        Node::Foreach { items, .. } => bindings.push(items),
        Node::Parallel { branches, .. } => {
            for branch in branches {
                bindings.push(&branch.input);
            }
        }
        Node::Saga { input, .. } => bindings.push(input),
        Node::Fail { .. } => {}
    }
    for binding in bindings {
        if binding.depth() > MAX_DATA_DEPTH {
            return Err(Error::invalid("binding exceeds maximum depth"));
        }
        for reference in binding.references() {
            let loop_node = matches!(
                node,
                Node::While { .. } | Node::DoWhile { .. } | Node::Repeat { .. }
            );
            let foreach_node = matches!(node, Node::Foreach { .. });
            match reference {
                Reference::LoopState | Reference::LoopIndex if !ctx.in_loop && !loop_node => {
                    return Err(Error::invalid("loop context reference used outside a loop"));
                }
                Reference::ItemValue | Reference::ItemIndex if !ctx.in_foreach && !foreach_node => {
                    return Err(Error::invalid(
                        "item context reference used outside foreach",
                    ));
                }
                Reference::ForwardInput | Reference::ForwardOutput if !ctx.in_compensation => {
                    if !matches!(
                        node,
                        Node::Activity {
                            compensation: Some(_),
                            ..
                        }
                    ) {
                        return Err(Error::invalid(
                            "forward context reference used outside compensation",
                        ));
                    }
                }
                Reference::NodeOutput { .. } if !available.contains(reference) => {
                    return Err(Error::invalid(format!(
                        "reference {} is not available on every incoming path",
                        reference.as_stable()
                    )));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn detect_cycle(start: &str, outgoing: &HashMap<String, Vec<(Edge, String)>>) -> Result<()> {
    let mut stack = Vec::new();
    let mut on_stack = HashSet::new();
    let mut seen = HashSet::new();
    fn visit(
        key: &str,
        outgoing: &HashMap<String, Vec<(Edge, String)>>,
        stack: &mut Vec<String>,
        on_stack: &mut HashSet<String>,
        seen: &mut HashSet<String>,
    ) -> Result<()> {
        if !on_stack.insert(key.to_owned()) {
            return Err(Error::invalid(format!("cycle through node {key}")));
        }
        stack.push(key.to_owned());
        if seen.insert(key.to_owned()) {
            if let Some(edges) = outgoing.get(key) {
                for (_, target) in edges {
                    visit(target, outgoing, stack, on_stack, seen)?;
                }
            }
        }
        on_stack.remove(key);
        stack.pop();
        Ok(())
    }
    visit(start, outgoing, &mut stack, &mut on_stack, &mut seen)
}

pub fn digest_of(definition: &Definition) -> Result<Digest> {
    let mut value = serde_json::to_value(definition)
        .map_err(|err| Error::invalid(format!("canonicalize: {err}")))?;
    if let Some(obj) = value.as_object_mut() {
        obj.remove("digest");
    }
    let bytes = canonical_json(&value)?;
    if bytes.len() > MAX_NORMALIZED_BYTES {
        return Err(Error::invalid("normalized definition exceeds 256 KiB"));
    }
    let hash = Sha256::digest(&bytes);
    Ok(Digest(hex::encode(hash)))
}

pub fn definitions_equivalent(left: &Definition, right: &Definition) -> bool {
    left.digest == right.digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Catalog {
        Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ))
        .unwrap()
    }

    #[test]
    fn compiles_all_fixtures() {
        let catalog = catalog();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/specs/v1/examples");
        for name in [
            "sequence.yaml",
            "while.yaml",
            "do-while.yaml",
            "repeat.yaml",
            "foreach.yaml",
            "parallel.yaml",
            "events.yaml",
            "timeout-recovery.yaml",
            "saga.yaml",
            "nested-saga.yaml",
            "timers.yaml",
            "remote.yaml",
            "events-in-foreach.yaml",
            "nested-controls.yaml",
        ] {
            let text = std::fs::read_to_string(format!("{dir}/{name}")).unwrap();
            compile_yaml(&text, &catalog).unwrap_or_else(|err| panic!("{name}: {err}"));
        }
    }

    #[test]
    fn digest_is_stable() {
        let catalog = catalog();
        let text = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let a = compile_yaml(text, &catalog).unwrap();
        let b = compile_yaml(text, &catalog).unwrap();
        assert_eq!(a.digest, b.digest);
    }

    #[test]
    fn digest_changes_when_semantics_change() {
        let catalog = catalog();
        let original = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let changed = original.replace("max_attempts: 5", "max_attempts: 4");
        let a = compile_yaml(original, &catalog).unwrap();
        let b = compile_yaml(&changed, &catalog).unwrap();
        assert_ne!(a.digest, b.digest);
    }

    #[test]
    fn bindings_and_conditions_match_spec() {
        let catalog = catalog();
        let missing_path = r#"
dsl: graphrun/v1
id: missing_path
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {from: workflow.input, path: /missing}
"#;
        compile_yaml(missing_path, &catalog).expect("missing path compiles; evaluation rejects it");

        let empty_all = r#"
dsl: graphrun/v1
id: empty_all
version: 1
input_schema: counter/v1
output_schema: counter/v1
start: count
nodes:
  count:
    kind: while
    state_schema: counter/v1
    state: {from: workflow.input}
    condition:
      all: []
    max_iterations: 1
    body:
      input_schema: counter/v1
      output_schema: counter/v1
      start: done
      nodes:
        done:
          kind: complete
          output: {from: scope.input}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.count.output}
"#;
        let err = compile_yaml(empty_all, &catalog).unwrap_err();
        assert!(
            err.message
                .contains("all/any requires at least one condition"),
            "{}",
            err.message
        );

        let unknown_ref = r#"
dsl: graphrun/v1
id: bad_ref
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {from: not.a.reference}
"#;
        let err = compile_yaml(unknown_ref, &catalog).unwrap_err();
        assert!(err.message.contains("unknown reference"), "{}", err.message);

        let tuple_array = r#"
dsl: graphrun/v1
id: schemas
version: 1
input_schema:
  tuple:
    - counter/v1
    - counter/v1
output_schema:
  array: counter/v1
start: finish
nodes:
  finish:
    kind: complete
    output:
      array:
        - {from: workflow.input, path: /0}
        - {from: workflow.input, path: /1}
"#;
        compile_yaml(tuple_array, &catalog).expect("tuple and array constructors compile");

        let null_literal = r#"
dsl: graphrun/v1
id: null_out
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: finish
nodes:
  finish:
    kind: complete
    output: {literal: null}
"#;
        compile_yaml(null_literal, &catalog).expect("explicit null is a value");
    }

    #[test]
    fn rejects_yaml_tags_and_anchors() {
        let err = crate::yaml::parse_yaml("a: &id 1\nb: *id\n").unwrap_err();
        assert!(
            err.message.contains("anchor") || err.message.contains("alias"),
            "{}",
            err.message
        );
    }
}
