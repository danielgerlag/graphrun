use crate::binding::Binding;
use crate::catalog::Catalog;
use crate::compiler::{digest_of, validate_definition};
use crate::error::{Error, Result};
use crate::ids::{ActivityKey, NodeKey, valid_ascii_name};
use crate::ir::{
    ChooseCase, Compensation, ConsumeFrom, DSL, Definition, Digest, FORMAT_VERSION, FailError,
    Node, ParallelBranch, Region as IrRegion, RegionPath, SignalDecl,
};
use crate::policy::{COMPENSATION_ATTEMPT_TIMEOUT, FORWARD_ATTEMPT_TIMEOUT, RetryPolicy};
use crate::schema::{DurablePayload, SchemaRef};
use crate::time::duration_to_millis;
use crate::value::Value;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn next_builder_scope() -> ScopeId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    ScopeId(NEXT.fetch_add(1, Ordering::Relaxed))
}

fn check_binding_paths(binding: &Binding) -> Result<()> {
    match binding {
        Binding::From {
            path: Some(path), ..
        } => {
            if !path.starts_with('/') {
                return Err(Error::invalid(format!(
                    "JSON Pointer {path} must start with /"
                )));
            }
            Ok(())
        }
        Binding::Object { fields } => fields.values().try_for_each(check_binding_paths),
        Binding::Array { items } => items.iter().try_for_each(check_binding_paths),
        _ => Ok(()),
    }
}

#[derive(Clone, Copy)]
struct ScopeId(u64);

pub struct ActivityRef<I, O> {
    key: ActivityKey,
    _i: PhantomData<fn() -> I>,
    _o: PhantomData<fn() -> O>,
}

impl<I: DurablePayload, O: DurablePayload> ActivityRef<I, O> {
    pub fn from_catalog(catalog: &Catalog, name: &str, version: u32) -> Result<Self> {
        let key = ActivityKey::new(name, version);
        let contract = catalog.activity(&key)?;
        if contract.input_schema != I::schema_ref() || contract.output_schema != O::schema_ref() {
            return Err(Error::invalid(format!(
                "activity {}/v{} does not match the requested payload schemas",
                name, version
            )));
        }
        Ok(Self {
            key,
            _i: PhantomData,
            _o: PhantomData,
        })
    }

    pub fn key(&self) -> &ActivityKey {
        &self.key
    }
}

pub struct SignalRef<T> {
    name: String,
    schema: SchemaRef,
    _t: PhantomData<fn() -> T>,
}

impl<T: DurablePayload> SignalRef<T> {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if !valid_ascii_name(&name) {
            return Err(Error::invalid(format!("invalid signal name {name}")));
        }
        Ok(Self {
            name,
            schema: T::schema_ref(),
            _t: PhantomData,
        })
    }
}

pub struct ValueRef<T> {
    binding: Binding,
    scope: ScopeId,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for ValueRef<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            scope: self.scope,
            _t: PhantomData,
        }
    }
}

impl<T> ValueRef<T> {
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
}

pub struct InputMapping<T> {
    binding: Binding,
    _t: PhantomData<fn() -> T>,
}

pub trait IntoInput<T> {
    fn into_binding(self) -> Binding;
}

impl<T> IntoInput<T> for ValueRef<T> {
    fn into_binding(self) -> Binding {
        self.binding
    }
}

impl<T> IntoInput<T> for InputMapping<T> {
    fn into_binding(self) -> Binding {
        self.binding
    }
}

pub struct NodeRef<O> {
    key: NodeKey,
    scope: ScopeId,
    _o: PhantomData<fn() -> O>,
}

impl<O> NodeRef<O> {
    pub fn output(&self) -> ValueRef<O> {
        ValueRef {
            binding: Binding::from_ref(crate::binding::Reference::NodeOutput {
                node: self.key.clone(),
            }),
            scope: self.scope,
            _t: PhantomData,
        }
    }

    pub fn entry(&self) -> EntryPort {
        EntryPort {
            node: self.key.clone(),
            scope: self.scope,
        }
    }

    pub fn exit(&self) -> ExitPort {
        ExitPort {
            node: self.key.clone(),
            scope: self.scope,
            timeout: false,
        }
    }
}

pub struct TimedWaitRef<T> {
    key: NodeKey,
    scope: ScopeId,
    _t: PhantomData<fn() -> T>,
}

impl<T> TimedWaitRef<T> {
    pub fn entry(&self) -> EntryPort {
        EntryPort {
            node: self.key.clone(),
            scope: self.scope,
        }
    }

    pub fn success_port(&self) -> ExitPort {
        ExitPort {
            node: self.key.clone(),
            scope: self.scope,
            timeout: false,
        }
    }

    pub fn timeout_port(&self) -> ExitPort {
        ExitPort {
            node: self.key.clone(),
            scope: self.scope,
            timeout: true,
        }
    }

    pub fn output(&self) -> ValueRef<T> {
        ValueRef {
            binding: Binding::from_ref(crate::binding::Reference::NodeOutput {
                node: self.key.clone(),
            }),
            scope: self.scope,
            _t: PhantomData,
        }
    }
}

pub struct TerminalRef<O> {
    key: NodeKey,
    scope: ScopeId,
    _o: PhantomData<fn() -> O>,
}

impl<O> TerminalRef<O> {
    pub fn entry(&self) -> EntryPort {
        EntryPort {
            node: self.key.clone(),
            scope: self.scope,
        }
    }
}

pub struct EntryPort {
    node: NodeKey,
    scope: ScopeId,
}

pub struct ExitPort {
    node: NodeKey,
    scope: ScopeId,
    timeout: bool,
}

pub struct Branch<I, O> {
    pub name: String,
    pub input: Binding,
    pub body: Region<I, O>,
}

mod parallel_sealed {
    pub trait Sealed {}
}

/// An ordered tuple of 1 to 16 [`Branch`] values. Its output is the tuple of
/// branch outputs in the same order.
///
/// ```compile_fail
/// use graphrun::builder::{Branch, NodeRef, RegionBuilder};
/// fn wrong_output(mut builder: RegionBuilder<String>, branch: Branch<String, i64>) {
///     let _: NodeRef<(String,)> = builder.parallel("p", (branch,)).unwrap();
/// }
/// ```
pub trait ParallelBranches: parallel_sealed::Sealed {
    type Output: DurablePayload;

    #[doc(hidden)]
    fn into_parts(self) -> (Vec<ParallelBranch>, BTreeMap<String, SignalDecl>);
}

macro_rules! impl_parallel_branches {
    ($($input:ident $output:ident $branch:ident),+) => {
        impl<$($input: DurablePayload, $output: DurablePayload),+> parallel_sealed::Sealed
            for ($(Branch<$input, $output>,)+) {}

        impl<$($input: DurablePayload, $output: DurablePayload),+> ParallelBranches
            for ($(Branch<$input, $output>,)+)
        {
            type Output = ($($output,)+);

            fn into_parts(self) -> (Vec<ParallelBranch>, BTreeMap<String, SignalDecl>) {
                let ($($branch,)+) = self;
                let mut branches = Vec::new();
                let mut signals = BTreeMap::new();
                $(
                    branches.push(ParallelBranch {
                        name: $branch.name,
                        input: $branch.input,
                        body: $branch.body.ir,
                    });
                    signals.extend($branch.body.signals);
                )+
                (branches, signals)
            }
        }
    };
}

impl_parallel_branches!(I0 O0 b0);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10, I11 O11 b11);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10, I11 O11 b11, I12 O12 b12);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10, I11 O11 b11, I12 O12 b12, I13 O13 b13);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10, I11 O11 b11, I12 O12 b12, I13 O13 b13, I14 O14 b14);
impl_parallel_branches!(I0 O0 b0, I1 O1 b1, I2 O2 b2, I3 O3 b3, I4 O4 b4, I5 O5 b5, I6 O6 b6, I7 O7 b7, I8 O8 b8, I9 O9 b9, I10 O10 b10, I11 O11 b11, I12 O12 b12, I13 O13 b13, I14 O14 b14, I15 O15 b15);

pub struct Case<I, O> {
    pub name: String,
    pub when: crate::binding::Condition,
    pub body: Region<I, O>,
}

pub struct Region<I, O> {
    ir: IrRegion,
    signals: BTreeMap<String, SignalDecl>,
    _i: PhantomData<fn() -> I>,
    _o: PhantomData<fn() -> O>,
}

impl<I, O> Region<I, O> {
    pub fn ir(&self) -> &IrRegion {
        &self.ir
    }
}

struct Draft {
    scope: ScopeId,
    _input_schema: SchemaRef,
    nodes: BTreeMap<String, Node>,
    start: Option<NodeKey>,
    tail: Option<NodeKey>,
    signals: BTreeMap<String, SignalDecl>,
}

pub struct RegionGraphBuilder<I> {
    draft: Draft,
    _i: PhantomData<fn() -> I>,
}

pub struct RegionBuilder<I> {
    graph: RegionGraphBuilder<I>,
}

impl<I: DurablePayload> RegionBuilder<I> {
    pub fn new() -> Self {
        Self {
            graph: RegionGraphBuilder::new(),
        }
    }

    pub fn input(&self) -> ValueRef<I> {
        self.graph.input()
    }

    pub fn workflow_input(&self) -> ValueRef<I> {
        self.graph.workflow_input()
    }

    pub fn literal<T: DurablePayload + Serialize>(&self, value: T) -> Result<ValueRef<T>> {
        self.graph.literal(value)
    }

    pub fn activity<A, B>(
        &mut self,
        key: &str,
        activity: &ActivityRef<A, B>,
        input: impl IntoInput<A>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node = self.graph.declare_activity(key, activity, input)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn repeat<S: DurablePayload>(
        &mut self,
        key: &str,
        count: ValueRef<i64>,
        state: ValueRef<S>,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node = self
            .graph
            .declare_repeat(key, count, state, body, max_iterations)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn while_loop<S: DurablePayload>(
        &mut self,
        key: &str,
        state: ValueRef<S>,
        condition: crate::binding::Condition,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node = self
            .graph
            .declare_while(key, state, condition, body, max_iterations)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn do_while<S: DurablePayload>(
        &mut self,
        key: &str,
        state: ValueRef<S>,
        condition: crate::binding::Condition,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node = self
            .graph
            .declare_do_while(key, state, condition, body, max_iterations)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn foreach<A, B>(
        &mut self,
        key: &str,
        items: ValueRef<Vec<A>>,
        body: Region<A, B>,
        max_items: u32,
        max_concurrency: u32,
    ) -> Result<NodeRef<Vec<B>>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node = self
            .graph
            .declare_foreach(key, items, body, max_items, max_concurrency)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn choose<A, B>(
        &mut self,
        key: &str,
        input: impl IntoInput<A>,
        cases: Vec<Case<A, B>>,
        default: Region<A, B>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node = self.graph.declare_choose(key, input, cases, default)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn saga<A, B>(
        &mut self,
        key: &str,
        input: ValueRef<A>,
        body: Region<A, B>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node = self.graph.declare_saga(key, input, body)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn delay(&mut self, key: &str, duration: Duration) -> Result<NodeRef<()>> {
        let node = self.graph.declare_delay(key, duration)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn wait_until(&mut self, key: &str, at_ms: i64) -> Result<NodeRef<()>> {
        let node = self.graph.declare_wait_until(key, at_ms)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn wait_signal<T: DurablePayload>(
        &mut self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: ValueRef<String>,
    ) -> Result<NodeRef<T>> {
        let node = self.graph.declare_wait_signal(key, signal, correlation)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    pub fn complete<O: DurablePayload>(
        self,
        key: &str,
        output: ValueRef<O>,
    ) -> Result<Region<I, O>> {
        let mut graph = self.graph;
        let terminal = graph.declare_complete::<O>(key, output)?;
        if let Some(tail) = graph.draft.tail.clone() {
            graph.connect(
                ExitPort {
                    node: tail,
                    scope: graph.draft.scope,
                    timeout: false,
                },
                terminal.entry(),
            )?;
        } else {
            graph.start_at(terminal.entry())?;
        }
        graph.finish()
    }

    pub fn fail<O: DurablePayload>(
        self,
        key: &str,
        code: &str,
        message: &str,
    ) -> Result<Region<I, O>> {
        let mut graph = self.graph;
        let terminal = graph.declare_fail::<O>(key, code, message)?;
        if let Some(tail) = graph.draft.tail.clone() {
            graph.connect(
                ExitPort {
                    node: tail,
                    scope: graph.draft.scope,
                    timeout: false,
                },
                terminal.entry(),
            )?;
        } else {
            graph.start_at(terminal.entry())?;
        }
        graph.finish()
    }

    pub fn compensate<O, U>(
        &mut self,
        node: &NodeRef<O>,
        compensator: &ActivityRef<O, U>,
    ) -> Result<()>
    where
        O: DurablePayload,
        U: DurablePayload,
    {
        self.graph.attach_compensation(node, compensator)
    }

    pub fn parallel2<A, B, IA, IB>(
        &mut self,
        key: &str,
        left: Branch<IA, A>,
        right: Branch<IB, B>,
    ) -> Result<NodeRef<(A, B)>>
    where
        A: DurablePayload,
        B: DurablePayload,
        IA: DurablePayload,
        IB: DurablePayload,
    {
        self.parallel(key, (left, right))
    }

    /// Append a parallel node with 1 to 16 ordered, heterogeneous branches.
    pub fn parallel<B: ParallelBranches>(
        &mut self,
        key: &str,
        branches: B,
    ) -> Result<NodeRef<B::Output>> {
        let node = self.graph.declare_parallel(key, branches)?;
        self.link_tail(node.entry())?;
        self.graph.draft.tail = Some(node.key.clone());
        Ok(node)
    }

    fn link_tail(&mut self, entry: EntryPort) -> Result<()> {
        if let Some(tail) = self.graph.draft.tail.clone() {
            self.graph.connect(
                ExitPort {
                    node: tail,
                    scope: self.graph.draft.scope,
                    timeout: false,
                },
                entry,
            )
        } else {
            self.graph.start_at(entry)
        }
    }
}

impl<I: DurablePayload> Default for RegionBuilder<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: DurablePayload> RegionGraphBuilder<I> {
    pub fn new() -> Self {
        Self {
            draft: Draft {
                scope: next_builder_scope(),
                _input_schema: I::schema_ref(),
                nodes: BTreeMap::new(),
                start: None,
                tail: None,
                signals: BTreeMap::new(),
            },
            _i: PhantomData,
        }
    }

    pub fn input(&self) -> ValueRef<I> {
        ValueRef {
            binding: Binding::from_ref(crate::binding::Reference::ScopeInput),
            scope: self.draft.scope,
            _t: PhantomData,
        }
    }

    pub fn workflow_input(&self) -> ValueRef<I> {
        ValueRef {
            binding: Binding::from_ref(crate::binding::Reference::WorkflowInput),
            scope: self.draft.scope,
            _t: PhantomData,
        }
    }

    pub fn literal<T: DurablePayload + Serialize>(&self, value: T) -> Result<ValueRef<T>> {
        let json = serde_json::to_value(&value)
            .map_err(|err| Error::invalid(format!("literal encode: {err}")))?;
        Ok(ValueRef {
            binding: Binding::literal(crate::value::Value::from_json(json)?),
            scope: self.draft.scope,
            _t: PhantomData,
        })
    }

    pub fn map_input<T: DurablePayload>(&self, binding: Binding) -> Result<InputMapping<T>> {
        if binding.depth() > crate::limits::MAX_DATA_DEPTH {
            return Err(Error::invalid("binding exceeds maximum depth"));
        }
        check_binding_paths(&binding)?;
        Ok(InputMapping {
            binding,
            _t: PhantomData,
        })
    }

    pub fn start_at(&mut self, entry: EntryPort) -> Result<()> {
        self.same_scope(&entry.node, entry.scope)?;
        if self.draft.start.is_some() {
            return Err(Error::invalid("start_at already set"));
        }
        self.draft.start = Some(entry.node);
        Ok(())
    }

    pub fn connect(&mut self, exit: ExitPort, entry: EntryPort) -> Result<()> {
        self.same_scope(&exit.node, exit.scope)?;
        self.same_scope(&entry.node, entry.scope)?;
        let node = self
            .draft
            .nodes
            .get_mut(exit.node.as_str())
            .ok_or_else(|| Error::invalid("connect source does not exist"))?;
        match node {
            Node::WaitSignal {
                next, on_timeout, ..
            } => {
                if exit.timeout {
                    if on_timeout.is_some() {
                        return Err(Error::invalid("timeout port is already connected"));
                    }
                    *on_timeout = Some(entry.node);
                } else {
                    *next = entry.node;
                }
            }
            Node::Activity { next, .. }
            | Node::Choose { next, .. }
            | Node::Delay { next, .. }
            | Node::WaitUntil { next, .. }
            | Node::While { next, .. }
            | Node::DoWhile { next, .. }
            | Node::Repeat { next, .. }
            | Node::Foreach { next, .. }
            | Node::Parallel { next, .. }
            | Node::Saga { next, .. } => {
                if exit.timeout {
                    return Err(Error::invalid("node has no timeout port"));
                }
                *next = entry.node;
            }
            Node::Complete { .. } | Node::Fail { .. } => {
                return Err(Error::invalid("terminal nodes have no exit port"));
            }
        }
        Ok(())
    }

    pub fn declare_activity<A, B>(
        &mut self,
        key: &str,
        activity: &ActivityRef<A, B>,
        input: impl IntoInput<A>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Activity {
                activity: activity.key.clone(),
                input: input.into_binding(),
                timeout_ms: Some(duration_to_millis(FORWARD_ATTEMPT_TIMEOUT)?),
                retry: RetryPolicy::forward_default(),
                compensation: None,
                next: node_key.clone(),
            },
        );
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_timed_wait<T: DurablePayload>(
        &mut self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: ValueRef<String>,
        timeout: Duration,
    ) -> Result<TimedWaitRef<T>> {
        self.same_scope_value(&correlation)?;
        let node_key = self.insert_key(key)?;
        self.draft.signals.insert(
            signal.name.clone(),
            SignalDecl {
                schema: signal.schema.clone(),
            },
        );
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::WaitSignal {
                signal: signal.name.clone(),
                key: correlation.binding,
                timeout_ms: Some(duration_to_millis(timeout)?),
                consume_from: ConsumeFrom::Buffered,
                next: node_key.clone(),
                on_timeout: None,
            },
        );
        Ok(TimedWaitRef {
            key: node_key,
            scope: self.draft.scope,
            _t: PhantomData,
        })
    }

    pub fn declare_wait_signal<T: DurablePayload>(
        &mut self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: ValueRef<String>,
    ) -> Result<NodeRef<T>> {
        self.same_scope_value(&correlation)?;
        let node_key = self.insert_key(key)?;
        self.draft.signals.insert(
            signal.name.clone(),
            SignalDecl {
                schema: signal.schema.clone(),
            },
        );
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::WaitSignal {
                signal: signal.name.clone(),
                key: correlation.binding,
                timeout_ms: None,
                consume_from: ConsumeFrom::Buffered,
                next: node_key.clone(),
                on_timeout: None,
            },
        );
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_repeat<S: DurablePayload>(
        &mut self,
        key: &str,
        count: ValueRef<i64>,
        state: ValueRef<S>,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Repeat {
                count: count.binding,
                state: state.binding,
                state_schema: S::schema_ref(),
                max_iterations,
                body: body.ir,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(body.signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_while<S: DurablePayload>(
        &mut self,
        key: &str,
        state: ValueRef<S>,
        condition: crate::binding::Condition,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::While {
                state: state.binding,
                state_schema: S::schema_ref(),
                condition,
                max_iterations,
                body: body.ir,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(body.signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_do_while<S: DurablePayload>(
        &mut self,
        key: &str,
        state: ValueRef<S>,
        condition: crate::binding::Condition,
        body: Region<S, S>,
        max_iterations: u32,
    ) -> Result<NodeRef<S>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::DoWhile {
                state: state.binding,
                state_schema: S::schema_ref(),
                condition,
                max_iterations,
                body: body.ir,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(body.signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_choose<A, B>(
        &mut self,
        key: &str,
        input: impl IntoInput<A>,
        cases: Vec<Case<A, B>>,
        default: Region<A, B>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        if cases.is_empty() {
            return Err(Error::invalid("choose requires a nonempty cases list"));
        }
        let mut names = BTreeSet::new();
        let mut ir_cases = Vec::new();
        for case in cases {
            if !valid_ascii_name(&case.name) || !names.insert(case.name.clone()) {
                return Err(Error::invalid(format!(
                    "invalid or duplicate case name {}",
                    case.name
                )));
            }
            self.draft.signals.extend(case.body.signals);
            ir_cases.push(ChooseCase {
                name: case.name,
                when: case.when,
                body: case.body.ir,
            });
        }
        self.draft.signals.extend(default.signals);
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Choose {
                input: input.into_binding(),
                cases: ir_cases,
                default: default.ir,
                next: node_key.clone(),
            },
        );
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_foreach<A, B>(
        &mut self,
        key: &str,
        items: ValueRef<Vec<A>>,
        body: Region<A, B>,
        max_items: u32,
        max_concurrency: u32,
    ) -> Result<NodeRef<Vec<B>>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Foreach {
                items: items.binding,
                item_schema: A::schema_ref(),
                max_items,
                max_concurrency,
                body: body.ir,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(body.signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_saga<A, B>(
        &mut self,
        key: &str,
        input: ValueRef<A>,
        body: Region<A, B>,
    ) -> Result<NodeRef<B>>
    where
        A: DurablePayload,
        B: DurablePayload,
    {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Saga {
                input: input.binding,
                body: body.ir,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(body.signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_delay(&mut self, key: &str, duration: Duration) -> Result<NodeRef<()>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Delay {
                duration: Binding::literal(Value::String(format!(
                    "{}ms",
                    duration_to_millis(duration)?
                ))),
                next: node_key.clone(),
            },
        );
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_wait_until(&mut self, key: &str, at_ms: i64) -> Result<NodeRef<()>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::WaitUntil {
                at: Binding::literal(Value::Int(at_ms)),
                next: node_key.clone(),
            },
        );
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_complete<O: DurablePayload>(
        &mut self,
        key: &str,
        output: ValueRef<O>,
    ) -> Result<TerminalRef<O>> {
        self.same_scope_value(&output)?;
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Complete {
                output: output.binding,
            },
        );
        Ok(TerminalRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn declare_fail<O>(
        &mut self,
        key: &str,
        code: &str,
        message: &str,
    ) -> Result<TerminalRef<O>> {
        let node_key = self.insert_key(key)?;
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Fail {
                error: FailError {
                    code: code.to_owned(),
                    message: message.to_owned(),
                },
            },
        );
        Ok(TerminalRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }

    pub fn finish<O: DurablePayload>(self) -> Result<Region<I, O>> {
        let start = self
            .draft
            .start
            .ok_or_else(|| Error::invalid("region is missing start_at"))?;
        let ir = IrRegion {
            path: RegionPath::root(),
            input_schema: I::schema_ref(),
            output_schema: O::schema_ref(),
            start,
            nodes: self.draft.nodes,
        };
        Ok(Region {
            ir,
            signals: self.draft.signals,
            _i: PhantomData,
            _o: PhantomData,
        })
    }

    fn insert_key(&mut self, key: &str) -> Result<NodeKey> {
        let node_key = NodeKey::parse(key).map_err(Error::invalid)?;
        if self.draft.nodes.contains_key(node_key.as_str()) {
            return Err(Error::invalid(format!("duplicate node key {key}")));
        }
        Ok(node_key)
    }

    fn same_scope(&self, _node: &NodeKey, scope: ScopeId) -> Result<()> {
        if scope.0 != self.draft.scope.0 {
            return Err(Error::invalid("cross-region handle is not allowed"));
        }
        Ok(())
    }

    fn same_scope_value<T>(&self, value: &ValueRef<T>) -> Result<()> {
        if value.scope.0 != self.draft.scope.0 {
            return Err(Error::invalid("cross-region handle is not allowed"));
        }
        Ok(())
    }

    pub fn attach_compensation<O, U>(
        &mut self,
        node: &NodeRef<O>,
        compensator: &ActivityRef<O, U>,
    ) -> Result<()>
    where
        O: DurablePayload,
        U: DurablePayload,
    {
        self.set_compensation(
            &node.key,
            Compensation::Activity {
                activity: compensator.key.clone(),
                input: Binding::from_ref(crate::binding::Reference::ForwardOutput),
                timeout_ms: Some(duration_to_millis(COMPENSATION_ATTEMPT_TIMEOUT)?),
                retry: None,
            },
        )
    }

    fn set_compensation(&mut self, key: &NodeKey, compensation: Compensation) -> Result<()> {
        match self.draft.nodes.get_mut(key.as_str()) {
            Some(Node::Activity {
                compensation: slot, ..
            }) => {
                *slot = Some(compensation);
                Ok(())
            }
            _ => Err(Error::invalid(
                "compensation must attach to an activity node",
            )),
        }
    }

    pub fn declare_parallel2<A, B, IA, IB>(
        &mut self,
        key: &str,
        left: Branch<IA, A>,
        right: Branch<IB, B>,
    ) -> Result<NodeRef<(A, B)>>
    where
        A: DurablePayload,
        B: DurablePayload,
        IA: DurablePayload,
        IB: DurablePayload,
    {
        self.declare_parallel(key, (left, right))
    }

    /// Declare a parallel node; the tuple type witnesses the ordered output schema.
    pub fn declare_parallel<B: ParallelBranches>(
        &mut self,
        key: &str,
        branches: B,
    ) -> Result<NodeRef<B::Output>> {
        let node_key = self.insert_key(key)?;
        let (branches, signals) = branches.into_parts();
        let mut names = BTreeSet::new();
        for branch in &branches {
            if !valid_ascii_name(&branch.name) || !names.insert(branch.name.clone()) {
                return Err(Error::invalid(format!(
                    "invalid or duplicate branch name {}",
                    branch.name
                )));
            }
        }
        self.draft.nodes.insert(
            node_key.as_str().to_owned(),
            Node::Parallel {
                branches,
                next: node_key.clone(),
            },
        );
        self.draft.signals.extend(signals);
        Ok(NodeRef {
            key: node_key,
            scope: self.draft.scope,
            _o: PhantomData,
        })
    }
}

impl<I: DurablePayload> Default for RegionGraphBuilder<I> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WorkflowBuilder<I, O> {
    name: String,
    version: u32,
    root: Region<I, O>,
}

impl<I: DurablePayload, O: DurablePayload> WorkflowBuilder<I, O> {
    pub fn new(name: impl Into<String>, version: u32, root: Region<I, O>) -> Self {
        Self {
            name: name.into(),
            version,
            root,
        }
    }

    pub fn build(self, catalog: &Catalog) -> Result<Definition> {
        catalog.require_schema(&I::schema_ref())?;
        catalog.require_schema(&O::schema_ref())?;
        let mut definition = Definition {
            format_version: FORMAT_VERSION,
            dsl: DSL.to_owned(),
            id: self.name,
            version: self.version,
            input_schema: I::schema_ref(),
            output_schema: O::schema_ref(),
            signals: BTreeMap::new(),
            run_timeout_ms: None,
            root: self.root.ir,
            digest: Digest(String::new()),
        };
        definition.signals = self.root.signals;
        validate_definition(&definition, catalog)?;
        definition.digest = digest_of(&definition)?;
        Ok(definition)
    }
}
