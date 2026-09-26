//! Linear fluent graph builder.
//!
//! [`workflow`] starts a definition. [`region`] starts a nested body.
//! First step uses the run or region input; later steps use the previous
//! output. The published graph is still data.

use crate::binding::{Binding, Condition};
use crate::builder::{
    ActivityRef, Branch, Case, NodeRef, ParallelBranches, Region, RegionBuilder,
    RegionGraphBuilder, SignalRef, TimedWaitRef, ValueRef, WorkflowBuilder,
};
use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ir::Definition;
use crate::schema::DurablePayload;
use crate::value::Value;
use std::time::Duration;

/// Nested region. First activity uses `scope.input`. Completes as node `done`.
pub fn region<I: DurablePayload>() -> Sequence<I, I> {
    let inner = RegionBuilder::new();
    let last = inner.input();
    Sequence {
        inner,
        last,
        last_activity: None,
    }
}

/// Root workflow named `id` at version 1. First activity uses `workflow.input`.
/// Completes as node `finish`.
///
/// ```
/// use graphrun::{payload, workflow, Catalog};
/// #[derive(Clone, serde::Serialize, serde::Deserialize)]
/// struct Counter { value: i64 }
/// payload!(Counter, "counter");
/// let catalog = Catalog::from_json(br#"{
///   "format": "graphrun.catalog/v1",
///   "schemas": {
///     "counter/v1": {
///       "type": "object",
///       "required": ["value"],
///       "additionalProperties": false,
///       "properties": {"value": {"type": "integer"}}
///     }
///   },
///   "activities": [{
///     "name": "counter.increment",
///     "version": 1,
///     "input_schema": "counter/v1",
///     "output_schema": "counter/v1",
///     "execution": "async",
///     "effects": "pure",
///     "recovery": "RetrySafe",
///     "error_codes": []
///   }]
/// }"#).unwrap();
/// let inc = catalog.activity_v1::<Counter, Counter>("counter.increment").unwrap();
/// let def = workflow::<Counter>("hello")
///     .activity("inc", &inc)
///     .unwrap()
///     .finish(&catalog)
///     .unwrap();
/// assert_eq!(def.id, "hello");
/// ```
pub fn workflow<I: DurablePayload>(id: impl Into<String>) -> Workflow<I, I> {
    let inner = RegionBuilder::new();
    let last = inner.workflow_input();
    Workflow {
        name: id.into(),
        seq: Sequence {
            inner,
            last,
            last_activity: None,
        },
    }
}

pub struct Sequence<I, Cur> {
    inner: RegionBuilder<I>,
    last: ValueRef<Cur>,
    last_activity: Option<NodeRef<Cur>>,
}

pub struct Workflow<I, Cur> {
    name: String,
    seq: Sequence<I, Cur>,
}

impl<I: DurablePayload, Cur: DurablePayload> Sequence<I, Cur> {
    pub fn activity<O: DurablePayload>(
        mut self,
        key: &str,
        activity: &ActivityRef<Cur, O>,
    ) -> Result<Sequence<I, O>> {
        let node = self.inner.activity(key, activity, self.last)?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: Some(node),
        })
    }

    pub fn compensate<U: DurablePayload>(mut self, activity: &ActivityRef<Cur, U>) -> Result<Self> {
        let node = self
            .last_activity
            .as_ref()
            .ok_or_else(|| Error::invalid("compensate must follow an activity"))?;
        self.inner.compensate(node, activity)?;
        Ok(self)
    }

    pub fn wait_signal<T: DurablePayload>(
        mut self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: &str,
    ) -> Result<Sequence<I, T>> {
        let corr = self.inner.literal(correlation.to_owned())?;
        let node = self.inner.wait_signal(key, signal, corr)?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: None,
        })
    }

    pub fn while_lt(
        mut self,
        key: &str,
        path: &str,
        value: i64,
        body: Region<Cur, Cur>,
        max_iterations: u32,
    ) -> Result<Self> {
        let node = self.inner.while_loop(
            key,
            self.last.clone(),
            Condition::lt_loop(path, value),
            body,
            max_iterations,
        )?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: None,
        })
    }

    pub fn repeat(
        mut self,
        key: &str,
        count: i64,
        body: Region<Cur, Cur>,
        max_iterations: u32,
    ) -> Result<Self> {
        let n = self.inner.literal(count)?;
        let node = self
            .inner
            .repeat(key, n, self.last.clone(), body, max_iterations)?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: None,
        })
    }

    pub fn saga<B: DurablePayload>(
        mut self,
        key: &str,
        body: Region<Cur, B>,
    ) -> Result<Sequence<I, B>> {
        let node = self.inner.saga(key, self.last.clone(), body)?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: None,
        })
    }

    pub fn choose(self, key: impl Into<String>) -> Choose<I, Cur, Cur> {
        Choose {
            name: None,
            seq: self,
            key: key.into(),
            cases: Vec::new(),
        }
    }

    pub fn parallel(self, key: impl Into<String>) -> ParallelStart<I, Cur> {
        ParallelStart {
            name: None,
            seq: self,
            key: key.into(),
        }
    }

    pub fn fail<O: DurablePayload>(
        self,
        key: &str,
        code: &str,
        message: &str,
    ) -> Result<Region<I, O>> {
        self.inner.fail(key, code, message)
    }

    pub fn complete(self, key: &str) -> Result<Region<I, Cur>> {
        self.inner.complete(key, self.last)
    }

    /// Complete this region as node `done`.
    pub fn finish(self) -> Result<Region<I, Cur>> {
        self.complete("done")
    }
}

impl<I: DurablePayload, A: DurablePayload> Sequence<I, Vec<A>> {
    pub fn foreach<B: DurablePayload>(
        mut self,
        key: &str,
        body: Region<A, B>,
        max_items: u32,
        max_concurrency: u32,
    ) -> Result<Sequence<I, Vec<B>>> {
        let node = self
            .inner
            .foreach(key, self.last.clone(), body, max_items, max_concurrency)?;
        Ok(Sequence {
            inner: self.inner,
            last: node.output(),
            last_activity: None,
        })
    }
}

impl<I: DurablePayload, Cur: DurablePayload> Workflow<I, Cur> {
    pub fn activity<O: DurablePayload>(
        self,
        key: &str,
        activity: &ActivityRef<Cur, O>,
    ) -> Result<Workflow<I, O>> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.activity(key, activity)?,
        })
    }

    pub fn compensate<U: DurablePayload>(self, activity: &ActivityRef<Cur, U>) -> Result<Self> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.compensate(activity)?,
        })
    }

    pub fn wait_signal<T: DurablePayload>(
        self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: &str,
    ) -> Result<Workflow<I, T>> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.wait_signal(key, signal, correlation)?,
        })
    }

    pub fn while_lt(
        self,
        key: &str,
        path: &str,
        value: i64,
        body: Region<Cur, Cur>,
        max_iterations: u32,
    ) -> Result<Self> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.while_lt(key, path, value, body, max_iterations)?,
        })
    }

    pub fn repeat(
        self,
        key: &str,
        count: i64,
        body: Region<Cur, Cur>,
        max_iterations: u32,
    ) -> Result<Self> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.repeat(key, count, body, max_iterations)?,
        })
    }

    pub fn saga<B: DurablePayload>(
        self,
        key: &str,
        body: Region<Cur, B>,
    ) -> Result<Workflow<I, B>> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.saga(key, body)?,
        })
    }

    pub fn choose(self, key: impl Into<String>) -> Choose<I, Cur, Cur> {
        let mut choose = self.seq.choose(key);
        choose.name = Some(self.name);
        choose
    }

    pub fn parallel(self, key: impl Into<String>) -> ParallelStart<I, Cur> {
        let mut parallel = self.seq.parallel(key);
        parallel.name = Some(self.name);
        parallel
    }

    /// Complete as node `finish` and compile.
    pub fn finish(self, catalog: &Catalog) -> Result<Definition> {
        let region = self.seq.complete("finish")?;
        WorkflowBuilder::new(self.name, 1, region).build(catalog)
    }
}

impl<I: DurablePayload> Workflow<I, I> {
    pub fn timed_wait<T: DurablePayload>(
        self,
        key: &str,
        signal: &SignalRef<T>,
        correlation: &str,
        timeout: Duration,
    ) -> Result<TimedWaitFlow<I, T>> {
        let mut graph = RegionGraphBuilder::<I>::new();
        let input = graph.workflow_input();
        let corr = graph.literal(correlation.to_owned())?;
        let wait = graph.declare_timed_wait(key, signal, corr, timeout)?;
        Ok(TimedWaitFlow {
            name: self.name,
            graph,
            wait,
            input,
        })
    }
}

impl<I: DurablePayload, A: DurablePayload> Workflow<I, Vec<A>> {
    pub fn foreach<B: DurablePayload>(
        self,
        key: &str,
        body: Region<A, B>,
        max_items: u32,
        max_concurrency: u32,
    ) -> Result<Workflow<I, Vec<B>>> {
        Ok(Workflow {
            name: self.name,
            seq: self.seq.foreach(key, body, max_items, max_concurrency)?,
        })
    }
}

pub struct Choose<I, A, B> {
    name: Option<String>,
    seq: Sequence<I, A>,
    key: String,
    cases: Vec<Case<A, B>>,
}

impl<I: DurablePayload, A: DurablePayload, B: DurablePayload> Choose<I, A, B> {
    pub fn when(
        mut self,
        case: impl Into<String>,
        condition: Condition,
        body: Region<A, B>,
    ) -> Self {
        self.cases.push(Case {
            name: case.into(),
            when: condition,
            body,
        });
        self
    }

    pub fn when_eq(
        self,
        case: impl Into<String>,
        path: &str,
        value: impl Into<Value>,
        body: Region<A, B>,
    ) -> Self {
        self.when(case, Condition::eq_input(path, value.into()), body)
    }

    pub fn when_true(self, case: impl Into<String>, path: &str, body: Region<A, B>) -> Self {
        self.when(case, Condition::flag_true(path), body)
    }

    pub fn otherwise(self, default: Region<A, B>) -> Result<AfterChoose<I, B>> {
        let workflow_name = self.name;
        let mut seq = self.seq;
        let node = seq.inner.choose(&self.key, seq.last, self.cases, default)?;
        let seq = Sequence {
            inner: seq.inner,
            last: node.output(),
            last_activity: None,
        };
        Ok(AfterChoose {
            name: workflow_name,
            seq,
        })
    }
}

pub struct AfterChoose<I, Cur> {
    name: Option<String>,
    seq: Sequence<I, Cur>,
}

impl<I: DurablePayload, Cur: DurablePayload> AfterChoose<I, Cur> {
    pub fn finish_region(self) -> Result<Region<I, Cur>> {
        self.seq.finish()
    }

    pub fn complete(self, key: &str) -> Result<Region<I, Cur>> {
        self.seq.complete(key)
    }

    pub fn finish(self, catalog: &Catalog) -> Result<Definition> {
        let name = self.name.ok_or_else(|| {
            Error::invalid("choose.otherwise().finish(catalog) needs workflow(..)")
        })?;
        let region = self.seq.complete("finish")?;
        WorkflowBuilder::new(name, 1, region).build(catalog)
    }

    pub fn into_sequence(self) -> Sequence<I, Cur> {
        self.seq
    }
}

pub struct ParallelStart<I, Cur> {
    name: Option<String>,
    seq: Sequence<I, Cur>,
    key: String,
}

mod parallel_bodies_sealed {
    pub trait Sealed {}
}

/// An ordered tuple of 1 to 16 named regions, each accepting the current
/// sequence value and producing its own output type.
///
/// ```compile_fail
/// use graphrun::{flow::region, builder::Region};
/// fn wrong_input(body: Region<i64, i64>) {
///     let _ = region::<String>().parallel("p").branches((("wrong", body),));
/// }
/// ```
pub trait ParallelBodies<Cur: DurablePayload>: parallel_bodies_sealed::Sealed {
    type Output: DurablePayload;
    type Branches: ParallelBranches<Output = Self::Output>;

    #[doc(hidden)]
    fn bind(self, input: Binding) -> Self::Branches;
}

macro_rules! impl_parallel_bodies {
    ($($name:ident $output:ident $branch:ident),+) => {
        impl<Cur: DurablePayload, $($name: Into<String>, $output: DurablePayload),+>
            parallel_bodies_sealed::Sealed for ($(($name, Region<Cur, $output>),)+) {}

        impl<Cur: DurablePayload, $($name: Into<String>, $output: DurablePayload),+>
            ParallelBodies<Cur> for ($(($name, Region<Cur, $output>),)+)
        {
            type Output = ($($output,)+);
            type Branches = ($(Branch<Cur, $output>,)+);

            fn bind(self, input: Binding) -> Self::Branches {
                let ($($branch,)+) = self;
                ($(Branch {
                    name: $branch.0.into(),
                    input: input.clone(),
                    body: $branch.1,
                },)+)
            }
        }
    };
}

impl_parallel_bodies!(N0 O0 b0);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10, N11 O11 b11);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10, N11 O11 b11, N12 O12 b12);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10, N11 O11 b11, N12 O12 b12, N13 O13 b13);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10, N11 O11 b11, N12 O12 b12, N13 O13 b13, N14 O14 b14);
impl_parallel_bodies!(N0 O0 b0, N1 O1 b1, N2 O2 b2, N3 O3 b3, N4 O4 b4, N5 O5 b5, N6 O6 b6, N7 O7 b7, N8 O8 b8, N9 O9 b9, N10 O10 b10, N11 O11 b11, N12 O12 b12, N13 O13 b13, N14 O14 b14, N15 O15 b15);

impl<I: DurablePayload, Cur: DurablePayload> ParallelStart<I, Cur> {
    /// Compose 1 to 16 named regions; tuple position determines output position.
    pub fn branches<B: ParallelBodies<Cur>>(
        self,
        bodies: B,
    ) -> Result<AfterParallel<I, B::Output>> {
        let mut seq = self.seq;
        let branches = bodies.bind(seq.last.binding().clone());
        let node = seq.inner.parallel(&self.key, branches)?;
        Ok(AfterParallel {
            name: self.name,
            seq: Sequence {
                inner: seq.inner,
                last: node.output(),
                last_activity: None,
            },
        })
    }

    pub fn branch<A: DurablePayload>(
        self,
        name: impl Into<String>,
        body: Region<Cur, A>,
    ) -> ParallelOne<I, Cur, A> {
        ParallelOne {
            workflow_name: self.name,
            seq: self.seq,
            key: self.key,
            left_name: name.into(),
            left: body,
        }
    }
}

pub struct ParallelOne<I, Cur, A> {
    workflow_name: Option<String>,
    seq: Sequence<I, Cur>,
    key: String,
    left_name: String,
    left: Region<Cur, A>,
}

impl<I: DurablePayload, Cur: DurablePayload, A: DurablePayload> ParallelOne<I, Cur, A> {
    pub fn branch<B: DurablePayload>(
        self,
        name: impl Into<String>,
        body: Region<Cur, B>,
    ) -> Result<AfterParallel<I, (A, B)>> {
        ParallelStart {
            name: self.workflow_name,
            seq: self.seq,
            key: self.key,
        }
        .branches(((self.left_name, self.left), (name.into(), body)))
    }
}

pub struct AfterParallel<I, Cur> {
    name: Option<String>,
    seq: Sequence<I, Cur>,
}

impl<I: DurablePayload, Cur: DurablePayload> AfterParallel<I, Cur> {
    pub fn finish_region(self) -> Result<Region<I, Cur>> {
        self.seq.finish()
    }

    pub fn complete(self, key: &str) -> Result<Region<I, Cur>> {
        self.seq.complete(key)
    }

    pub fn into_sequence(self) -> Sequence<I, Cur> {
        self.seq
    }

    pub fn finish(self, catalog: &Catalog) -> Result<Definition> {
        let name = self
            .name
            .ok_or_else(|| Error::invalid("parallel.finish(catalog) needs workflow(..)"))?;
        let region = self.seq.complete("finish")?;
        WorkflowBuilder::new(name, 1, region).build(catalog)
    }
}

pub struct TimedWaitFlow<I, T> {
    name: String,
    graph: RegionGraphBuilder<I>,
    wait: TimedWaitRef<T>,
    input: ValueRef<I>,
}

impl<I: DurablePayload, T: DurablePayload> TimedWaitFlow<I, T> {
    pub fn on_timeout<R: DurablePayload>(
        mut self,
        key: &str,
        activity: &ActivityRef<I, R>,
    ) -> Result<TimedWaitDone<I>> {
        let recover = self
            .graph
            .declare_activity(key, activity, self.input.clone())?;
        let finish = self.graph.declare_complete::<I>("finish", self.input)?;
        self.graph.start_at(self.wait.entry())?;
        self.graph
            .connect(self.wait.success_port(), finish.entry())?;
        self.graph
            .connect(self.wait.timeout_port(), recover.entry())?;
        self.graph.connect(recover.exit(), finish.entry())?;
        Ok(TimedWaitDone {
            name: self.name,
            graph: self.graph,
        })
    }
}

pub struct TimedWaitDone<I> {
    name: String,
    graph: RegionGraphBuilder<I>,
}

impl<I: DurablePayload> TimedWaitDone<I> {
    pub fn finish(self, catalog: &Catalog) -> Result<Definition> {
        let region = self.graph.finish::<I>()?;
        WorkflowBuilder::new(self.name, 1, region).build(catalog)
    }
}
