use crate::binding::{Binding, Condition};
use crate::ids::{ActivityKey, NodeKey};
use crate::policy::RetryPolicy;
use crate::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DSL: &str = "graphrun/v1";
pub const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathSeg {
    Root,
    Node(String),
    Body,
    Branch(String),
    Case(String),
    Default,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionPath(pub Vec<PathSeg>);

impl RegionPath {
    pub fn root() -> Self {
        Self(vec![PathSeg::Root])
    }

    pub fn child(&self, seg: PathSeg) -> Self {
        let mut next = self.0.clone();
        next.push(seg);
        Self(next)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalDecl {
    pub schema: SchemaRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    pub format_version: u32,
    pub dsl: String,
    pub id: String,
    pub version: u32,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub signals: BTreeMap<String, SignalDecl>,
    pub run_timeout_ms: Option<u64>,
    pub root: Region,
    pub digest: Digest,
}

impl Definition {
    pub fn activity_keys(&self) -> Vec<crate::ids::ActivityKey> {
        let mut keys = Vec::new();
        collect_region_activities(&self.root, &mut keys);
        keys.sort_by(|a, b| a.name.cmp(&b.name).then(a.version.cmp(&b.version)));
        keys.dedup();
        keys
    }
}

fn collect_region_activities(region: &Region, keys: &mut Vec<crate::ids::ActivityKey>) {
    for node in region.nodes.values() {
        match node {
            Node::Activity {
                activity,
                compensation,
                ..
            } => {
                keys.push(activity.clone());
                if let Some(Compensation::Activity {
                    activity: handler, ..
                }) = compensation
                {
                    keys.push(handler.clone());
                }
            }
            Node::Choose { cases, default, .. } => {
                for case in cases {
                    collect_region_activities(&case.body, keys);
                }
                collect_region_activities(default, keys);
            }
            Node::While { body, .. }
            | Node::DoWhile { body, .. }
            | Node::Repeat { body, .. }
            | Node::Foreach { body, .. }
            | Node::Saga { body, .. } => collect_region_activities(body, keys),
            Node::Parallel { branches, .. } => {
                for branch in branches {
                    collect_region_activities(&branch.body, keys);
                }
            }
            Node::Delay { .. }
            | Node::WaitUntil { .. }
            | Node::WaitSignal { .. }
            | Node::Complete { .. }
            | Node::Fail { .. } => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Region {
    pub path: RegionPath,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub start: NodeKey,
    pub nodes: BTreeMap<String, Node>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumeFrom {
    Buffered,
    AfterActivation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Compensation {
    Activity {
        activity: ActivityKey,
        input: Binding,
        timeout_ms: Option<u64>,
        retry: Option<RetryPolicy>,
    },
    Irreversible {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailError {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChooseCase {
    pub name: String,
    pub when: Condition,
    pub body: Region,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParallelBranch {
    pub name: String,
    pub input: Binding,
    pub body: Region,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
    Activity {
        activity: ActivityKey,
        input: Binding,
        timeout_ms: Option<u64>,
        retry: RetryPolicy,
        compensation: Option<Compensation>,
        next: NodeKey,
    },
    Choose {
        input: Binding,
        cases: Vec<ChooseCase>,
        default: Region,
        next: NodeKey,
    },
    Delay {
        duration: Binding,
        next: NodeKey,
    },
    WaitUntil {
        at: Binding,
        next: NodeKey,
    },
    WaitSignal {
        signal: String,
        key: Binding,
        timeout_ms: Option<u64>,
        consume_from: ConsumeFrom,
        next: NodeKey,
        on_timeout: Option<NodeKey>,
    },
    Complete {
        output: Binding,
    },
    Fail {
        error: FailError,
    },
    While {
        state: Binding,
        state_schema: SchemaRef,
        condition: Condition,
        max_iterations: u32,
        body: Region,
        next: NodeKey,
    },
    DoWhile {
        state: Binding,
        state_schema: SchemaRef,
        condition: Condition,
        max_iterations: u32,
        body: Region,
        next: NodeKey,
    },
    Repeat {
        count: Binding,
        state: Binding,
        state_schema: SchemaRef,
        max_iterations: u32,
        body: Region,
        next: NodeKey,
    },
    Foreach {
        items: Binding,
        item_schema: SchemaRef,
        max_items: u32,
        max_concurrency: u32,
        body: Region,
        next: NodeKey,
    },
    Parallel {
        branches: Vec<ParallelBranch>,
        next: NodeKey,
    },
    Saga {
        input: Binding,
        body: Region,
        next: NodeKey,
    },
}

impl Node {
    pub fn successors(&self) -> Vec<(&str, &NodeKey)> {
        match self {
            Self::Activity { next, .. }
            | Self::Choose { next, .. }
            | Self::Delay { next, .. }
            | Self::WaitUntil { next, .. }
            | Self::While { next, .. }
            | Self::DoWhile { next, .. }
            | Self::Repeat { next, .. }
            | Self::Foreach { next, .. }
            | Self::Parallel { next, .. }
            | Self::Saga { next, .. } => vec![("next", next)],
            Self::WaitSignal {
                next, on_timeout, ..
            } => {
                let mut edges = vec![("next", next)];
                if let Some(timeout) = on_timeout {
                    edges.push(("on_timeout", timeout));
                }
                edges
            }
            Self::Complete { .. } | Self::Fail { .. } => Vec::new(),
        }
    }

    pub fn child_regions(&self) -> Vec<(&str, &Region)> {
        match self {
            Self::Choose { cases, default, .. } => {
                let mut regions: Vec<(&str, &Region)> =
                    cases.iter().map(|case| ("case", &case.body)).collect();
                regions.push(("default", default));
                regions
            }
            Self::While { body, .. }
            | Self::DoWhile { body, .. }
            | Self::Repeat { body, .. }
            | Self::Foreach { body, .. }
            | Self::Saga { body, .. } => vec![("body", body)],
            Self::Parallel { branches, .. } => branches
                .iter()
                .map(|branch| ("branch", &branch.body))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub fn output_on_success_only(&self) -> bool {
        matches!(self, Self::WaitSignal { .. })
    }
}
