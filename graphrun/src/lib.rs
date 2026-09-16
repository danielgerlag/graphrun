//! Graphrun durable workflow engine.

#![allow(clippy::too_many_arguments)]
#![allow(clippy::collapsible_if)]

pub mod binding;
pub mod builder;
pub mod catalog;
pub mod compiler;
pub mod domain;
pub mod error;
pub mod ids;
pub mod ir;
pub mod limits;
pub mod policy;
pub mod schema;
pub mod time;
pub mod value;
pub mod yaml;

pub use builder::{
    ActivityRef, Branch, EntryPort, ExitPort, InputMapping, NodeRef, Region, RegionBuilder,
    RegionGraphBuilder, SignalRef, TerminalRef, TimedWaitRef, ValueRef, WorkflowBuilder,
};
pub use catalog::Catalog;
pub use compiler::compile_yaml;
pub use error::{Error, ErrorKind, Result};
pub use ir::Definition;
pub use schema::{DurablePayload, SchemaRef};
