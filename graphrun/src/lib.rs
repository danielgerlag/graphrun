//! Embedded workflow engine.
//!
//! Open [`Engine::local`] on a directory in your process. YAML and a typed
//! builder compile to one IR. This crate does not start a server.
//!
//! This release runs built-in fixture handlers for catalog names such as
//! `counter.increment`. You cannot register your own activity bodies yet.
//! Unknown names echo their input.
//!
//! Graphs are YAML or [`flow::workflow`]. See the crate README for a paste-and-run example.

#![allow(clippy::too_many_arguments)]
#![allow(clippy::collapsible_if)]

pub mod binding;
pub mod builder;
pub mod catalog;
pub mod client;
pub mod cluster;
pub mod compiler;
pub mod domain;
pub mod engine;
pub mod error;
pub mod flow;
#[doc(hidden)]
pub mod generated;
pub mod ids;
pub mod ir;
pub mod limits;
pub mod policy;
pub mod provider;
#[doc(hidden)]
pub mod rpc;
pub mod schema;
#[doc(hidden)]
pub mod storage;
pub mod time;
pub mod tls;
pub mod value;
#[doc(hidden)]
pub mod write;
pub mod yaml;

pub use builder::{
    ActivityRef, Branch, Case, EntryPort, ExitPort, InputMapping, NodeRef, Region, RegionBuilder,
    RegionGraphBuilder, SignalRef, TerminalRef, TimedWaitRef, ValueRef, WorkflowBuilder,
};
pub use catalog::Catalog;
pub use client::GrpcClient;
pub use cluster::MemberConfig;
pub use compiler::compile_yaml;
pub use domain::{State, reconstruct, run_events, run_output};
pub use engine::{
    ControlRequest, ControlResponse, Engine, LedgerEntry, connect_control, ledger_get, replay,
};
pub use error::{Error, ErrorKind, Result};
pub use flow::{region, workflow};
pub use ids::{EventId, RunId};
pub use ir::Definition;
pub use schema::{DurablePayload, SchemaRef};
pub use tls::{CertificateAuthority, TlsMaterial, generate_ca, issue_node};
pub use value::Value;
