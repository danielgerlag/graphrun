//! Durable workflow engine.
//!
//! YAML and a typed Rust builder compile to one IR. [`Engine::local`] is one
//! Raft member on a redb file. A cluster uses the same write path.
//!
//! ```no_run
//! use graphrun::{Catalog, Engine, Value};
//! use std::time::Duration;
//!
//! # async fn run() -> graphrun::Result<()> {
//! let catalog = Catalog::from_json(br#"{"format":"graphrun.catalog/v1"}"#)?;
//! let engine = Engine::local("/tmp/graphrun-demo").await?;
//! let run = engine
//!     .start_yaml("dsl: graphrun/v1\nid: empty\nversion: 1\ninput_schema: unit/v1\noutput_schema: unit/v1\nstart: finish\nnodes:\n  finish:\n    kind: complete\n    output: {from: workflow.input}\n", &catalog, Value::Null)
//!     .await?;
//! let _output = engine.wait_terminal(run, Duration::from_secs(10)).await?;
//! engine.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the crate README for a reserve-then-charge example and the CLI.

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
pub mod generated;
pub mod ids;
pub mod ir;
pub mod limits;
pub mod policy;
pub mod provider;
pub mod rpc;
pub mod schema;
pub mod storage;
pub mod time;
pub mod tls;
pub mod value;
pub mod write;
pub mod yaml;

pub use builder::{
    ActivityRef, Branch, EntryPort, ExitPort, InputMapping, NodeRef, Region, RegionBuilder,
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
pub use ids::{EventId, RunId};
pub use ir::Definition;
pub use schema::{DurablePayload, SchemaRef};
pub use tls::{CertificateAuthority, TlsMaterial, generate_ca, issue_node};
pub use value::Value;
