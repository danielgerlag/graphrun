//! Embedded durable workflow engine for Rust.
//!
//! Open [`Engine::local`] on a directory in your process. YAML and
//! [`workflow`] compile to one IR. This crate does not start a server.
//!
//! Register handlers with [`Engine::builder`]. Unregistered names fail.
//! [`Engine::local`] enables sample fixture handlers.
//!
//! See the crate README, `examples/hello_world.rs`, and `examples/order.rs`.

#![allow(clippy::too_many_arguments)]
#![allow(clippy::collapsible_if)]

pub mod binding;
pub mod builder;
pub mod catalog;
pub mod client;
mod clock;
pub mod cluster;
pub mod compiler;
pub mod domain;
pub mod engine;
pub mod error;
pub mod flow;
#[doc(hidden)]
pub mod generated;
pub mod handlers;
pub mod history;
pub mod ids;
pub mod ir;
pub mod limits;
pub mod policy;
pub mod provider;
pub mod publication;
#[doc(hidden)]
pub mod rpc;
pub mod schema;
#[doc(hidden)]
pub mod storage;
pub mod time;
pub mod tls;
pub mod value;
pub mod worker;
pub mod worker_contract;
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
    ControlRequest, ControlResponse, Engine, LedgerEntry, LocalBuilder, connect_control,
    ledger_get, replay,
};
pub use error::{Error, ErrorKind, Result};
pub use flow::{region, workflow};
pub use handlers::Handlers;
pub use history::{HistoryPage, RecordedEvent, reconstruct_at};
pub use ids::{EventId, RunId};
pub use ir::Definition;
pub use schema::{DurablePayload, SchemaRef};
pub use tls::{CertificateAuthority, TlsMaterial, generate_ca, issue_node};
pub use value::Value;
pub use worker::{ActivityError, HandlerContext, Observed, Worker, WorkerBuilder};
pub use worker_contract::WorkerCapability;
