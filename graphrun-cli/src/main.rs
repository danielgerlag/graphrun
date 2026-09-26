use clap::{Args, Parser, Subcommand};
use graphrun::ids::CommandId;
use graphrun::storage::load_domain_readonly;
use graphrun::{
    Catalog, ControlRequest, Engine, EventId, GrpcClient, RunId, TlsMaterial, Value, compile_yaml,
    connect_control, replay,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "graphrun",
    version,
    about = "Optional operator CLI for the graphrun library"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Debug, Clone)]
struct ConnectArgs {
    #[arg(long)]
    local_dir: Option<PathBuf>,
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long)]
    ca: Option<PathBuf>,
    #[arg(long)]
    cert: Option<PathBuf>,
    #[arg(long = "tls-key", id = "tls_key")]
    key: Option<PathBuf>,
    #[arg(long)]
    server_name: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Validate {
        #[arg(long)]
        definition: PathBuf,
        #[arg(long)]
        catalog: PathBuf,
    },
    Publish {
        #[arg(long)]
        definition: Option<PathBuf>,
        #[arg(long)]
        catalog: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        catalog_version: u32,
        #[arg(long)]
        catalog_command_id: Option<String>,
        #[arg(long)]
        definition_command_id: Option<String>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    CommandResult {
        #[arg(long)]
        command_id: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Start {
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        definition: Option<PathBuf>,
        #[arg(long)]
        catalog: Option<PathBuf>,
        #[arg(long)]
        version: Option<u32>,
        #[arg(long)]
        start_key: Option<String>,
        #[arg(long)]
        command_id: Option<String>,
        #[arg(long)]
        input: PathBuf,
        #[command(flatten)]
        connect: ConnectArgs,
        #[arg(long, default_value_t = 60_000)]
        wait_ms: u64,
        #[arg(long, default_value_t = false)]
        no_wait: bool,
    },
    Signal {
        #[arg(long)]
        run: String,
        #[arg(long)]
        name: String,
        #[arg(long, id = "wait_key")]
        key: String,
        #[arg(long)]
        event_id: String,
        #[arg(long)]
        payload: PathBuf,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Cancel {
        #[arg(long)]
        run: String,
        #[arg(long)]
        reason: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Inspect {
        #[arg(long)]
        run: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    List {
        #[command(flatten)]
        connect: ConnectArgs,
    },
    History {
        #[arg(long)]
        run: String,
        #[arg(long, default_value_t = 0)]
        after_sequence: u64,
        #[arg(long, default_value_t = 100)]
        page_size: u32,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Replay {
        #[arg(long)]
        run: Option<String>,
        #[arg(long)]
        through_sequence: Option<u64>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Resolve {
        #[arg(long)]
        run: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long, default_value_t = false)]
        abandon: bool,
        #[arg(long, default_value_t = false)]
        ack: bool,
        #[arg(long)]
        blocked_input: Option<PathBuf>,
        #[arg(long)]
        forward: Option<String>,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },
    Backup {
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        local_dir: PathBuf,
    },
    Compact {
        #[arg(long)]
        local_dir: PathBuf,
    },
    Restore {
        #[arg(long)]
        from: PathBuf,
        #[arg(long)]
        local_dir: PathBuf,
        #[arg(long)]
        confirm: bool,
        #[arg(long)]
        reason: Option<String>,
    },
    Serve {
        #[arg(long)]
        local_dir: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterCommands {
    Health {
        #[command(flatten)]
        connect: ConnectArgs,
    },
    AcknowledgeClock {
        #[arg(long)]
        reason: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Join {
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        addr: String,
        #[arg(long = "peer-ca")]
        peer_ca: PathBuf,
        #[arg(long = "peer-cert")]
        peer_cert: PathBuf,
        #[arg(long = "peer-tls-key")]
        peer_key: PathBuf,
        #[arg(long = "peer-server-name")]
        peer_server_name: String,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Promote {
        #[arg(long)]
        node_id: u64,
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Remove {
        #[arg(long)]
        node_id: u64,
        #[command(flatten)]
        connect: ConnectArgs,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{}", serde_json::json!({"status":"error","message":err}));
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<(), String> {
    match Cli::parse().command {
        Commands::Validate {
            definition,
            catalog,
        } => {
            let digest = validate(&definition, &catalog)?;
            println!("{}", serde_json::json!({"status":"ok","digest":digest}));
            Ok(())
        }
        Commands::Start {
            workflow,
            definition,
            catalog,
            version,
            start_key,
            command_id,
            input,
            connect,
            wait_ms,
            no_wait,
        } => {
            let input = load_value(&input)?;
            let wait = if no_wait { None } else { Some(wait_ms) };
            if definition.is_none() && catalog.is_none() {
                let workflow =
                    workflow.ok_or_else(|| "published start requires --workflow".to_owned())?;
                let start_key =
                    start_key.ok_or_else(|| "published start requires --start-key".to_owned())?;
                let command_id = command_id.unwrap_or_else(|| CommandId::generate().to_hex());
                let id = CommandId::from_hex(&command_id)?;
                if let Some(mut client) = grpc_client(&connect).await? {
                    let run = client
                        .start_published_with_command(&workflow, version, &start_key, &input, id)
                        .await
                        .map_err(|err| err.to_string())?;
                    let body = if let Some(ms) = wait {
                        wait_grpc(&mut client, run, ms).await?
                    } else {
                        serde_json::json!({"run":run.to_hex(),"status":"started"})
                    };
                    println!("{}", with_command(body, &command_id));
                    return Ok(());
                }
                let local_dir = require_local(connect.local_dir)?;
                let body = dispatch(
                    &local_dir,
                    ControlRequest::StartPublished {
                        workflow,
                        version,
                        start_key,
                        input,
                        command_id: command_id.clone(),
                        wait_ms: wait,
                    },
                    true,
                )
                .await?;
                println!("{}", with_command(body, &command_id));
                return Ok(());
            }
            if version.is_some() || start_key.is_some() {
                return Err("inline start cannot use --version or --start-key".to_owned());
            }
            let yaml = load_workflow(workflow, definition)?;
            let catalog = load_catalog(
                &catalog.ok_or_else(|| "inline start requires --catalog".to_owned())?,
            )?;
            if let Some(mut client) = grpc_client(&connect).await? {
                let id = parse_command(command_id)?;
                let run = client
                    .start_with_command(&yaml, &catalog, &input, id)
                    .await
                    .map_err(|err| err.to_string())?;
                let body = if let Some(ms) = wait {
                    wait_grpc(&mut client, run, ms).await?
                } else {
                    serde_json::json!({"run": run.to_hex(), "status": "started"})
                };
                println!("{}", with_command(body, &id.to_hex()));
                return Ok(());
            }
            if command_id.is_some() {
                return Err(
                    "inline local start does not support --command-id; publish and use --start-key"
                        .to_owned(),
                );
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(
                &local_dir,
                ControlRequest::Start {
                    yaml,
                    catalog,
                    input,
                    wait_ms: wait,
                },
                true,
            )
            .await?;
            println!("{body}");
            Ok(())
        }
        Commands::Signal {
            run,
            name,
            key,
            event_id,
            payload,
            connect,
        } => {
            let payload = load_value(&payload)?;
            if let Some(mut client) = grpc_client(&connect).await? {
                let id = RunId::from_hex(&run)?;
                let event = EventId::from_hex(&event_id)?;
                client
                    .signal(id, event, &name, &key, &payload)
                    .await
                    .map_err(|err| err.to_string())?;
                println!("{}", serde_json::json!({"status":"ok"}));
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(
                &local_dir,
                ControlRequest::Signal {
                    run,
                    name,
                    key,
                    event_id,
                    payload,
                },
                false,
            )
            .await?;
            println!("{body}");
            Ok(())
        }
        Commands::Cancel {
            run,
            reason,
            connect,
        } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                client
                    .cancel(RunId::from_hex(&run)?, &reason)
                    .await
                    .map_err(|err| err.to_string())?;
                println!("{}", serde_json::json!({"status":"ok"}));
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::Cancel { run, reason }, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::Inspect { run, connect } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let view = client
                    .inspect(RunId::from_hex(&run)?)
                    .await
                    .map_err(|err| err.to_string())?;
                println!("{view}");
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::Inspect { run }, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::List { connect } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let view = client.list().await.map_err(|err| err.to_string())?;
                println!("{view}");
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::List, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::History {
            run,
            after_sequence,
            page_size,
            connect,
        } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let view = client
                    .history_page(RunId::from_hex(&run)?, after_sequence, page_size)
                    .await
                    .map_err(|err| err.to_string())?;
                println!(
                    "{}",
                    serde_json::to_value(view).map_err(|err| err.to_string())?
                );
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(
                &local_dir,
                ControlRequest::History {
                    run,
                    after_sequence,
                    page_size,
                },
                false,
            )
            .await?;
            println!("{body}");
            Ok(())
        }
        Commands::Replay {
            run,
            through_sequence,
            connect,
        } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let run = RunId::from_hex(
                    &run.ok_or_else(|| "remote replay requires --run".to_owned())?,
                )?;
                let through = match through_sequence {
                    Some(through) => through,
                    None => {
                        client
                            .history_page(run, 0, 1)
                            .await
                            .map_err(|err| err.to_string())?
                            .retained_through
                    }
                };
                let view = client
                    .reconstruct_at(run, through)
                    .await
                    .map_err(|err| err.to_string())?;
                println!("{view}");
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            if let Some(run) = run {
                let id = RunId::from_hex(&run)?;
                if local_dir.join("control.sock").exists() {
                    let through = match through_sequence {
                        Some(through) => through,
                        None => {
                            let page = dispatch(
                                &local_dir,
                                ControlRequest::History {
                                    run: run.clone(),
                                    after_sequence: 0,
                                    page_size: 1,
                                },
                                false,
                            )
                            .await?;
                            page["retained_through"]
                                .as_u64()
                                .ok_or_else(|| "missing retained history boundary".to_owned())?
                        }
                    };
                    let body = dispatch(
                        &local_dir,
                        ControlRequest::Replay {
                            run,
                            through_sequence: through,
                        },
                        false,
                    )
                    .await?;
                    println!("{body}");
                } else {
                    let state = load_domain_readonly(local_dir.join("member.redb"))
                        .map_err(|err| err.to_string())?;
                    let through = through_sequence
                        .unwrap_or_else(|| graphrun::run_events(&state, id).len() as u64);
                    let view = graphrun::history::replay_view(&state, id, through, read_time()?)
                        .map_err(|err| err.to_string())?;
                    println!("{view}");
                }
            } else {
                if through_sequence.is_some() {
                    return Err("--through-sequence requires --run".to_owned());
                }
                let state = replay(&local_dir).map_err(|err| err.to_string())?;
                println!(
                    "{}",
                    serde_json::json!({
                        "runs": state.runs.len(),
                        "events": state.history.values().map(Vec::len).sum::<usize>(),
                    })
                );
            }
            Ok(())
        }
        Commands::Publish {
            definition,
            catalog: catalog_path,
            catalog_version,
            catalog_command_id,
            definition_command_id,
            connect,
        } => {
            if definition.is_none() && catalog_path.is_none() {
                return Err("publish requires --catalog and/or --definition".to_owned());
            }
            let catalog = catalog_path.map(|path| load_catalog(&path)).transpose()?;
            let yaml = definition
                .map(|path| std::fs::read_to_string(path).map_err(|err| err.to_string()))
                .transpose()?;
            let mut results = Vec::new();
            if let Some(mut client) = grpc_client(&connect).await? {
                if let Some(catalog) = &catalog {
                    let id = parse_command(catalog_command_id.clone())?;
                    results.push(
                        serde_json::to_value(
                            client
                                .publish_catalog_with_command(catalog_version, catalog, id)
                                .await
                                .map_err(|err| err.to_string())?,
                        )
                        .map_err(|err| err.to_string())?,
                    );
                }
                if let Some(yaml) = &yaml {
                    let id = parse_command(definition_command_id.clone())?;
                    results.push(
                        serde_json::to_value(
                            client
                                .publish_definition_with_command(yaml, catalog_version, id)
                                .await
                                .map_err(|err| err.to_string())?,
                        )
                        .map_err(|err| err.to_string())?,
                    );
                }
            } else {
                let local_dir = require_local(connect.local_dir)?;
                if let Some(catalog) = catalog {
                    let id = parse_command(catalog_command_id)?;
                    results.push(
                        dispatch(
                            &local_dir,
                            ControlRequest::PublishCatalog {
                                version: catalog_version,
                                catalog,
                                command_id: id.to_hex(),
                            },
                            true,
                        )
                        .await?,
                    );
                }
                if let Some(yaml) = yaml {
                    let id = parse_command(definition_command_id)?;
                    results.push(
                        dispatch(
                            &local_dir,
                            ControlRequest::PublishDefinition {
                                yaml,
                                catalog_version,
                                command_id: id.to_hex(),
                            },
                            true,
                        )
                        .await?,
                    );
                }
            }
            println!("{}", serde_json::json!({"status":"ok","results":results}));
            Ok(())
        }
        Commands::CommandResult {
            command_id,
            connect,
        } => {
            let id = CommandId::from_hex(&command_id)?;
            if let Some(mut client) = grpc_client(&connect).await? {
                let receipt = client
                    .command_result(id)
                    .await
                    .map_err(|err| err.to_string())?;
                println!(
                    "{}",
                    serde_json::to_value(receipt).map_err(|err| err.to_string())?
                );
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let result = dispatch(
                &local_dir,
                ControlRequest::CommandResult { command_id },
                false,
            )
            .await?;
            println!("{result}");
            Ok(())
        }
        Commands::Backup { out, local_dir } => {
            Engine::backup(&local_dir, &out).map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::json!({"status":"ok","from": local_dir.display().to_string()})
            );
            Ok(())
        }
        Commands::Compact { local_dir } => {
            let outcome = graphrun::storage::compact_offline(local_dir.join("member.redb"))
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::json!({
                    "status": if outcome.complete { "ok" } else { "incomplete" },
                    "passes": outcome.passes,
                    "local_dir": local_dir.display().to_string(),
                })
            );
            if outcome.complete {
                Ok(())
            } else {
                Err("offline compaction stopped after 16 passes; rerun to finish".to_owned())
            }
        }
        Commands::Restore {
            from,
            local_dir,
            confirm,
            reason,
        } => {
            if !confirm {
                return Err("restore requires --confirm and --reason".to_owned());
            }
            let reason = reason.unwrap_or_default();
            if reason.is_empty() {
                return Err("restore requires --reason".to_owned());
            }
            Engine::restore(&from, &local_dir, &reason).map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::json!({"status":"ok","restored": local_dir.display().to_string()})
            );
            Ok(())
        }
        Commands::Serve { local_dir } => {
            let engine = Engine::local(&local_dir)
                .await
                .map_err(|err| err.to_string())?;
            #[cfg(unix)]
            {
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .map_err(|err| err.to_string())?;
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                tokio::signal::ctrl_c()
                    .await
                    .map_err(|err| err.to_string())?;
            }
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(())
        }
        Commands::Cluster {
            command: ClusterCommands::Health { connect },
        } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let view = client.list().await.map_err(|err| err.to_string())?;
                println!("{}", serde_json::json!({"status":"ok","runs": view}));
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::Health, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::Cluster {
            command: ClusterCommands::AcknowledgeClock { reason, connect },
        } => {
            if reason.trim().is_empty() {
                return Err("clock acknowledgement requires --reason".to_owned());
            }
            if let Some(mut client) = grpc_client(&connect).await? {
                client
                    .acknowledge_clock(&reason)
                    .await
                    .map_err(|err| err.to_string())?;
            } else {
                let local_dir = require_local(connect.local_dir)?;
                dispatch(
                    &local_dir,
                    ControlRequest::AcknowledgeClock { reason },
                    false,
                )
                .await?;
            }
            println!("{}", serde_json::json!({"status":"ok"}));
            Ok(())
        }
        Commands::Cluster {
            command:
                ClusterCommands::Join {
                    node_id,
                    addr,
                    peer_ca,
                    peer_cert,
                    peer_key,
                    peer_server_name,
                    connect,
                },
        } => {
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(
                &local_dir,
                ControlRequest::Join {
                    node_id,
                    addr,
                    ca_pem: std::fs::read_to_string(peer_ca).map_err(|err| err.to_string())?,
                    cert_pem: std::fs::read_to_string(peer_cert).map_err(|err| err.to_string())?,
                    key_pem: std::fs::read_to_string(peer_key).map_err(|err| err.to_string())?,
                    server_name: peer_server_name,
                },
                false,
            )
            .await?;
            println!("{body}");
            Ok(())
        }
        Commands::Cluster {
            command: ClusterCommands::Promote { node_id, connect },
        } => {
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::Promote { node_id }, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::Cluster {
            command: ClusterCommands::Remove { node_id, connect },
        } => {
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::Remove { node_id }, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::Resolve {
            run,
            reason,
            confirm,
            abandon,
            ack,
            blocked_input,
            forward,
            connect,
        } => {
            if reason.is_empty() {
                return Err("resolve requires --reason".to_owned());
            }
            let local_dir = require_local(connect.local_dir)?;
            let req = if ack {
                ControlRequest::Acknowledge { reason }
            } else if abandon {
                if !confirm {
                    return Err(
                        "compensation abandonment requires --confirm and --reason".to_owned()
                    );
                }
                ControlRequest::Abandon { run, reason }
            } else if let (Some(path), Some(forward)) = (blocked_input, forward) {
                ControlRequest::ResolveBlocked {
                    run,
                    forward,
                    input: load_value(&path)?,
                }
            } else {
                return Err(
                    "resolve requires --ack, --abandon --confirm, or --blocked-input with --forward"
                        .to_owned(),
                );
            };
            let body = dispatch(&local_dir, req, false).await?;
            println!("{body}");
            Ok(())
        }
    }
}

fn require_local(local_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    local_dir.ok_or_else(|| "--local-dir or --endpoint is required".to_owned())
}

fn parse_command(id: Option<String>) -> Result<CommandId, String> {
    id.map(|value| CommandId::from_hex(&value))
        .transpose()
        .map(|id| id.unwrap_or_else(CommandId::generate))
}

async fn grpc_client(connect: &ConnectArgs) -> Result<Option<GrpcClient>, String> {
    let Some(endpoint) = &connect.endpoint else {
        return Ok(None);
    };
    let ca = connect
        .ca
        .as_ref()
        .ok_or_else(|| "--ca is required with --endpoint".to_owned())?;
    let cert = connect
        .cert
        .as_ref()
        .ok_or_else(|| "--cert is required with --endpoint".to_owned())?;
    let key = connect
        .key
        .as_ref()
        .ok_or_else(|| "--tls-key is required with --endpoint".to_owned())?;
    let server_name = connect
        .server_name
        .clone()
        .ok_or_else(|| "--server-name is required with --endpoint".to_owned())?;
    let tls = TlsMaterial {
        ca_pem: std::fs::read_to_string(ca).map_err(|err| err.to_string())?,
        cert_pem: std::fs::read_to_string(cert).map_err(|err| err.to_string())?,
        key_pem: std::fs::read_to_string(key).map_err(|err| err.to_string())?,
        server_name,
    };
    GrpcClient::connect(endpoint, &tls)
        .await
        .map(Some)
        .map_err(|err| err.to_string())
}

fn load_workflow(workflow: Option<String>, definition: Option<PathBuf>) -> Result<String, String> {
    if let Some(path) = definition {
        return std::fs::read_to_string(path).map_err(|err| err.to_string());
    }
    let Some(workflow) = workflow else {
        return Err("start requires --definition or --workflow".to_owned());
    };
    let path = PathBuf::from(&workflow);
    if path.exists() {
        return std::fs::read_to_string(path).map_err(|err| err.to_string());
    }
    Err(format!("workflow {workflow} not found"))
}

fn load_catalog(path: &PathBuf) -> Result<Catalog, String> {
    let bytes = std::fs::read(path).map_err(|err| err.to_string())?;
    Catalog::from_json(&bytes).map_err(|err| err.to_string())
}

fn load_value(path: &PathBuf) -> Result<Value, String> {
    let text = std::fs::read_to_string(path).map_err(|err| err.to_string())?;
    serde_json::from_str(&text).map_err(|err| err.to_string())
}

fn read_time() -> Result<graphrun::time::EngineTime, String> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .as_millis() as u64;
    Ok(graphrun::time::EngineTime::from_millis(millis))
}

fn validate(definition: &PathBuf, catalog: &PathBuf) -> Result<String, String> {
    let catalog = load_catalog(catalog)?;
    let text = std::fs::read_to_string(definition).map_err(|err| err.to_string())?;
    let compiled = compile_yaml(&text, &catalog).map_err(|err| err.to_string())?;
    Ok(compiled.digest.0)
}

fn with_command(mut body: serde_json::Value, id: &str) -> serde_json::Value {
    body["command_id"] = serde_json::Value::String(id.to_owned());
    body
}

async fn wait_grpc(
    client: &mut GrpcClient,
    run: RunId,
    wait_ms: u64,
) -> Result<serde_json::Value, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    loop {
        let view = client.inspect(run).await.map_err(|err| err.to_string())?;
        match view["status"].as_str() {
            Some("succeeded") => {
                return Ok(
                    serde_json::json!({"run":run.to_hex(),"status":"succeeded","output":view["output"]}),
                );
            }
            Some("failed") => {
                return Err(format!("run {} failed: {}", run.to_hex(), view["error"]));
            }
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for run {}; outcome remains pending",
                run.to_hex()
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn dispatch(
    local_dir: &PathBuf,
    req: ControlRequest,
    start_if_needed: bool,
) -> Result<serde_json::Value, String> {
    let sock = local_dir.join("control.sock");
    if sock.exists() {
        let uncertain_id = match &req {
            ControlRequest::StartPublished { command_id, .. }
            | ControlRequest::PublishCatalog { command_id, .. }
            | ControlRequest::PublishDefinition { command_id, .. } => Some(command_id.clone()),
            _ => None,
        };
        let resp = tokio::time::timeout(Duration::from_secs(5), connect_control(&sock, req))
            .await
            .map_err(|_| "local control deadline exceeded".to_owned())
            .and_then(|result| result.map_err(|err| err.to_string()))
            .map_err(|err| match uncertain_id {
                Some(id) => format!("unknown outcome for command {id}; retry/query same ID: {err}"),
                None => err,
            })?;
        return response_body(resp);
    }
    match &req {
        ControlRequest::History {
            run,
            after_sequence,
            page_size,
        } => {
            let state = load_domain_readonly(local_dir.join("member.redb"))
                .map_err(|err| err.to_string())?;
            let page = graphrun::history::page(
                &state,
                RunId::from_hex(run)?,
                *after_sequence,
                *page_size,
                read_time()?,
            )
            .map_err(|err| err.to_string())?;
            return serde_json::to_value(page).map_err(|err| err.to_string());
        }
        ControlRequest::Replay {
            run,
            through_sequence,
        } => {
            let state = load_domain_readonly(local_dir.join("member.redb"))
                .map_err(|err| err.to_string())?;
            return graphrun::history::replay_view(
                &state,
                RunId::from_hex(run)?,
                *through_sequence,
                read_time()?,
            )
            .map_err(|err| err.to_string());
        }
        _ => {}
    }
    if !start_if_needed
        && matches!(
            req,
            ControlRequest::Inspect { .. } | ControlRequest::List | ControlRequest::Health
        )
    {
        let engine = Engine::local(local_dir)
            .await
            .map_err(|err| err.to_string())?;
        let resp = match req {
            ControlRequest::Inspect { run } => {
                let id = RunId::from_hex(&run)?;
                engine
                    .inspect_json(id)
                    .await
                    .map_err(|err| err.to_string())?
            }
            ControlRequest::List => engine.list().await.map_err(|err| err.to_string())?,
            ControlRequest::Health => engine.health().await,
            _ => unreachable!(),
        };
        engine.shutdown().await.map_err(|err| err.to_string())?;
        return Ok(resp);
    }
    match req {
        ControlRequest::PublishCatalog {
            version,
            catalog,
            command_id,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let result = engine
                .publish_catalog_with_command(version, catalog, CommandId::from_hex(&command_id)?)
                .await
                .map_err(|err| err.to_string());
            engine.shutdown().await.map_err(|err| err.to_string())?;
            serde_json::to_value(result?).map_err(|err| err.to_string())
        }
        ControlRequest::PublishDefinition {
            yaml,
            catalog_version,
            command_id,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let result = engine
                .publish_definition_yaml_with_command(
                    &yaml,
                    catalog_version,
                    CommandId::from_hex(&command_id)?,
                )
                .await
                .map_err(|err| err.to_string());
            engine.shutdown().await.map_err(|err| err.to_string())?;
            serde_json::to_value(result?).map_err(|err| err.to_string())
        }
        ControlRequest::StartPublished {
            workflow,
            version,
            start_key,
            input,
            command_id,
            wait_ms,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let result = async {
                let run = engine
                    .start_published_with_command(
                        &workflow,
                        version,
                        &start_key,
                        input,
                        CommandId::from_hex(&command_id)?,
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                if let Some(ms) = wait_ms {
                    let output = engine
                        .wait_terminal(run, Duration::from_millis(ms))
                        .await
                        .map_err(|err| format!("run {}: {err}", run.to_hex()))?;
                    Ok(serde_json::json!({"run":run.to_hex(),"status":"succeeded","output":output}))
                } else {
                    Ok(serde_json::json!({"run":run.to_hex(),"status":"started"}))
                }
            }
            .await;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            result
        }
        ControlRequest::CommandResult { command_id } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let result = engine
                .command_result(CommandId::from_hex(&command_id)?)
                .await
                .map_err(|err| err.to_string());
            engine.shutdown().await.map_err(|err| err.to_string())?;
            serde_json::to_value(result?).map_err(|err| err.to_string())
        }
        ControlRequest::Start {
            yaml,
            catalog,
            input,
            wait_ms,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let run = engine
                .start_yaml(&yaml, &catalog, input)
                .await
                .map_err(|err| err.to_string())?;
            let body = if let Some(ms) = wait_ms {
                match engine.wait_terminal(run, Duration::from_millis(ms)).await {
                    Ok(output) => serde_json::json!({
                        "run": run.to_hex(),
                        "status": "succeeded",
                        "output": output,
                    }),
                    Err(err) => {
                        engine.shutdown().await.map_err(|err| err.to_string())?;
                        return Err(format!("run {}: {err}", run.to_hex()));
                    }
                }
            } else {
                serde_json::json!({"run": run.to_hex(), "status": "started"})
            };
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(body)
        }
        ControlRequest::Signal {
            run,
            name,
            key,
            event_id,
            payload,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let run = RunId::from_hex(&run)?;
            let event_id = EventId::from_hex(&event_id)?;
            engine
                .signal(run, event_id, &name, &key, payload)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        ControlRequest::Cancel { run, reason } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let run = RunId::from_hex(&run)?;
            engine
                .cancel(run, &reason)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        ControlRequest::Acknowledge { reason } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            engine
                .acknowledge_recovery(&reason)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        ControlRequest::AcknowledgeClock { reason } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            engine
                .acknowledge_clock(&reason)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        ControlRequest::Abandon { run, reason } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let run = RunId::from_hex(&run)?;
            engine
                .abandon_compensation(run, &reason)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        ControlRequest::ResolveBlocked {
            run,
            forward,
            input,
        } => {
            let engine = Engine::local(local_dir)
                .await
                .map_err(|err| err.to_string())?;
            let run = RunId::from_hex(&run)?;
            let forward = graphrun::ids::ActivationId::from_hex(&forward)?;
            engine
                .resolve_blocked(run, forward, input)
                .await
                .map_err(|err| err.to_string())?;
            engine.shutdown().await.map_err(|err| err.to_string())?;
            Ok(serde_json::json!({"status":"ok"}))
        }
        _ => Err("engine is not running; start a local member first".to_owned()),
    }
}

fn response_body(resp: graphrun::ControlResponse) -> Result<serde_json::Value, String> {
    if resp.ok {
        Ok(resp.body)
    } else {
        Err(resp
            .error
            .unwrap_or_else(|| "control request failed".to_owned()))
    }
}
