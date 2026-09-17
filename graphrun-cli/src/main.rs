use clap::{Args, Parser, Subcommand};
use graphrun::{
    Catalog, ControlRequest, Engine, EventId, GrpcClient, RunId, TlsMaterial, Value, compile_yaml,
    connect_control, replay,
};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "graphrun", version, about = "Graphrun production CLI")]
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
        #[arg(long)]
        local_dir: Option<PathBuf>,
    },
    Start {
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        definition: Option<PathBuf>,
        #[arg(long)]
        catalog: PathBuf,
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
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Replay {
        #[arg(long)]
        run: Option<String>,
        #[arg(long)]
        local_dir: PathBuf,
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
            eprintln!("{err}");
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
            input,
            connect,
            wait_ms,
            no_wait,
        } => {
            let yaml = load_workflow(workflow, definition)?;
            let catalog = load_catalog(&catalog)?;
            let input = load_value(&input)?;
            let wait = if no_wait { None } else { Some(wait_ms) };
            if let Some(mut client) = grpc_client(&connect).await? {
                let run = client
                    .start(&yaml, &catalog, &input)
                    .await
                    .map_err(|err| err.to_string())?;
                let body = serde_json::json!({"run": run.to_hex(), "status": "started"});
                println!("{body}");
                return Ok(());
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
        Commands::History { run, connect } => {
            if let Some(mut client) = grpc_client(&connect).await? {
                let view = client
                    .history(RunId::from_hex(&run)?)
                    .await
                    .map_err(|err| err.to_string())?;
                println!("{view}");
                return Ok(());
            }
            let local_dir = require_local(connect.local_dir)?;
            let body = dispatch(&local_dir, ControlRequest::History { run }, false).await?;
            println!("{body}");
            Ok(())
        }
        Commands::Replay { run, local_dir } => {
            let state = replay(&local_dir).map_err(|err| err.to_string())?;
            if let Some(run) = run {
                let id = RunId::from_hex(&run)?;
                let events = graphrun::run_events(&state, id);
                println!(
                    "{}",
                    serde_json::json!({
                        "run": run,
                        "events": events,
                        "output": graphrun::run_output(&state, id),
                    })
                );
            } else {
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
            local_dir,
        } => {
            let Some(definition) = definition else {
                return Err("publish requires --definition".to_owned());
            };
            let Some(catalog_path) = catalog_path else {
                return Err("publish requires --catalog".to_owned());
            };
            let digest = validate(&definition, &catalog_path)?;
            if let Some(local_dir) = local_dir {
                std::fs::create_dir_all(&local_dir).map_err(|err| err.to_string())?;
                std::fs::copy(&definition, local_dir.join("published.yaml"))
                    .map_err(|err| err.to_string())?;
                std::fs::copy(&catalog_path, local_dir.join("published-catalog.json"))
                    .map_err(|err| err.to_string())?;
            }
            println!("{}", serde_json::json!({"status":"ok","digest":digest}));
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

fn validate(definition: &PathBuf, catalog: &PathBuf) -> Result<String, String> {
    let catalog = load_catalog(catalog)?;
    let text = std::fs::read_to_string(definition).map_err(|err| err.to_string())?;
    let compiled = compile_yaml(&text, &catalog).map_err(|err| err.to_string())?;
    Ok(compiled.digest.0)
}

async fn dispatch(
    local_dir: &PathBuf,
    req: ControlRequest,
    start_if_needed: bool,
) -> Result<serde_json::Value, String> {
    let sock = local_dir.join("control.sock");
    if sock.exists() {
        let resp = connect_control(&sock, req)
            .await
            .map_err(|err| err.to_string())?;
        return response_body(resp);
    }
    if !start_if_needed
        && matches!(
            req,
            ControlRequest::Inspect { .. }
                | ControlRequest::List
                | ControlRequest::History { .. }
                | ControlRequest::Health
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
            ControlRequest::List => engine.list().await,
            ControlRequest::History { run } => {
                let id = RunId::from_hex(&run)?;
                let events = engine.history(id).await.map_err(|err| err.to_string())?;
                serde_json::to_value(events).map_err(|err| err.to_string())?
            }
            ControlRequest::Health => engine.health().await,
            _ => unreachable!(),
        };
        engine.shutdown().await.map_err(|err| err.to_string())?;
        return Ok(resp);
    }
    match req {
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
                    Err(err) => serde_json::json!({
                        "run": run.to_hex(),
                        "status": "pending",
                        "error": err.to_string(),
                    }),
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
