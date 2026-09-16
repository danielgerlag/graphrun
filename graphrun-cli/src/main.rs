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
    #[arg(long)]
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
        #[arg(long)]
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
}

#[derive(Subcommand, Debug)]
enum ClusterCommands {
    Health {
        #[command(flatten)]
        connect: ConnectArgs,
    },
    Join,
    Promote,
    Remove,
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
            std::fs::create_dir_all(&out).map_err(|err| err.to_string())?;
            let src = local_dir.join("member.redb");
            std::fs::copy(&src, out.join("member.redb")).map_err(|err| err.to_string())?;
            let identity = local_dir.join("identity.json");
            if identity.exists() {
                std::fs::copy(&identity, out.join("identity.json"))
                    .map_err(|err| err.to_string())?;
            }
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
            if reason.as_deref().unwrap_or("").is_empty() {
                return Err("restore requires --reason".to_owned());
            }
            std::fs::create_dir_all(&local_dir).map_err(|err| err.to_string())?;
            std::fs::copy(from.join("member.redb"), local_dir.join("member.redb"))
                .map_err(|err| err.to_string())?;
            let identity = from.join("identity.json");
            if identity.exists() {
                std::fs::copy(&identity, local_dir.join("identity.json"))
                    .map_err(|err| err.to_string())?;
            }
            println!(
                "{}",
                serde_json::json!({"status":"ok","restored": local_dir.display().to_string()})
            );
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
        Commands::Resolve { .. } | Commands::Cluster { .. } => {
            Err("cluster membership commands are not implemented in this build".to_owned())
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
        .ok_or_else(|| "--key is required with --endpoint".to_owned())?;
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
            ControlRequest::Health => serde_json::json!({"status":"ok","mode":"local"}),
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
