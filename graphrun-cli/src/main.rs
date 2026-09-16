use clap::{Parser, Subcommand};
use graphrun::{Catalog, compile_yaml};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "graphrun", version, about = "Graphrun production CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
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
        workflow: String,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        local_dir: Option<PathBuf>,
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
    },
    Cancel {
        #[arg(long)]
        run: String,
        #[arg(long)]
        reason: String,
    },
    Inspect {
        #[arg(long)]
        run: String,
    },
    List,
    History {
        #[arg(long)]
        run: String,
    },
    Replay {
        #[arg(long)]
        run: String,
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
    },
    Restore {
        #[arg(long)]
        from: PathBuf,
        #[arg(long)]
        confirm: bool,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Subcommand)]
enum ClusterCommands {
    Health,
    Join,
    Promote,
    Remove,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Commands::Validate {
            definition,
            catalog,
        } => match validate(&definition, &catalog) {
            Ok(digest) => {
                println!("{}", serde_json::json!({"status":"ok","digest":digest}));
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(2)
            }
        },
        Commands::Replay { .. } => {
            eprintln!("replay is read-only and is not wired to a live engine yet");
            ExitCode::from(2)
        }
        other => {
            eprintln!(
                "command {:?} is not implemented in this build",
                std::mem::discriminant(&other)
            );
            ExitCode::from(2)
        }
    }
}

fn validate(definition: &PathBuf, catalog: &PathBuf) -> Result<String, String> {
    let catalog_bytes = std::fs::read(catalog).map_err(|err| err.to_string())?;
    let catalog = Catalog::from_json(&catalog_bytes).map_err(|err| err.to_string())?;
    let text = std::fs::read_to_string(definition).map_err(|err| err.to_string())?;
    let compiled = compile_yaml(&text, &catalog).map_err(|err| err.to_string())?;
    Ok(compiled.digest.0)
}
