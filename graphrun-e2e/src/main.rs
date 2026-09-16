use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "graphrun-e2e", version, about = "Graphrun verification driver")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Verify {
        #[arg(long)]
        cli: PathBuf,
        #[arg(long)]
        matrix: PathBuf,
        #[arg(long)]
        artifacts: PathBuf,
    },
    #[command(name = "fixture-member")]
    FixtureMember,
    #[command(name = "fixture-worker")]
    FixtureWorker,
    #[command(name = "fixture-provider")]
    FixtureProvider,
}

#[derive(Serialize, Deserialize, Clone)]
struct CaseResult {
    id: String,
    requirement: String,
    layer: String,
    scenario: String,
    status: String,
    command: String,
    duration_ms: u128,
    expected: String,
    actual: String,
    artifacts: Vec<String>,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Commands::Verify {
            cli,
            matrix,
            artifacts,
        } => verify(&cli, &matrix, &artifacts),
        Commands::FixtureMember | Commands::FixtureWorker | Commands::FixtureProvider => {
            eprintln!("fixture subprocesses are not implemented yet");
            ExitCode::from(2)
        }
    }
}

fn verify(cli: &Path, matrix: &Path, artifacts: &Path) -> ExitCode {
    let started = Instant::now();
    if let Err(err) = fs::create_dir_all(artifacts) {
        eprintln!("cannot create artifacts dir: {err}");
        return ExitCode::from(2);
    }
    let rows = match load_matrix(matrix) {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };
    let mut results = Vec::new();
    let mut failed = false;
    for row in &rows {
        let result = run_case(cli, artifacts, row);
        if result.status != "PASS" {
            failed = true;
        }
        let path = artifacts.join(format!("{}.json", row.id));
        if let Err(err) = fs::write(&path, serde_json::to_vec_pretty(&result).unwrap()) {
            eprintln!("cannot write {}: {err}", path.display());
            failed = true;
        }
        results.push(result);
    }
    let report = serde_json::json!({
        "run_id": format!("e2e-{}", started.elapsed().as_millis()),
        "cli": cli.display().to_string(),
        "matrix": matrix.display().to_string(),
        "results": results,
    });
    let report_path = artifacts.join("report.json");
    if let Err(err) = fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()) {
        eprintln!("cannot write report: {err}");
        return ExitCode::from(2);
    }
    if failed {
        eprintln!(
            "verification failed; {} cases, report {}",
            rows.len(),
            report_path.display()
        );
        ExitCode::from(1)
    } else {
        println!("verification passed; {} cases", rows.len());
        ExitCode::SUCCESS
    }
}

struct MatrixRow {
    id: String,
    requirement: String,
    layer: String,
    scenario: String,
    pass_criterion: String,
}

fn load_matrix(path: &Path) -> Result<Vec<MatrixRow>, String> {
    let text = fs::read_to_string(path).map_err(|err| err.to_string())?;
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 || line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 5 {
            return Err(format!("matrix line {} is malformed", i + 1));
        }
        rows.push(MatrixRow {
            id: cols[0].to_owned(),
            requirement: cols[1].to_owned(),
            layer: cols[2].to_owned(),
            scenario: cols[3].to_owned(),
            pass_criterion: cols[4].to_owned(),
        });
    }
    if rows.is_empty() {
        return Err("matrix has no cases".to_owned());
    }
    Ok(rows)
}

fn run_case(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let started = Instant::now();
    if row.id == "DSL-001" {
        return yaml_case(cli, artifacts, row, started);
    }
    CaseResult {
        id: row.id.clone(),
        requirement: row.requirement.clone(),
        layer: row.layer.clone(),
        scenario: row.scenario.clone(),
        status: "FAIL".to_owned(),
        command: "unimplemented".to_owned(),
        duration_ms: started.elapsed().as_millis(),
        expected: row.pass_criterion.clone(),
        actual: "no implementation evidence yet".to_owned(),
        artifacts: Vec::new(),
    }
}

fn yaml_case(cli: &Path, artifacts: &Path, row: &MatrixRow, started: Instant) -> CaseResult {
    let examples = PathBuf::from("docs/specs/v1/examples");
    let catalog = examples.join("activity-catalog.json");
    let mut failures = Vec::new();
    let mut ok = 0;
    if let Ok(entries) = fs::read_dir(&examples) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
                continue;
            }
            let output = Command::new(cli)
                .args([
                    "validate",
                    "--definition",
                    path.to_str().unwrap(),
                    "--catalog",
                    catalog.to_str().unwrap(),
                ])
                .output();
            match output {
                Ok(output) if output.status.success() => ok += 1,
                Ok(output) => failures.push(format!(
                    "{}: {}",
                    path.display(),
                    String::from_utf8_lossy(&output.stderr)
                )),
                Err(err) => failures.push(err.to_string()),
            }
        }
    }
    let status = if failures.is_empty() && ok > 0 {
        "PASS"
    } else {
        "FAIL"
    };
    let log = artifacts.join(format!("{}-validate.log", row.id));
    let _ = fs::write(&log, failures.join("\n"));
    CaseResult {
        id: row.id.clone(),
        requirement: row.requirement.clone(),
        layer: row.layer.clone(),
        scenario: row.scenario.clone(),
        status: status.to_owned(),
        command: format!(
            "{} validate --definition <yaml> --catalog {}",
            cli.display(),
            catalog.display()
        ),
        duration_ms: started.elapsed().as_millis(),
        expected: row.pass_criterion.clone(),
        actual: format!("{ok} yaml fixtures validated, {} failures", failures.len()),
        artifacts: vec![log.display().to_string()],
    }
}
