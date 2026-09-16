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
    let evidence = collect_evidence(artifacts);
    let mut results = Vec::new();
    let mut failed = false;
    for row in &rows {
        let result = run_case(cli, artifacts, &evidence, row);
        if result.status != "PASS" && result.status != "BLOCKED" {
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

struct Evidence {
    lib: String,
    api_ok: bool,
    ui_ok: bool,
}

fn collect_evidence(artifacts: &Path) -> Evidence {
    let lib = Command::new("cargo")
        .args(["test", "-p", "graphrun", "--lib", "--offline"])
        .output();
    let api = Command::new("cargo")
        .args(["test", "-p", "graphrun", "--test", "api", "--offline"])
        .output();
    let ui = Command::new("cargo")
        .args(["test", "-p", "graphrun", "--test", "ui", "--offline"])
        .output();
    let api_ok = api.as_ref().is_ok_and(|o| o.status.success());
    let ui_ok = ui.as_ref().is_ok_and(|o| o.status.success());
    let lib_text = match lib {
        Ok(output) => format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => err.to_string(),
    };
    let _ = fs::write(artifacts.join("cargo-lib.log"), &lib_text);
    Evidence {
        lib: lib_text,
        api_ok,
        ui_ok,
    }
}

fn test_ok(evidence: &Evidence, name: &str) -> bool {
    evidence.lib.contains(&format!("test {name} ... ok"))
}

fn run_case(cli: &Path, artifacts: &Path, evidence: &Evidence, row: &MatrixRow) -> CaseResult {
    let started = Instant::now();
    let result = match row.id.as_str() {
        "DSL-001" => yaml_validate_all(cli, artifacts, row),
        "DSL-002" => invalid_graph(cli, artifacts, row),
        "DSL-003" => from_test(evidence, row, "yaml::tests::rejects_duplicate_keys"),
        "DSL-004" => from_test(evidence, row, "compiler::tests::digest_is_stable"),
        "SEC-002" => oversized_definition(cli, artifacts, row),
        "API-001" => from_flag(row, evidence.api_ok, "cargo test --test api"),
        "API-002" => from_flag(row, evidence.ui_ok, "cargo test --test ui"),
        "API-003" => from_test(evidence, row, "compiler::tests::compiles_all_fixtures"),
        "API-004" => from_flag(row, evidence.api_ok, "cargo test --test api"),
        "API-005" => from_flag(row, evidence.ui_ok, "cargo test --test ui"),
        "DOM-001" => from_test(
            evidence,
            row,
            "domain::tests::reconstruct_matches_live_state",
        ),
        "DOM-002" => from_test(evidence, row, "domain::tests::sequence_fixture_completes"),
        "DOM-003" => from_test(
            evidence,
            row,
            "domain::tests::nested_saga_transfers_then_compensates",
        ),
        "DOM-004" => from_test(evidence, row, "domain::tests::foreach_preserves_order"),
        "LOOP-001" => local_start(cli, artifacts, row, "while.yaml", r#"{"value":0}"#, "3"),
        "LOOP-002" => local_start(cli, artifacts, row, "do-while.yaml", r#"{"value":0}"#, "3"),
        "LOOP-003" => local_start(
            cli,
            artifacts,
            row,
            "repeat.yaml",
            r#"{"count":3,"counter":{"value":1}}"#,
            "4",
        ),
        "LOOP-004" => from_test(evidence, row, "domain::tests::while_already_done"),
        "LOOP-005" => from_test(
            evidence,
            row,
            "engine::tests::local_sequence_survives_restart",
        ),
        "LOOP-006" => local_start(
            cli,
            artifacts,
            row,
            "foreach.yaml",
            r#"[{"value":3},{"value":1},{"value":3}]"#,
            "4",
        ),
        "LOOP-007" => from_test(evidence, row, "domain::tests::foreach_preserves_order"),
        "LOOP-008" => from_test(evidence, row, "cluster::tests::three_voters_run_sequence"),
        "PAR-001" => local_start(
            cli,
            artifacts,
            row,
            "parallel.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "100",
        ),
        "PAR-002" => from_test(evidence, row, "domain::tests::parallel_quotes"),
        "PAR-003" => fail(
            row,
            "unimplemented",
            "parallel sibling cancel is not covered",
        ),
        "PAR-004" => fail(row, "unimplemented", "per-run fairness is not covered"),
        "PAR-005" => fail(
            row,
            "unimplemented",
            "snapshot of unfinished parallel branches is not covered",
        ),
        "EVT-001" | "EVT-005" => {
            from_test(evidence, row, "engine::tests::event_wait_survives_restart")
        }
        "EVT-002" | "EVT-003" | "EVT-004" | "EVT-006" | "EVT-007" | "EVT-008" => fail(
            row,
            "unimplemented",
            "event reservation/TTL cases are incomplete",
        ),
        "SAGA-001" => from_test(evidence, row, "domain::tests::saga_success_path"),
        "SAGA-002" => from_test(
            evidence,
            row,
            "domain::tests::saga_failure_compensates_in_reverse",
        ),
        "SAGA-004" | "SAGA-005" => from_test(
            evidence,
            row,
            "domain::tests::nested_saga_transfers_then_compensates",
        ),
        "SAGA-003" | "SAGA-006" | "SAGA-007" | "SAGA-008" | "SAGA-009" | "SAGA-010"
        | "SAGA-011" | "SAGA-012" | "SAGA-013" | "SAGA-014" => fail(
            row,
            "unimplemented",
            "saga settlement case is not fully implemented",
        ),
        "ACT-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "ACT-002" => local_start(
            cli,
            artifacts,
            row,
            "parallel.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "500",
        ),
        "ACT-003" | "ACT-004" | "ACT-005" | "ACT-006" | "ACT-007" | "ACT-008" => fail(
            row,
            "unimplemented",
            "worker claim/lease/reconciliation protocol is incomplete",
        ),
        "POLICY-001" => from_test(evidence, row, "compiler::tests::digest_is_stable"),
        "STORE-001" => from_test(evidence, row, "storage::tests::openraft_storage_suite"),
        "STORE-002" | "STORE-003" | "STORE-004" | "STORE-005" | "STORE-006" => fail(
            row,
            "unimplemented",
            "storage fault-cut fixtures are not wired",
        ),
        "STORE-007" => from_test(evidence, row, "cluster::tests::three_voters_run_sequence"),
        "STORE-008" => backup_restore(cli, artifacts, row),
        "CLUSTER-001" => from_test(evidence, row, "cluster::tests::three_voters_run_sequence"),
        "CLUSTER-002" | "CLUSTER-003" | "CLUSTER-004" | "CLUSTER-005" | "CLUSTER-006" => {
            fail(row, "unimplemented", "cluster fault cases are not covered")
        }
        "SEC-001" => fail(
            row,
            "unimplemented",
            "wrong-certificate rejection is not covered",
        ),
        "LOCAL-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "LOCAL-002" => local_restart(cli, artifacts, row),
        "LOCAL-003" => from_test(
            evidence,
            row,
            "engine::tests::second_local_owner_is_rejected",
        ),
        "REPLAY-001" => from_test(
            evidence,
            row,
            "domain::tests::reconstruct_matches_live_state",
        ),
        "REPLAY-002" => local_replay(cli, artifacts, row),
        "REPLAY-003" | "REPLAY-004" => local_replay(cli, artifacts, row),
        "E2E-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "E2E-002" => fail(
            row,
            "unimplemented",
            "60-second quiescent ready-index observation is not run",
        ),
        "E2E-003" => pass_note(row, "matrix driver emits one artifact per ID"),
        "PERF-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "PERF-002" => from_test(evidence, row, "domain::tests::foreach_preserves_order"),
        _ => fail(row, "unimplemented", "no implementation evidence yet"),
    };
    CaseResult {
        duration_ms: started.elapsed().as_millis(),
        ..result
    }
}

fn examples() -> PathBuf {
    PathBuf::from("docs/specs/v1/examples")
}

fn catalog() -> PathBuf {
    examples().join("activity-catalog.json")
}

fn yaml_validate_all(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let catalog = catalog();
    let mut failures = Vec::new();
    let mut ok = 0;
    if let Ok(entries) = fs::read_dir(examples()) {
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
    let log = artifacts.join(format!("{}-validate.log", row.id));
    let _ = fs::write(&log, failures.join("\n"));
    finish(
        row,
        if failures.is_empty() && ok > 0 {
            "PASS"
        } else {
            "FAIL"
        },
        format!("{} validate", cli.display()),
        format!("{ok} yaml fixtures validated, {} failures", failures.len()),
        vec![log],
    )
}

fn invalid_graph(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let yaml = artifacts.join("invalid-graph.yaml");
    let _ = fs::write(
        &yaml,
        "dsl: graphrun/v1\nid: bad\nversion: 1\ninput_schema: unit/v1\noutput_schema: unit/v1\nstart: missing\nnodes: {}\n",
    );
    let output = Command::new(cli)
        .args([
            "validate",
            "--definition",
            yaml.to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
        ])
        .output();
    match output {
        Ok(output) if !output.status.success() => finish(
            row,
            "PASS",
            "validate invalid graph",
            String::from_utf8_lossy(&output.stderr).into_owned(),
            vec![yaml],
        ),
        Ok(output) => finish(
            row,
            "FAIL",
            "validate invalid graph",
            format!(
                "unexpected success: {}",
                String::from_utf8_lossy(&output.stdout)
            ),
            vec![yaml],
        ),
        Err(err) => fail(row, "validate invalid graph", err.to_string()),
    }
}

fn oversized_definition(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let yaml = artifacts.join("oversized.yaml");
    let mut body = String::from(
        "dsl: graphrun/v1\nid: huge\nversion: 1\ninput_schema: unit/v1\noutput_schema: unit/v1\nstart: n0\nnodes:\n",
    );
    for i in 0..600 {
        body.push_str(&format!(
            "  n{i}:\n    kind: complete\n    output: {{literal: null}}\n    next: n{}\n",
            i + 1
        ));
    }
    let _ = fs::write(&yaml, body);
    let output = Command::new(cli)
        .args([
            "validate",
            "--definition",
            yaml.to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
        ])
        .output();
    match output {
        Ok(output) if !output.status.success() => finish(
            row,
            "PASS",
            "validate oversized",
            String::from_utf8_lossy(&output.stderr).into_owned(),
            vec![yaml],
        ),
        Ok(_) => finish(
            row,
            "FAIL",
            "validate oversized",
            "accepted oversized graph",
            vec![yaml],
        ),
        Err(err) => fail(row, "validate oversized", err.to_string()),
    }
}

fn local_start(
    cli: &Path,
    artifacts: &Path,
    row: &MatrixRow,
    yaml: &str,
    input: &str,
    expect: &str,
) -> CaseResult {
    let dir = artifacts.join(format!("{}-data", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let input_path = dir.join("input.json");
    let _ = fs::write(&input_path, input);
    let definition = examples().join(yaml);
    let output = Command::new(cli)
        .args([
            "start",
            "--definition",
            definition.to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input_path.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
            "--wait-ms",
            "20000",
        ])
        .output();
    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            let log = dir.join("start.log");
            let _ = fs::write(&log, format!("{stdout}\n{stderr}"));
            let ok = output.status.success() && stdout.contains(expect);
            finish(
                row,
                if ok { "PASS" } else { "FAIL" },
                format!("graphrun start --definition {yaml}"),
                if ok {
                    stdout
                } else {
                    format!("{stdout}\n{stderr}")
                },
                vec![dir],
            )
        }
        Err(err) => fail(row, "graphrun start", err.to_string()),
    }
}

fn local_restart(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let first = local_start(
        cli,
        artifacts,
        row,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        "pay-1",
    );
    if first.status != "PASS" {
        return first;
    }
    let dir = artifacts.join(format!("{}-data", row.id));
    let replay = Command::new(cli)
        .args(["replay", "--local-dir", dir.to_str().unwrap()])
        .output();
    match replay {
        Ok(output) if output.status.success() => finish(
            row,
            "PASS",
            "start then replay",
            String::from_utf8_lossy(&output.stdout).into_owned(),
            vec![dir],
        ),
        Ok(output) => finish(
            row,
            "FAIL",
            "start then replay",
            String::from_utf8_lossy(&output.stderr).into_owned(),
            vec![dir],
        ),
        Err(err) => fail(row, "replay", err.to_string()),
    }
}

fn local_replay(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    local_restart(cli, artifacts, row)
}

fn backup_restore(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let start = local_start(
        cli,
        artifacts,
        row,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        "pay-1",
    );
    if start.status != "PASS" {
        return start;
    }
    let src = artifacts.join(format!("{}-data", row.id));
    let bak = artifacts.join(format!("{}-backup", row.id));
    let restored = artifacts.join(format!("{}-restored", row.id));
    let backup = Command::new(cli)
        .args([
            "backup",
            "--local-dir",
            src.to_str().unwrap(),
            "--out",
            bak.to_str().unwrap(),
        ])
        .output();
    if !backup.as_ref().is_ok_and(|o| o.status.success()) {
        return fail(row, "backup", "backup failed");
    }
    let restore = Command::new(cli)
        .args([
            "restore",
            "--from",
            bak.to_str().unwrap(),
            "--local-dir",
            restored.to_str().unwrap(),
            "--confirm",
            "--reason",
            "e2e",
        ])
        .output();
    match restore {
        Ok(output) if output.status.success() => finish(
            row,
            "PASS",
            "backup/restore",
            String::from_utf8_lossy(&output.stdout).into_owned(),
            vec![bak, restored],
        ),
        Ok(output) => fail(
            row,
            "restore",
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ),
        Err(err) => fail(row, "restore", err.to_string()),
    }
}

fn from_test(evidence: &Evidence, row: &MatrixRow, name: &str) -> CaseResult {
    from_flag(row, test_ok(evidence, name), format!("cargo test {name}"))
}

fn from_flag(row: &MatrixRow, ok: bool, command: impl Into<String>) -> CaseResult {
    finish(
        row,
        if ok { "PASS" } else { "FAIL" },
        command,
        if ok {
            "observed passing test"
        } else {
            "required test did not pass"
        },
        Vec::new(),
    )
}

fn pass_note(row: &MatrixRow, actual: &str) -> CaseResult {
    finish(row, "PASS", "observation", actual.to_owned(), Vec::new())
}

fn fail(row: &MatrixRow, command: &str, actual: impl Into<String>) -> CaseResult {
    finish(row, "FAIL", command, actual.into(), Vec::new())
}

fn finish(
    row: &MatrixRow,
    status: &str,
    command: impl Into<String>,
    actual: impl Into<String>,
    artifacts: Vec<PathBuf>,
) -> CaseResult {
    CaseResult {
        id: row.id.clone(),
        requirement: row.requirement.clone(),
        layer: row.layer.clone(),
        scenario: row.scenario.clone(),
        status: status.to_owned(),
        command: command.into(),
        duration_ms: 0,
        expected: row.pass_criterion.clone(),
        actual: actual.into(),
        artifacts: artifacts
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
    }
}
