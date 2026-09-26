use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

static ACTIVE_CHILDREN: AtomicUsize = AtomicUsize::new(0);
static CHILD_LEDGER_ERROR: AtomicBool = AtomicBool::new(false);
static CHILD_EXITS: Mutex<Vec<ChildExit>> = Mutex::new(Vec::new());

#[derive(Serialize)]
struct ChildExit {
    pid: u32,
    status: Option<String>,
    error: Option<String>,
}

#[derive(Parser)]
#[command(name = "graphrun-e2e", version, about = "Graphrun verification driver")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    SmokeCluster {
        #[arg(long)]
        cli: PathBuf,
        #[arg(long)]
        artifacts: PathBuf,
    },
    SmokeQuiescence {
        #[arg(long)]
        cli: PathBuf,
        #[arg(long)]
        artifacts: PathBuf,
    },
    Verify {
        #[arg(long)]
        cli: PathBuf,
        #[arg(long)]
        matrix: PathBuf,
        #[arg(long)]
        artifacts: PathBuf,
        #[arg(long)]
        release_certification: bool,
    },
    #[command(name = "fixture-member")]
    FixtureMember {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        bind: String,
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        ca: PathBuf,
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        server_name: String,
        #[arg(long)]
        initialize: bool,
        #[arg(long)]
        host_activities: bool,
        #[arg(long = "peer")]
        peer: Vec<String>,
    },
    #[command(name = "fixture-worker")]
    FixtureWorker {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        ca: PathBuf,
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        #[arg(long)]
        server_name: String,
    },
    #[command(name = "fixture-provider")]
    FixtureProvider {
        #[arg(long)]
        bind: Option<String>,
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    run_id: String,
    source_sha256: String,
    cli_sha256: String,
    cli_version: String,
    driver_sha256: String,
    driver_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    performance: Option<PerformanceEvidence>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct PerformanceEvidence {
    hardware: String,
    reference_hardware_available: bool,
    measurements: Vec<String>,
    reason: String,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Commands::SmokeCluster { cli, artifacts } => {
            smoke_case(&cli, &artifacts, "CLUSTER-001", cluster_three_and_workers)
        }
        Commands::SmokeQuiescence { cli, artifacts } => {
            smoke_case(&cli, &artifacts, "E2E-002", e2e_no_ready_scan)
        }
        Commands::Verify {
            cli,
            matrix,
            artifacts,
            release_certification,
        } => verify(&cli, &matrix, &artifacts, release_certification),
        Commands::FixtureMember {
            data_dir,
            bind,
            node_id,
            ca,
            cert,
            key,
            server_name,
            initialize,
            host_activities,
            peer,
        } => fixture_member(
            data_dir,
            bind,
            node_id,
            ca,
            cert,
            key,
            server_name,
            initialize,
            host_activities,
            peer,
        ),
        Commands::FixtureWorker {
            endpoint,
            ca,
            cert,
            key,
            server_name,
        } => fixture_worker(endpoint, ca, cert, key, server_name),
        Commands::FixtureProvider { bind, data_dir } => fixture_provider(bind, data_dir),
    }
}

fn smoke_case(
    cli: &Path,
    artifacts: &Path,
    case_id: &str,
    run: fn(&Path, &Path, &MatrixRow) -> CaseResult,
) -> ExitCode {
    if let Err(error) = fs::create_dir_all(artifacts) {
        eprintln!("cannot create smoke artifacts: {error}");
        return ExitCode::from(2);
    }
    let matrix = match parse_matrix(include_str!("../../docs/specs/v1/verification-matrix.tsv")) {
        Ok(matrix) => matrix,
        Err(error) => {
            eprintln!("cannot load smoke case: {error}");
            return ExitCode::from(2);
        }
    };
    let Some(row) = matrix.into_iter().find(|row| row.id == case_id) else {
        eprintln!("{case_id} case missing from verification matrix");
        return ExitCode::from(2);
    };
    let result = run(cli, artifacts, &row);
    println!(
        "{}",
        serde_json::json!({
            "kind": "smoke-not-certification",
            "case_id": result.id,
            "status": result.status,
            "actual": result.actual,
            "artifacts": result.artifacts,
        })
    );
    if result.status == "PASS" {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn verify(cli: &Path, matrix: &Path, artifacts: &Path, strict: bool) -> ExitCode {
    if ACTIVE_CHILDREN.load(Ordering::SeqCst) != 0 {
        eprintln!("cannot start verification with owned child processes still active");
        return ExitCode::from(2);
    }
    match CHILD_EXITS.lock() {
        Ok(mut exits) => exits.clear(),
        Err(err) => {
            eprintln!("cannot reset child exit ledger: {err}");
            return ExitCode::from(2);
        }
    }
    CHILD_LEDGER_ERROR.store(false, Ordering::SeqCst);
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
    let run_dir = match tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(artifacts)
    {
        Ok(dir) => dir.keep(),
        Err(err) => {
            eprintln!("cannot create fresh evidence directory: {err}");
            return ExitCode::from(2);
        }
    };
    let mut context = match RunContext::new(cli, &run_dir) {
        Ok(context) => context,
        Err(err) => {
            eprintln!("cannot identify source/binaries: {err}");
            return ExitCode::from(2);
        }
    };
    let cli_check = Command::new(cli).arg("--version").output();
    let cli_error = match cli_check {
        _ if context.cli_sha256.starts_with("UNAVAILABLE: ") => Some(context.cli_sha256.clone()),
        Ok(output) if output.status.success() => {
            context.cli_version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if context.cli_version.is_empty() {
                Some("CLI --version returned no version identifier".to_owned())
            } else {
                None
            }
        }
        Ok(output) => Some(format!(
            "CLI --version exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )),
        Err(err) => Some(format!("cannot execute CLI {}: {err}", cli.display())),
    };
    let evidence = if cli_error.is_none() && rows.iter().any(|row| row.id != "E2E-003") {
        match collect_evidence(&run_dir) {
            Ok(evidence) => Some(evidence),
            Err(err) => {
                eprintln!("cannot collect test evidence: {err}");
                None
            }
        }
    } else {
        None
    };
    let mut results = Vec::new();
    for row in rows.iter().filter(|row| row.id != "E2E-003") {
        let result = match (&cli_error, &evidence) {
            (Some(err), _) => fail(row, "CLI preflight", err),
            (_, None) => fail(
                row,
                "test evidence preflight",
                "test suite evidence unavailable",
            ),
            (None, Some(evidence)) => run_case(cli, &run_dir, evidence, row),
        };
        let result = context.bind(enforce_case(row, result, &run_dir));
        if let Err(err) = write_case(&run_dir, &result) {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
        results.push(result);
    }
    if rows.iter().any(|row| row.id == "E2E-003") {
        let result = context.bind(e2e_report_complete(&run_dir, &rows, &results, &context));
        if let Err(err) = write_case(&run_dir, &result) {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
        results.push(result);
    }
    let source_integrity = match source_fingerprint(&run_dir) {
        Ok(fingerprint) if fingerprint == context.source_sha256 => None,
        Ok(_) => Some("source files changed during verification".to_owned()),
        Err(err) => Some(format!("cannot recheck source fingerprint: {err}")),
    };
    let cli_integrity = match hash_file(cli) {
        Ok(hash) if hash == context.cli_sha256 => None,
        Ok(_) => Some("CLI binary changed during verification".to_owned()),
        Err(err) if cli_error.is_some() => Some(format!("CLI unavailable: {err}")),
        Err(err) => Some(format!("cannot recheck CLI fingerprint: {err}")),
    };
    let driver_integrity = match std::env::current_exe()
        .map_err(|err| err.to_string())
        .and_then(|path| hash_file(&path))
    {
        Ok(hash) if hash == context.driver_sha256 => None,
        Ok(_) => Some("verification driver changed during run".to_owned()),
        Err(err) => Some(format!("cannot recheck driver fingerprint: {err}")),
    };
    let integrity = source_integrity.or(cli_integrity).or(driver_integrity);
    if let Some(reason) = &integrity {
        for result in &mut results {
            if result.status != "FAIL" {
                result.status = "FAIL".to_owned();
                result.actual = format!("evidence invalidated: {reason}");
                if let Err(err) = write_case(&run_dir, result) {
                    eprintln!("{err}");
                    return ExitCode::from(2);
                }
            }
        }
    }
    let coverage = validate_coverage(&rows, &results, &run_dir, &context);
    let (exit_code, release_certified) = verification_gate(&results, strict);
    let canonical_matrix = release_matrix_complete(&rows);
    let matrix_complete = canonical_matrix.is_ok();
    let failed = exit_code != ExitCode::SUCCESS
        || integrity.is_some()
        || coverage.is_err()
        || !matrix_complete;
    let certified = release_certified && matrix_complete && !failed;
    let report = serde_json::json!({
        "run_id": context.run_id,
        "source_sha256": context.source_sha256,
        "cli_sha256": context.cli_sha256,
        "cli_version": context.cli_version,
        "driver_sha256": context.driver_sha256,
        "driver_version": context.driver_version,
        "cli": cli.display().to_string(),
        "matrix": matrix.display().to_string(),
        "release_matrix_complete": matrix_complete,
        "release_matrix_error": canonical_matrix.err(),
        "strict_release_certification": strict,
        "release_certified": certified,
        "coverage_error": coverage.err(),
        "source_integrity_error": integrity,
        "results": results,
    });
    let report_bytes = match serde_json::to_vec_pretty(&report) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("cannot serialize report: {err}");
            return ExitCode::from(2);
        }
    };
    let report_path = run_dir.join("report.json");
    if let Err(err) = fs::write(&report_path, &report_bytes)
        .and_then(|()| fs::write(artifacts.join("report.json"), &report_bytes))
    {
        eprintln!("cannot write report {}: {err}", report_path.display());
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
        println!(
            "verification passed; {} cases, release_certified={}",
            rows.len(),
            certified
        );
        ExitCode::SUCCESS
    }
}

struct RunContext {
    run_id: String,
    source_sha256: String,
    cli_sha256: String,
    cli_version: String,
    driver_sha256: String,
    driver_version: String,
}

impl RunContext {
    fn new(cli: &Path, run_dir: &Path) -> Result<Self, String> {
        let run_id = run_dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("invalid evidence directory name")?
            .to_owned();
        let driver = std::env::current_exe().map_err(|err| err.to_string())?;
        Ok(Self {
            run_id,
            source_sha256: source_fingerprint(run_dir)?,
            cli_sha256: hash_file(cli).unwrap_or_else(|err| format!("UNAVAILABLE: {err}")),
            cli_version: "UNAVAILABLE".to_owned(),
            driver_sha256: hash_file(&driver)?,
            driver_version: env!("CARGO_PKG_VERSION").to_owned(),
        })
    }

    fn bind(&self, mut result: CaseResult) -> CaseResult {
        result.run_id.clone_from(&self.run_id);
        result.source_sha256.clone_from(&self.source_sha256);
        result.cli_sha256.clone_from(&self.cli_sha256);
        result.cli_version.clone_from(&self.cli_version);
        result.driver_sha256.clone_from(&self.driver_sha256);
        result.driver_version.clone_from(&self.driver_version);
        result
    }
}

fn hash_file(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|err| format!("{}: {err}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn source_fingerprint(run_dir: &Path) -> Result<String, String> {
    let root = fs::canonicalize(std::env::current_dir().map_err(|err| err.to_string())?)
        .map_err(|err| err.to_string())?;
    let artifact_root = fs::canonicalize(
        run_dir
            .parent()
            .ok_or_else(|| "run directory has no artifacts root".to_owned())?,
    )
    .map_err(|err| err.to_string())?;
    if root.starts_with(&artifact_root) {
        return Err("artifacts root must not contain the source repository".to_owned());
    }
    let mut pending = vec![root.clone()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries =
            fs::read_dir(&directory).map_err(|err| format!("{}: {err}", directory.display()))?;
        for entry in entries {
            let entry = entry.map_err(|err| err.to_string())?;
            let path = entry.path();
            if path.starts_with(&artifact_root)
                || matches!(
                    path.file_name().and_then(|name| name.to_str()),
                    Some(".git" | "target" | "target-e2e-artifacts")
                )
            {
                continue;
            }
            let kind = entry.file_type().map_err(|err| err.to_string())?;
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() {
                files.push(path);
            } else {
                return Err(format!("unsupported source entry {}", path.display()));
            }
        }
    }
    files.sort_unstable();
    let mut digest = Sha256::new();
    for file in files {
        let relative = file.strip_prefix(&root).map_err(|err| err.to_string())?;
        digest.update(
            relative
                .to_str()
                .ok_or_else(|| format!("invalid source path {}", file.display()))?
                .as_bytes(),
        );
        digest.update([0]);
        digest.update(fs::read(&file).map_err(|err| format!("{}: {err}", file.display()))?);
        digest.update([0]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn write_case(run_dir: &Path, result: &CaseResult) -> Result<(), String> {
    let path = run_dir.join(format!("{}.json", result.id));
    let bytes = serde_json::to_vec_pretty(result).map_err(|err| err.to_string())?;
    fs::write(&path, bytes).map_err(|err| format!("cannot write {}: {err}", path.display()))
}

fn check_case_artifacts(result: &CaseResult, run_dir: &Path) -> Result<(), String> {
    if result.status != "PASS" && result.status != "BLOCKED" {
        return Ok(());
    }
    if result.artifacts.is_empty() {
        return Err(format!("{} has no fresh artifact paths", result.id));
    }
    let root = fs::canonicalize(run_dir).map_err(|err| err.to_string())?;
    for artifact in &result.artifacts {
        let path = fs::canonicalize(artifact)
            .map_err(|err| format!("{} artifact {artifact}: {err}", result.id))?;
        if !path.starts_with(&root) {
            return Err(format!(
                "{} artifact {artifact} is outside the current run",
                result.id
            ));
        }
    }
    if result.status == "BLOCKED" {
        let perf = result
            .performance
            .as_ref()
            .ok_or_else(|| format!("{} lacks performance evidence", result.id))?;
        if !result.id.starts_with("PERF-")
            || perf.reference_hardware_available
            || perf.hardware.is_empty()
            || perf.measurements.is_empty()
            || perf.reason.is_empty()
        {
            return Err(format!("{} is not a measured hardware blocker", result.id));
        }
    }
    Ok(())
}

fn enforce_case(row: &MatrixRow, result: CaseResult, run_dir: &Path) -> CaseResult {
    match check_case_artifacts(&result, run_dir) {
        Ok(()) => result,
        Err(err) => fail(row, &result.command, err),
    }
}

fn validate_coverage(
    rows: &[MatrixRow],
    results: &[CaseResult],
    run_dir: &Path,
    context: &RunContext,
) -> Result<(), String> {
    let mut expected = HashMap::new();
    for row in rows {
        if expected.insert(row.id.as_str(), row).is_some() {
            return Err(format!("duplicate matrix ID {}", row.id));
        }
    }
    if results.len() != expected.len() {
        return Err(format!(
            "expected {} cases, got {}",
            expected.len(),
            results.len()
        ));
    }
    let mut seen = HashSet::new();
    for result in results {
        let row = expected
            .get(result.id.as_str())
            .ok_or_else(|| format!("unexpected case {}", result.id))?;
        if !seen.insert(result.id.as_str()) {
            return Err(format!("duplicate case {}", result.id));
        }
        if result.requirement != row.requirement
            || result.layer != row.layer
            || result.scenario != row.scenario
            || result.expected != row.pass_criterion
            || result.run_id != context.run_id
            || result.source_sha256 != context.source_sha256
            || result.cli_sha256 != context.cli_sha256
            || result.cli_version != context.cli_version
            || result.driver_sha256 != context.driver_sha256
            || result.driver_version != context.driver_version
        {
            return Err(format!("{} has stale/mismatched case identity", result.id));
        }
        if !matches!(result.status.as_str(), "PASS" | "FAIL" | "BLOCKED") {
            return Err(format!(
                "{} has invalid status {}",
                result.id, result.status
            ));
        }
        check_case_artifacts(result, run_dir)?;
        let path = run_dir.join(format!("{}.json", result.id));
        let bytes = fs::read(&path).map_err(|err| format!("{}: {err}", path.display()))?;
        let saved: CaseResult =
            serde_json::from_slice(&bytes).map_err(|err| format!("{}: {err}", path.display()))?;
        if &saved != result {
            return Err(format!("{} does not match fresh case record", result.id));
        }
    }
    Ok(())
}

fn verification_gate(results: &[CaseResult], strict: bool) -> (ExitCode, bool) {
    let certified = results.iter().all(|result| result.status == "PASS");
    let allowed = results.iter().all(|result| {
        result.status == "PASS"
            || (!strict
                && result.status == "BLOCKED"
                && result.id.starts_with("PERF-")
                && result.performance.as_ref().is_some_and(|perf| {
                    !perf.reference_hardware_available
                        && !perf.hardware.is_empty()
                        && !perf.measurements.is_empty()
                        && !perf.reason.is_empty()
                }))
    });
    (
        if allowed {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(1)
        },
        certified,
    )
}

#[derive(Clone)]
struct MatrixRow {
    id: String,
    requirement: String,
    layer: String,
    scenario: String,
    pass_criterion: String,
}

fn load_matrix(path: &Path) -> Result<Vec<MatrixRow>, String> {
    let text = fs::read_to_string(path).map_err(|err| err.to_string())?;
    parse_matrix(&text)
}

fn release_matrix_complete(rows: &[MatrixRow]) -> Result<(), String> {
    let expected = parse_matrix(include_str!("../../docs/specs/v1/verification-matrix.tsv"))?;
    let mut supplied = HashMap::new();
    for row in rows {
        if supplied.insert(row.id.as_str(), row).is_some() {
            return Err(format!("duplicate matrix ID {}", row.id));
        }
    }
    for case in &expected {
        let row = supplied
            .get(case.id.as_str())
            .ok_or_else(|| format!("missing mandatory matrix ID {}", case.id))?;
        if row.requirement != case.requirement
            || row.layer != case.layer
            || row.scenario != case.scenario
            || row.pass_criterion != case.pass_criterion
        {
            return Err(format!(
                "matrix row {} differs from canonical matrix",
                case.id
            ));
        }
    }
    if let Some(row) = rows
        .iter()
        .find(|row| !expected.iter().any(|case| case.id == row.id))
    {
        return Err(format!("unexpected matrix ID {}", row.id));
    }
    Ok(())
}

fn parse_matrix(text: &str) -> Result<Vec<MatrixRow>, String> {
    let mut rows = Vec::new();
    let mut ids = HashSet::new();
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            if line != "id\trequirement\tlayer\tscenario\tpass_criterion" {
                return Err("matrix header is invalid".to_owned());
            }
            continue;
        }
        if line.trim().is_empty() {
            return Err(format!("matrix line {} is blank", i + 1));
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() != 5 || cols.iter().any(|column| column.is_empty()) {
            return Err(format!("matrix line {} is malformed", i + 1));
        }
        if !cols[0]
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(format!("matrix line {} has an unsafe ID", i + 1));
        }
        if !ids.insert(cols[0]) {
            return Err(format!("matrix line {} duplicates ID {}", i + 1, cols[0]));
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

struct TestSuite {
    command: String,
    log: PathBuf,
    passed: HashSet<String>,
    success: bool,
    duration_ms: u128,
}

impl TestSuite {
    fn run(run_dir: &Path, label: &str, args: &[&str]) -> Result<Self, String> {
        let command = format!("cargo {}", args.join(" "));
        let started = Instant::now();
        let output = Command::new("cargo").args(args).output();
        let duration_ms = started.elapsed().as_millis();
        let (text, test_output, exited) = match output {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                (
                    format!("{stdout}\n{}", String::from_utf8_lossy(&output.stderr)),
                    stdout,
                    output.status.success(),
                )
            }
            Err(err) => (format!("cannot run {command}: {err}"), String::new(), false),
        };
        let log = run_dir.join(format!("cargo-{label}.log"));
        fs::write(&log, &text).map_err(|err| format!("{}: {err}", log.display()))?;
        Ok(Self::from_output(
            command,
            log,
            &test_output,
            exited,
            duration_ms,
        ))
    }

    fn from_output(
        command: String,
        log: PathBuf,
        text: &str,
        exited: bool,
        duration_ms: u128,
    ) -> Self {
        let (passed, complete) = parse_test_results(text);
        Self {
            command,
            log,
            passed,
            success: exited && complete,
            duration_ms,
        }
    }

    fn passed(&self, name: &str) -> bool {
        self.success && self.passed.contains(name)
    }
}

fn parse_test_results(text: &str) -> (HashSet<String>, bool) {
    let passed: HashSet<_> = text
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("test ")
                .and_then(|line| line.strip_suffix(" ... ok"))
                .map(str::to_owned)
        })
        .collect();
    let summary = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("test result: ok. "))
        .filter_map(|line| line.split_whitespace().next()?.parse::<usize>().ok())
        .next();
    let complete = summary.is_some_and(|count| count > 0 && count == passed.len());
    (passed, complete)
}

struct Evidence {
    lib: TestSuite,
    api: TestSuite,
    ui: TestSuite,
    crash: TestSuite,
    snapshot: TestSuite,
    driver: TestSuite,
}

fn collect_evidence(run_dir: &Path) -> Result<Evidence, String> {
    Ok(Evidence {
        lib: TestSuite::run(
            run_dir,
            "lib",
            &[
                "test", "-p", "graphrun", "--lib", "--locked", "--color", "never",
            ],
        )?,
        api: TestSuite::run(
            run_dir,
            "api",
            &[
                "test", "-p", "graphrun", "--test", "api", "--locked", "--color", "never",
            ],
        )?,
        ui: TestSuite::run(
            run_dir,
            "ui",
            &[
                "test", "-p", "graphrun", "--test", "ui", "--locked", "--color", "never",
            ],
        )?,
        crash: TestSuite::run(
            run_dir,
            "crash",
            &[
                "test",
                "-p",
                "graphrun",
                "--test",
                "crash_cut",
                "--features",
                "fault-injection",
                "--locked",
                "--color",
                "never",
            ],
        )?,
        snapshot: TestSuite::run(
            run_dir,
            "snapshot",
            &[
                "test",
                "-p",
                "graphrun",
                "--lib",
                "snapshot_controller_fires_at_20000_entries",
                "--locked",
                "--color",
                "never",
                "--",
                "--ignored",
                "--show-output",
            ],
        )?,
        driver: TestSuite::run(
            run_dir,
            "driver",
            &[
                "test",
                "-p",
                "graphrun-e2e",
                "--bin",
                "graphrun-e2e",
                "--locked",
                "--color",
                "never",
            ],
        )?,
    })
}

fn run_case(cli: &Path, artifacts: &Path, evidence: &Evidence, row: &MatrixRow) -> CaseResult {
    let started = Instant::now();
    let result = match row.id.as_str() {
        "DSL-001" => yaml_validate_all(cli, artifacts, row),
        "DSL-002" => invalid_graph(cli, artifacts, row),
        "DSL-003" => from_tests(
            evidence,
            row,
            &[
                "compiler::tests::bindings_and_conditions_match_spec",
                "domain::tests::binding_eval_missing_null_and_no_coercion",
            ],
        ),
        "DSL-004" => from_tests(
            evidence,
            row,
            &[
                "compiler::tests::digest_is_stable",
                "compiler::tests::digest_changes_when_semantics_change",
            ],
        ),
        "SEC-002" => all_of(
            row,
            vec![
                oversized_definition(cli, artifacts, row),
                from_test(
                    evidence,
                    row,
                    "compiler::tests::rejects_yaml_tags_and_anchors",
                ),
            ],
        ),
        "API-001" => from_suite(row, &evidence.api, &[]),
        "API-002" => from_suite(row, &evidence.ui, &[]),
        "API-003" => from_api_test(evidence, row, "yaml_and_builder_while_match"),
        "API-004" => from_api_test(evidence, row, "field_path_and_cross_scope_errors"),
        "API-005" => from_api_test(
            evidence,
            row,
            "timed_wait_yaml_and_builder_and_invalid_edge",
        ),
        "DOM-001" => from_test(
            evidence,
            row,
            "domain::tests::reconstruct_matches_live_state",
        ),
        "DOM-002" => from_test(
            evidence,
            row,
            "domain::tests::duplicate_command_does_not_reapply",
        ),
        "DOM-003" => from_test(
            evidence,
            row,
            "domain::tests::nested_child_complete_does_not_complete_root",
        ),
        "DOM-004" => from_test(
            evidence,
            row,
            "domain::tests::generated_nested_controls_match_oracle",
        ),
        "LOOP-001" => all_of(
            row,
            vec![
                local_start(cli, artifacts, row, "while.yaml", r#"{"value":0}"#, "3"),
                local_start_in(
                    cli,
                    artifacts,
                    row,
                    "while-zero",
                    "while.yaml",
                    r#"{"value":5}"#,
                    r#""value":5"#,
                ),
            ],
        ),
        "LOOP-002" => local_start(cli, artifacts, row, "do-while.yaml", r#"{"value":0}"#, "3"),
        "LOOP-003" => local_start(
            cli,
            artifacts,
            row,
            "repeat.yaml",
            r#"{"count":3,"counter":{"value":1}}"#,
            "4",
        ),
        "LOOP-004" => from_test(evidence, row, "domain::tests::loop_limit_boundary"),
        "LOOP-005" => from_test(
            evidence,
            row,
            "domain::tests::loop_retry_is_not_another_iteration",
        ),
        "LOOP-006" => local_start(
            cli,
            artifacts,
            row,
            "foreach.yaml",
            r#"[{"value":3},{"value":1},{"value":3}]"#,
            "4",
        ),
        "LOOP-007" => from_test(
            evidence,
            row,
            "domain::tests::foreach_respects_concurrency_window",
        ),
        "LOOP-008" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::nested_controls_survive_leader_loss",
                ),
                cluster_nested_survives_leader_kill(cli, artifacts, row),
            ],
        ),
        "PAR-001" => local_start(
            cli,
            artifacts,
            row,
            "parallel.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "100",
        ),
        "PAR-002" => from_test(evidence, row, "domain::tests::parallel_join_is_idempotent"),
        "PAR-003" => from_test(
            evidence,
            row,
            "domain::tests::parallel_sibling_cancels_on_branch_failure",
        ),
        "PAR-004" => from_test(
            evidence,
            row,
            "domain::tests::claim_fairness_shares_capacity",
        ),
        "PAR-005" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "engine::tests::snapshot_unfinished_parallel_branches",
                ),
                snapshot_unfinished_parallel_cli(cli, artifacts, row),
            ],
        ),
        "EVT-001" => from_test(
            evidence,
            row,
            "domain::tests::event_before_wait_is_consumed",
        ),
        "EVT-002" => from_test(evidence, row, "domain::tests::event_identity_and_fifo"),
        "EVT-003" => from_test(
            evidence,
            row,
            "domain::tests::repeated_waits_and_concurrent_keys",
        ),
        "EVT-004" => from_test(evidence, row, "domain::tests::reserved_event_survives_ttl"),
        "EVT-006" => from_test(evidence, row, "domain::tests::inbox_quota_is_explicit"),
        "EVT-007" => from_test(evidence, row, "domain::tests::reserved_event_survives_ttl"),
        "EVT-008" => from_test(evidence, row, "domain::tests::cancel_releases_reservation"),
        "EVT-005" => all_of(
            row,
            vec![
                from_test(evidence, row, "cluster::tests::leader_loss_mid_event_wait"),
                cluster_event_survives_leader_kill(cli, artifacts, row),
            ],
        ),
        "SAGA-001" => from_test(
            evidence,
            row,
            "domain::tests::obligation_registers_with_forward_success",
        ),
        "SAGA-002" => from_test(
            evidence,
            row,
            "engine::tests::local_saga_failure_compensates",
        ),
        "SAGA-004" => from_test(evidence, row, "engine::tests::local_nested_saga_transfers"),
        "SAGA-005" => from_test(
            evidence,
            row,
            "domain::tests::nested_saga_does_not_double_compensate",
        ),
        "SAGA-012" => from_test(
            evidence,
            row,
            "domain::tests::compensation_binding_failure_keeps_forward_success",
        ),
        "SAGA-003" => from_test(
            evidence,
            row,
            "domain::tests::parallel_saga_compensates_after_join",
        ),
        "SAGA-008" => from_test(
            evidence,
            row,
            "domain::tests::compensation_reported_error_policy",
        ),
        "SAGA-009" => from_tests(
            evidence,
            row,
            &[
                "domain::tests::irreversible_effect_fails_closed",
                "domain::tests::operator_abandons_blocked_compensation",
            ],
        ),
        "SAGA-010" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "engine::tests::cancel_during_saga_compensates",
                ),
                cancel_saga_cli(cli, artifacts, row),
            ],
        ),
        "SAGA-011" => from_test(
            evidence,
            row,
            "domain::tests::successful_saga_does_not_reopen_on_later_failure",
        ),
        "SAGA-013" => from_test(
            evidence,
            row,
            "domain::tests::compensation_reported_error_policy",
        ),
        "SAGA-014" => from_test(
            evidence,
            row,
            "domain::tests::operator_resolves_blocked_compensation",
        ),
        "SAGA-006" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "engine::tests::compensation_resumes_after_restart",
                ),
                compensation_restart_cli(cli, artifacts, row),
            ],
        ),
        "SAGA-007" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "domain::tests::unknown_blocks_undo_until_applied",
                ),
                provider_delayed_forward(cli, artifacts, row),
            ],
        ),
        "ACT-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "ACT-002" => from_test(
            evidence,
            row,
            "engine::tests::blocking_cancel_does_not_prove_termination",
        ),
        "ACT-007" => from_test(
            evidence,
            row,
            "domain::tests::renewal_does_not_extend_attempt_deadline",
        ),
        "ACT-004" => from_tests(
            evidence,
            row,
            &[
                "domain::tests::loop_retry_is_not_another_iteration",
                "domain::tests::terminal_error_does_not_retry",
            ],
        ),
        "ACT-008" => from_test(
            evidence,
            row,
            "domain::tests::run_deadline_blocks_forward_claims_not_settlement",
        ),
        "ACT-003" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::forged_worker_results_are_rejected",
                ),
                forged_worker_process(cli, artifacts, row),
            ],
        ),
        "ACT-005" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::remote_recon_and_idempotent_effect",
                ),
                provider_recon_outcomes(artifacts, row),
            ],
        ),
        "ACT-006" => provider_idempotent_effect(artifacts, row),
        "POLICY-001" => from_test(
            evidence,
            row,
            "domain::tests::captured_policy_defaults_are_stable",
        ),
        "STORE-001" => from_test(evidence, row, "storage::tests::openraft_storage_suite"),
        "STORE-002" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "storage::tests::log_cut_after_persist_keeps_entries",
                ),
                from_suite(row, &evidence.crash, &[]),
            ],
        ),
        "STORE-003" => from_test(
            evidence,
            row,
            "storage::tests::apply_cut_does_not_mix_transactions",
        ),
        "STORE-004" => from_test(
            evidence,
            row,
            "storage::tests::snapshot_install_cut_keeps_generation",
        ),
        "STORE-005" => from_test(
            evidence,
            row,
            "storage::tests::purge_does_not_drop_domain_history",
        ),
        "STORE-006" => from_tests(
            evidence,
            row,
            &[
                "storage::tests::unapplied_credits_bound_append_without_truncating_reads",
                "write::tests::unapplied_credits_reject_above_limit",
            ],
        ),
        "STORE-007" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::learner_catchup_then_voter_replacement",
                ),
                cluster_join_promote(cli, artifacts, row),
            ],
        ),
        "STORE-008" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "engine::tests::logical_restore_suspends_until_authorized",
                ),
                disaster_restore_cli(cli, artifacts, row),
            ],
        ),
        "CLUSTER-001" => cluster_three_and_workers(cli, artifacts, row),
        "CLUSTER-002" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::leader_loss_does_not_reset_ready_work",
                ),
                cluster_leader_kill_keeps_result(cli, artifacts, row),
            ],
        ),
        "CLUSTER-005" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::workers_scale_without_membership_change",
                ),
                cluster_extra_workers(cli, artifacts, row),
            ],
        ),
        "CLUSTER-003" => all_of(
            row,
            vec![
                from_test(
                    evidence,
                    row,
                    "cluster::tests::quorum_loss_does_not_invent_authority",
                ),
                cluster_quorum_loss(cli, artifacts, row),
            ],
        ),
        "CLUSTER-004" => from_test(evidence, row, "cluster::tests::clock_rollback_fails_closed"),
        "CLUSTER-006" => from_test(
            evidence,
            row,
            "domain::tests::expired_claim_rejects_renewal",
        ),
        "SEC-001" => from_test(evidence, row, "cluster::tests::wrong_ca_is_rejected"),
        "LOCAL-001" => local_start(
            cli,
            artifacts,
            row,
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        "LOCAL-002" => local_restart_wait(cli, artifacts, row),
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
        "REPLAY-002" => replay_readonly(cli, artifacts, row),
        "REPLAY-004" => all_of(
            row,
            vec![
                from_test(evidence, row, "engine::tests::snapshot_writes_file"),
                from_suite(
                    row,
                    &evidence.snapshot,
                    &["engine::tests::snapshot_controller_fires_at_20000_entries"],
                ),
            ],
        ),
        "REPLAY-003" => from_test(
            evidence,
            row,
            "domain::tests::expired_history_range_is_unavailable",
        ),
        "E2E-001" => all_of(
            row,
            vec![
                e2e_all_yaml(cli, artifacts, row),
                rust_builder_sequence(artifacts, row),
            ],
        ),
        "E2E-002" => e2e_no_ready_scan(cli, artifacts, row),
        "PERF-001" => perf_command_compile(cli, artifacts, row),
        "PERF-002" => perf_history_snapshot(cli, artifacts, evidence, row),
        "GATE-001" => from_suite(
            row,
            &evidence.driver,
            &[
                "tests::coverage_rejects_missing_duplicate_and_stale_cases",
                "tests::test_evidence_requires_executed_pass_and_successful_exit",
            ],
        ),
        "GATE-002" => from_suite(
            row,
            &evidence.driver,
            &["tests::status_and_certification_gate"],
        ),
        "CONTRACT-001" | "CONTRACT-002" | "CONTRACT-003" => fail(
            row,
            "contract acceptance",
            "normative contract defined; runtime acceptance not implemented yet",
        ),
        _ => fail(row, "unimplemented", "no implementation evidence yet"),
    };
    CaseResult {
        duration_ms: started.elapsed().as_millis().max(result.duration_ms),
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
    let mut observations = Vec::new();
    let mut ok = 0;
    match fs::read_dir(examples()) {
        Ok(entries) => {
            for entry in entries {
                let path = match entry {
                    Ok(entry) => entry.path(),
                    Err(err) => {
                        failures.push(format!("cannot enumerate examples: {err}"));
                        continue;
                    }
                };
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
                    Ok(output) if output.status.success() => {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        let valid = serde_json::from_slice::<serde_json::Value>(&output.stdout)
                            .ok()
                            .is_some_and(|body| {
                                body.get("status").and_then(serde_json::Value::as_str) == Some("ok")
                                    && body
                                        .get("digest")
                                        .and_then(serde_json::Value::as_str)
                                        .is_some()
                            });
                        observations.push(format!("{}: {stdout}", path.display()));
                        if valid {
                            ok += 1;
                        } else {
                            failures.push(format!("{}: invalid validation result", path.display()));
                        }
                    }
                    Ok(output) => failures.push(format!(
                        "{}: {}",
                        path.display(),
                        String::from_utf8_lossy(&output.stderr)
                    )),
                    Err(err) => failures.push(format!("{}: {err}", path.display())),
                }
            }
        }
        Err(err) => failures.push(format!("cannot read examples directory: {err}")),
    }
    let log = artifacts.join(format!("{}-validate.log", row.id));
    if let Err(err) = fs::write(
        &log,
        format!("{}\n{}", observations.join("\n"), failures.join("\n")),
    ) {
        return fail(
            row,
            "validate fixtures",
            format!("cannot write evidence: {err}"),
        );
    }
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
        if i + 1 == 600 {
            body.push_str(&format!(
                "  n{i}:\n    kind: complete\n    output: {{literal: null}}\n"
            ));
        } else {
            body.push_str(&format!(
                "  n{i}:\n    kind: delay\n    duration: \"1ms\"\n    next: n{}\n",
                i + 1
            ));
        }
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
    local_start_in(cli, artifacts, row, "data", yaml, input, expect)
}

fn local_start_in(
    cli: &Path,
    artifacts: &Path,
    row: &MatrixRow,
    name: &str,
    yaml: &str,
    input: &str,
    expect: &str,
) -> CaseResult {
    let dir = artifacts.join(format!("{}-{name}", row.id));
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
            if let Err(err) = fs::write(&log, format!("{stdout}\n{stderr}")) {
                return fail(
                    row,
                    "graphrun start",
                    format!("cannot write evidence: {err}"),
                );
            }
            let body = serde_json::from_str::<serde_json::Value>(&stdout);
            let ok = output.status.success()
                && body.as_ref().is_ok_and(|body| {
                    body.get("status").and_then(serde_json::Value::as_str) == Some("succeeded")
                        && (expect == "succeeded"
                            || body
                                .get("output")
                                .and_then(|value| serde_json::to_string(value).ok())
                                .is_some_and(|output| output.contains(expect)))
                });
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

fn from_test(evidence: &Evidence, row: &MatrixRow, name: &str) -> CaseResult {
    from_suite(row, &evidence.lib, &[name])
}

fn from_api_test(evidence: &Evidence, row: &MatrixRow, name: &str) -> CaseResult {
    from_suite(row, &evidence.api, &[name])
}

fn from_tests(evidence: &Evidence, row: &MatrixRow, names: &[&str]) -> CaseResult {
    from_suite(row, &evidence.lib, names)
}

fn from_suite(row: &MatrixRow, suite: &TestSuite, names: &[&str]) -> CaseResult {
    let missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !suite.passed(name))
        .collect();
    let mut result = finish(
        row,
        if suite.success && missing.is_empty() {
            "PASS"
        } else {
            "FAIL"
        },
        &suite.command,
        if suite.success && missing.is_empty() {
            format!("{} executed tests passed", suite.passed.len())
        } else {
            format!(
                "suite failed or missing/ignored tests: {}",
                missing.join(", ")
            )
        },
        vec![suite.log.clone()],
    );
    result.duration_ms = suite.duration_ms;
    result
}

fn fail(row: &MatrixRow, command: &str, actual: impl Into<String>) -> CaseResult {
    finish(row, "FAIL", command, actual.into(), Vec::new())
}

fn all_of(row: &MatrixRow, parts: Vec<CaseResult>) -> CaseResult {
    let ok = parts.iter().all(|part| part.status == "PASS");
    let duration_ms = parts.iter().map(|part| part.duration_ms).sum();
    let command = parts
        .iter()
        .map(|part| part.command.clone())
        .collect::<Vec<_>>()
        .join(" ; ");
    let actual = parts
        .iter()
        .map(|part| format!("{}: {}", part.status, part.actual))
        .collect::<Vec<_>>()
        .join(" | ");
    let artifacts: Vec<String> = parts.into_iter().flat_map(|part| part.artifacts).collect();
    let mut result = finish(
        row,
        if ok { "PASS" } else { "FAIL" },
        command,
        actual,
        artifacts.into_iter().map(PathBuf::from).collect(),
    );
    result.duration_ms = duration_ms;
    result
}

fn parse_peer(
    spec: &str,
    ca_pem: &str,
) -> Result<(u64, (std::net::SocketAddr, graphrun::TlsMaterial)), String> {
    let parts: Vec<&str> = spec.split('|').collect();
    if parts.len() != 5 {
        return Err(format!(
            "peer spec must be id|addr|cert|key|server_name: {spec}"
        ));
    }
    let id: u64 = parts[0].parse().map_err(|err| format!("peer id: {err}"))?;
    let addr: std::net::SocketAddr = parts[1]
        .parse()
        .map_err(|err| format!("peer addr: {err}"))?;
    Ok((
        id,
        (
            addr,
            graphrun::TlsMaterial {
                ca_pem: ca_pem.to_owned(),
                cert_pem: fs::read_to_string(parts[2]).map_err(|err| err.to_string())?,
                key_pem: fs::read_to_string(parts[3]).map_err(|err| err.to_string())?,
                server_name: parts[4].to_owned(),
            },
        ),
    ))
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
        run_id: String::new(),
        source_sha256: String::new(),
        cli_sha256: String::new(),
        cli_version: String::new(),
        driver_sha256: String::new(),
        driver_version: String::new(),
        performance: None,
    }
}

struct ChildProc(Child);

impl ChildProc {
    fn new(child: Child) -> Self {
        ACTIVE_CHILDREN.fetch_add(1, Ordering::SeqCst);
        Self(child)
    }
}

impl Drop for ChildProc {
    fn drop(&mut self) {
        let outcome = terminate_status(&mut self.0);
        let event = ChildExit {
            pid: self.0.id(),
            status: outcome.as_ref().ok().map(ToString::to_string),
            error: outcome.err(),
        };
        if let Ok(mut exits) = CHILD_EXITS.lock() {
            exits.push(event);
        } else {
            CHILD_LEDGER_ERROR.store(true, Ordering::SeqCst);
        }
        ACTIVE_CHILDREN.fetch_sub(1, Ordering::SeqCst);
    }
}

fn terminate(child: &mut Child) {
    let _ = terminate_status(child);
}

fn terminate_status(child: &mut Child) -> Result<std::process::ExitStatus, String> {
    if let Some(status) = child.try_wait().map_err(|err| err.to_string())? {
        return Ok(status);
    }
    let pid = child.id().to_string();
    let signalled = Command::new("kill")
        .args(["-TERM", &pid])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if signalled {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait().map_err(|err| err.to_string())? {
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    child.kill().map_err(|err| err.to_string())?;
    child.wait().map_err(|err| err.to_string())
}

fn wait_path(path: &Path, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

fn run_cli(cli: &Path, args: &[&str]) -> (bool, String, String) {
    run_cli_timeout(cli, args, Duration::from_secs(60))
}

fn run_cli_timeout(cli: &Path, args: &[&str], timeout: Duration) -> (bool, String, String) {
    let mut child = match Command::new(cli)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => return (false, String::new(), err.to_string()),
    };
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = String::new();
                let mut stderr = String::new();
                if let Some(mut out) = child.stdout.take() {
                    let _ = out.read_to_string(&mut stdout);
                }
                if let Some(mut err) = child.stderr.take() {
                    let _ = err.read_to_string(&mut stderr);
                }
                return (status.success(), stdout, stderr);
            }
            Ok(None) if Instant::now() >= deadline => {
                terminate(&mut child);
                let _ = child.wait();
                return (false, String::new(), "timed out".to_owned());
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(err) => {
                terminate(&mut child);
                return (false, String::new(), err.to_string());
            }
        }
    }
}

fn issue_test_node(
    ca: &graphrun::CertificateAuthority,
    id: u64,
) -> graphrun::Result<graphrun::TlsMaterial> {
    use graphrun::tls::{ClusterId, PeerRole, PrincipalId, PrincipalIdentity, issue_principal};
    use sha2::Digest;
    let cluster = ClusterId::parse(hex::encode(&sha2::Sha256::digest(ca.pem.as_bytes())[..16]))?;
    let identity = PrincipalIdentity::new(
        cluster,
        PrincipalId::parse(id.to_string())?,
        [
            PeerRole::Member,
            PeerRole::Worker,
            PeerRole::Client,
            PeerRole::Admin,
        ],
    )?;
    issue_principal(ca, &identity, &format!("node-{id}.graphrun.local"))
}

fn unused_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn local_restart_wait(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-data", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let mut serve = match Command::new(cli)
        .args(["serve", "--local-dir", dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "graphrun serve", err.to_string()),
    };
    if !wait_path(&dir.join("control.sock"), Duration::from_secs(10)) {
        return fail(row, "graphrun serve", "control socket did not appear");
    }
    std::thread::sleep(Duration::from_millis(200));
    let input_path = dir.join("input.json");
    let _ = fs::write(&input_path, r#"{"key":"k1"}"#);
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("events.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input_path.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        return fail(
            row,
            "graphrun start --no-wait",
            format!("{stdout}\n{stderr}"),
        );
    }
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    if run.is_empty() {
        return fail(row, "graphrun start --no-wait", stdout);
    }
    let inspect_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("\"signal\":\"approval\"") {
            break;
        }
        if Instant::now() >= inspect_deadline {
            return fail(row, "inspect pending wait", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    terminate(&mut serve.0);
    drop(serve);
    let sock = dir.join("control.sock");
    let gone = Instant::now() + Duration::from_secs(5);
    while sock.exists() && Instant::now() < gone {
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = fs::remove_file(&sock);
    let mut serve = match Command::new(cli)
        .args(["serve", "--local-dir", dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "graphrun serve restart", err.to_string()),
    };
    if !wait_path(&dir.join("control.sock"), Duration::from_secs(10)) {
        return fail(
            row,
            "graphrun serve restart",
            "control socket missing after restart",
        );
    }
    std::thread::sleep(Duration::from_millis(300));
    let payload = dir.join("approval.json");
    let _ = fs::write(&payload, r#"{"approved":true}"#);
    let event_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "signal",
            "--run",
            &run,
            "--name",
            "approval",
            "--key",
            "k1",
            "--event-id",
            event_id,
            "--payload",
            payload.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
        ],
    );
    if !ok {
        return fail(row, "graphrun signal", format!("{stdout}\n{stderr}"));
    }
    let done = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("succeeded") && body.contains("true") {
            terminate(&mut serve.0);
            return finish(
                row,
                "PASS",
                "graphrun serve restart + signal",
                body,
                vec![dir],
            );
        }
        if Instant::now() >= done {
            terminate(&mut serve.0);
            return fail(row, "inspect after restart", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn replay_readonly(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let started = local_start(
        cli,
        artifacts,
        row,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        "pay-1",
    );
    if started.status != "PASS" {
        return started;
    }
    let dir = artifacts.join(format!("{}-data", row.id));
    let db = dir.join("member.redb");
    let before = match fs::read(&db) {
        Ok(bytes) => bytes,
        Err(err) => return fail(row, "read store", err.to_string()),
    };
    let run = match serde_json::from_str::<serde_json::Value>(&started.actual)
        .ok()
        .and_then(|body| body.get("run")?.as_str().map(str::to_owned))
    {
        Some(run) if run.len() == 32 && run.bytes().all(|byte| byte.is_ascii_hexdigit()) => run,
        _ => {
            return fail(
                row,
                "graphrun replay",
                "start did not return a valid run ID",
            );
        }
    };
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "replay",
            "--local-dir",
            dir.to_str().unwrap(),
            "--run",
            &run,
        ],
    );
    if !ok {
        return fail(row, "graphrun replay", format!("{stdout}\n{stderr}"));
    }
    let replay = match serde_json::from_str::<serde_json::Value>(&stdout) {
        Ok(body) => body,
        Err(err) => return fail(row, "graphrun replay", format!("invalid JSON: {err}")),
    };
    if replay.get("run").and_then(serde_json::Value::as_str) != Some(run.as_str())
        || replay
            .get("events")
            .and_then(serde_json::Value::as_array)
            .is_none_or(|events| events.is_empty())
        || !replay
            .get("output")
            .and_then(|output| serde_json::to_string(output).ok())
            .is_some_and(|output| output.contains("pay-1"))
    {
        return fail(
            row,
            "graphrun replay",
            "replay did not contain the expected run evidence",
        );
    }
    let after = match fs::read(&db) {
        Ok(bytes) => bytes,
        Err(err) => return fail(row, "read store after replay", err.to_string()),
    };
    if after != before {
        return fail(row, "graphrun replay", "replay wrote the store");
    }
    finish(
        row,
        "PASS",
        "graphrun replay --local-dir",
        stdout,
        vec![dir],
    )
}

fn e2e_all_yaml(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let cases = [
        (
            "sequence.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            "pay-1",
        ),
        ("while.yaml", r#"{"value":0}"#, r#""value":3"#),
        ("do-while.yaml", r#"{"value":0}"#, r#""value":3"#),
        (
            "repeat.yaml",
            r#"{"count":3,"counter":{"value":1}}"#,
            r#""value":4"#,
        ),
        (
            "foreach.yaml",
            r#"[{"value":3},{"value":1},{"value":3}]"#,
            r#""value":4"#,
        ),
        (
            "parallel.yaml",
            r#"{"order_id":"o1","amount":1000}"#,
            r#""cents":100"#,
        ),
        ("saga.yaml", r#"{"order_id":"o1","amount":1000}"#, "pay-1"),
        ("timers.yaml", "null", "null"),
        ("remote.yaml", r#"{"value":2}"#, r#""value":2"#),
        (
            "nested-controls.yaml",
            r#"[{"value":1},{"value":4}]"#,
            r#""value":4"#,
        ),
        ("timeout-recovery.yaml", r#"{"value":1}"#, r#""value":1"#),
    ];
    let mut logs = Vec::new();
    let mut case_artifacts = Vec::new();
    for (yaml, input, expect) in cases {
        let result = local_start_in(
            cli,
            artifacts,
            row,
            yaml.trim_end_matches(".yaml"),
            yaml,
            input,
            expect,
        );
        logs.push(format!("{yaml}: {} {}", result.status, result.actual));
        case_artifacts.extend(result.artifacts.into_iter().map(PathBuf::from));
        if result.status != "PASS" {
            return finish(
                row,
                "FAIL",
                format!("graphrun start {yaml}"),
                logs.join("\n"),
                case_artifacts,
            );
        }
    }
    finish(
        row,
        "PASS",
        "graphrun start all completing yaml fixtures",
        logs.join("\n"),
        case_artifacts,
    )
}

fn e2e_report_complete(
    run_dir: &Path,
    rows: &[MatrixRow],
    results: &[CaseResult],
    context: &RunContext,
) -> CaseResult {
    let row = rows.iter().find(|row| row.id == "E2E-003").unwrap();
    let other_rows: Vec<MatrixRow> = rows
        .iter()
        .filter(|row| row.id != "E2E-003")
        .cloned()
        .collect();
    if let Err(err) = validate_coverage(&other_rows, results, run_dir, context) {
        return fail(row, "fresh in-run matrix coverage", err);
    }
    let active = ACTIVE_CHILDREN.load(Ordering::SeqCst);
    if active != 0 {
        return fail(
            row,
            "owned process cleanup",
            format!("{active} owned child processes not reaped"),
        );
    }
    if CHILD_LEDGER_ERROR.load(Ordering::SeqCst) {
        return fail(
            row,
            "owned process cleanup",
            "child exit ledger is unavailable",
        );
    }
    let exits = match CHILD_EXITS.lock() {
        Ok(exits) => exits,
        Err(err) => return fail(row, "owned process cleanup", err.to_string()),
    };
    if exits
        .iter()
        .any(|exit| exit.status.is_none() || exit.error.is_some())
    {
        return fail(
            row,
            "owned process cleanup",
            "a child has no confirmed exit status",
        );
    }
    let path = run_dir.join("owned-children.json");
    let bytes = match serde_json::to_vec_pretty(&*exits) {
        Ok(bytes) => bytes,
        Err(err) => return fail(row, "owned process cleanup", err.to_string()),
    };
    if let Err(err) = fs::write(&path, bytes) {
        return fail(row, "owned process cleanup", err.to_string());
    }
    finish(
        row,
        "PASS",
        "fresh in-run matrix coverage and owned process exits",
        format!(
            "{} other cases validated; {} owned processes reaped",
            results.len(),
            exits.len()
        ),
        vec![path],
    )
}

fn cluster_local_health(
    cli: &Path,
    cluster: &LiveCluster,
    node: usize,
) -> Result<serde_json::Value, String> {
    let member_dir = cluster.dir.join(format!("m{}", node + 1));
    let (ok, body, error) = run_cli_timeout(
        cli,
        &[
            "cluster",
            "health",
            "--local-dir",
            member_dir
                .to_str()
                .ok_or_else(|| "member directory is not UTF-8".to_owned())?,
        ],
        Duration::from_secs(5),
    );
    if !ok {
        return Err(format!(
            "member {} health failed: {body}\n{error}",
            node + 1
        ));
    }
    serde_json::from_str(&body).map_err(|err| format!("member {} health: {err}", node + 1))
}

fn e2e_no_ready_scan(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(error) => return fail(row, "boot three voting members", error),
    };
    let definition = cluster.dir.join("quiescent-wait.yaml");
    if let Err(error) = fs::write(
        &definition,
        "\
dsl: graphrun/v1
id: quiescent_wait
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  continue: {schema: unit/v1}
start: pause
nodes:
  pause:
    kind: wait_signal
    signal: continue
    key: {literal: k1}
    timeout: null
    next: finish
  finish:
    kind: complete
    output: {literal: null}
",
    ) {
        return fail(row, "write wait definition", error.to_string());
    }
    let run = match cluster_start(cli, &cluster, &definition.to_string_lossy(), "null", false) {
        Ok(run) => run,
        Err(error) => return fail(row, "start indefinite wait", error),
    };
    let deadline = Instant::now() + Duration::from_secs(45);
    let before = loop {
        let statuses: Result<Vec<_>, _> = (0..3)
            .map(|node| cluster_local_health(cli, &cluster, node))
            .collect();
        if let Ok(statuses) = &statuses
            && statuses.iter().all(|status| {
                status["active_runs"] == 1
                    && status["pending_waits"] == 1
                    && status["ready_index_discovery_reads"].as_u64().is_some()
                    && (status["state"] == "Follower"
                        || (status["state"] == "Leader"
                            && status["scheduler_observed_revision"]
                                == status["schedule_revision"]))
            })
        {
            break statuses.clone();
        }
        if Instant::now() >= deadline {
            return fail(row, "three-member active wait", format!("{statuses:?}"));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(60));
    let after: Vec<_> = match (0..3)
        .map(|node| cluster_local_health(cli, &cluster, node))
        .collect::<Result<_, _>>()
    {
        Ok(after) => after,
        Err(error) => return fail(row, "three-member final health", error),
    };
    let elapsed = started.elapsed();
    let stable = before.iter().zip(&after).all(|(old, new)| {
        new["active_runs"] == 1
            && new["pending_waits"] == 1
            && old["ready_index_discovery_reads"] == new["ready_index_discovery_reads"]
    });
    let proof = serde_json::json!({
        "run": run,
        "elapsed_ms": elapsed.as_millis(),
        "before": before,
        "after": after,
    });
    let path = cluster.dir.join("quiescent-read-counts.json");
    if let Err(error) = fs::write(&path, proof.to_string()) {
        return fail(row, "write read-count evidence", error.to_string());
    }
    if !stable || elapsed < Duration::from_secs(60) {
        return fail(row, "60s three-voter active wait", proof.to_string());
    }
    finish(
        row,
        "PASS",
        "60s active quiescent three-voter cluster with two independent workers",
        proof.to_string(),
        vec![cluster.dir.clone(), path],
    )
}

fn perf_command_compile(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let catalog = catalog();
    let yaml = examples().join("sequence.yaml");
    let started = Instant::now();
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "validate",
            "--definition",
            yaml.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
        ],
    );
    let compile_ms = started.elapsed().as_millis();
    if !ok {
        return fail(row, "validate timing", format!("{stdout}\n{stderr}"));
    }
    let start = Instant::now();
    let started_run = local_start(
        cli,
        artifacts,
        row,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        "pay-1",
    );
    let local_ms = start.elapsed().as_millis();
    if started_run.status != "PASS" {
        return fail(row, "local readiness timing", started_run.actual);
    }
    let yaml512 = artifacts.join("PERF-001-512.yaml");
    let mut body512 = String::from(
        "dsl: graphrun/v1\nid: n512\nversion: 1\ninput_schema: unit/v1\noutput_schema: unit/v1\nstart: n0\nnodes:\n",
    );
    for i in 0..512 {
        if i + 1 == 512 {
            body512.push_str(&format!(
                "  n{i}:\n    kind: complete\n    output: {{literal: null}}\n"
            ));
        } else {
            body512.push_str(&format!(
                "  n{i}:\n    kind: delay\n    duration: \"1ms\"\n    next: n{}\n",
                i + 1
            ));
        }
    }
    if let Err(err) = fs::write(&yaml512, body512) {
        return fail(row, "512-node fixture", err.to_string());
    }
    let compile512_started = Instant::now();
    let (ok512, _, _) = run_cli(
        cli,
        &[
            "validate",
            "--definition",
            yaml512.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
        ],
    );
    let compile512_ms = compile512_started.elapsed().as_millis();
    if !ok512 {
        return fail(row, "512-node validation", "validation failed");
    }
    let (cmds, p95_us, wall_ms) = match measure_progress_rate(artifacts) {
        Ok(rate) => rate,
        Err(err) => return fail(row, "progress-rate measurement", err),
    };
    let body = format!(
        "validate_cli_wall_ms={compile_ms} validate_512_cli_wall_ms={compile512_ms} local_start_wall_ms={local_ms} progress_commands=1000 progress_commands_per_s={cmds:.1} progress_p95_us={p95_us} progress_wall_ms={wall_ms}"
    );
    perf_blocked(
        row,
        artifacts,
        "measured CLI validation/local startup/single-member progress",
        &body,
        vec![
            format!("validate_cli_wall_ms={compile_ms}"),
            format!("validate_512_cli_wall_ms={compile512_ms}"),
            format!("local_start_wall_ms={local_ms}"),
            format!("progress_commands_per_s={cmds:.1}"),
            format!("progress_p95_us={p95_us}"),
            format!("progress_wall_ms={wall_ms}"),
        ],
    )
}

fn measure_progress_rate(artifacts: &Path) -> Result<(f64, u128, u128), String> {
    let dir = artifacts.join("PERF-001-rate");
    let _ = fs::create_dir_all(&dir);
    let rt = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
    rt.block_on(async {
        let engine = graphrun::Engine::local(&dir)
            .await
            .map_err(|err| err.to_string())?;
        let catalog =
            graphrun::Catalog::from_json(&fs::read(catalog()).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?;
        let yaml =
            fs::read_to_string(examples().join("events.yaml")).map_err(|err| err.to_string())?;
        let input = graphrun::Value::Object(
            [("key".to_owned(), graphrun::Value::String("k1".to_owned()))]
                .into_iter()
                .collect(),
        );
        let run = engine
            .start_yaml(&yaml, &catalog, input)
            .await
            .map_err(|err| err.to_string())?;
        const N: usize = 1_000;
        let mut samples = Vec::with_capacity(N);
        let wall = Instant::now();
        for _ in 0..N {
            let one = Instant::now();
            engine
                .commit_progress(run)
                .await
                .map_err(|err| err.to_string())?;
            samples.push(one.elapsed().as_micros());
        }
        let wall_ms = wall.elapsed().as_millis();
        let _ = engine.shutdown().await;
        samples.sort_unstable();
        let p95 = samples[(N * 95 / 100).min(N - 1)];
        let cmds_per_s = if wall_ms == 0 {
            0.0
        } else {
            (N as f64) / (wall_ms as f64 / 1000.0)
        };
        Ok((cmds_per_s, p95, wall_ms))
    })
}

fn perf_history_snapshot(
    cli: &Path,
    artifacts: &Path,
    evidence: &Evidence,
    row: &MatrixRow,
) -> CaseResult {
    let nested_started = Instant::now();
    let nested = local_start_in(
        cli,
        artifacts,
        row,
        "nested",
        "nested-controls.yaml",
        r#"[{"value":1},{"value":4}]"#,
        "succeeded",
    );
    let nested_ms = nested_started.elapsed().as_millis();
    if nested.status != "PASS" {
        return fail(row, "nested timing", nested.actual);
    }
    let run = match serde_json::from_str::<serde_json::Value>(&nested.actual)
        .ok()
        .and_then(|body| body.get("run")?.as_str().map(str::to_owned))
        .filter(|run| run.len() == 32 && run.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        Some(run) => run,
        None => return fail(row, "nested timing", "start did not return a valid run ID"),
    };
    let dir = artifacts.join(format!("{}-nested", row.id));
    let started = Instant::now();
    let (history_ok, history, history_err) = run_cli(
        cli,
        &[
            "history",
            "--run",
            &run,
            "--local-dir",
            dir.to_str().unwrap(),
        ],
    );
    let history_ms = started.elapsed().as_millis();
    if !history_ok || serde_json::from_str::<serde_json::Value>(&history).is_err() {
        return fail(row, "history timing", format!("{history}\n{history_err}"));
    }
    if !evidence
        .snapshot
        .passed("engine::tests::snapshot_controller_fires_at_20000_entries")
    {
        return fail(
            row,
            "snapshot measurement",
            "snapshot test did not execute and pass",
        );
    }
    let snapshot_note = match fs::read_to_string(&evidence.snapshot.log) {
        Ok(note) => note,
        Err(err) => return fail(row, "snapshot measurement", err.to_string()),
    };
    let snapshot_line = match snapshot_note
        .lines()
        .find(|line| line.contains("snapshot fill writes="))
    {
        Some(line) => line,
        None => return fail(row, "snapshot measurement", "snapshot progress is missing"),
    };
    let body = format!(
        "nested_cli_wall_ms={nested_ms} history_cli_wall_ms={history_ms} snapshot_test_wall_ms={} {snapshot_line}",
        evidence.snapshot.duration_ms
    );
    perf_blocked(
        row,
        artifacts,
        "measured nested/history/snapshot test on this host",
        &body,
        vec![
            format!("nested_cli_wall_ms={nested_ms}"),
            format!("history_cli_wall_ms={history_ms}"),
            format!("snapshot_test_wall_ms={}", evidence.snapshot.duration_ms),
            snapshot_line.to_owned(),
        ],
    )
}

fn perf_blocked(
    row: &MatrixRow,
    artifacts: &Path,
    command: &str,
    body: &str,
    measurements: Vec<String>,
) -> CaseResult {
    let measure_path = artifacts.join(format!("{}-measure.txt", row.id));
    let hardware = format!(
        "os={} arch={} logical_cpus={} observed_members=1 memory=unmeasured storage=unmeasured",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()
            .map(|cpus| cpus.get().to_string())
            .unwrap_or_else(|err| format!("unavailable ({err})"))
    );
    let reason = "reference three same-region 4-vCPU/8-GiB SSD members unavailable; local measurement is not a reference benchmark";
    if let Err(err) = fs::write(&measure_path, format!("{body}\n{hardware}\n{reason}\n")) {
        return fail(row, command, format!("measurement write failed: {err}"));
    }
    let mut result = finish(
        row,
        "BLOCKED",
        command,
        format!("{body}; {hardware}; {reason}"),
        vec![measure_path],
    );
    result.performance = Some(PerformanceEvidence {
        hardware,
        reference_hardware_available: false,
        measurements,
        reason: reason.to_owned(),
    });
    result
}

struct LiveCluster {
    dir: PathBuf,
    ca: graphrun::CertificateAuthority,
    ca_path: PathBuf,
    certs: Vec<(PathBuf, PathBuf, String)>,
    addrs: Vec<String>,
    members: Vec<ChildProc>,
    workers: Vec<ChildProc>,
    e2e: PathBuf,
}

fn boot_three(artifacts: &Path, row: &MatrixRow, workers: usize) -> Result<LiveCluster, String> {
    let dir = artifacts.join(format!("{}-data", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let ca = graphrun::generate_ca().map_err(|err| err.to_string())?;
    let materials = [
        issue_test_node(&ca, 1).map_err(|err| err.to_string())?,
        issue_test_node(&ca, 2).map_err(|err| err.to_string())?,
        issue_test_node(&ca, 3).map_err(|err| err.to_string())?,
    ];
    let ca_path = dir.join("ca.pem");
    fs::write(&ca_path, &ca.pem).map_err(|err| err.to_string())?;
    let mut certs = Vec::new();
    for (i, material) in materials.iter().enumerate() {
        let cert = dir.join(format!("n{}.cert.pem", i + 1));
        let key = dir.join(format!("n{}.key.pem", i + 1));
        fs::write(&cert, &material.cert_pem).map_err(|err| err.to_string())?;
        fs::write(&key, &material.key_pem).map_err(|err| err.to_string())?;
        certs.push((cert, key, material.server_name.clone()));
    }
    let addrs: Vec<String> = (0..3)
        .map(|_| format!("127.0.0.1:{}", unused_port()))
        .collect();
    let e2e = std::env::current_exe().map_err(|err| err.to_string())?;
    let mut cluster = LiveCluster {
        dir,
        ca,
        ca_path,
        certs,
        addrs,
        members: Vec::new(),
        workers: Vec::new(),
        e2e,
    };
    for node in [2usize, 3] {
        cluster
            .members
            .push(spawn_fixture_member(&cluster, node, false)?);
    }
    std::thread::sleep(Duration::from_millis(400));
    cluster
        .members
        .push(spawn_fixture_member(&cluster, 1, true)?);
    let tcp_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < tcp_deadline {
        if std::net::TcpStream::connect(&cluster.addrs[0]).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));
    for i in 0..workers {
        cluster.workers.push(spawn_fixture_worker(&cluster, i)?);
    }
    if workers > 0 {
        std::thread::sleep(Duration::from_millis(800));
    }
    Ok(cluster)
}

fn spawn_fixture_member(
    cluster: &LiveCluster,
    node: usize,
    initialize: bool,
) -> Result<ChildProc, String> {
    let data = cluster.dir.join(format!("m{node}"));
    let _ = fs::create_dir_all(&data);
    let log = cluster.dir.join(format!("member-{node}.log"));
    let mut cmd = Command::new(&cluster.e2e);
    cmd.args([
        "fixture-member",
        "--data-dir",
        data.to_str().unwrap(),
        "--bind",
        &cluster.addrs[node - 1],
        "--node-id",
        &node.to_string(),
        "--ca",
        cluster.ca_path.to_str().unwrap(),
        "--cert",
        cluster.certs[node - 1].0.to_str().unwrap(),
        "--key",
        cluster.certs[node - 1].1.to_str().unwrap(),
        "--server-name",
        &cluster.certs[node - 1].2,
    ]);
    if initialize {
        cmd.arg("--initialize");
    }
    for other in 1usize..=cluster.addrs.len() {
        if other != node {
            cmd.arg("--peer").arg(format!(
                "{}|{}|{}|{}|{}",
                other,
                cluster.addrs[other - 1],
                cluster.certs[other - 1].0.display(),
                cluster.certs[other - 1].1.display(),
                cluster.certs[other - 1].2
            ));
        }
    }
    cmd.stdout(Stdio::null())
        .stderr(fs::File::create(log).map_err(|err| err.to_string())?);
    cmd.spawn()
        .map(ChildProc::new)
        .map_err(|err| err.to_string())
}

fn spawn_fixture_worker(cluster: &LiveCluster, i: usize) -> Result<ChildProc, String> {
    spawn_worker_on(cluster, i, 0)
}

fn spawn_workers_on_survivors(
    cluster: &mut LiveCluster,
    count: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last = "no worker stayed up on survivors".to_owned();
    let mut next = cluster.workers.len();
    while cluster.workers.len() < count {
        if Instant::now() >= deadline {
            return Err(last);
        }
        for endpoint in 1..cluster.addrs.len() {
            if cluster.workers.len() >= count {
                break;
            }
            match spawn_worker_on(cluster, next, endpoint) {
                Ok(mut worker) => {
                    std::thread::sleep(Duration::from_millis(250));
                    match worker.0.try_wait() {
                        Ok(None) => {
                            cluster.workers.push(worker);
                            next += 1;
                        }
                        Ok(Some(status)) => last = format!("worker exited {status}"),
                        Err(err) => last = err.to_string(),
                    }
                }
                Err(err) => last = err,
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

fn spawn_worker_on(
    cluster: &LiveCluster,
    i: usize,
    endpoint_node: usize,
) -> Result<ChildProc, String> {
    let cert = &cluster.certs[endpoint_node];
    let log = cluster.dir.join(format!("worker-{}.log", i + 1));
    let mut cmd = Command::new(&cluster.e2e);
    cmd.args([
        "fixture-worker",
        "--endpoint",
        &format!("https://{}", cluster.addrs[endpoint_node]),
        "--ca",
        cluster.ca_path.to_str().unwrap(),
        "--cert",
        cert.0.to_str().unwrap(),
        "--key",
        cert.1.to_str().unwrap(),
        "--server-name",
        &cert.2,
    ]);
    cmd.stdout(Stdio::null());
    if let Ok(file) = fs::File::create(log) {
        cmd.stderr(file);
    }
    cmd.spawn()
        .map(ChildProc::new)
        .map_err(|err| err.to_string())
}

fn cluster_connect<'a>(cluster: &'a LiveCluster, node: usize, extra: &[&'a str]) -> Vec<String> {
    let mut args: Vec<String> = extra.iter().map(|part| (*part).to_owned()).collect();
    args.extend([
        "--endpoint".to_owned(),
        format!("https://{}", cluster.addrs[node]),
        "--ca".to_owned(),
        cluster.ca_path.display().to_string(),
        "--cert".to_owned(),
        cluster.certs[0].0.display().to_string(),
        "--tls-key".to_owned(),
        cluster.certs[0].1.display().to_string(),
        "--server-name".to_owned(),
        cluster.certs[node].2.clone(),
    ]);
    args
}

fn cluster_start(
    cli: &Path,
    cluster: &LiveCluster,
    yaml: &str,
    input: &str,
    wait: bool,
) -> Result<String, String> {
    let input_path = cluster.dir.join("input.json");
    fs::write(&input_path, input).map_err(|err| err.to_string())?;
    let definition = if Path::new(yaml).is_absolute() {
        PathBuf::from(yaml)
    } else {
        examples().join(yaml)
    };
    let mut last = String::new();
    let command_id = graphrun::ids::CommandId::generate().to_hex();
    let deadline = Instant::now() + Duration::from_secs(25);
    while Instant::now() < deadline {
        for node in 0..cluster.addrs.len() {
            let mut args = vec![
                "start".to_owned(),
                "--definition".to_owned(),
                definition.display().to_string(),
                "--catalog".to_owned(),
                catalog().display().to_string(),
                "--input".to_owned(),
                input_path.display().to_string(),
                "--command-id".to_owned(),
                command_id.clone(),
            ];
            if !wait {
                args.push("--no-wait".to_owned());
            }
            args.extend(cluster_connect(cluster, node, &[]));
            let strs: Vec<&str> = args.iter().map(String::as_str).collect();
            let (ok, stdout, stderr) = run_cli(cli, &strs);
            last = format!("{stdout}\n{stderr}");
            if ok {
                match serde_json::from_str::<serde_json::Value>(&stdout) {
                    Ok(response) => match response.get("run").and_then(|run| run.as_str()) {
                        Some(run) => match graphrun::RunId::from_hex(run) {
                            Ok(_) => return Ok(run.to_owned()),
                            Err(error) => last = format!("invalid start run identity: {error}"),
                        },
                        None => last = format!("start response has no run: {stdout}"),
                    },
                    Err(error) => last = format!("invalid start response: {error}: {stdout}"),
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(last)
}

fn cluster_inspect(cli: &Path, cluster: &LiveCluster, node: usize, run: &str) -> String {
    let args = cluster_connect(cluster, node, &["inspect", "--run", run]);
    let strs: Vec<&str> = args.iter().map(String::as_str).collect();
    let (_, body, stderr) = run_cli_timeout(cli, &strs, Duration::from_secs(3));
    if body.is_empty() { stderr } else { body }
}

fn inspect_succeeded(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    value.get("status").and_then(|status| status.as_str()) == Some("succeeded")
        && value.get("output").is_some_and(|output| !output.is_null())
}

fn inspect_known_run(body: &str, run: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    value.get("run").and_then(|id| id.as_str()) == Some(run)
        && value.get("error").is_none_or(|error| error.is_null())
}

fn wait_nodes_succeeded(
    cli: &Path,
    cluster: &LiveCluster,
    run: &str,
    nodes: impl IntoIterator<Item = usize> + Clone,
    min_nodes: usize,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    loop {
        let mut ok = 0;
        for node in nodes.clone() {
            last = cluster_inspect(cli, cluster, node, run);
            if inspect_succeeded(&last) {
                ok += 1;
            }
        }
        if ok >= min_nodes {
            return Ok(last);
        }
        if Instant::now() >= deadline {
            return Err(last);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn wait_inspect(
    cli: &Path,
    cluster: &LiveCluster,
    run: &str,
    needle: &str,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    let mut last = String::new();
    while Instant::now() < deadline {
        for node in 0..cluster.addrs.len() {
            last = cluster_inspect(cli, cluster, node, run);
            if last.contains(needle) {
                return Ok(last);
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err(last)
}

fn cluster_three_and_workers(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let run = match cluster_start(
        cli,
        &cluster,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        true,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start", err),
    };
    match wait_inspect(cli, &cluster, &run, "pay-1", Duration::from_secs(20)) {
        Ok(body) if body.contains("succeeded") => finish(
            row,
            "PASS",
            "3 fixture-member + 2 fixture-worker + production CLI",
            body,
            vec![cluster.dir.clone()],
        ),
        Ok(body) | Err(body) => fail(row, "cluster inspect", body),
    }
}

fn cluster_leader_kill_keeps_result(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let run = match cluster_start(
        cli,
        &cluster,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        true,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start", err),
    };
    if let Err(err) = wait_inspect(cli, &cluster, &run, "pay-1", Duration::from_secs(20)) {
        return fail(row, "cluster inspect", err);
    }
    terminate(&mut cluster.members[2].0);
    let body = cluster_inspect(cli, &cluster, 1, &run);
    if body.contains("succeeded") && body.contains("pay-1") {
        finish(
            row,
            "PASS",
            "kill initializing leader; follower inspect keeps output",
            body,
            vec![cluster.dir.clone()],
        )
    } else {
        fail(row, "inspect after leader kill", body)
    }
}

fn cluster_quorum_loss(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 0) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    terminate(&mut cluster.members[0].0);
    terminate(&mut cluster.members[1].0);
    let input = cluster.dir.join("input.json");
    let _ = fs::write(&input, r#"{"order_id":"o1","amount":1000}"#);
    let args = cluster_connect(
        &cluster,
        0,
        &[
            "start",
            "--definition",
            examples().join("sequence.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--no-wait",
        ],
    );
    let strs: Vec<&str> = args.iter().map(String::as_str).collect();
    let (ok, stdout, stderr) = run_cli_timeout(cli, &strs, Duration::from_secs(5));
    if ok {
        return fail(
            row,
            "start after quorum loss",
            format!("start succeeded without quorum: {stdout}"),
        );
    }
    finish(
        row,
        "PASS",
        "two of three members killed; start does not invent authority",
        format!("{stdout}\n{stderr}"),
        vec![cluster.dir.clone()],
    )
}

fn cluster_extra_workers(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    match spawn_fixture_worker(&cluster, 2) {
        Ok(worker) => cluster.workers.push(worker),
        Err(err) => return fail(row, "extra worker", err),
    }
    match spawn_fixture_worker(&cluster, 3) {
        Ok(worker) => cluster.workers.push(worker),
        Err(err) => return fail(row, "extra worker", err),
    }
    std::thread::sleep(Duration::from_millis(400));
    let run = match cluster_start(
        cli,
        &cluster,
        "sequence.yaml",
        r#"{"order_id":"o2","amount":1000}"#,
        true,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start", err),
    };
    let body = match wait_inspect(cli, &cluster, &run, "pay-1", Duration::from_secs(20)) {
        Ok(body) => body,
        Err(err) => return fail(row, "inspect extra workers", err),
    };
    let health = {
        let args = cluster_connect(&cluster, 0, &["cluster", "health"]);
        let strs: Vec<&str> = args.iter().map(String::as_str).collect();
        let (_, stdout, stderr) = run_cli(cli, &strs);
        format!("{stdout}\n{stderr}")
    };
    if body.contains("succeeded") && cluster.workers.len() == 4 {
        finish(
            row,
            "PASS",
            "four workers on unchanged three-voter membership",
            format!("{body}\n{health}"),
            vec![cluster.dir.clone()],
        )
    } else {
        fail(row, "extra workers", format!("{body}\n{health}"))
    }
}

fn cluster_event_survives_leader_kill(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let run = match cluster_start(cli, &cluster, "events.yaml", r#"{"key":"k1"}"#, false) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start events", err),
    };
    if let Err(err) = wait_inspect(
        cli,
        &cluster,
        &run,
        "\"signal\":\"approval\"",
        Duration::from_secs(15),
    ) {
        return fail(row, "pending wait", err);
    }
    terminate(&mut cluster.members[2].0);
    std::thread::sleep(Duration::from_millis(800));
    let payload = cluster.dir.join("approval.json");
    let _ = fs::write(&payload, r#"{"approved":true}"#);
    let mut last = String::new();
    let mut signaled = false;
    let signal_deadline = Instant::now() + Duration::from_secs(12);
    while Instant::now() < signal_deadline && !signaled {
        for node in 1..cluster.addrs.len() {
            let args = cluster_connect(
                &cluster,
                node,
                &[
                    "signal",
                    "--run",
                    &run,
                    "--name",
                    "approval",
                    "--key",
                    "k1",
                    "--event-id",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--payload",
                    payload.to_str().unwrap(),
                ],
            );
            let strs: Vec<&str> = args.iter().map(String::as_str).collect();
            let (ok, stdout, stderr) = run_cli_timeout(cli, &strs, Duration::from_secs(4));
            last = format!("{stdout}\n{stderr}");
            if ok {
                signaled = true;
                break;
            }
        }
        if !signaled {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    if !signaled {
        return fail(row, "signal after leader kill", last);
    }
    match wait_inspect(cli, &cluster, &run, "succeeded", Duration::from_secs(20)) {
        Ok(body) if body.contains("true") => finish(
            row,
            "PASS",
            "event wait survives leader kill",
            body,
            vec![cluster.dir.clone()],
        ),
        Ok(body) | Err(body) => fail(row, "inspect after signal", body),
    }
}

fn cluster_nested_survives_leader_kill(
    cli: &Path,
    artifacts: &Path,
    row: &MatrixRow,
) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 0) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let run = match cluster_start(
        cli,
        &cluster,
        "nested-controls.yaml",
        r#"[{"value":1},{"value":4}]"#,
        false,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start nested", err),
    };
    let replicated = Instant::now() + Duration::from_secs(20);
    let mut last = String::new();
    loop {
        let mut seen = 0;
        for node in 0..cluster.addrs.len() {
            last = cluster_inspect(cli, &cluster, node, &run);
            if inspect_known_run(&last, &run) {
                seen += 1;
            }
        }
        if seen >= 2 {
            break;
        }
        if Instant::now() >= replicated {
            return fail(row, "replicate nested start before kill", last);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    terminate(&mut cluster.members[2].0);
    if let Err(err) = spawn_workers_on_survivors(&mut cluster, 2, Duration::from_secs(15)) {
        return fail(row, "workers on survivors", err);
    }
    match wait_nodes_succeeded(
        cli,
        &cluster,
        &run,
        1..cluster.addrs.len(),
        1,
        Duration::from_secs(40),
    ) {
        Ok(body) => finish(
            row,
            "PASS",
            "nested-controls output survives leader kill",
            body,
            vec![cluster.dir.clone()],
        ),
        Err(last) => fail(row, "inspect nested after leader kill", last),
    }
}

fn cluster_join_promote(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let mut cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let material = match graphrun::issue_node(&cluster.ca, 4) {
        Ok(material) => material,
        Err(err) => return fail(row, "issue node 4", err.to_string()),
    };
    let cert = cluster.dir.join("n4.cert.pem");
    let key = cluster.dir.join("n4.key.pem");
    let _ = fs::write(&cert, &material.cert_pem);
    let _ = fs::write(&key, &material.key_pem);
    cluster
        .certs
        .push((cert.clone(), key.clone(), material.server_name.clone()));
    cluster.addrs.push(format!("127.0.0.1:{}", unused_port()));
    match spawn_fixture_member(&cluster, 4, false) {
        Ok(child) => cluster.members.push(child),
        Err(err) => return fail(row, "fixture-member 4", err),
    }
    std::thread::sleep(Duration::from_millis(400));
    let leader = cluster.dir.join("m1");
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "cluster",
            "join",
            "--local-dir",
            leader.to_str().unwrap(),
            "--node-id",
            "4",
            "--addr",
            &cluster.addrs[3],
            "--peer-ca",
            cluster.ca_path.to_str().unwrap(),
            "--peer-cert",
            cert.to_str().unwrap(),
            "--peer-tls-key",
            key.to_str().unwrap(),
            "--peer-server-name",
            &material.server_name,
        ],
    );
    if !ok {
        return fail(row, "cluster join", format!("{stdout}\n{stderr}"));
    }
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "cluster",
            "promote",
            "--local-dir",
            leader.to_str().unwrap(),
            "--node-id",
            "4",
        ],
    );
    if !ok {
        return fail(row, "cluster promote", format!("{stdout}\n{stderr}"));
    }
    let run = match cluster_start(
        cli,
        &cluster,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        true,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "start after promote", err),
    };
    match wait_inspect(cli, &cluster, &run, "pay-1", Duration::from_secs(20)) {
        Ok(body) if body.contains("succeeded") => finish(
            row,
            "PASS",
            "learner join + promote then sequence via CLI",
            body,
            vec![cluster.dir.clone()],
        ),
        Ok(body) | Err(body) => fail(row, "inspect after promote", body),
    }
}

fn serve_local(cli: &Path, dir: &Path) -> Result<ChildProc, String> {
    let child = Command::new(cli)
        .args(["serve", "--local-dir", dir.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| err.to_string())?;
    if !wait_path(&dir.join("control.sock"), Duration::from_secs(10)) {
        return Err("control socket missing".to_owned());
    }
    Ok(ChildProc::new(child))
}

fn snapshot_unfinished_parallel_cli(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-snap", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let yaml = dir.join("unfinished-parallel.yaml");
    let _ = fs::write(
        &yaml,
        r#"
dsl: graphrun/v1
id: unfinished_parallel
version: 1
input_schema: unit/v1
output_schema: {tuple: [approval/v1, approval/v1]}
signals:
  approval: {schema: approval/v1}
start: both
nodes:
  both:
    kind: parallel
    branches:
      - name: left
        input: {literal: null}
        body:
          input_schema: unit/v1
          output_schema: approval/v1
          start: wait
          nodes:
            wait:
              kind: wait_signal
              signal: approval
              key: {literal: "k1"}
              consume_from: buffered
              timeout: null
              next: done
            done:
              kind: complete
              output: {from: nodes.wait.output}
      - name: right
        input: {literal: null}
        body:
          input_schema: unit/v1
          output_schema: approval/v1
          start: wait
          nodes:
            wait:
              kind: wait_signal
              signal: approval
              key: {literal: "k2"}
              consume_from: buffered
              timeout: null
              next: done
            done:
              kind: complete
              output: {from: nodes.wait.output}
    next: finish
  finish:
    kind: complete
    output: {from: nodes.both.output}
"#,
    );
    let mut serve = match serve_local(cli, &dir) {
        Ok(serve) => serve,
        Err(err) => return fail(row, "graphrun serve", err),
    };
    let input = dir.join("input.json");
    let _ = fs::write(&input, "null");
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            yaml.to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        terminate(&mut serve.0);
        return fail(row, "start parallel", format!("{stdout}\n{stderr}"));
    }
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    let ready = Instant::now() + Duration::from_secs(10);
    let opened = loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("\"key\":\"k1\"") && body.contains("\"key\":\"k2\"") {
            break body;
        }
        if Instant::now() >= ready {
            terminate(&mut serve.0);
            return fail(row, "parallel waits", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let sock = dir.join("control.sock");
    let snap = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt.block_on(graphrun::connect_control(
            &sock,
            graphrun::ControlRequest::Snapshot,
        )),
        Err(err) => {
            terminate(&mut serve.0);
            return fail(row, "tokio", err.to_string());
        }
    };
    match snap {
        Ok(resp) if resp.ok => {}
        Ok(resp) => {
            terminate(&mut serve.0);
            return fail(
                row,
                "snapshot",
                resp.error.unwrap_or_else(|| "snapshot failed".to_owned()),
            );
        }
        Err(err) => {
            terminate(&mut serve.0);
            return fail(row, "snapshot", err.to_string());
        }
    }
    std::thread::sleep(Duration::from_millis(200));
    let snaps = dir.join("snapshots");
    let snap_count = snaps
        .read_dir()
        .map(|entries| entries.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    if snap_count == 0 {
        terminate(&mut serve.0);
        return fail(row, "snapshot files", "no snapshot files");
    }
    let payload = dir.join("approval.json");
    let _ = fs::write(&payload, r#"{"approved":true}"#);
    for (key, event_id) in [
        ("k1", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ("k2", "cccccccccccccccccccccccccccccccc"),
    ] {
        let (ok, stdout, stderr) = run_cli(
            cli,
            &[
                "signal",
                "--run",
                &run,
                "--name",
                "approval",
                "--key",
                key,
                "--event-id",
                event_id,
                "--payload",
                payload.to_str().unwrap(),
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if !ok {
            terminate(&mut serve.0);
            return fail(row, "signal", format!("{key} {stdout}\n{stderr}"));
        }
    }
    let done = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("succeeded") {
            terminate(&mut serve.0);
            return finish(
                row,
                "PASS",
                "snapshot while two parallel waits pending; both complete",
                format!("waits={opened}\nsnapshots={snap_count}\n{body}"),
                vec![dir],
            );
        }
        if Instant::now() >= done {
            terminate(&mut serve.0);
            return fail(row, "inspect after signals", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cancel_saga_cli(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-cancel", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let mut serve = match serve_local(cli, &dir) {
        Ok(serve) => serve,
        Err(err) => return fail(row, "graphrun serve", err),
    };
    let input = dir.join("input.json");
    let _ = fs::write(
        &input,
        r#"{"order_id":"o1","amount":1000,"fail_after_payment":true}"#,
    );
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("saga.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        terminate(&mut serve.0);
        return fail(row, "start saga", format!("{stdout}\n{stderr}"));
    }
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    let ready = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("\"status\":\"open\"") || body.contains("\"status\":\"active\"") {
            break;
        }
        if Instant::now() >= ready {
            terminate(&mut serve.0);
            return fail(row, "wait obligation", body);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut cancel_ok = false;
    let mut cancel_log = String::new();
    for _ in 0..8 {
        let (ok, stdout, stderr) = run_cli(
            cli,
            &[
                "cancel",
                "--run",
                &run,
                "--reason",
                "stop",
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        cancel_log = format!("{stdout}\n{stderr}");
        if ok {
            cancel_ok = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if !cancel_ok && !cancel_log.contains("not active") {
        terminate(&mut serve.0);
        return fail(row, "cancel", cancel_log);
    }
    let done = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("failed")
            && (body.contains("compensated") || body.contains("run.cancelled"))
        {
            terminate(&mut serve.0);
            return finish(row, "PASS", "graphrun cancel during saga", body, vec![dir]);
        }
        if Instant::now() >= done {
            terminate(&mut serve.0);
            return fail(row, "inspect after cancel", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn compensation_restart_cli(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-restart", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let mut serve = match serve_local(cli, &dir) {
        Ok(serve) => serve,
        Err(err) => return fail(row, "graphrun serve", err),
    };
    let input = dir.join("input.json");
    let _ = fs::write(
        &input,
        r#"{"order_id":"o1","amount":1000,"fail_after_payment":true}"#,
    );
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("saga.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            dir.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        terminate(&mut serve.0);
        return fail(row, "start saga", format!("{stdout}\n{stderr}"));
    }
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    let mid = Instant::now() + Duration::from_secs(12);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("\"status\":\"compensating\"")
            || body.contains("\"status\":\"compensated\"")
            || body.contains("\"status\":\"failed\"")
        {
            break;
        }
        if Instant::now() >= mid {
            terminate(&mut serve.0);
            return fail(row, "wait compensation", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    terminate(&mut serve.0);
    drop(serve);
    let gone = Instant::now() + Duration::from_secs(5);
    while dir.join("control.sock").exists() && Instant::now() < gone {
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = fs::remove_file(dir.join("control.sock"));
    let mut serve = match serve_local(cli, &dir) {
        Ok(serve) => serve,
        Err(err) => return fail(row, "serve restart", err),
    };
    let done = Instant::now() + Duration::from_secs(25);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        if body.contains("\"status\":\"failed\"") && body.contains("\"status\":\"compensated\"") {
            terminate(&mut serve.0);
            return finish(
                row,
                "PASS",
                "saga compensation survives serve restart",
                body,
                vec![dir],
            );
        }
        if Instant::now() >= done {
            terminate(&mut serve.0);
            return fail(row, "inspect after restart", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn forged_worker_process(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-forged", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let ca = match graphrun::generate_ca() {
        Ok(ca) => ca,
        Err(err) => return fail(row, "generate_ca", err.to_string()),
    };
    let tls = match issue_test_node(&ca, 1) {
        Ok(tls) => tls,
        Err(err) => return fail(row, "issue_node", err.to_string()),
    };
    let ca_path = dir.join("ca.pem");
    let cert = dir.join("n1.cert.pem");
    let key = dir.join("n1.key.pem");
    let _ = fs::write(&ca_path, &ca.pem);
    let _ = fs::write(&cert, &tls.cert_pem);
    let _ = fs::write(&key, &tls.key_pem);
    let addr = format!("127.0.0.1:{}", unused_port());
    let data = dir.join("m1");
    let _ = fs::create_dir_all(&data);
    let e2e = match std::env::current_exe() {
        Ok(path) => path,
        Err(err) => return fail(row, "current_exe", err.to_string()),
    };
    let log = dir.join("member-1.log");
    let mut cmd = Command::new(&e2e);
    cmd.args([
        "fixture-member",
        "--data-dir",
        data.to_str().unwrap(),
        "--bind",
        &addr,
        "--node-id",
        "1",
        "--ca",
        ca_path.to_str().unwrap(),
        "--cert",
        cert.to_str().unwrap(),
        "--key",
        key.to_str().unwrap(),
        "--server-name",
        &tls.server_name,
        "--initialize",
    ]);
    cmd.stdout(Stdio::null());
    if let Ok(file) = fs::File::create(log) {
        cmd.stderr(file);
    }
    let _member = match cmd.spawn() {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "fixture-member", err.to_string()),
    };
    let tcp_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < tcp_deadline {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let input = dir.join("input.json");
    let _ = fs::write(&input, r#"{"order_id":"o1","amount":1000}"#);
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("sequence.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--endpoint",
            &format!("https://{addr}"),
            "--ca",
            ca_path.to_str().unwrap(),
            "--cert",
            cert.to_str().unwrap(),
            "--tls-key",
            key.to_str().unwrap(),
            "--server-name",
            &tls.server_name,
            "--no-wait",
        ],
    );
    if !ok {
        return fail(row, "start", format!("{stdout}\n{stderr}"));
    }
    let run = stdout
        .split('"')
        .find(|part| part.len() == 32)
        .unwrap_or("")
        .to_owned();
    let tls_clone = tls.clone();
    let addr_clone = addr.clone();
    let forged = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
        rt.block_on(async move {
            graphrun::tls::install_provider();
            let channel = tonic::transport::Channel::from_shared(format!("https://{addr_clone}"))
                .map_err(|err| err.to_string())?
                .tls_config(graphrun::rpc::client_tls(&tls_clone).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?
                .connect()
                .await
                .map_err(|err| err.to_string())?;
            let mut client = graphrun::generated::worker_client::WorkerClient::new(channel);
            let session = graphrun::ids::WorkerSessionId::generate();
            let catalog = graphrun::Catalog::from_json(include_bytes!(
                "../../docs/specs/v1/examples/activity-catalog.json"
            ))
            .map_err(|err| err.to_string())?;
            let capability = graphrun::worker_contract::capability_for(
                &catalog,
                &graphrun::ids::ActivityKey::new("inventory.reserve", 1),
                graphrun::ids::ExecutionRole::Forward,
            )
            .map_err(|err| err.to_string())?;
            let _ = client
                .register(graphrun::generated::RegisterRequest {
                    session_id: session.to_hex(),
                    capacity: 8,
                    capabilities: vec![capability.to_wire()],
                    principal_id: "1".to_owned(),
                    protocol_min: 1,
                    protocol_max: 1,
                    command_id: graphrun::ids::CommandId::generate().to_hex(),
                })
                .await
                .map_err(|err| err.to_string())?;
            let mut assignment = None;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while tokio::time::Instant::now() < deadline {
                let resp = client
                    .claim(graphrun::generated::ClaimRequest {
                        command_id: graphrun::ids::CommandId::generate().to_hex(),
                        session_id: session.to_hex(),
                        capacity: 8,
                    })
                    .await
                    .map_err(|err| err.to_string())?
                    .into_inner();
                if !resp.assignments.is_empty() {
                    assignment = Some(resp.assignments[0].clone());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let assignment = assignment.ok_or_else(|| "no claim".to_owned())?;
            let stale = client
                .report(graphrun::generated::ReportRequest {
                    command_id: graphrun::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    run_id: assignment.run_id.clone(),
                    activation_id: assignment.activation_id.clone(),
                    generation: assignment.generation.saturating_add(9),
                    revision: assignment.revision,
                    output_json: serde_json::to_vec(&serde_json::json!({
                        "order_id": "o1",
                        "amount": 1000,
                        "reservation_id": "forged"
                    }))
                    .unwrap_or_default(),
                    output_schema_digest: assignment.output_schema_digest.clone(),
                    ..Default::default()
                })
                .await
                .map_err(|err| err.to_string())?
                .into_inner();
            if stale.error.is_empty() {
                return Err("stale generation was accepted".to_owned());
            }
            let input: graphrun::Value =
                serde_json::from_slice(&assignment.input_json).unwrap_or(graphrun::Value::Null);
            let output = graphrun::engine::builtin_handler(&assignment.activity_name, &input)
                .map_err(|err| err.to_string())?;
            let accepted = client
                .report(graphrun::generated::ReportRequest {
                    command_id: graphrun::ids::CommandId::generate().to_hex(),
                    session_id: session.to_hex(),
                    run_id: assignment.run_id,
                    activation_id: assignment.activation_id,
                    generation: assignment.generation,
                    revision: assignment.revision,
                    output_json: serde_json::to_vec(&output).unwrap_or_default(),
                    output_schema_digest: assignment.output_schema_digest,
                    ..Default::default()
                })
                .await
                .map_err(|err| err.to_string())?
                .into_inner();
            if !accepted.error.is_empty() {
                return Err(format!("valid report rejected: {}", accepted.error));
            }
            Ok(stale.error)
        })
    })
    .join()
    .unwrap_or_else(|_| Err("forged worker thread panicked".to_owned()));
    let forged_err = match forged {
        Ok(err) => err,
        Err(err) => return fail(row, "forged report", err),
    };
    let worker_log = dir.join("worker-1.log");
    let mut worker_cmd = Command::new(&e2e);
    worker_cmd.args([
        "fixture-worker",
        "--endpoint",
        &format!("https://{addr}"),
        "--ca",
        ca_path.to_str().unwrap(),
        "--cert",
        cert.to_str().unwrap(),
        "--key",
        key.to_str().unwrap(),
        "--server-name",
        &tls.server_name,
    ]);
    worker_cmd.stdout(Stdio::null());
    if let Ok(file) = fs::File::create(worker_log) {
        worker_cmd.stderr(file);
    }
    let _worker = match worker_cmd.spawn() {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "fixture-worker", err.to_string()),
    };
    let done = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--endpoint",
                &format!("https://{addr}"),
                "--ca",
                ca_path.to_str().unwrap(),
                "--cert",
                cert.to_str().unwrap(),
                "--tls-key",
                key.to_str().unwrap(),
                "--server-name",
                &tls.server_name,
            ],
        );
        if body.contains("succeeded") && body.contains("pay-1") {
            return finish(
                row,
                "PASS",
                "forged worker report rejected; valid claim still completes",
                format!("forged_error={forged_err}\n{body}"),
                vec![dir],
            );
        }
        if Instant::now() >= done {
            return fail(row, "valid report", body);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn spawn_provider(dir: &Path) -> Result<(ChildProc, String), String> {
    let provider_dir = dir.join("provider");
    let _ = fs::create_dir_all(&provider_dir);
    let e2e = std::env::current_exe().map_err(|err| err.to_string())?;
    let mut provider = Command::new(&e2e)
        .args([
            "fixture-provider",
            "--data-dir",
            provider_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| err.to_string())?;
    let stdout = provider
        .stdout
        .take()
        .ok_or_else(|| "missing stdout".to_owned())?;
    let mut url = String::new();
    let mut reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            break;
        }
        if let Some(rest) = line.trim().strip_prefix("GRAPHUN_PROVIDER_URL=") {
            url = rest.to_owned();
        }
        if line.trim() == "READY" {
            break;
        }
    }
    if url.is_empty() {
        return Err("did not print GRAPHUN_PROVIDER_URL".to_owned());
    }
    Ok((ChildProc::new(provider), url))
}

fn provider_idempotent_effect(artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-data", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let (_provider, url) = match spawn_provider(&dir) {
        Ok(pair) => pair,
        Err(err) => return fail(row, "fixture-provider", err),
    };
    let output = graphrun::Value::Object(
        [(
            "payment_id".to_owned(),
            graphrun::Value::String("pay-1".to_owned()),
        )]
        .into_iter()
        .collect(),
    );
    let first = match graphrun::provider::apply_effect(&url, "pay-key", "forward", &output) {
        Ok(resp) => resp,
        Err(err) => return fail(row, "provider apply", err.to_string()),
    };
    let second = match graphrun::provider::apply_effect(&url, "pay-key", "forward", &output) {
        Ok(resp) => resp,
        Err(err) => return fail(row, "provider retry", err.to_string()),
    };
    if first.logical != 1 || second.logical != 1 || second.physical != 2 {
        return fail(
            row,
            "provider ledger",
            format!("first={first:?} second={second:?}"),
        );
    }
    let undo = match graphrun::provider::apply_effect(
        &url,
        "pay-key:undo",
        "compensate",
        &graphrun::Value::Null,
    ) {
        Ok(resp) => resp,
        Err(err) => return fail(row, "provider compensate", err.to_string()),
    };
    if undo.logical != 1 || undo.physical != 1 {
        return fail(row, "provider compensate", format!("{undo:?}"));
    }
    finish(
        row,
        "PASS",
        "fixture-provider HTTP ledger",
        format!(
            "physical={} logical={} undo_physical={}",
            second.physical, second.logical, undo.physical
        ),
        vec![dir],
    )
}

fn provider_recon_outcomes(artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-recon", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let (_provider, url) = match spawn_provider(&dir) {
        Ok(pair) => pair,
        Err(err) => return fail(row, "fixture-provider", err),
    };
    let output = graphrun::Value::Object(
        [("value".to_owned(), graphrun::Value::Int(2))]
            .into_iter()
            .collect(),
    );
    if let Err(err) = graphrun::provider::apply_effect(&url, "applied-key", "forward", &output) {
        return fail(row, "apply applied-key", err.to_string());
    }
    if let Err(err) = graphrun::provider::set_effect(
        &url,
        "not-applied-key",
        graphrun::provider::EffectStatus::NotApplied,
    ) {
        return fail(row, "set not_applied", err.to_string());
    }
    if let Err(err) = graphrun::provider::set_effect(
        &url,
        "unknown-key",
        graphrun::provider::EffectStatus::Unknown,
    ) {
        return fail(row, "set unknown", err.to_string());
    }
    let applied = match graphrun::provider::probe_effect(&url, "applied-key") {
        Ok(resp) => resp,
        Err(err) => return fail(row, "probe applied", err.to_string()),
    };
    let not_applied = match graphrun::provider::probe_effect(&url, "not-applied-key") {
        Ok(resp) => resp,
        Err(err) => return fail(row, "probe not_applied", err.to_string()),
    };
    let unknown = match graphrun::provider::probe_effect(&url, "unknown-key") {
        Ok(resp) => resp,
        Err(err) => return fail(row, "probe unknown", err.to_string()),
    };
    if applied.status != graphrun::provider::EffectStatus::Applied
        || not_applied.status != graphrun::provider::EffectStatus::NotApplied
        || unknown.status != graphrun::provider::EffectStatus::Unknown
    {
        return fail(
            row,
            "provider recon statuses",
            format!("applied={applied:?} not={not_applied:?} unknown={unknown:?}"),
        );
    }
    finish(
        row,
        "PASS",
        "fixture-provider Applied/NotApplied/Unknown probes",
        format!(
            "applied={:?} not_applied={:?} unknown={:?}",
            applied.status, not_applied.status, unknown.status
        ),
        vec![dir],
    )
}

fn provider_delayed_forward(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-delay", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let (_provider, url) = match spawn_provider(&dir) {
        Ok(pair) => pair,
        Err(err) => return fail(row, "fixture-provider", err),
    };
    if let Err(err) = graphrun::provider::hold_effect(&url, "*") {
        return fail(row, "hold *", err.to_string());
    }
    let local = dir.join("local");
    let _ = fs::create_dir_all(&local);
    let mut serve = match Command::new(cli)
        .args(["serve", "--local-dir", local.to_str().unwrap()])
        .env("GRAPHUN_PROVIDER_URL", &url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "graphrun serve", err.to_string()),
    };
    if !wait_path(&local.join("control.sock"), Duration::from_secs(10)) {
        return fail(row, "graphrun serve", "control socket missing");
    }
    let input = dir.join("input.json");
    let _ = fs::write(
        &input,
        r#"{"order_id":"o1","amount":1000,"fail_after_payment":true}"#,
    );
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("saga.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            local.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        terminate(&mut serve.0);
        return fail(row, "start saga", format!("{stdout}\n{stderr}"));
    }
    std::thread::sleep(Duration::from_millis(1500));
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    let (_, body, _) = run_cli(
        cli,
        &[
            "inspect",
            "--run",
            &run,
            "--local-dir",
            local.to_str().unwrap(),
        ],
    );
    let ledger = match graphrun::provider::dump_ledger(&url) {
        Ok(value) => value,
        Err(err) => {
            terminate(&mut serve.0);
            return fail(row, "dump ledger", err.to_string());
        }
    };
    terminate(&mut serve.0);
    let pending = body.contains("\"status\":\"active\"")
        && !body.contains("\"status\":\"succeeded\"")
        && ledger.to_string().contains("pending");
    if !pending {
        return fail(
            row,
            "delayed forward",
            format!("inspect={body}\nledger={ledger}"),
        );
    }
    finish(
        row,
        "PASS",
        "held provider blocks saga forward and undo",
        format!("inspect={body} ledger={ledger}"),
        vec![dir],
    )
}

fn rust_builder_sequence(artifacts: &Path, row: &MatrixRow) -> CaseResult {
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Order {
        order_id: String,
        amount: i64,
    }
    impl graphrun::DurablePayload for Order {
        fn schema_ref() -> graphrun::SchemaRef {
            graphrun::SchemaRef::named("order", 1).unwrap()
        }
    }
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct ReservedOrder {
        order_id: String,
        amount: i64,
        reservation_id: String,
    }
    impl graphrun::DurablePayload for ReservedOrder {
        fn schema_ref() -> graphrun::SchemaRef {
            graphrun::SchemaRef::named("reserved_order", 1).unwrap()
        }
    }
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Receipt {
        order_id: String,
        amount: i64,
        payment_id: String,
    }
    impl graphrun::DurablePayload for Receipt {
        fn schema_ref() -> graphrun::SchemaRef {
            graphrun::SchemaRef::named("receipt", 1).unwrap()
        }
    }
    let dir = artifacts.join(format!("{}-rust", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => return fail(row, "tokio", err.to_string()),
    };
    let result = rt.block_on(async {
        let catalog =
            graphrun::Catalog::from_json(&fs::read(catalog()).map_err(|err| err.to_string())?)
                .map_err(|err| err.to_string())?;
        let reserve = catalog
            .activity_ref::<Order, ReservedOrder>("inventory.reserve", 1)
            .map_err(|err| err.to_string())?;
        let charge = catalog
            .activity_ref::<ReservedOrder, Receipt>("payment.charge", 1)
            .map_err(|err| err.to_string())?;
        let mut root = graphrun::RegionBuilder::<Order>::new();
        let reserved = root
            .activity("reserve", &reserve, root.input())
            .map_err(|err| err.to_string())?;
        let charged = root
            .activity("charge", &charge, reserved.output())
            .map_err(|err| err.to_string())?;
        let root = root
            .complete("finish", charged.output())
            .map_err(|err| err.to_string())?;
        let definition = graphrun::WorkflowBuilder::new("sequence", 1, root)
            .build(&catalog)
            .map_err(|err| err.to_string())?;
        let engine = graphrun::Engine::local(&dir)
            .await
            .map_err(|err| err.to_string())?;
        let input = Order {
            order_id: "o1".to_owned(),
            amount: 1000,
        };
        let value = serde_json::to_value(&input).map_err(|err| err.to_string())?;
        let input = serde_json::from_value(value).map_err(|err| err.to_string())?;
        let run = engine
            .start(definition, catalog, input)
            .await
            .map_err(|err| err.to_string())?;
        let output = engine
            .wait_terminal(run, Duration::from_secs(10))
            .await
            .map_err(|err| err.to_string())?;
        engine.shutdown().await.map_err(|err| err.to_string())?;
        Ok::<_, String>(output)
    });
    match result {
        Ok(output) => {
            let text = serde_json::to_string(&output).unwrap_or_default();
            if text.contains("pay-1") {
                finish(
                    row,
                    "PASS",
                    "Engine::local rust builder sequence",
                    text,
                    vec![dir],
                )
            } else {
                fail(row, "rust builder output", text)
            }
        }
        Err(err) => fail(row, "rust builder", err),
    }
}

fn disaster_restore_cli(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-cli", row.id));
    let _ = fs::remove_dir_all(&dir);
    let src = dir.join("src");
    let backup = dir.join("backup");
    let dest = dir.join("dest");
    let _ = fs::create_dir_all(&src);
    let mut serve = match Command::new(cli)
        .args(["serve", "--local-dir", src.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "graphrun serve", err.to_string()),
    };
    if !wait_path(&src.join("control.sock"), Duration::from_secs(10)) {
        return fail(row, "graphrun serve", "control socket did not appear");
    }
    let input = src.join("input.json");
    let _ = fs::write(&input, r#"{"key":"k1"}"#);
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("events.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            src.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if !ok {
        return fail(row, "start events", format!("{stdout}\n{stderr}"));
    }
    let run = stdout
        .split('"')
        .skip_while(|part| *part != "run")
        .nth(2)
        .unwrap_or("")
        .to_owned();
    if run.is_empty() {
        return fail(row, "start events", stdout);
    }
    let wait_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                src.to_str().unwrap(),
            ],
        );
        if body.contains("\"signal\":\"approval\"") {
            break;
        }
        if Instant::now() >= wait_deadline {
            return fail(row, "pending wait", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    terminate(&mut serve.0);
    drop(serve);
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "backup",
            "--local-dir",
            src.to_str().unwrap(),
            "--out",
            backup.to_str().unwrap(),
        ],
    );
    if !ok {
        return fail(row, "backup", format!("{stdout}\n{stderr}"));
    }
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "restore",
            "--from",
            backup.to_str().unwrap(),
            "--local-dir",
            dest.to_str().unwrap(),
            "--confirm",
            "--reason",
            "disaster",
        ],
    );
    if !ok {
        return fail(row, "restore", format!("{stdout}\n{stderr}"));
    }
    let identity = fs::read_to_string(dest.join("identity.json")).unwrap_or_default();
    if !identity.contains("graphrun-restored-") {
        return fail(row, "restore identity", identity);
    }
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "start",
            "--definition",
            examples().join("events.yaml").to_str().unwrap(),
            "--catalog",
            catalog().to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            dest.to_str().unwrap(),
            "--no-wait",
        ],
    );
    if ok || !(stdout.contains("suspended") || stderr.contains("suspended")) {
        return fail(
            row,
            "start while suspended",
            format!("ok={ok} {stdout}\n{stderr}"),
        );
    }
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "resolve",
            "--run",
            &run,
            "--ack",
            "--reason",
            "operator authorized restore",
            "--local-dir",
            dest.to_str().unwrap(),
        ],
    );
    if !ok {
        return fail(row, "resolve --ack", format!("{stdout}\n{stderr}"));
    }
    let mut serve = match Command::new(cli)
        .args(["serve", "--local-dir", dest.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => ChildProc::new(child),
        Err(err) => return fail(row, "serve restored", err.to_string()),
    };
    if !wait_path(&dest.join("control.sock"), Duration::from_secs(10)) {
        return fail(row, "serve restored", "control socket missing");
    }
    let payload = dest.join("approval.json");
    let _ = fs::write(&payload, r#"{"approved":true}"#);
    let (ok, stdout, stderr) = run_cli(
        cli,
        &[
            "signal",
            "--run",
            &run,
            "--name",
            "approval",
            "--key",
            "k1",
            "--event-id",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--payload",
            payload.to_str().unwrap(),
            "--local-dir",
            dest.to_str().unwrap(),
        ],
    );
    if !ok {
        terminate(&mut serve.0);
        return fail(row, "signal restored", format!("{stdout}\n{stderr}"));
    }
    let done = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body, _) = run_cli(
            cli,
            &[
                "inspect",
                "--run",
                &run,
                "--local-dir",
                dest.to_str().unwrap(),
            ],
        );
        if body.contains("succeeded") && body.contains("true") {
            terminate(&mut serve.0);
            return finish(
                row,
                "PASS",
                "backup/restore/ack via production CLI",
                body,
                vec![dir],
            );
        }
        if Instant::now() >= done {
            terminate(&mut serve.0);
            return fail(row, "inspect restored", body);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn fixture_provider(bind: Option<String>, data_dir: PathBuf) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };
    rt.block_on(async move {
        let bind = bind.unwrap_or_else(|| "127.0.0.1:0".to_owned());
        let listener = match tokio::net::TcpListener::bind(&bind).await {
            Ok(listener) => listener,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        };
        match listener.local_addr() {
            Ok(addr) => {
                println!("GRAPHUN_PROVIDER_URL=http://{addr}");
                println!("READY");
                let _ = std::io::stdout().flush();
            }
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        }
        if let Err(err) = graphrun::provider::serve(listener, data_dir).await {
            eprintln!("{err}");
            ExitCode::from(2)
        } else {
            ExitCode::SUCCESS
        }
    })
}

fn read_tls(
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    server_name: String,
) -> Result<graphrun::TlsMaterial, String> {
    Ok(graphrun::TlsMaterial {
        ca_pem: fs::read_to_string(ca).map_err(|err| err.to_string())?,
        cert_pem: fs::read_to_string(cert).map_err(|err| err.to_string())?,
        key_pem: fs::read_to_string(key).map_err(|err| err.to_string())?,
        server_name,
    })
}

#[allow(clippy::too_many_arguments)]
fn fixture_member(
    data_dir: PathBuf,
    bind: String,
    node_id: u64,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    server_name: String,
    initialize: bool,
    host_activities: bool,
    peer: Vec<String>,
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };
    rt.block_on(async move {
        let tls = match read_tls(ca, cert, key, server_name) {
            Ok(tls) => tls,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        };
        let bind = match bind.parse() {
            Ok(bind) => bind,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        };
        let mut peers = std::collections::BTreeMap::new();
        for spec in peer {
            match parse_peer(&spec, &tls.ca_pem) {
                Ok((id, value)) => {
                    peers.insert(id, value);
                }
                Err(err) => {
                    eprintln!("{err}");
                    return ExitCode::from(2);
                }
            }
        }
        match graphrun::Engine::member(graphrun::MemberConfig {
            data_dir,
            node_id,
            bind,
            peers,
            tls,
            host_activities,
            initialize,
        })
        .await
        {
            Ok(engine) => loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let _ = &engine;
            },
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(2)
            }
        }
    })
}

fn fixture_worker(
    endpoint: String,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    server_name: String,
) -> ExitCode {
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };
    rt.block_on(async move {
        let tls = match read_tls(ca, cert, key, server_name) {
            Ok(tls) => tls,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        };
        let result = graphrun::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        ));
        let result = match result {
            Ok(catalog) => {
                graphrun::Worker::builder(endpoint, tls, catalog)
                    .fixture_handlers()
                    .open()
                    .await
            }
            Err(err) => Err(err),
        };
        match result {
            Ok(worker) => match worker.run().await {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    eprintln!("{err}");
                    ExitCode::from(2)
                }
            },
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(2)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_node_cert_uses_numeric_member_id_and_node_dns() {
        use graphrun::tls::{PeerRole, PrincipalId};

        let ca = graphrun::generate_ca().unwrap();
        let tls = issue_test_node(&ca, 2).unwrap();
        assert_eq!(tls.server_name, "node-2.graphrun.local");
        let cluster = graphrun::tls::cluster_id_from_ca(&ca.pem).unwrap();
        let peer = graphrun::tls::verify_peer_identity(
            &ca.pem,
            &cluster,
            &graphrun::tls::load_certs(&tls.cert_pem).unwrap(),
        )
        .unwrap();
        assert_eq!(*peer.principal_id(), PrincipalId::parse("2").unwrap());
        assert_eq!(
            peer.roles().collect::<Vec<_>>(),
            [
                PeerRole::Member,
                PeerRole::Worker,
                PeerRole::Client,
                PeerRole::Admin,
            ]
        );
        peer.require_member_identity(&cluster, &PrincipalId::parse("2").unwrap())
            .unwrap();
    }

    fn row(id: &str) -> MatrixRow {
        MatrixRow {
            id: id.to_owned(),
            requirement: "REQ-E2E".to_owned(),
            layer: "unit".to_owned(),
            scenario: "gate".to_owned(),
            pass_criterion: "current evidence".to_owned(),
        }
    }

    fn context(run_dir: &Path) -> RunContext {
        RunContext {
            run_id: run_dir.file_name().unwrap().to_string_lossy().into_owned(),
            source_sha256: "source-1".to_owned(),
            cli_sha256: "cli-1".to_owned(),
            cli_version: "0.1.test".to_owned(),
            driver_sha256: "driver-1".to_owned(),
            driver_version: "test".to_owned(),
        }
    }

    fn passing_case(row: &MatrixRow, run_dir: &Path, context: &RunContext) -> CaseResult {
        let log = run_dir.join(format!("{}.log", row.id));
        fs::write(&log, "observed").unwrap();
        context.bind(finish(row, "PASS", "test", "observed", vec![log]))
    }

    #[test]
    fn coverage_rejects_missing_duplicate_and_stale_cases() {
        let parent = tempfile::tempdir().unwrap();
        let run_dir = parent.path().join("run-new");
        fs::create_dir(&run_dir).unwrap();
        let old_run = parent.path().join("run-old");
        fs::create_dir(&old_run).unwrap();
        let rows = vec![row("A"), row("B")];
        let context = context(&run_dir);
        let first = passing_case(&rows[0], &run_dir, &context);
        let second = passing_case(&rows[1], &run_dir, &context);
        write_case(&run_dir, &first).unwrap();
        write_case(&run_dir, &second).unwrap();
        let results = vec![first.clone(), second.clone()];
        assert!(validate_coverage(&rows, &results, &run_dir, &context).is_ok());
        fs::remove_file(run_dir.join("B.json")).unwrap();
        assert!(validate_coverage(&rows, &results, &run_dir, &context).is_err());
        write_case(&run_dir, &second).unwrap();
        assert!(
            validate_coverage(&rows, &[first.clone(), first.clone()], &run_dir, &context)
                .unwrap_err()
                .contains("duplicate")
        );
        let mut stale = second.clone();
        stale.run_id = "run-old".to_owned();
        write_case(&run_dir, &stale).unwrap();
        assert!(
            validate_coverage(&rows, &[first.clone(), stale], &run_dir, &context)
                .unwrap_err()
                .contains("stale")
        );
        let old_log = old_run.join("B.log");
        fs::write(&old_log, "stale").unwrap();
        let mut outside = second.clone();
        outside.artifacts = vec![old_log.display().to_string()];
        assert_eq!(enforce_case(&rows[1], outside, &run_dir).status, "FAIL");
        let mut missing = second;
        missing.artifacts = vec![run_dir.join("absent.log").display().to_string()];
        assert_eq!(enforce_case(&rows[1], missing, &run_dir).status, "FAIL");
    }

    #[test]
    fn test_evidence_requires_executed_pass_and_successful_exit() {
        let log = PathBuf::from("current-suite.log");
        let good = "running 1 test\ntest domain::tests::required ... ok\n\
                    test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n";
        let suite = TestSuite::from_output("cargo test".to_owned(), log.clone(), good, true, 1);
        assert!(suite.passed("domain::tests::required"));
        assert!(!suite.passed("domain::tests::missing"));
        let failed_exit =
            TestSuite::from_output("cargo test".to_owned(), log.clone(), good, false, 1);
        assert!(!failed_exit.passed("domain::tests::required"));
        let ignored = TestSuite::from_output(
            "cargo test".to_owned(),
            log.clone(),
            "running 1 test\ntest domain::tests::required ... ignored\n\
             test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n",
            true,
            1,
        );
        assert!(!ignored.passed("domain::tests::required"));
        let filtered = TestSuite::from_output(
            "cargo test".to_owned(),
            log.clone(),
            "running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out\n",
            true,
            1,
        );
        assert!(!filtered.success);
        let forged = TestSuite::from_output(
            "cargo test".to_owned(),
            log,
            "test domain::tests::required ... ok\n\
             test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n",
            true,
            1,
        );
        assert!(!forged.passed("domain::tests::required"));
    }

    #[test]
    fn status_and_certification_gate() {
        let parent = tempfile::tempdir().unwrap();
        let run_dir = parent.path().join("run-new");
        fs::create_dir(&run_dir).unwrap();
        let context = context(&run_dir);
        let functional = row("LOCAL-001");
        assert!(release_matrix_complete(std::slice::from_ref(&functional)).is_err());
        let passing = passing_case(&functional, &run_dir, &context);
        assert_eq!(
            verification_gate(std::slice::from_ref(&passing), false),
            (ExitCode::SUCCESS, true)
        );
        let failure = context.bind(fail(&functional, "test", "missing"));
        assert_eq!(
            verification_gate(&[passing.clone(), failure], false),
            (ExitCode::from(1), false)
        );
        let blocked = context.bind(finish(
            &functional,
            "BLOCKED",
            "test",
            "no hardware",
            vec![run_dir.clone()],
        ));
        assert_eq!(
            verification_gate(&[blocked], false),
            (ExitCode::from(1), false)
        );
        let perf = row("PERF-001");
        let mut measured = context.bind(finish(
            &perf,
            "BLOCKED",
            "benchmark",
            "reference hardware unavailable",
            vec![run_dir.clone()],
        ));
        assert_eq!(
            verification_gate(&[measured.clone()], false),
            (ExitCode::from(1), false)
        );
        measured.performance = Some(PerformanceEvidence {
            hardware: "os=windows logical_cpus=8 observed_members=1".to_owned(),
            reference_hardware_available: false,
            measurements: vec!["commands_per_s=100".to_owned()],
            reason: "reference hardware unavailable".to_owned(),
        });
        assert_eq!(
            verification_gate(&[passing.clone(), measured.clone()], false),
            (ExitCode::SUCCESS, false)
        );
        assert_eq!(
            verification_gate(&[passing, measured.clone()], true),
            (ExitCode::from(1), false)
        );
        measured.performance.as_mut().unwrap().measurements.clear();
        assert_eq!(
            verification_gate(&[measured], false),
            (ExitCode::from(1), false)
        );
    }

    #[test]
    fn canonical_matrix_rejects_missing_duplicate_and_malformed_rows() {
        let canonical =
            parse_matrix(include_str!("../../docs/specs/v1/verification-matrix.tsv")).unwrap();
        assert!(release_matrix_complete(&canonical).is_ok());

        let subset = vec![
            canonical
                .iter()
                .find(|row| row.id == "E2E-003")
                .unwrap()
                .clone(),
        ];
        assert!(
            release_matrix_complete(&subset)
                .unwrap_err()
                .contains("missing mandatory matrix ID")
        );

        let mut mismatched = canonical.clone();
        mismatched[0].pass_criterion.push_str(" changed");
        assert!(
            release_matrix_complete(&mismatched)
                .unwrap_err()
                .contains("differs from canonical")
        );

        let mut duplicate = canonical.clone();
        duplicate.push(canonical[0].clone());
        assert!(
            release_matrix_complete(&duplicate)
                .unwrap_err()
                .contains("duplicate matrix ID")
        );
        let mut unexpected = canonical;
        unexpected.push(row("OTHER-001"));
        assert!(
            release_matrix_complete(&unexpected)
                .unwrap_err()
                .contains("unexpected matrix ID")
        );
        let header = "id\trequirement\tlayer\tscenario\tpass_criterion\n";
        let valid = "E2E-003\tREQ-E2E\te2e\treport\tcomplete\n";
        assert!(
            parse_matrix(&format!("{header}{valid}{valid}"))
                .err()
                .unwrap()
                .contains("duplicates ID")
        );
        assert!(parse_matrix(&format!("{header}E2E-003\tREQ-E2E\te2e\treport\n")).is_err());
        assert!(
            parse_matrix(&format!("{header}bad/id\tREQ-E2E\te2e\treport\tcomplete\n")).is_err()
        );
    }

    #[test]
    fn passing_subset_with_real_cli_fails_normal_verification() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    workspace.join(path)
                }
            })
            .unwrap_or_else(|| workspace.join("target"));
        let build = Command::new("cargo")
            .current_dir(workspace)
            .args([
                "build",
                "-p",
                "graphrun-cli",
                "--bin",
                "graphrun",
                "--locked",
            ])
            .output()
            .unwrap();
        assert!(
            build.status.success(),
            "CLI build failed: {}",
            String::from_utf8_lossy(&build.stderr)
        );
        let cli = target_dir
            .join("debug")
            .join(format!("graphrun{}", std::env::consts::EXE_SUFFIX));
        assert!(cli.is_file());

        let temp = tempfile::Builder::new()
            .prefix("matrix-gate-")
            .tempdir_in(&target_dir)
            .unwrap();
        let matrix = temp.path().join("matrix.tsv");
        let artifacts = temp.path().join("evidence");
        let case = parse_matrix(include_str!("../../docs/specs/v1/verification-matrix.tsv"))
            .unwrap()
            .into_iter()
            .find(|row| row.id == "E2E-003")
            .unwrap();
        fs::write(
            &matrix,
            format!(
                "id\trequirement\tlayer\tscenario\tpass_criterion\n{}\t{}\t{}\t{}\t{}\n",
                case.id, case.requirement, case.layer, case.scenario, case.pass_criterion
            ),
        )
        .unwrap();

        assert_ne!(verify(&cli, &matrix, &artifacts, false), ExitCode::SUCCESS);
        let first: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.join("report.json")).unwrap()).unwrap();
        assert_eq!(first["results"][0]["status"], "PASS");
        assert_eq!(first["coverage_error"], serde_json::Value::Null);
        assert_eq!(first["source_integrity_error"], serde_json::Value::Null);
        assert_eq!(first["release_matrix_complete"], false);
        assert_eq!(first["release_certified"], false);
        assert!(
            first["release_matrix_error"]
                .as_str()
                .unwrap()
                .contains("missing mandatory matrix ID")
        );
        assert!(first["cli_version"].as_str().unwrap().contains("graphrun"));

        assert_ne!(verify(&cli, &matrix, &artifacts, false), ExitCode::SUCCESS);
        let second: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.join("report.json")).unwrap()).unwrap();
        assert_ne!(first["run_id"], second["run_id"]);
        for report in [&first, &second] {
            let run_dir = artifacts.join(report["run_id"].as_str().unwrap());
            assert!(run_dir.join("E2E-003.json").is_file());
            assert!(run_dir.join("report.json").is_file());
        }
    }

    #[test]
    fn missing_cli_creates_fresh_failed_case_records() {
        let temp = tempfile::tempdir().unwrap();
        let matrix = temp.path().join("matrix.tsv");
        let artifacts = temp.path().join("evidence");
        fs::write(
            &matrix,
            "id\trequirement\tlayer\tscenario\tpass_criterion\n\
             LOCAL-001\tREQ-LOCAL\te2e\tlocal\tmust run\n\
             PERF-001\tREQ-E2E\tperformance\tperf\tmust measure\n",
        )
        .unwrap();
        let cli = temp.path().join("missing-cli");
        assert_ne!(verify(&cli, &matrix, &artifacts, false), ExitCode::SUCCESS);
        let first: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.join("report.json")).unwrap()).unwrap();
        assert_eq!(first["release_certified"], false);
        assert!(
            first["results"]
                .as_array()
                .unwrap()
                .iter()
                .all(|result| result["status"] == "FAIL")
        );
        assert_ne!(verify(&cli, &matrix, &artifacts, false), ExitCode::SUCCESS);
        let second: serde_json::Value =
            serde_json::from_slice(&fs::read(artifacts.join("report.json")).unwrap()).unwrap();
        assert_ne!(first["run_id"], second["run_id"]);
        let first_run = artifacts.join(first["run_id"].as_str().unwrap());
        let second_run = artifacts.join(second["run_id"].as_str().unwrap());
        assert!(first_run.join("LOCAL-001.json").is_file());
        assert!(second_run.join("LOCAL-001.json").is_file());
        assert!(second_run.join("PERF-001.json").is_file());
    }
}
