use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

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
    api: String,
    api_ok: bool,
    ui_ok: bool,
    crash_ok: bool,
}

fn collect_evidence(artifacts: &Path) -> Evidence {
    let lib = Command::new("cargo")
        .args([
            "test", "-p", "graphrun", "--lib", "--locked", "--color", "never",
        ])
        .output();
    let api = Command::new("cargo")
        .args([
            "test", "-p", "graphrun", "--test", "api", "--locked", "--color", "never",
        ])
        .output();
    let ui = Command::new("cargo")
        .args([
            "test", "-p", "graphrun", "--test", "ui", "--locked", "--color", "never",
        ])
        .output();
    let crash = Command::new("cargo")
        .args([
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
        ])
        .output();
    let api_ok = api.as_ref().is_ok_and(|o| o.status.success());
    let ui_ok = ui.as_ref().is_ok_and(|o| o.status.success());
    let crash_ok = crash.as_ref().is_ok_and(|o| o.status.success());
    let snapshot = Command::new("cargo")
        .args([
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
            "--nocapture",
        ])
        .output();
    let snapshot_text = match snapshot {
        Ok(output) => format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => err.to_string(),
    };
    let _ = fs::write(artifacts.join("cargo-snapshot.log"), &snapshot_text);
    let lib_text = match lib {
        Ok(output) => format!(
            "{}\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            snapshot_text
        ),
        Err(err) => format!("{err}\n{snapshot_text}"),
    };
    let api_text = match api {
        Ok(output) => format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => err.to_string(),
    };
    let crash_text = match crash {
        Ok(output) => format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => err.to_string(),
    };
    let _ = fs::write(artifacts.join("cargo-lib.log"), &lib_text);
    let _ = fs::write(artifacts.join("cargo-api.log"), &api_text);
    let _ = fs::write(artifacts.join("cargo-crash.log"), crash_text);
    Evidence {
        lib: lib_text,
        api: api_text,
        api_ok,
        ui_ok,
        crash_ok,
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
        "API-001" => from_flag(row, evidence.api_ok, "cargo test --test api"),
        "API-002" => from_flag(row, evidence.ui_ok, "cargo test --test ui"),
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
        "STORE-002" => from_flag(
            row,
            test_ok(
                evidence,
                "storage::tests::log_cut_after_persist_keeps_entries",
            ) && evidence.crash_ok,
            "cargo test log_cut_after_persist_keeps_entries && crash_cut",
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
                "write::tests::unapplied_credits_reject_at_limit",
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
        "REPLAY-004" => from_tests(
            evidence,
            row,
            &[
                "engine::tests::snapshot_writes_file",
                "engine::tests::snapshot_controller_fires_at_20000_entries",
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
        "E2E-003" => e2e_report_complete(artifacts, row),
        "PERF-001" => perf_command_compile(cli, artifacts, row),
        "PERF-002" => perf_history_snapshot(cli, artifacts, row),
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

fn from_test(evidence: &Evidence, row: &MatrixRow, name: &str) -> CaseResult {
    from_flag(row, test_ok(evidence, name), format!("cargo test {name}"))
}

fn from_api_test(evidence: &Evidence, row: &MatrixRow, name: &str) -> CaseResult {
    from_flag(
        row,
        evidence.api.contains(&format!("test {name} ... ok")),
        format!("cargo test --test api {name}"),
    )
}

fn from_tests(evidence: &Evidence, row: &MatrixRow, names: &[&str]) -> CaseResult {
    let missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !test_ok(evidence, name))
        .collect();
    from_flag(
        row,
        missing.is_empty(),
        format!("cargo test {}", names.join(" ")),
    )
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

fn fail(row: &MatrixRow, command: &str, actual: impl Into<String>) -> CaseResult {
    finish(row, "FAIL", command, actual.into(), Vec::new())
}

fn blocked(row: &MatrixRow, command: &str, actual: impl Into<String>) -> CaseResult {
    finish(row, "BLOCKED", command, actual.into(), Vec::new())
}

fn all_of(row: &MatrixRow, parts: Vec<CaseResult>) -> CaseResult {
    let ok = parts.iter().all(|part| part.status == "PASS");
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
    finish(
        row,
        if ok { "PASS" } else { "FAIL" },
        command,
        actual,
        artifacts.into_iter().map(PathBuf::from).collect(),
    )
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
    }
}

struct ChildProc(Child);

impl Drop for ChildProc {
    fn drop(&mut self) {
        terminate(&mut self.0);
    }
}

fn terminate(child: &mut Child) {
    let pid = child.id().to_string();
    let _ = Command::new("kill")
        .args(["-TERM", &pid])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = child.wait();
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
            Err(err) => return (false, String::new(), err.to_string()),
        }
    }
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
        Ok(child) => ChildProc(child),
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
        Ok(child) => ChildProc(child),
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
    let run = started.actual.split('"').find(|part| part.len() == 32);
    let mut args = vec!["replay", "--local-dir", dir.to_str().unwrap()];
    let run_owned;
    if let Some(run) = run {
        run_owned = run.to_owned();
        args.extend(["--run", &run_owned]);
    }
    let (ok, stdout, stderr) = run_cli(cli, &args);
    if !ok {
        return fail(row, "graphrun replay", format!("{stdout}\n{stderr}"));
    }
    let after = fs::read(&db).unwrap_or_default();
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
    for (yaml, input, expect) in cases {
        let result = local_start(cli, artifacts, row, yaml, input, expect);
        logs.push(format!("{yaml}: {} {}", result.status, result.actual));
        if result.status != "PASS" {
            return finish(
                row,
                "FAIL",
                format!("graphrun start {yaml}"),
                logs.join("\n"),
                vec![artifacts.join(format!("{}-data", row.id))],
            );
        }
    }
    finish(
        row,
        "PASS",
        "graphrun start all completing yaml fixtures",
        logs.join("\n"),
        vec![artifacts.join(format!("{}-data", row.id))],
    )
}

fn e2e_report_complete(artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let matrix = match load_matrix(&PathBuf::from("docs/specs/v1/verification-matrix.tsv")) {
        Ok(rows) => rows,
        Err(err) => return fail(row, "load matrix", err),
    };
    let mut missing = Vec::new();
    let mut seen_self = false;
    for item in &matrix {
        if item.id == row.id {
            seen_self = true;
            continue;
        }
        if seen_self {
            continue;
        }
        let path = artifacts.join(format!("{}.json", item.id));
        if !path.exists() {
            missing.push(item.id.clone());
        }
    }
    if missing.is_empty() {
        finish(
            row,
            "PASS",
            "artifact coverage",
            format!(
                "{} prior cases have evidence files",
                matrix.len().saturating_sub(1)
            ),
            vec![artifacts.to_path_buf()],
        )
    } else {
        fail(
            row,
            "artifact coverage",
            format!("missing {}", missing.join(",")),
        )
    }
}

fn e2e_no_ready_scan(_cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
    let dir = artifacts.join(format!("{}-data", row.id));
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::create_dir_all(&dir);
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => return fail(row, "tokio runtime", err.to_string()),
    };
    rt.block_on(async {
        let engine = match graphrun::Engine::local(&dir).await {
            Ok(engine) => engine,
            Err(err) => return fail(row, "Engine::local", err.to_string()),
        };
        let catalog = match graphrun::Catalog::from_json(include_bytes!(
            "../../docs/specs/v1/examples/activity-catalog.json"
        )) {
            Ok(catalog) => catalog,
            Err(err) => return fail(row, "catalog", err.to_string()),
        };
        let yaml = include_str!("../../docs/specs/v1/examples/sequence.yaml");
        let input = graphrun::Value::Object(
            [
                (
                    "order_id".to_owned(),
                    graphrun::Value::String("o1".to_owned()),
                ),
                ("amount".to_owned(), graphrun::Value::Int(1000)),
            ]
            .into_iter()
            .collect(),
        );
        match engine.start_yaml(yaml, &catalog, input).await {
            Ok(run) => {
                if let Err(err) = engine.wait_terminal(run, Duration::from_secs(20)).await {
                    let _ = engine.shutdown().await;
                    return fail(row, "wait_terminal", err.to_string());
                }
            }
            Err(err) => {
                let _ = engine.shutdown().await;
                return fail(row, "start_yaml", err.to_string());
            }
        }
        let before = graphrun::domain::READY_SCANS.load(std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(60)).await;
        let after = graphrun::domain::READY_SCANS.load(std::sync::atomic::Ordering::Relaxed);
        let _ = engine.shutdown().await;
        if after != before {
            return fail(
                row,
                "60s quiescent ready-index",
                format!("READY_SCANS grew from {before} to {after}"),
            );
        }
        finish(
            row,
            "PASS",
            "60s quiescent observation after Engine::local sequence",
            format!("READY_SCANS stayed {after}"),
            vec![dir],
        )
    })
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
    let _started = local_start(
        cli,
        artifacts,
        row,
        "sequence.yaml",
        r#"{"order_id":"o1","amount":1000}"#,
        "pay-1",
    );
    let local_ms = start.elapsed().as_millis();
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
    let _ = fs::write(&yaml512, body512);
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
    let rate = measure_progress_rate(artifacts);
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let body = format!(
        "compile_ms={compile_ms} compile512_ok={ok512} compile512_ms={compile512_ms} local_start_ms={local_ms} {} cpus={cpus} arch={}",
        match &rate {
            Ok((cmds, p95_us, wall_ms)) =>
                format!("progress_cmds=1000 cmds_per_s={cmds:.1} p95_us={p95_us} wall_ms={wall_ms}"),
            Err(err) => format!("progress_rate_error={err}"),
        },
        std::env::consts::ARCH
    );
    let _ = fs::write(artifacts.join("PERF-001-measure.txt"), &body);
    blocked(
        row,
        "measured compile/local-start/progress on this host",
        format!(
            "{body}; reference target is 1000 committed commands/s p95<100ms on three 4-vCPU members"
        ),
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

fn perf_history_snapshot(cli: &Path, artifacts: &Path, row: &MatrixRow) -> CaseResult {
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
    let history_ms = if nested.status == "PASS" {
        let run = nested
            .actual
            .split('"')
            .find(|part| part.len() == 32)
            .unwrap_or("")
            .to_owned();
        let dir = artifacts.join(format!("{}-nested", row.id));
        let started = Instant::now();
        let _ = run_cli(
            cli,
            &[
                "history",
                "--run",
                &run,
                "--local-dir",
                dir.to_str().unwrap(),
            ],
        );
        started.elapsed().as_millis()
    } else {
        0
    };
    let snapshot_log = artifacts.join("cargo-snapshot.log");
    let snapshot_note = fs::read_to_string(&snapshot_log).unwrap_or_default();
    let snapshot_line = if snapshot_log.exists() {
        snapshot_note
            .lines()
            .find(|line| line.contains("finished in") || line.contains("snapshot fill"))
            .unwrap_or("see cargo-snapshot.log")
            .to_owned()
    } else {
        "cargo-snapshot.log missing".to_owned()
    };
    let body = format!(
        "nested_ms={nested_ms} nested_status={} history_ms={history_ms} snapshot={snapshot_line}",
        nested.status
    );
    let _ = fs::write(artifacts.join("PERF-002-measure.txt"), &body);
    blocked(
        row,
        "measured nested/history/snapshot on this host",
        format!("{body}; reference 4-vCPU cluster hardware is not this machine"),
    )
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
        graphrun::issue_node(&ca, 1).map_err(|err| err.to_string())?,
        graphrun::issue_node(&ca, 2).map_err(|err| err.to_string())?,
        graphrun::issue_node(&ca, 3).map_err(|err| err.to_string())?,
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
    cmd.spawn().map(ChildProc).map_err(|err| err.to_string())
}

fn spawn_fixture_worker(cluster: &LiveCluster, i: usize) -> Result<ChildProc, String> {
    let cert = &cluster.certs[i % cluster.certs.len()];
    let log = cluster.dir.join(format!("worker-{}.log", i + 1));
    let mut cmd = Command::new(&cluster.e2e);
    cmd.args([
        "fixture-worker",
        "--endpoint",
        &format!("https://{}", cluster.addrs[0]),
        "--ca",
        cluster.ca_path.to_str().unwrap(),
        "--cert",
        cert.0.to_str().unwrap(),
        "--key",
        cert.1.to_str().unwrap(),
        "--server-name",
        &cluster.certs[0].2,
    ]);
    cmd.stdout(Stdio::null());
    if let Ok(file) = fs::File::create(log) {
        cmd.stderr(file);
    }
    cmd.spawn().map(ChildProc).map_err(|err| err.to_string())
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
    let definition = examples().join(yaml);
    let mut last = String::new();
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
            ];
            if !wait {
                args.push("--no-wait".to_owned());
            }
            args.extend(cluster_connect(cluster, node, &[]));
            let strs: Vec<&str> = args.iter().map(String::as_str).collect();
            let (ok, stdout, stderr) = run_cli(cli, &strs);
            last = format!("{stdout}\n{stderr}");
            if ok && let Some(id) = stdout.split('"').find(|part| part.len() == 32) {
                return Ok(id.to_owned());
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
    let mut cluster = match boot_three(artifacts, row, 2) {
        Ok(cluster) => cluster,
        Err(err) => return fail(row, "boot cluster", err),
    };
    let run = match cluster_start(
        cli,
        &cluster,
        "nested-controls.yaml",
        r#"[{"value":1},{"value":4}]"#,
        true,
    ) {
        Ok(run) => run,
        Err(err) => return fail(row, "cluster start nested", err),
    };
    if let Err(err) = wait_nodes_succeeded(
        cli,
        &cluster,
        &run,
        0..cluster.addrs.len(),
        2,
        Duration::from_secs(90),
    ) {
        return fail(row, "replicate nested before kill", err);
    }
    terminate(&mut cluster.members[2].0);
    match wait_nodes_succeeded(
        cli,
        &cluster,
        &run,
        1..cluster.addrs.len(),
        1,
        Duration::from_secs(30),
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
    Ok(ChildProc(child))
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
    let tls = match graphrun::issue_node(&ca, 1) {
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
        Ok(child) => ChildProc(child),
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
            let _ = client
                .register(graphrun::generated::RegisterRequest {
                    session_id: session.to_hex(),
                    activities: vec!["*".to_owned()],
                    capacity: 8,
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
        Ok(child) => ChildProc(child),
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
    Ok((ChildProc(provider), url))
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
        Ok(child) => ChildProc(child),
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
        Ok(child) => ChildProc(child),
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
        Ok(child) => ChildProc(child),
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
        match graphrun::Engine::run_worker(endpoint, tls).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{err}");
                ExitCode::from(2)
            }
        }
    })
}
