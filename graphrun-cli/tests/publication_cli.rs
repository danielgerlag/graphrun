use std::process::Command;

#[test]
fn publish_and_start_report_real_failure_and_timeout_as_errors() {
    let root = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("cli-{}", graphrun::ids::CommandId::generate()));
    let store = root.join("store");
    std::fs::create_dir_all(&root).unwrap();
    let fail = root.join("fail.yaml");
    let wait = root.join("wait.yaml");
    let input = root.join("input.json");
    std::fs::write(&input, "null").unwrap();
    std::fs::write(
        &fail,
        "\
dsl: graphrun/v1
id: cli_fail
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: fail
nodes:
  fail:
    kind: fail
    error: {code: publication.failed, message: expected failure}
",
    )
    .unwrap();
    std::fs::write(
        &wait,
        "\
dsl: graphrun/v1
id: cli_wait
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  approval: {schema: unit/v1}
start: wait
nodes:
  wait:
    kind: wait_signal
    signal: approval
    key: {literal: gate}
    consume_from: buffered
    timeout: '1d'
    next: done
    on_timeout: done
  done:
    kind: complete
    output: {literal: null}
",
    )
    .unwrap();

    let cli = env!("CARGO_BIN_EXE_graphrun");
    let catalog = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("docs/specs/v1/examples/activity-catalog.json");
    let publish = |args: &[&str]| {
        let output = Command::new(cli).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "{:?}",
            String::from_utf8_lossy(&output.stderr)
        );
        let body: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(body["status"], "ok");
    };
    publish(&[
        "publish",
        "--catalog",
        catalog.to_str().unwrap(),
        "--local-dir",
        store.to_str().unwrap(),
    ]);
    publish(&[
        "publish",
        "--definition",
        fail.to_str().unwrap(),
        "--local-dir",
        store.to_str().unwrap(),
    ]);
    publish(&[
        "publish",
        "--definition",
        wait.to_str().unwrap(),
        "--local-dir",
        store.to_str().unwrap(),
    ]);
    let failed = Command::new(cli)
        .args([
            "start",
            "--workflow",
            "cli_fail",
            "--start-key",
            "f",
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            store.to_str().unwrap(),
            "--wait-ms",
            "3000",
        ])
        .output()
        .unwrap();
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("publication.failed"));
    let failure_json: serde_json::Value = serde_json::from_slice(&failed.stderr).unwrap();
    assert_eq!(failure_json["status"], "error");
    assert!(!String::from_utf8_lossy(&failed.stdout).contains("\"status\":\"pending\""));

    let timeout = Command::new(cli)
        .args([
            "start",
            "--workflow",
            "cli_wait",
            "--start-key",
            "t",
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            store.to_str().unwrap(),
            "--wait-ms",
            "0",
        ])
        .output()
        .unwrap();
    assert!(!timeout.status.success());
    assert!(String::from_utf8_lossy(&timeout.stderr).contains("timed out waiting"));
    let timeout_json: serde_json::Value = serde_json::from_slice(&timeout.stderr).unwrap();
    assert_eq!(timeout_json["status"], "error");
    assert!(timeout.stdout.is_empty(), "timeout must not print success");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_control_socket_does_not_report_failure_or_timeout_as_success() {
    let root = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("cli-live-{}", graphrun::ids::CommandId::generate()));
    let store = root.join("store");
    std::fs::create_dir_all(&root).unwrap();
    let input = root.join("input.json");
    std::fs::write(&input, "null").unwrap();
    let engine = graphrun::Engine::local(&store).await.unwrap();
    let catalog = graphrun::Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    engine.publish_catalog(1, catalog).await.unwrap();
    engine
        .publish_definition_yaml(
            "dsl: graphrun/v1
id: live_fail
version: 1
input_schema: unit/v1
output_schema: unit/v1
start: fail
nodes:
  fail:
    kind: fail
    error: {code: live.failed, message: expected failure}
",
            1,
        )
        .await
        .unwrap();
    engine
        .publish_definition_yaml(
            "dsl: graphrun/v1
id: live_wait
version: 1
input_schema: unit/v1
output_schema: unit/v1
signals:
  approval: {schema: unit/v1}
start: wait
nodes:
  wait:
    kind: wait_signal
    signal: approval
    key: {literal: gate}
    consume_from: buffered
    timeout: '1d'
    next: done
    on_timeout: done
  done:
    kind: complete
    output: {literal: null}
",
            1,
        )
        .await
        .unwrap();

    let cli = env!("CARGO_BIN_EXE_graphrun");
    let fail = tokio::process::Command::new(cli)
        .args([
            "start",
            "--workflow",
            "live_fail",
            "--start-key",
            "failure",
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            store.to_str().unwrap(),
            "--wait-ms",
            "3000",
        ])
        .output()
        .await
        .unwrap();
    assert!(!fail.status.success());
    assert!(fail.stdout.is_empty());
    let failure: serde_json::Value = serde_json::from_slice(&fail.stderr).unwrap();
    assert_eq!(failure["status"], "error");
    assert!(failure["message"].as_str().unwrap().contains("live.failed"));

    let timeout = tokio::process::Command::new(cli)
        .args([
            "start",
            "--workflow",
            "live_wait",
            "--start-key",
            "timeout",
            "--input",
            input.to_str().unwrap(),
            "--local-dir",
            store.to_str().unwrap(),
            "--wait-ms",
            "0",
        ])
        .output()
        .await
        .unwrap();
    assert!(!timeout.status.success());
    assert!(timeout.stdout.is_empty());
    let timed_out: serde_json::Value = serde_json::from_slice(&timeout.stderr).unwrap();
    assert_eq!(timed_out["status"], "error");
    assert!(
        timed_out["message"]
            .as_str()
            .unwrap()
            .contains("timed out waiting")
    );
    engine.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
