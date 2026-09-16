use graphrun::{Catalog, Engine, Value, compile_yaml};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

async fn child_main(dir: PathBuf) {
    let engine = Engine::local(&dir).await.expect("child engine");
    graphrun::storage::inject_cut("after-log");
    let catalog = Catalog::from_json(include_bytes!(
        "../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .unwrap();
    let yaml = include_str!("../../docs/specs/v1/examples/sequence.yaml");
    let definition = compile_yaml(yaml, &catalog).unwrap();
    let input = Value::Object(BTreeMap::from([
        ("order_id".to_owned(), Value::String("o1".to_owned())),
        ("amount".to_owned(), Value::Int(1000)),
    ]));
    let _ = engine.start(definition, catalog, input).await;
    std::process::exit(2);
}

#[test]
fn reopen_after_child_cut() {
    if let Ok(dir) = std::env::var("GRAPHUN_CRASH_DIR") {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(child_main(PathBuf::from(dir)));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let status = Command::new(exe)
        .arg("reopen_after_child_cut")
        .arg("--exact")
        .env("GRAPHUN_CRASH_DIR", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "child should exit after the cut");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let engine = Engine::local(dir.path())
            .await
            .expect("parent reopens the same directory");
        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.shutdown().await.unwrap();
    });
}
