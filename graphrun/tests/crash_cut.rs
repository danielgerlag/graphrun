use graphrun::{Catalog, Engine, Value, compile_yaml};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

async fn child_main(dir: PathBuf) {
    let engine = Engine::local(&dir).await.expect("child engine");
    graphrun::storage::inject_cut_any_thread("after-log");
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
    std::fs::write(dir.join("cut-ready"), b"after-log").expect("write cut barrier");
    std::future::pending::<()>().await;
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
    let stderr = dir.path().join("child.stderr");
    let mut child = Command::new(exe)
        .arg("reopen_after_child_cut")
        .arg("--exact")
        .env("GRAPHUN_CRASH_DIR", dir.path())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !dir.path().join("cut-ready").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "child exited {status} before crash barrier: {}",
                std::fs::read_to_string(&stderr).unwrap_or_default()
            );
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "child did not reach crash barrier: {}",
                std::fs::read_to_string(&stderr).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let killed_pid = child.id();
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "owned child {killed_pid} must be killed");
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

#[test]
fn reopen_after_owned_heavy_apply_interruption() {
    if let Ok(dir) = std::env::var("GRAPHUN_HEAVY_CRASH_DIR") {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = PathBuf::from(dir);
            let engine = Engine::local(&dir).await.unwrap();
            let catalog = Catalog::from_json(include_bytes!(
                "../../docs/specs/v1/examples/activity-catalog.json"
            ))
            .unwrap();
            let run = engine
                .start_yaml(
                    include_str!("../../docs/specs/v1/examples/events.yaml"),
                    &catalog,
                    Value::Object(BTreeMap::from([(
                        "key".to_owned(),
                        Value::String("k1".to_owned()),
                    )])),
                )
                .await
                .unwrap();
            for index in 1..=1_000 {
                engine.commit_progress(run).await.unwrap();
                if index == 200 {
                    std::fs::write(dir.join("heavy-ready"), run.to_hex()).unwrap();
                }
            }
            std::future::pending::<()>().await;
        });
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let stderr = dir.path().join("heavy.stderr");
    let mut child = Command::new(exe)
        .arg("reopen_after_owned_heavy_apply_interruption")
        .arg("--exact")
        .env("GRAPHUN_HEAVY_CRASH_DIR", dir.path())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !dir.path().join("heavy-ready").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "child exited {status} before heavy-apply barrier: {}",
                std::fs::read_to_string(&stderr).unwrap_or_default()
            );
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!(
                "child did not reach heavy-apply barrier: {}",
                std::fs::read_to_string(&stderr).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let run = graphrun::ids::RunId::from_hex(
        std::fs::read_to_string(dir.path().join("heavy-ready"))
            .unwrap()
            .trim(),
    )
    .unwrap();
    let killed_pid = child.id();
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "owned child {killed_pid} must be killed");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let engine = Engine::local(dir.path())
            .await
            .expect("member must reopen after heavy applied write interruption");
        let state = engine.inspect(run).await.unwrap();
        assert!(state.runs.contains_key(&run));
        assert!(!state.history_records[&run].is_empty());
        engine.shutdown().await.unwrap();
    });
}
