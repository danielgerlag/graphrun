use clap::Parser;
use graphrun::{CertificateAuthority, TlsMaterial, generate_ca, issue_node};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    bridge: PathBuf,
    #[arg(long)]
    writer: PathBuf,
    #[arg(long)]
    cli: PathBuf,
    #[arg(long)]
    bridge_source: String,
    #[arg(long)]
    writer_source: String,
    #[arg(long)]
    artifacts: PathBuf,
    #[arg(long)]
    run_id: Option<String>,
}

#[derive(Serialize)]
struct Observation {
    step: String,
    expected: String,
    actual: Value,
    passed: bool,
}

#[derive(Serialize)]
struct Binary {
    role: &'static str,
    path: String,
    source_revision: String,
    version: String,
    sha256_before: String,
    sha256_after: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SnapshotMetadata {
    #[prost(uint32, tag = "1")]
    framing_version: u32,
    #[prost(uint64, tag = "2")]
    generation: u64,
    #[prost(bytes = "vec", tag = "3")]
    applied_json: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    membership_json: Vec<u8>,
    #[prost(uint64, tag = "5")]
    payload_bytes: u64,
    #[prost(string, repeated, tag = "6")]
    record_formats: Vec<String>,
    #[prost(uint64, tag = "7")]
    record_count: u64,
}

fn snapshot_metadata(file: &Path) -> Result<SnapshotMetadata, String> {
    use prost::Message;
    let mut file = fs::File::open(file).map_err(|err| err.to_string())?;
    let mut magic = [0u8; 21];
    file.read_exact(&mut magic).map_err(|err| err.to_string())?;
    if &magic != b"graphrun.snapshot/v1\0" {
        return Err("snapshot framing version is unsupported".to_owned());
    }
    let mut size = [0u8; 4];
    file.read_exact(&mut size).map_err(|err| err.to_string())?;
    let len = u32::from_le_bytes(size) as usize;
    if len == 0 || len > 1024 * 1024 {
        return Err("snapshot manifest frame is invalid".to_owned());
    }
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes).map_err(|err| err.to_string())?;
    SnapshotMetadata::decode(&*bytes).map_err(|err| err.to_string())
}

fn registry_for_store(store: &Path) -> Result<(Value, Value), String> {
    use redb::{ReadableDatabase, TableDefinition};
    const META: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("meta");
    let db = redb::ReadOnlyDatabase::open(store).map_err(|err| err.to_string())?;
    let read = db.begin_read().map_err(|err| err.to_string())?;
    let meta = read.open_table(META).map_err(|err| err.to_string())?;
    let registry = meta
        .get("snapshot_registry")
        .map_err(|err| err.to_string())?
        .ok_or("active snapshot registry is missing")?;
    let manifest = meta
        .get("store_manifest")
        .map_err(|err| err.to_string())?
        .ok_or("store manifest is missing")?;
    Ok((
        serde_json::from_slice(registry.value()).map_err(|err| err.to_string())?,
        serde_json::from_slice(manifest.value()).map_err(|err| err.to_string())?,
    ))
}

struct Member {
    id: usize,
    label: String,
    child: Child,
}

enum StartupExpectation {
    Ready,
    Rejected,
}

impl Drop for Member {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

struct Cluster {
    root: PathBuf,
    cli: PathBuf,
    ca: PathBuf,
    certs: Vec<(PathBuf, PathBuf, String)>,
    addrs: Vec<SocketAddr>,
    processes: Vec<Member>,
    observations: Vec<Observation>,
    workflow_run_id: Option<String>,
    preparation_command_id: Option<String>,
    activation_command_id: Option<String>,
}

impl Cluster {
    fn new(root: PathBuf, cli: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&root).map_err(|err| err.to_string())?;
        let root = fs::canonicalize(root).map_err(|err| err.to_string())?;
        let ca = generate_ca().map_err(|err| err.to_string())?;
        let ca_path = root.join("ca.pem");
        fs::write(&ca_path, ca.pem.as_bytes()).map_err(|err| err.to_string())?;
        let certs = (1..=3)
            .map(|id| save_cert(&root, &ca, id))
            .collect::<Result<Vec<_>, _>>()?;
        let addrs = (0..3)
            .map(|_| {
                TcpListener::bind("127.0.0.1:0")
                    .and_then(|listener| listener.local_addr())
                    .map_err(|err| err.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        fs::write(
            root.join("workflow.yaml"),
            include_str!("../../../docs/specs/v1/examples/events.yaml"),
        )
        .map_err(|err| err.to_string())?;
        fs::write(
            root.join("catalog.json"),
            include_bytes!("../../../docs/specs/v1/examples/activity-catalog.json"),
        )
        .map_err(|err| err.to_string())?;
        fs::write(root.join("input.json"), br#"{"key":"format-rollout"}"#)
            .map_err(|err| err.to_string())?;
        fs::write(root.join("approval.json"), br#"{"approved":true}"#)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            root,
            cli,
            ca: ca_path,
            certs,
            addrs,
            processes: Vec::new(),
            observations: Vec::new(),
            workflow_run_id: None,
            preparation_command_id: None,
            activation_command_id: None,
        })
    }

    fn dir(&self, id: usize) -> PathBuf {
        self.root.join(format!("m{id}"))
    }

    fn marker(&self, id: usize) -> PathBuf {
        self.dir(id).join("shutdown.request")
    }

    fn spawn(
        &mut self,
        binary: &Path,
        id: usize,
        initialize: bool,
        label: &str,
        expectation: StartupExpectation,
    ) -> Result<(), String> {
        fs::create_dir_all(self.dir(id)).map_err(|err| err.to_string())?;
        if self.marker(id).exists() {
            fs::remove_file(self.marker(id)).map_err(|err| err.to_string())?;
        }
        let log = fs::File::create(self.root.join(format!("member-{id}-{label}.log")))
            .map_err(|err| err.to_string())?;
        let mut cmd = Command::new(binary);
        cmd.args([
            "fixture-member",
            "--data-dir",
            self.dir(id).to_str().ok_or("member path is not UTF-8")?,
            "--bind",
            &self.addrs[id - 1].to_string(),
            "--node-id",
            &id.to_string(),
            "--ca",
            self.ca.to_str().ok_or("CA path is not UTF-8")?,
            "--cert",
            self.certs[id - 1]
                .0
                .to_str()
                .ok_or("certificate path is not UTF-8")?,
            "--key",
            self.certs[id - 1]
                .1
                .to_str()
                .ok_or("private key path is not UTF-8")?,
            "--server-name",
            &self.certs[id - 1].2,
            "--shutdown-on-file",
            self.marker(id).to_str().ok_or("marker path is not UTF-8")?,
        ]);
        if initialize {
            cmd.arg("--initialize").arg("--host-activities");
        }
        for other in 1..=3 {
            if other != id {
                cmd.arg("--peer").arg(format!(
                    "{}|{}|{}|{}|{}",
                    other,
                    self.addrs[other - 1],
                    self.certs[other - 1].0.display(),
                    self.certs[other - 1].1.display(),
                    self.certs[other - 1].2
                ));
            }
        }
        cmd.stdout(Stdio::from(log.try_clone().map_err(|err| err.to_string())?))
            .stderr(Stdio::from(log));
        let child = cmd.spawn().map_err(|err| err.to_string())?;
        println!("member {id} {label} PID {}", child.id());
        self.processes.push(Member {
            id,
            label: label.to_owned(),
            child,
        });
        match expectation {
            StartupExpectation::Ready => self.wait_member_ready(id, label),
            StartupExpectation::Rejected => Ok(()),
        }
    }

    fn wait_member_ready(&mut self, id: usize, label: &str) -> Result<(), String> {
        let socket = self.dir(id).join("control.sock");
        let log_path = self.root.join(format!("member-{id}-{label}.log"));
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let process = self.processes.last_mut().ok_or("member process missing")?;
            if let Some(status) = process.child.try_wait().map_err(|err| err.to_string())? {
                let log = fs::read_to_string(&log_path)
                    .map_err(|err| format!("{}: {err}", log_path.display()))?;
                return Err(format!(
                    "member {id} {label} exited {status} before control socket was ready: {log}"
                ));
            }
            if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "member {id} {label} PID {} did not bind {}",
                    process.child.id(),
                    socket.display()
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn stop(&mut self, id: usize) -> Result<(), String> {
        fs::write(self.marker(id), b"stop").map_err(|err| err.to_string())?;
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let process = self
                .processes
                .iter_mut()
                .rev()
                .find(|member| member.id == id)
                .ok_or("member process missing")?;
            if let Some(status) = process.child.try_wait().map_err(|err| err.to_string())? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("member {id} exited {status}"))
                };
            }
            if Instant::now() >= deadline {
                return Err(format!("member {id} did not stop"));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn cli(&self, args: &[String]) -> Result<Value, String> {
        let output = Command::new(&self.cli)
            .args(args)
            .output()
            .map_err(|err| err.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "CLI {:?} exited {}: {}",
                args,
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|err| format!("CLI {:?} returned invalid JSON: {err}", args))
    }

    fn remote(&self, id: usize, command: &[&str]) -> Result<Value, String> {
        let mut args = command
            .iter()
            .map(|item| (*item).to_owned())
            .collect::<Vec<_>>();
        args.extend([
            "--endpoint".to_owned(),
            format!("https://{}", self.addrs[id - 1]),
            "--ca".to_owned(),
            self.ca.display().to_string(),
            "--cert".to_owned(),
            self.certs[0].0.display().to_string(),
            "--tls-key".to_owned(),
            self.certs[0].1.display().to_string(),
            "--server-name".to_owned(),
            self.certs[id - 1].2.clone(),
        ]);
        self.cli(&args)
    }

    fn remote_leader(&self, command: &[&str]) -> Result<Value, String> {
        let mut failures = Vec::new();
        for id in [2, 3] {
            match self.remote(id, &["cluster", "format", "status"]) {
                Ok(status) if status["quorum"] == true => match self.remote(id, command) {
                    Ok(result) => return Ok(result),
                    Err(err) => failures.push(format!("member {id}: {err}")),
                },
                Ok(status) => failures.push(format!("member {id} has no quorum: {status}")),
                Err(err) => failures.push(format!("member {id}: {err}")),
            }
        }
        Err(format!("no current writer leader: {}", failures.join("; ")))
    }

    fn new_writer_leader(&self) -> Result<usize, String> {
        for id in [2, 3] {
            if let Ok(status) = self.remote(id, &["cluster", "format", "status"])
                && status["quorum"] == true
            {
                return Ok(id);
            }
        }
        Err("new writers have no live quorum leader".to_owned())
    }

    fn local(&self, id: usize, command: &[&str]) -> Result<Value, String> {
        if !self.dir(id).join("control.sock").exists() {
            return Err(format!("member {id} control socket not ready"));
        }
        let mut args = command
            .iter()
            .map(|item| (*item).to_owned())
            .collect::<Vec<_>>();
        args.extend(["--local-dir".to_owned(), self.dir(id).display().to_string()]);
        self.cli(&args)
    }

    fn await_value(
        &self,
        label: &str,
        mut read: impl FnMut() -> Result<Value, String>,
        match_value: impl Fn(&Value) -> bool,
    ) -> Result<Value, String> {
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            let last = read();
            if let Ok(value) = &last
                && match_value(value)
            {
                return Ok(value.clone());
            }
            if Instant::now() >= deadline {
                return Err(format!("{label} timed out: {last:?}"));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn check(
        &mut self,
        step: &str,
        expected: &str,
        actual: Value,
        valid: bool,
    ) -> Result<(), String> {
        self.observations.push(Observation {
            step: step.to_owned(),
            expected: expected.to_owned(),
            actual,
            passed: valid,
        });
        if valid {
            Ok(())
        } else {
            Err(format!("{step}: {expected}"))
        }
    }

    fn finish(&mut self) -> Vec<Value> {
        let root = self.root.clone();
        self.processes
            .iter_mut()
            .map(|member| {
                let pid = member.child.id();
                let (terminated, status) = match member.child.try_wait() {
                    Ok(Some(status)) => (false, status.to_string()),
                    Ok(None) => {
                        let kill = member.child.kill();
                        match (kill, member.child.wait()) {
                            (Ok(()), Ok(status)) => (true, status.to_string()),
                            (Err(err), _) | (_, Err(err)) => (true, format!("reap error: {err}")),
                        }
                    }
                    Err(err) => (false, format!("wait error: {err}")),
                };
                json!({
                    "member_id":member.id,
                    "label":member.label,
                    "pid":pid,
                    "terminated":terminated,
                    "status":status,
                    "log":root.join(format!("member-{}-{}.log",member.id,member.label)).display().to_string(),
                    "store":root.join(format!("m{}",member.id)).join("member.redb").display().to_string(),
                    "snapshot_dir":root.join(format!("m{}",member.id)).join("snapshots").display().to_string()
                })
            })
            .collect()
    }
}

fn save_cert(
    root: &Path,
    ca: &CertificateAuthority,
    id: usize,
) -> Result<(PathBuf, PathBuf, String), String> {
    let TlsMaterial {
        cert_pem,
        key_pem,
        server_name,
        ..
    } = issue_node(ca, id as u64).map_err(|err| err.to_string())?;
    let cert = root.join(format!("n{id}.cert.pem"));
    let key = root.join(format!("n{id}.key.pem"));
    fs::write(&cert, cert_pem).map_err(|err| err.to_string())?;
    fs::write(&key, key_pem).map_err(|err| err.to_string())?;
    Ok((cert, key, server_name))
}

fn sha(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|err| format!("{}: {err}", path.display()))
}

fn evidence(role: &'static str, path: &Path, revision: String) -> Result<Binary, String> {
    if !matches!(revision.len(), 40 | 64) || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!(
            "{role} needs a pinned 40-hex source commit or 64-hex source digest"
        ));
    }
    let path = fs::canonicalize(path).map_err(|err| err.to_string())?;
    let output = Command::new(&path)
        .arg("--version")
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "{} --version exited {}",
            path.display(),
            output.status
        ));
    }
    Ok(Binary {
        role,
        path: path.display().to_string(),
        source_revision: revision,
        version: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        sha256_before: sha(&path)?,
        sha256_after: String::new(),
    })
}

fn run(cluster: &mut Cluster, bridge: &Path, writer: &Path) -> Result<(), String> {
    cluster.spawn(writer, 2, false, "writer", StartupExpectation::Ready)?;
    cluster.spawn(writer, 3, false, "writer", StartupExpectation::Ready)?;
    cluster.spawn(bridge, 1, true, "bridge", StartupExpectation::Ready)?;
    cluster.await_value(
        "bridge leadership",
        || cluster.remote(1, &["cluster", "format", "status"]),
        |value| value["quorum"] == true && value["writer_format"] == 4,
    )?;
    let start = cluster.local(
        1,
        &[
            "start",
            "--definition",
            cluster
                .root
                .join("workflow.yaml")
                .to_str()
                .ok_or("workflow path is not UTF-8")?,
            "--catalog",
            cluster
                .root
                .join("catalog.json")
                .to_str()
                .ok_or("catalog path is not UTF-8")?,
            "--input",
            cluster
                .root
                .join("input.json")
                .to_str()
                .ok_or("input path is not UTF-8")?,
            "--no-wait",
        ],
    )?;
    let run = start["run"]
        .as_str()
        .ok_or("start returned no run ID")?
        .to_owned();
    cluster.workflow_run_id = Some(run.clone());
    cluster.await_value(
        "pre-activation event wait",
        || cluster.local(1, &["inspect", "--run", &run]),
        |value| {
            value["pending_waits"]
                .as_array()
                .is_some_and(|waits| !waits.is_empty())
        },
    )?;
    cluster.check(
        "retained pre-activation wait",
        "a real active run waits on a signal before reader preparation",
        start,
        true,
    )?;

    let prepare_id = graphrun::ids::CommandId::generate().to_hex();
    cluster.preparation_command_id = Some(prepare_id.clone());
    let prepared = cluster.remote(
        1,
        &[
            "cluster",
            "format",
            "prepare",
            "--target",
            "5",
            "--command-id",
            &prepare_id,
        ],
    )?;
    cluster.check(
        "reader-first preparation",
        "all three members durably prepared readers at writer v4",
        prepared.clone(),
        prepared["applied"] == true && prepared["active_writer"] == 4,
    )?;
    let ready = cluster.remote(1, &["cluster", "format", "status"])?;
    cluster.check(
        "complete roster",
        "every configured data-bearing member has a committed reader proof",
        ready.clone(),
        ready["prepared"]
            .as_object()
            .is_some_and(|map| map.len() == 3),
    )?;
    let denied = cluster.remote(
        1,
        &[
            "cluster",
            "format",
            "activate",
            "--target",
            "5",
            "--command-id",
            &graphrun::ids::CommandId::generate().to_hex(),
        ],
    );
    cluster.check(
        "old writer cannot activate",
        "bridge writer capability 4 rejects activation without a new-writer binary",
        json!({"result": denied}),
        denied.is_err(),
    )?;
    cluster.stop(1)?;

    let (leader, _) = cluster
        .await_value(
            "new-writer election",
            || {
                for id in [2, 3] {
                    if let Ok(status) = cluster.remote(id, &["cluster", "format", "status"])
                        && status["quorum"] == true
                    {
                        return Ok(json!({"member":id,"status":status}));
                    }
                }
                Err("new writers have no quorum leader".to_owned())
            },
            |value| {
                value["member"]
                    .as_u64()
                    .is_some_and(|id| id == 2 || id == 3)
            },
        )
        .and_then(|value| {
            let leader = value["member"].as_u64().ok_or("leader ID missing")? as usize;
            Ok((leader, value))
        })?;
    cluster.spawn(bridge, 1, false, "bridge-return", StartupExpectation::Ready)?;
    let before_activation = cluster.await_value(
        "bridge catch-up before activation",
        || cluster.local(1, &["cluster", "health"]),
        |value| {
            value["state"] == "Follower"
                && value["last_applied"].as_u64().is_some_and(|index| {
                    index
                        >= prepared["applied_log_id"]["index"]
                            .as_u64()
                            .unwrap_or(u64::MAX)
                })
        },
    )?;
    cluster.check(
        "mixed writer followers",
        "bridge is a live follower while new writer owns quorum",
        before_activation,
        true,
    )?;

    let activation_id = graphrun::ids::CommandId::generate().to_hex();
    cluster.activation_command_id = Some(activation_id.clone());
    let activated = cluster.remote(
        leader,
        &[
            "cluster",
            "format",
            "activate",
            "--target",
            "5",
            "--command-id",
            &activation_id,
        ],
    )?;
    cluster.check(
        "committed writer activation",
        "new leader committed writer 5 against the prepared roster",
        activated.clone(),
        activated["applied"] == true && activated["active_writer"] == 5,
    )?;
    let activation_index = activated["applied_log_id"]["index"]
        .as_u64()
        .ok_or("activation has no committed log index")?;
    let bridge_applied = cluster.await_value(
        "old bridge applies activated entry",
        || cluster.local(1, &["cluster", "health"]),
        |value| {
            value["state"] == "Follower"
                && value["last_applied"]
                    .as_u64()
                    .is_some_and(|index| index >= activation_index)
        },
    )?;
    cluster.check(
        "old reader applies new policy",
        "running bridge applied the committed v5 policy without gaining leadership",
        bridge_applied,
        true,
    )?;
    let output = cluster.root.join("approval.json");
    let signal_id = graphrun::EventId::generate().to_hex();
    let signal = cluster.await_value(
        "post-activation signal receipt",
        || {
            cluster.remote_leader(&[
                "signal",
                "--run",
                &run,
                "--name",
                "approval",
                "--key",
                "format-rollout",
                "--event-id",
                &signal_id,
                "--payload",
                output.to_str().ok_or("payload path is not UTF-8")?,
            ])
        },
        |value| value["status"] == "ok",
    )?;
    cluster.check(
        "new-format workflow progress",
        "new writer accepts a signal against the pre-activation run",
        signal,
        true,
    )?;
    let mut leader_applied = 0;
    for id in [2, 3] {
        let applied = cluster.local(id, &["cluster", "health"])?["last_applied"]
            .as_u64()
            .ok_or_else(|| format!("writer {id} health omitted its applied index"))?;
        leader_applied = leader_applied.max(applied);
    }
    if leader_applied <= activation_index {
        return Err("signal receipt did not advance the new writer's applied log".to_owned());
    }
    let bridge_after_signal = cluster.await_value(
        "bridge signal application",
        || cluster.local(1, &["cluster", "health"]),
        |value| {
            value["last_applied"]
                .as_u64()
                .is_some_and(|index| index >= leader_applied)
        },
    )?;
    cluster.check(
        "old reader applies v3 inbox update",
        "old bridge reads new-format ordered inbox state while remaining a follower",
        bridge_after_signal.clone(),
        bridge_after_signal["state"] == "Follower",
    )?;
    let result = cluster.await_value(
        "historical workflow completion",
        || cluster.remote_leader(&["inspect", "--run", &run]),
        |value| value["status"] == "succeeded",
    )?;
    cluster.check(
        "retained event projection",
        "the old run completed with the exact approved output",
        result.clone(),
        result["output"]["approved"] == true,
    )?;
    let history = cluster.remote_leader(&["history", "--run", &run])?;
    cluster.check(
        "retained history page",
        "pre-activation and post-activation events retain their sequence range",
        history.clone(),
        history["retained_through"]
            .as_u64()
            .is_some_and(|index| index > 1),
    )?;
    let through = history["retained_through"]
        .as_u64()
        .ok_or("history has no retained upper boundary")?;
    let replay = cluster.remote_leader(&[
        "replay",
        "--run",
        &run,
        "--through-sequence",
        &through.to_string(),
    ])?;
    cluster.check(
        "versioned history reconstruction",
        "read-only event replay returns the same exact result as live inspection",
        replay.clone(),
        replay["status"] == result["status"] && replay["output"] == result["output"],
    )?;
    let snapshot_leader = cluster.new_writer_leader()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    let snapshot_result = runtime
        .block_on(graphrun::connect_control(
            cluster.dir(snapshot_leader).join("control.sock"),
            graphrun::ControlRequest::Snapshot,
        ))
        .map_err(|err| err.to_string())?;
    if !snapshot_result.ok {
        return Err(format!(
            "new writer snapshot request failed: {:?}",
            snapshot_result.error
        ));
    }
    let snapshot = cluster.await_value(
        "framed v5 snapshot publication",
        || {
            let files = fs::read_dir(cluster.dir(snapshot_leader).join("snapshots"))
                .map_err(|err| err.to_string())?;
            for file in files {
                let path = file.map_err(|err| err.to_string())?.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "snap")
                {
                    return Ok(json!({"file":path.display().to_string()}));
                }
            }
            Err("snapshot file has not been published".to_owned())
        },
        |value| value["file"].as_str().is_some(),
    )?;
    cluster.check(
        "v5 framed snapshot file",
        "current writer publishes an on-disk Raft snapshot after retained history progresses",
        snapshot,
        true,
    )?;
    cluster.stop(1)?;
    let backup = cluster.root.join("bridge-backup");
    graphrun::Engine::backup(cluster.dir(1), &backup).map_err(|err| err.to_string())?;
    let backed = graphrun::Engine::read_backup(&backup).map_err(|err| err.to_string())?;
    let run_id = graphrun::RunId::from_hex(&run).map_err(|err| err.to_string())?;
    let checkpoint = backed
        .checkpoints
        .get(&run_id)
        .ok_or("terminal run has no retained checkpoint")?;
    cluster.check(
        "backup reader versions",
        "backup preserves the v5 writer policy, old run history and its v2 checkpoint",
        json!({
            "writer":backed.format_policy.active_writer,
            "history":backed.history.len(),
            "checkpoints":backed.checkpoints.len(),
            "checkpoint_format":checkpoint.format,
            "checkpoint_through":checkpoint.through_run_sequence
        }),
        backed.format_policy.active_writer == 5
            && backed.history.contains_key(&run_id)
            && checkpoint.format == graphrun::history::CHECKPOINT_FORMAT
            && checkpoint.through_run_sequence == through,
    )?;
    cluster.spawn(
        bridge,
        1,
        false,
        "rejected-restart",
        StartupExpectation::Rejected,
    )?;
    let restart = cluster.processes.last_mut().ok_or("restart PID missing")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let exit = loop {
        if let Some(status) = restart.child.try_wait().map_err(|err| err.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err("bridge reopened the activated writer store".to_owned());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let observed = fs::read_to_string(cluster.root.join("member-1-rejected-restart.log"))
        .map_err(|err| err.to_string())?;
    cluster.check(
        "old binary restart fenced",
        "bridge cannot reopen a member directory with writer format 5",
        json!({"status":exit.to_string(),"log":observed}),
        !exit.success() && observed.contains("binary writer capability 4"),
    )?;
    cluster.stop(2)?;
    cluster.stop(3)?;
    let (registry, store_manifest) =
        registry_for_store(&cluster.dir(snapshot_leader).join("member.redb"))?;
    let file_name = registry["file_name"]
        .as_str()
        .ok_or("active snapshot registry has no file")?;
    let snapshot_path = cluster
        .dir(snapshot_leader)
        .join("snapshots")
        .join(file_name);
    let metadata = snapshot_metadata(&snapshot_path)?;
    cluster.check(
        "v5 snapshot registry and readers",
        "authoritative snapshot generation names a verified file with both retained record readers",
        json!({
            "member":snapshot_leader,
            "registry":registry,
            "store_format":store_manifest["format"],
            "writer_format":store_manifest["writer_format"],
            "snapshot_path":snapshot_path.display().to_string(),
            "framing_version":metadata.framing_version,
            "record_formats":metadata.record_formats,
            "record_count":metadata.record_count
        }),
        registry["generation"] == store_manifest["active_generation"]
            && metadata.framing_version == 1
            && metadata.record_count > 0
            && metadata
                .record_formats
                .iter()
                .any(|format| format == "graphrun.state-record/v2")
            && metadata
                .record_formats
                .iter()
                .any(|format| format == "graphrun.state-record/v3"),
    )?;
    for id in [1, 2, 3] {
        let state = graphrun::storage::load_domain_readonly(cluster.dir(id).join("member.redb"))
            .map_err(|err| err.to_string())?;
        cluster.check(
            &format!("member {id} retained format"),
            "all three real directories have writer policy 5 and retained run history",
            json!({"writer":state.format_policy.active_writer,"history":state.history.len()}),
            state.format_policy.active_writer == 5
                && state.history.keys().any(|key| key.to_hex() == run),
        )?;
    }
    Ok(())
}

fn main() {
    let args = Args::parse();
    let run_id = args
        .run_id
        .unwrap_or_else(|| graphrun::ids::CommandId::generate().to_hex());
    if run_id.is_empty()
        || run_id.len() > 96
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        eprintln!("run ID must be 1..=96 ASCII letters, digits, hyphens, or underscores");
        std::process::exit(2);
    }

    let root = args.artifacts.join(&run_id);
    let mut cluster = match Cluster::new(root.clone(), args.cli.clone()) {
        Ok(cluster) => cluster,
        Err(err) => {
            eprintln!("format rollout setup: {err}");
            std::process::exit(1);
        }
    };
    let driver = std::env::current_exe().expect("driver executable");
    let driver_before = sha(&driver).expect("driver SHA-256");
    let mut binaries = [
        evidence("old_reader", &args.bridge, args.bridge_source),
        evidence("current_release", &args.writer, args.writer_source.clone()),
        evidence("cli", &args.cli, args.writer_source),
    ];
    let start = Instant::now();
    let outcome = match (&binaries[0], &binaries[1], &binaries[2]) {
        (Ok(old), Ok(new), Ok(_)) if old.sha256_before != new.sha256_before => {
            run(&mut cluster, &args.bridge, &args.writer)
        }
        (Ok(_), Ok(_), Ok(_)) => Err("old and new binaries are identical".to_owned()),
        _ => Err("a shipping binary or its source identity is unavailable".to_owned()),
    };
    let mut integrity = true;
    for binary in binaries.iter_mut().filter_map(|value| value.as_mut().ok()) {
        binary.sha256_after =
            sha(Path::new(&binary.path)).unwrap_or_else(|err| format!("ERROR: {err}"));
        integrity &= binary.sha256_after == binary.sha256_before;
    }
    let passed = outcome.is_ok() && integrity;
    let driver_after = sha(&driver).expect("driver SHA-256 after run");
    let passed = passed && driver_before == driver_after;
    let exits = cluster.finish();
    let report = json!({
        "run_id":run_id,
        "status":if passed {"PASS"} else {"FAIL"},
        "scope":"focused production mixed-binary smoke, not matrix CONTRACT-002",
        "error":outcome.err(),
        "duration_ms":start.elapsed().as_millis(),
        "binary_integrity":integrity,
        "driver":{"path":driver.display().to_string(),"sha256_before":driver_before,"sha256_after":driver_after},
        "binaries":binaries.iter().map(|binary| binary.as_ref().ok()).collect::<Vec<_>>(),
        "observations":std::mem::take(&mut cluster.observations),
        "process_exits":exits,
        "member_stores":(1..=3).map(|id| cluster.dir(id).join("member.redb").display().to_string()).collect::<Vec<_>>(),
        "backup_manifest":cluster.root.join("bridge-backup").join("manifest.json").display().to_string(),
        "workflow_run_id":cluster.workflow_run_id,
        "preparation_command_id":cluster.preparation_command_id,
        "activation_command_id":cluster.activation_command_id,
    });
    drop(cluster);
    if let Err(err) = fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report).expect("report serializes"),
    ) {
        eprintln!("cannot write rollout report: {err}");
        std::process::exit(1);
    }
    println!("{}", root.join("report.json").display());
    if !passed {
        eprintln!("{}", report["error"]);
        std::process::exit(1);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn local_health_does_not_invoke_cli_without_member_socket() {
        let dir = tempfile::tempdir().unwrap();
        let invoked = dir.path().join("cli-invoked");
        let cli = dir.path().join("cli");
        fs::write(
            &cli,
            format!(
                "#!/bin/sh\nprintf called > '{}'\nprintf '{{\"status\":\"ok\"}}\\n'\n",
                invoked.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&cli, fs::Permissions::from_mode(0o700)).unwrap();
        let cluster = Cluster::new(dir.path().join("members"), cli).unwrap();
        let error = cluster
            .local(1, &["cluster", "health"])
            .expect_err("unready member must not be probed through the CLI");
        assert!(error.contains("control socket not ready"), "{error}");
        assert!(!invoked.exists(), "unready member CLI was invoked");
    }

    #[test]
    fn member_restart_reports_child_exit_before_probing_local_health() {
        let dir = tempfile::tempdir().unwrap();
        let failing = dir.path().join("failing-member");
        fs::write(
            &failing,
            b"#!/bin/sh\nprintf 'owner conflict' >&2\nexit 2\n",
        )
        .unwrap();
        fs::set_permissions(&failing, fs::Permissions::from_mode(0o700)).unwrap();
        let mut cluster = Cluster::new(dir.path().join("members"), failing.clone()).unwrap();
        let error = cluster
            .spawn(
                &failing,
                1,
                false,
                "bridge-return",
                StartupExpectation::Ready,
            )
            .unwrap_err();
        assert!(error.contains("exit status: 2"), "{error}");
        assert!(error.contains("owner conflict"), "{error}");
    }

    #[test]
    fn expected_old_binary_rejection_does_not_wait_for_a_socket() {
        let dir = tempfile::tempdir().unwrap();
        let rejected = dir.path().join("old-member");
        fs::write(&rejected, b"#!/bin/sh\nexit 2\n").unwrap();
        fs::set_permissions(&rejected, fs::Permissions::from_mode(0o700)).unwrap();
        let mut cluster = Cluster::new(dir.path().join("members"), rejected.clone()).unwrap();
        cluster
            .spawn(
                &rejected,
                1,
                false,
                "rejected-restart",
                StartupExpectation::Rejected,
            )
            .unwrap();
        let process = cluster.processes.last_mut().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = process.child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(2));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "old binary did not reject startup"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
