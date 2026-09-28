use clap::Parser;
use graphrun::ids::CommandId;
use graphrun::{
    Catalog, CertificateAuthority, ControlRequest, ControlResponse, TlsMaterial, connect_control,
    generate_ca, issue_node,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
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
    bridge_source: String,
    #[arg(long)]
    writer_source: String,
    #[arg(long)]
    bridge_build_log: PathBuf,
    #[arg(long)]
    writer_build_log: PathBuf,
    #[arg(long)]
    artifacts: PathBuf,
}

#[derive(Serialize)]
struct Observation {
    step: String,
    expected: String,
    actual: Value,
    passed: bool,
}

#[derive(Serialize)]
struct BinaryEvidence {
    role: &'static str,
    path: String,
    source_revision: String,
    version: String,
    sha256_before: String,
    sha256_after: String,
    build_log: String,
    build_log_sha256: String,
    features: &'static str,
}

struct Member {
    id: usize,
    child: Child,
}

struct Members(Vec<Member>);

impl Members {
    fn spawn(
        cluster: &ProofCluster,
        id: usize,
        executable: &Path,
        initialize: bool,
        suffix: &str,
    ) -> Result<Member, String> {
        let dir = cluster.member_dir(id);
        fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
        let log = fs::File::create(cluster.root.join(format!("member-{id}-{suffix}.log")))
            .map_err(|err| err.to_string())?;
        let mut command = Command::new(executable);
        command
            .args([
                "fixture-member",
                "--data-dir",
                dir.to_str().ok_or("member path is not UTF-8")?,
                "--bind",
                &cluster.addresses[id - 1].to_string(),
                "--node-id",
                &id.to_string(),
                "--ca",
                cluster.ca_path.to_str().ok_or("CA path is not UTF-8")?,
                "--cert",
                cluster.certs[id - 1]
                    .0
                    .to_str()
                    .ok_or("certificate path is not UTF-8")?,
                "--key",
                cluster.certs[id - 1]
                    .1
                    .to_str()
                    .ok_or("private key path is not UTF-8")?,
                "--server-name",
                &cluster.certs[id - 1].2,
                "--shutdown-on-file",
                dir.join("shutdown.request")
                    .to_str()
                    .ok_or("shutdown path is not UTF-8")?,
            ])
            .env(
                "GRAPHRUN_FORMAT_PROOF_PARTITION",
                dir.join("partition.marker"),
            );
        if initialize {
            command.arg("--initialize");
        }
        for other in 1..=3 {
            if other != id {
                command.arg("--peer").arg(format!(
                    "{}|{}|{}|{}|{}",
                    other,
                    cluster.addresses[other - 1],
                    cluster.certs[other - 1].0.display(),
                    cluster.certs[other - 1].1.display(),
                    cluster.certs[other - 1].2
                ));
            }
        }
        command
            .stdout(Stdio::from(log.try_clone().map_err(|err| err.to_string())?))
            .stderr(Stdio::from(log));
        let child = command.spawn().map_err(|err| err.to_string())?;
        println!("member {id} PID {}", child.id());
        Ok(Member { id, child })
    }
}

impl Drop for Members {
    fn drop(&mut self) {
        for member in &mut self.0 {
            if member.child.try_wait().ok().flatten().is_none() {
                let _ = member.child.kill();
            }
            let _ = member.child.wait();
        }
    }
}

struct ProofCluster {
    root: PathBuf,
    ca_path: PathBuf,
    certs: Vec<(PathBuf, PathBuf, String)>,
    addresses: Vec<SocketAddr>,
    members: Members,
    observations: Vec<Observation>,
}

impl ProofCluster {
    fn new(root: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&root).map_err(|err| err.to_string())?;
        let ca = generate_ca().map_err(|err| err.to_string())?;
        let ca_path = root.join("ca.pem");
        fs::write(&ca_path, &ca.pem).map_err(|err| err.to_string())?;
        let certs = (1..=3)
            .map(|id| save_member_cert(&root, &ca, id))
            .collect::<Result<_, _>>()?;
        let addresses = (0..3)
            .map(|_| {
                TcpListener::bind("127.0.0.1:0")
                    .and_then(|listener| listener.local_addr())
                    .map_err(|err| err.to_string())
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            root,
            ca_path,
            certs,
            addresses,
            members: Members(Vec::new()),
            observations: Vec::new(),
        })
    }

    fn member_dir(&self, id: usize) -> PathBuf {
        self.root.join(format!("m{id}"))
    }

    async fn control(&self, id: usize, request: ControlRequest) -> Result<ControlResponse, String> {
        tokio::time::timeout(
            Duration::from_secs(5),
            connect_control(self.member_dir(id).join("control.sock"), request),
        )
        .await
        .map_err(|_| format!("member {id} control request timed out"))?
        .map_err(|err| err.to_string())
    }

    async fn status(&self, id: usize, command_id: Option<&str>) -> Result<Value, String> {
        let response = self
            .control(
                id,
                ControlRequest::ProofStatus {
                    command_id: command_id.map(str::to_owned),
                },
            )
            .await?;
        if !response.ok {
            return Err(format!("member {id} status: {:?}", response.error));
        }
        Ok(response.body)
    }

    async fn wait_status(
        &self,
        id: usize,
        command_id: Option<&str>,
        condition: impl Fn(&Value) -> bool,
    ) -> Result<Value, String> {
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            let last = match self.status(id, command_id).await {
                Ok(status) => {
                    if condition(&status) {
                        return Ok(status);
                    }
                    status.to_string()
                }
                Err(err) => err,
            };
            if Instant::now() > deadline {
                return Err(format!("member {id} did not reach state: {last}"));
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    fn assert(
        &mut self,
        step: &str,
        expected: &str,
        actual: Value,
        passed: bool,
    ) -> Result<(), String> {
        self.observations.push(Observation {
            step: step.to_owned(),
            expected: expected.to_owned(),
            actual,
            passed,
        });
        if passed {
            Ok(())
        } else {
            Err(format!("{step}: {expected}"))
        }
    }
}

fn save_member_cert(
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

fn hash_file(path: &Path) -> Result<String, String> {
    fs::read(path)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|err| format!("{}: {err}", path.display()))
}

fn binary_evidence(
    role: &'static str,
    path: &Path,
    source_revision: String,
    features: &'static str,
    build_log: &Path,
) -> Result<BinaryEvidence, String> {
    if source_revision.len() != 40 || !source_revision.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!("{role} needs a pinned 40-hex source revision"));
    }
    let before = hash_file(path)?;
    let output = Command::new(path)
        .arg("--version")
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "{} --version failed: {}",
            path.display(),
            output.status
        ));
    }
    Ok(BinaryEvidence {
        role,
        path: path.display().to_string(),
        source_revision,
        version: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        sha256_before: before,
        sha256_after: hash_file(path)?,
        build_log: build_log.display().to_string(),
        build_log_sha256: hash_file(build_log)?,
        features,
    })
}

fn proof_catalog() -> Result<Catalog, String> {
    Catalog::from_json(include_bytes!(
        "../../../docs/specs/v1/examples/activity-catalog.json"
    ))
    .map_err(|err| err.to_string())
}

async fn run(cluster: &mut ProofCluster, bridge: &Path, writer: &Path) -> Result<(), String> {
    let member = Members::spawn(cluster, 2, writer, false, "new")?;
    cluster.members.0.push(member);
    let member = Members::spawn(cluster, 3, writer, false, "new")?;
    cluster.members.0.push(member);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let member = Members::spawn(cluster, 1, bridge, true, "bridge")?;
    cluster.members.0.push(member);
    let old = cluster
        .wait_status(1, None, |value| {
            value["state"] == "Leader" && value["quorum"] == true
        })
        .await?;
    cluster.assert(
        "old bridge leader",
        "v4 writer has a three-member quorum",
        old,
        true,
    )?;

    let receipt_id = CommandId::generate().to_hex();
    let catalog = proof_catalog()?;
    let receipt = cluster
        .control(
            1,
            ControlRequest::PublishCatalog {
                version: 742,
                catalog: catalog.clone(),
                command_id: receipt_id.clone(),
            },
        )
        .await?;
    cluster.assert(
        "cached command receipt",
        "the bridge has a committed receipt before partition",
        json!({"receipt": receipt, "id": receipt_id}),
        receipt.ok && cluster.status(1, Some(&receipt_id)).await?["cached_receipt"] == true,
    )?;
    for id in [2, 3] {
        cluster
            .wait_status(id, Some(&receipt_id), |value| {
                value["cached_receipt"] == true
            })
            .await?;
    }
    fs::write(cluster.member_dir(1).join("partition.marker"), b"partition")
        .map_err(|err| err.to_string())?;
    let leader = {
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            if let Some(id) = async {
                for id in [2, 3] {
                    if let Ok(value) = cluster.status(id, None).await
                        && value["state"] == "Leader"
                        && value["quorum"] == true
                    {
                        return Some(id);
                    }
                }
                None
            }
            .await
            {
                break id;
            }
            if Instant::now() >= deadline {
                return Err("two new writers did not elect a quorum leader".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    };
    let isolated = cluster.status(1, Some(&receipt_id)).await?;
    cluster.assert(
        "old leader partition",
        "bridge retains a cached receipt and sees itself leader but has no quorum",
        isolated.clone(),
        isolated["state"] == "Leader"
            && isolated["quorum"] == false
            && isolated["writer_format"] == 4
            && isolated["cached_receipt"] == true,
    )?;

    let activation = cluster
        .control(leader, ControlRequest::ProofActivate)
        .await?;
    cluster.assert(
        "replicated activation",
        "new writer commits activation through the ordered redb apply",
        json!({"leader": leader, "result": activation}),
        activation.ok,
    )?;
    for id in [2, 3] {
        let status = cluster
            .wait_status(id, None, |value| value["writer_format"] == 5)
            .await?;
        cluster.assert(
            &format!("writer {id} applied"),
            "applied writer format is 5 and capability is 5",
            status.clone(),
            status["binary_writer_capability"] == 5,
        )?;
    }
    let old = cluster.status(1, Some(&receipt_id)).await?;
    cluster.assert(
        "old leader still stale",
        "partitioned bridge remains at format 4 without quorum",
        old.clone(),
        old["writer_format"] == 4 && old["quorum"] == false,
    )?;

    let stale_id = CommandId::generate().to_hex();
    let stale = cluster
        .control(
            1,
            ControlRequest::ProofPropose {
                command_id: stale_id.clone(),
                direct: true,
            },
        )
        .await;
    cluster.assert(
        "stale direct client_write",
        "isolated old leader cannot confirm a committed write",
        json!(stale.as_ref().map(|value| (&value.ok, &value.error))),
        stale.as_ref().map_or(true, |response| !response.ok),
    )?;
    let cached_retry = cluster
        .control(
            1,
            ControlRequest::PublishCatalog {
                version: 742,
                catalog,
                command_id: receipt_id.clone(),
            },
        )
        .await;
    cluster.assert(
        "stale cached receipt retry",
        "an isolated old leader cannot return a successful cached write",
        json!(cached_retry.as_ref().map(|value| (&value.ok, &value.error))),
        cached_retry.as_ref().map_or(true, |response| !response.ok),
    )?;

    let new_id = CommandId::generate().to_hex();
    let new_write = cluster
        .control(
            leader,
            ControlRequest::ProofPropose {
                command_id: new_id.clone(),
                direct: false,
            },
        )
        .await?;
    cluster.assert(
        "new writer commit",
        "new writer can commit a command under activated policy",
        json!({"response": new_write, "leader": leader}),
        new_write.ok,
    )?;
    let legacy_id = CommandId::generate().to_hex();
    let legacy = cluster
        .control(
            leader,
            ControlRequest::ProofLegacyEntry {
                command_id: legacy_id.clone(),
            },
        )
        .await?;
    cluster.assert(
        "in-flight legacy entry",
        "a legacy-format entry committed by a current leader is rejected at ordered apply",
        json!(legacy),
        legacy.ok && legacy.body["entry_rejected"] == true,
    )?;
    for id in [2, 3] {
        let status = cluster.status(id, Some(&legacy_id)).await?;
        cluster.assert(
            &format!("legacy entry has no command result on member {id}"),
            "applied rejection did not write a domain command receipt",
            status.clone(),
            status["command_recorded"] == false,
        )?;
    }
    fs::remove_file(cluster.member_dir(1).join("partition.marker"))
        .map_err(|err| err.to_string())?;
    let joined = cluster
        .wait_status(1, Some(&receipt_id), |value| {
            value["writer_format"] == 5 && value["state"] == "Follower"
        })
        .await?;
    cluster.assert(
        "old leader catches up",
        "bridge applies format 5 but cannot write it",
        joined.clone(),
        joined["binary_writer_capability"] == 4 && joined["cached_receipt"] == true,
    )?;

    let election = cluster.control(1, ControlRequest::ProofElect).await?;
    cluster.assert(
        "manual election request",
        "direct trigger().elect() is exercised on the old bridge",
        json!(election),
        election.ok,
    )?;
    for kind in ["vote", "append"] {
        let probe = cluster
            .control(
                1,
                ControlRequest::ProofProbe {
                    target: if leader == 2 { 3 } else { 2 },
                    kind: kind.to_owned(),
                },
            )
            .await?;
        cluster.assert(
            &format!("old writer {kind}"),
            "authenticated member RPC receives a format FailedPrecondition before OpenRaft",
            json!(probe),
            probe.ok
                && probe.body["rejected"] == true
                && probe.body["reason"]
                    .as_str()
                    .is_some_and(|reason| reason.contains("FailedPrecondition")),
        )?;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let old = cluster.status(1, Some(&receipt_id)).await?;
    let winner = cluster.status(leader, None).await?;
    cluster.assert(
        "old bridge cannot regain quorum leadership",
        "old bridge has no quorum; new leader remains authoritative",
        json!({"old": old, "new": winner}),
        old["quorum"] == false
            && old["state"] != "Leader"
            && old["leader"] != 1
            && winner["quorum"] == true
            && winner["state"] == "Leader",
    )?;
    let after_id = CommandId::generate().to_hex();
    for direct in [false, true] {
        let proposal = cluster
            .control(
                1,
                ControlRequest::ProofPropose {
                    command_id: after_id.clone(),
                    direct,
                },
            )
            .await?;
        cluster.assert(
            if direct {
                "old direct post-activation write"
            } else {
                "old guarded post-activation write"
            },
            "old bridge cannot return a committed write",
            json!(proposal),
            !proposal.ok,
        )?;
    }
    let cached_id = CommandId::generate().to_hex();
    let cached_again = cluster
        .control(
            1,
            ControlRequest::ProofPropose {
                command_id: cached_id.clone(),
                direct: false,
            },
        )
        .await?;
    cluster.assert(
        "old cached receipt fence",
        "old bridge's cached receipt cannot bypass current writer eligibility",
        json!(cached_again),
        !cached_again.ok,
    )?;
    for id in [2, 3] {
        for denied in [&stale_id, &after_id, &cached_id] {
            let status = cluster.status(id, Some(denied)).await?;
            cluster.assert(
                &format!("member {id} denied {denied}"),
                "no old proposal has a committed command receipt",
                status.clone(),
                status["command_recorded"] == false,
            )?;
        }
    }
    let after = cluster
        .control(
            1,
            ControlRequest::PublishCatalog {
                version: 742,
                catalog: proof_catalog()?,
                command_id: receipt_id.clone(),
            },
        )
        .await?;
    cluster.assert(
        "cached publication receipt fenced",
        "old bridge cannot serve an already cached publication write after activation",
        json!(after),
        !after.ok,
    )?;
    let current = cluster.status(leader, Some(&receipt_id)).await?;
    cluster.assert(
        "original receipt preserved",
        "new leader retains the original committed receipt",
        current.clone(),
        current["cached_receipt"] == true,
    )?;
    fs::write(cluster.member_dir(1).join("shutdown.request"), b"stop")
        .map_err(|err| err.to_string())?;
    let old_member = cluster
        .members
        .0
        .iter_mut()
        .find(|member| member.id == 1)
        .ok_or("bridge process missing")?;
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = old_member.child.try_wait().map_err(|err| err.to_string())? {
            if !status.success() {
                return Err(format!("old bridge shutdown failed: {status}"));
            }
            break;
        }
        if Instant::now() >= deadline {
            return Err("old bridge failed to stop on request".to_owned());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let log = fs::File::create(cluster.root.join("member-1-restart.log"))
        .map_err(|err| err.to_string())?;
    let restart = Command::new(bridge)
        .args([
            "fixture-member",
            "--data-dir",
            cluster
                .member_dir(1)
                .to_str()
                .ok_or("member path is not UTF-8")?,
            "--bind",
            &cluster.addresses[0].to_string(),
            "--node-id",
            "1",
            "--ca",
            cluster.ca_path.to_str().ok_or("CA path is not UTF-8")?,
            "--cert",
            cluster.certs[0]
                .0
                .to_str()
                .ok_or("cert path is not UTF-8")?,
            "--key",
            cluster.certs[0].1.to_str().ok_or("key path is not UTF-8")?,
            "--server-name",
            &cluster.certs[0].2,
        ])
        .stdout(Stdio::from(log.try_clone().map_err(|err| err.to_string())?))
        .stderr(Stdio::from(log))
        .spawn()
        .map_err(|err| err.to_string())?;
    println!("restarted bridge PID {}", restart.id());
    cluster.members.0.push(Member {
        id: 100,
        child: restart,
    });
    let exit = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let restarted = cluster
                .members
                .0
                .iter_mut()
                .find(|member| member.id == 100)
                .ok_or("restarted bridge missing")?;
            if let Some(exit) = restarted.child.try_wait().map_err(|err| err.to_string())? {
                return Ok::<_, String>(exit);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| "old bridge did not fail startup after activation".to_owned())??;
    let startup_log = fs::read_to_string(cluster.root.join("member-1-restart.log"))
        .map_err(|err| err.to_string())?;
    cluster.assert(
        "old writer startup refused",
        "bridge binary rejects reopened member store at writer format 5",
        json!({"exit": exit.to_string(), "log": startup_log}),
        !exit.success() && startup_log.contains("proof writer capability 4"),
    )?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_id = CommandId::generate().to_hex();
    let root = args.artifacts.join(&run_id);
    let mut cluster = match ProofCluster::new(root.clone()) {
        Ok(cluster) => cluster,
        Err(err) => {
            eprintln!("proof setup failed: {err}");
            std::process::exit(1);
        }
    };
    let bridge_log = root.join("bridge-build.log");
    let writer_log = root.join("writer-build.log");
    let copy_logs = fs::copy(&args.bridge_build_log, &bridge_log)
        .and_then(|_| fs::copy(&args.writer_build_log, &writer_log));
    if let Err(err) = copy_logs {
        eprintln!("cannot retain proof build logs: {err}");
        drop(cluster);
        std::process::exit(1);
    }
    let old = binary_evidence(
        "old_reader",
        &args.bridge,
        args.bridge_source,
        "format-proof",
        &bridge_log,
    );
    let new = binary_evidence(
        "current_release",
        &args.writer,
        args.writer_source,
        "format-proof-new",
        &writer_log,
    );
    let start = Instant::now();
    let outcome = match (&old, &new) {
        (Ok(old), Ok(new))
            if old.sha256_before != new.sha256_before
                && old.source_revision != new.source_revision =>
        {
            run(&mut cluster, &args.bridge, &args.writer).await
        }
        (Ok(_), Ok(_)) => Err("proof requires distinct source and binary identities".to_owned()),
        (Err(err), _) | (_, Err(err)) => Err(err.clone()),
    };
    let mut binaries = [old.ok(), new.ok()];
    let mut integrity = true;
    for binary in binaries.iter_mut().flatten() {
        binary.sha256_after =
            hash_file(Path::new(&binary.path)).unwrap_or_else(|err| format!("ERROR: {err}"));
        integrity &= binary.sha256_before == binary.sha256_after;
    }
    let passed = outcome.is_ok() && integrity;
    let report = json!({
        "run_id": run_id,
        "status": if passed { "PASS" } else { "FAIL" },
        "scope": "OpenRaft vote, append and proposal feasibility proof only; not CONTRACT-002",
        "duration_ms": start.elapsed().as_millis(),
        "error": outcome.err(),
        "binary_integrity": integrity,
        "binaries": binaries,
        "observations": std::mem::take(&mut cluster.observations),
        "member_logs": ["member-1-bridge.log", "member-2-new.log", "member-3-new.log", "member-1-restart.log"],
    });
    if let Err(err) = fs::write(
        root.join("proof.json"),
        serde_json::to_vec_pretty(&report).expect("report serializes"),
    ) {
        eprintln!("cannot persist proof: {err}");
        drop(cluster);
        std::process::exit(1);
    }
    println!("{}", root.join("proof.json").display());
    drop(cluster);
    if !passed {
        eprintln!("{}", report["error"]);
        std::process::exit(1);
    }
}
