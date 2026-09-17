use crate::error::{Error, ErrorKind, Result};
use crate::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

pub fn provider_url() -> Option<String> {
    std::env::var("GRAPHUN_PROVIDER_URL")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Applied,
    NotApplied,
    Unknown,
    Pending,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EffectResponse {
    pub status: EffectStatus,
    pub physical: u32,
    pub logical: u32,
    pub output: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    physical: u32,
    logical: u32,
    status: EffectStatus,
    output: Option<Value>,
    kind: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LedgerFile {
    entries: BTreeMap<String, Entry>,
    holds: BTreeSet<String>,
}

struct Ledger {
    path: PathBuf,
    data: LedgerFile,
}

impl Ledger {
    fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data = if path.exists() {
            let bytes = std::fs::read(&path).map_err(|err| Error::invalid(err.to_string()))?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            LedgerFile::default()
        };
        Ok(Self { path, data })
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| Error::invalid(err.to_string()))?;
        }
        let body =
            serde_json::to_vec_pretty(&self.data).map_err(|err| Error::invalid(err.to_string()))?;
        std::fs::write(&self.path, body).map_err(|err| Error::invalid(err.to_string()))
    }

    fn apply(&mut self, key: &str, kind: &str, output: Value) -> Result<EffectResponse> {
        let held = self.is_held(key);
        {
            let entry = self.data.entries.entry(key.to_owned()).or_insert(Entry {
                physical: 0,
                logical: 0,
                status: EffectStatus::Unknown,
                output: None,
                kind: kind.to_owned(),
            });
            entry.physical = entry.physical.saturating_add(1);
            entry.kind = kind.to_owned();
            if held {
                if entry.logical == 0 {
                    entry.status = EffectStatus::Pending;
                }
            } else if entry.logical == 0 {
                entry.logical = 1;
                entry.status = EffectStatus::Applied;
                entry.output = Some(output);
            }
        }
        self.persist()?;
        if held {
            let entry = self.data.entries.get(key).expect("just inserted");
            return Ok(EffectResponse {
                status: EffectStatus::Pending,
                physical: entry.physical,
                logical: entry.logical,
                output: entry.output.clone(),
            });
        }
        let entry = self.data.entries.get(key).expect("just inserted");
        Ok(EffectResponse {
            status: entry.status,
            physical: entry.physical,
            logical: entry.logical,
            output: entry.output.clone(),
        })
    }

    fn is_held(&self, key: &str) -> bool {
        self.data.holds.contains(key) || self.data.holds.contains("*")
    }

    fn probe(&self, key: &str) -> EffectResponse {
        if self.is_held(key) {
            let physical = self
                .data
                .entries
                .get(key)
                .map(|entry| entry.physical)
                .unwrap_or(0);
            return EffectResponse {
                status: EffectStatus::Unknown,
                physical,
                logical: 0,
                output: None,
            };
        }
        match self.data.entries.get(key) {
            Some(entry) => EffectResponse {
                status: entry.status,
                physical: entry.physical,
                logical: entry.logical,
                output: entry.output.clone(),
            },
            None => EffectResponse {
                status: EffectStatus::NotApplied,
                physical: 0,
                logical: 0,
                output: None,
            },
        }
    }

    fn hold(&mut self, key: &str) -> Result<EffectResponse> {
        self.data.holds.insert(key.to_owned());
        self.persist()?;
        Ok(self.probe(key))
    }

    fn release(&mut self, key: &str) -> Result<EffectResponse> {
        self.data.holds.remove(key);
        self.persist()?;
        Ok(self.probe(key))
    }

    fn set_status(&mut self, key: &str, status: EffectStatus) -> Result<EffectResponse> {
        let entry = self.data.entries.entry(key.to_owned()).or_insert(Entry {
            physical: 0,
            logical: 0,
            status: EffectStatus::Unknown,
            output: None,
            kind: "set".to_owned(),
        });
        entry.status = status;
        if status == EffectStatus::NotApplied {
            entry.logical = 0;
        }
        self.persist()?;
        Ok(self.probe(key))
    }

    fn dump(&self) -> serde_json::Value {
        serde_json::to_value(&self.data).unwrap_or(serde_json::Value::Null)
    }
}

pub fn apply_effect(base: &str, key: &str, kind: &str, output: &Value) -> Result<EffectResponse> {
    exchange(
        base,
        "POST",
        "/v1/effect",
        Some(serde_json::json!({"key": key, "kind": kind, "output": output})),
    )
}

pub fn probe_effect(base: &str, key: &str) -> Result<EffectResponse> {
    exchange(
        base,
        "POST",
        "/v1/probe",
        Some(serde_json::json!({"key": key})),
    )
}

pub fn hold_effect(base: &str, key: &str) -> Result<EffectResponse> {
    exchange(
        base,
        "POST",
        "/v1/hold",
        Some(serde_json::json!({"key": key})),
    )
}

pub fn release_effect(base: &str, key: &str) -> Result<EffectResponse> {
    exchange(
        base,
        "POST",
        "/v1/release",
        Some(serde_json::json!({"key": key})),
    )
}

pub fn set_effect(base: &str, key: &str, status: EffectStatus) -> Result<EffectResponse> {
    exchange(
        base,
        "POST",
        "/v1/set",
        Some(serde_json::json!({"key": key, "status": status})),
    )
}

pub fn dump_ledger(base: &str) -> Result<serde_json::Value> {
    raw_exchange(base, "GET", "/v1/ledger", None)
}

fn exchange(
    base: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<EffectResponse> {
    let value = raw_exchange(base, method, path, body)?;
    serde_json::from_value(value).map_err(|err| Error::invalid(err.to_string()))
}

fn raw_exchange(
    base: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<serde_json::Value> {
    let addr = parse_http_addr(base)?;
    let host = addr.to_string();
    let payload = body
        .as_ref()
        .map(|value| serde_json::to_vec(value).unwrap_or_default())
        .unwrap_or_default();
    let header = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| Error::invalid(err.to_string()))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| Error::invalid(err.to_string()))?;
    stream
        .write_all(header.as_bytes())
        .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))?;
    if !payload.is_empty() {
        stream
            .write_all(&payload)
            .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))?;
    }
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|err| Error::new(ErrorKind::Unavailable, err.to_string()))?;
    let text = String::from_utf8_lossy(&buf);
    let Some((_, rest)) = text.split_once("\r\n\r\n") else {
        return Err(Error::invalid("provider response missing body"));
    };
    if rest.trim().is_empty() {
        return Ok(serde_json::json!({"status":"ok"}));
    }
    serde_json::from_str(rest.trim()).map_err(|err| Error::invalid(err.to_string()))
}

fn parse_http_addr(base: &str) -> Result<SocketAddr> {
    let trimmed = base.trim().trim_end_matches('/');
    let hostport = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| Error::invalid("GRAPHUN_PROVIDER_URL must be http://host:port"))?;
    hostport
        .parse()
        .map_err(|err| Error::invalid(format!("provider address: {err}")))
}

pub async fn serve(listener: TcpListener, data_dir: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&data_dir).map_err(|err| Error::invalid(err.to_string()))?;
    let ledger = Arc::new(Mutex::new(Ledger::open(data_dir.join("ledger.json"))?));
    let notify = Arc::new(Notify::new());
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let ledger = ledger.clone();
        let notify = notify.clone();
        tokio::spawn(async move {
            let Ok(req) = read_http(&mut stream).await else {
                return;
            };
            let response = handle_http(&ledger, &notify, &req).await;
            let body = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });
    }
}

async fn read_http(stream: &mut tokio::net::TcpStream) -> std::io::Result<String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(header_end) = find_header_end(&buf) {
            let headers = std::str::from_utf8(&buf[..header_end]).unwrap_or("");
            let length = content_length(headers).unwrap_or(0);
            if buf.len() >= header_end + 4 + length {
                break;
            }
        }
        if buf.len() > 1024 * 1024 {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value.trim().parse().ok();
        }
    }
    None
}

async fn handle_http(ledger: &Arc<Mutex<Ledger>>, notify: &Notify, req: &str) -> serde_json::Value {
    let Some((head, body)) = req.split_once("\r\n\r\n") else {
        return serde_json::json!({"error": "malformed"});
    };
    let first = head.lines().next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    let json: serde_json::Value =
        serde_json::from_str(body.trim()).unwrap_or(serde_json::Value::Null);
    if method == "GET" && path == "/health" {
        return serde_json::json!({"status":"ok"});
    }
    if method == "GET" && path == "/v1/ledger" {
        return ledger.lock().unwrap().dump();
    }
    let key = json
        .get("key")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if key.is_empty() {
        return serde_json::json!({"error": "key required"});
    }
    match (method, path) {
        ("POST", "/v1/effect") => {
            let kind = json
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("forward")
                .to_owned();
            let output = json
                .get("output")
                .cloned()
                .map(|value| serde_json::from_value(value).unwrap_or(Value::Null))
                .unwrap_or(Value::Null);
            let mut guard = ledger.lock().unwrap();
            match guard.apply(key, &kind, output) {
                Ok(resp) => serde_json::to_value(resp).unwrap_or(serde_json::Value::Null),
                Err(err) => serde_json::json!({"error": err.to_string()}),
            }
        }
        ("POST", "/v1/probe") => {
            let resp = ledger.lock().unwrap().probe(key);
            serde_json::to_value(resp).unwrap_or(serde_json::Value::Null)
        }
        ("POST", "/v1/hold") => match ledger.lock().unwrap().hold(key) {
            Ok(resp) => serde_json::to_value(resp).unwrap_or(serde_json::Value::Null),
            Err(err) => serde_json::json!({"error": err.to_string()}),
        },
        ("POST", "/v1/release") => match ledger.lock().unwrap().release(key) {
            Ok(resp) => {
                notify.notify_waiters();
                serde_json::to_value(resp).unwrap_or(serde_json::Value::Null)
            }
            Err(err) => serde_json::json!({"error": err.to_string()}),
        },
        ("POST", "/v1/set") => {
            let status = json
                .get("status")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| {
                    serde_json::from_value(serde_json::Value::String(value.to_owned())).ok()
                })
                .unwrap_or(EffectStatus::Unknown);
            match ledger.lock().unwrap().set_status(key, status) {
                Ok(resp) => serde_json::to_value(resp).unwrap_or(serde_json::Value::Null),
                Err(err) => serde_json::json!({"error": err.to_string()}),
            }
        }
        _ => serde_json::json!({"error": "unknown path"}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ledger_counts_physical_retries_once_logically() {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let data = dir.path().to_path_buf();
        tokio::spawn(async move {
            let _ = serve(listener, data).await;
        });
        let url = format!("http://{addr}");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if TcpStream::connect(addr).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let output = Value::Object(
            [("payment_id".to_owned(), Value::String("pay-1".to_owned()))]
                .into_iter()
                .collect(),
        );
        let url_clone = url.clone();
        let output_clone = output.clone();
        let first = tokio::task::spawn_blocking(move || {
            apply_effect(&url_clone, "k1", "forward", &output_clone)
        })
        .await
        .unwrap()
        .unwrap();
        let url_clone = url.clone();
        let output_clone = output.clone();
        let second = tokio::task::spawn_blocking(move || {
            apply_effect(&url_clone, "k1", "forward", &output_clone)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(first.logical, 1);
        assert_eq!(first.physical, 1);
        assert_eq!(second.logical, 1);
        assert_eq!(second.physical, 2);
        assert_eq!(second.output, Some(output));
        hold_effect(&url, "k2").unwrap();
        let pending = apply_effect(&url, "k2", "forward", &Value::Null).unwrap();
        assert_eq!(pending.status, EffectStatus::Pending);
        assert_eq!(
            probe_effect(&url, "k2").unwrap().status,
            EffectStatus::Unknown
        );
        release_effect(&url, "k2").unwrap();
        let applied = apply_effect(&url, "k2", "forward", &Value::Bool(true)).unwrap();
        assert_eq!(applied.status, EffectStatus::Applied);
        assert_eq!(applied.logical, 1);
        assert!(applied.physical >= 2);
        set_effect(&url, "k3", EffectStatus::NotApplied).unwrap();
        assert_eq!(
            probe_effect(&url, "k3").unwrap().status,
            EffectStatus::NotApplied
        );
        hold_effect(&url, "*").unwrap();
        let blocked = apply_effect(&url, "k4", "forward", &Value::Bool(true)).unwrap();
        assert_eq!(blocked.status, EffectStatus::Pending);
        assert_eq!(blocked.logical, 0);
        assert_eq!(
            probe_effect(&url, "k4").unwrap().status,
            EffectStatus::Unknown
        );
    }
}
