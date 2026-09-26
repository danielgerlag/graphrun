use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::generated::client_client::ClientClient;
use crate::generated::{
    CancelRequest, CommandResultRequest, HistoryRequest, InspectRequest, ListRequest,
    PublishCatalogRequest, PublishDefinitionRequest, SignalRequest, StartRequest,
};
use crate::ids::{CommandId, EventId, RunId};
use crate::rpc::client_tls;
use crate::tls::TlsMaterial;
use crate::value::Value;
use sha2::Digest;
use std::future::Future;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::{Code, Status};

pub struct GrpcClient {
    inner: ClientClient<Channel>,
    retry_budget: Duration,
    command_principal: Option<(String, String)>,
}

impl GrpcClient {
    pub async fn connect(endpoint: &str, tls: &TlsMaterial) -> Result<Self> {
        crate::tls::install_provider();
        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|err| Error::invalid(err.to_string()))?
            .tls_config(client_tls(tls)?)
            .map_err(|err| Error::invalid(err.to_string()))?
            .connect()
            .await
            .map_err(|err| Error::invalid(err.to_string()))?;
        let cluster = hex::encode(&sha2::Sha256::digest(tls.ca_pem.as_bytes())[..16]);
        let verified = crate::tls::ClusterId::parse(cluster)
            .ok()
            .and_then(|expected| {
                let certs = crate::tls::load_certs(&tls.cert_pem).ok()?;
                crate::tls::verify_peer_identity(&tls.ca_pem, &expected, &certs).ok()
            });
        Ok(Self {
            inner: ClientClient::new(channel),
            retry_budget: Duration::from_secs(5 * 60),
            command_principal: verified.map(|peer| {
                (
                    peer.cluster_id().as_str().to_owned(),
                    peer.principal_id().as_str().to_owned(),
                )
            }),
        })
    }

    pub async fn start(&mut self, yaml: &str, catalog: &Catalog, input: &Value) -> Result<RunId> {
        self.start_with_command(yaml, catalog, input, CommandId::generate())
            .await
    }

    pub async fn start_with_command(
        &mut self,
        yaml: &str,
        catalog: &Catalog,
        input: &Value,
        id: CommandId,
    ) -> Result<RunId> {
        let request = StartRequest {
            command_id: id.to_hex(),
            yaml: yaml.to_owned(),
            catalog_json: serde_json::to_vec(catalog)
                .map_err(|err| Error::invalid(err.to_string()))?,
            input_json: serde_json::to_vec(input).map_err(|err| Error::invalid(err.to_string()))?,
            workflow: String::new(),
            version: 0,
            start_key: String::new(),
        };
        let resp = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.start(request).await }
            })
            .await?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(Error::invalid(resp.error));
        }
        RunId::from_hex(&resp.run_id).map_err(Error::invalid)
    }

    pub async fn publish_catalog(
        &mut self,
        version: u32,
        catalog: &Catalog,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_catalog_with_command(version, catalog, CommandId::generate())
            .await
    }

    pub async fn publish_catalog_with_command(
        &mut self,
        version: u32,
        catalog: &Catalog,
        id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        let request = PublishCatalogRequest {
            command_id: id.to_hex(),
            version,
            catalog_json: serde_json::to_vec(catalog)
                .map_err(|err| Error::invalid(err.to_string()))?,
        };
        let response = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.publish_catalog(request).await }
            })
            .await?
            .into_inner();
        serde_json::from_slice(&response.result_json).map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn publish_definition(
        &mut self,
        yaml: &str,
        catalog_version: u32,
    ) -> Result<crate::publication::CommandResult> {
        self.publish_definition_with_command(yaml, catalog_version, CommandId::generate())
            .await
    }

    pub async fn publish_definition_with_command(
        &mut self,
        yaml: &str,
        catalog_version: u32,
        id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        let request = PublishDefinitionRequest {
            command_id: id.to_hex(),
            catalog_version,
            yaml: yaml.to_owned(),
        };
        let response = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.publish_definition(request).await }
            })
            .await?
            .into_inner();
        serde_json::from_slice(&response.result_json).map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn start_published(
        &mut self,
        workflow: &str,
        version: Option<u32>,
        start_key: &str,
        input: &Value,
    ) -> Result<RunId> {
        self.start_published_with_command(
            workflow,
            version,
            start_key,
            input,
            CommandId::generate(),
        )
        .await
    }

    pub async fn start_published_with_command(
        &mut self,
        workflow: &str,
        version: Option<u32>,
        start_key: &str,
        input: &Value,
        id: CommandId,
    ) -> Result<RunId> {
        let request = StartRequest {
            command_id: id.to_hex(),
            yaml: String::new(),
            catalog_json: Vec::new(),
            input_json: serde_json::to_vec(input).map_err(|err| Error::invalid(err.to_string()))?,
            workflow: workflow.to_owned(),
            version: version.unwrap_or(0),
            start_key: start_key.to_owned(),
        };
        let response = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.start(request).await }
            })
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(Error::invalid(response.error));
        }
        RunId::from_hex(&response.run_id).map_err(Error::invalid)
    }

    pub async fn command_result(
        &mut self,
        id: CommandId,
    ) -> Result<crate::publication::CommandResult> {
        let response = self
            .inner
            .get_command_result(CommandResultRequest {
                command_id: id.to_hex(),
            })
            .await
            .map_err(Self::map_status)?
            .into_inner();
        serde_json::from_slice(&response.result_json).map_err(|err| Error::invalid(err.to_string()))
    }

    async fn retry<T, F, Fut>(&self, id: CommandId, mut call: F) -> Result<tonic::Response<T>>
    where
        F: FnMut(ClientClient<Channel>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<T>, Status>>,
    {
        let deadline = tokio::time::Instant::now() + self.retry_budget;
        let mut backoff_ms = 250u64;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let identity = self.command_principal.as_ref().map_or_else(
                    || format!("command {id}"),
                    |(cluster, principal)| {
                        format!("cluster={cluster} principal={principal} command={id}")
                    },
                );
                return Err(Error::new(
                    crate::error::ErrorKind::DeadlineExceeded,
                    format!(
                        "unknown outcome for {identity}; query or retry with the same command ID"
                    ),
                ));
            }
            let attempt = tokio::time::timeout(
                remaining.min(Duration::from_secs(5)),
                call(self.inner.clone()),
            )
            .await;
            match attempt {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(status))
                    if !matches!(
                        status.code(),
                        Code::Unavailable
                            | Code::DeadlineExceeded
                            | Code::Unknown
                            | Code::Cancelled
                    ) =>
                {
                    return Err(Self::map_status(status));
                }
                _ => {}
            }
            let mut random = [0u8; 8];
            getrandom::fill(&mut random).map_err(|err| Error::invalid(err.to_string()))?;
            let jitter = 250 + u64::from_le_bytes(random) % (backoff_ms - 249);
            tokio::time::sleep(
                Duration::from_millis(jitter)
                    .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            )
            .await;
            backoff_ms = (backoff_ms * 2).min(5_000);
        }
    }

    pub async fn signal(
        &mut self,
        run: RunId,
        event_id: EventId,
        name: &str,
        key: &str,
        payload: &Value,
    ) -> Result<()> {
        self.signal_with_command(run, event_id, name, key, payload, CommandId::generate())
            .await
    }

    pub async fn signal_with_command(
        &mut self,
        run: RunId,
        event_id: EventId,
        name: &str,
        key: &str,
        payload: &Value,
        id: CommandId,
    ) -> Result<()> {
        let request = SignalRequest {
            command_id: id.to_hex(),
            run_id: run.to_hex(),
            event_id: event_id.to_hex(),
            name: name.to_owned(),
            key: key.to_owned(),
            payload_json: serde_json::to_vec(payload)
                .map_err(|err| Error::invalid(err.to_string()))?,
        };
        let resp = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.signal(request).await }
            })
            .await?
            .into_inner();
        ack(&resp.error)
    }

    fn map_status(status: Status) -> Error {
        let kind = match status.code() {
            Code::AlreadyExists => crate::error::ErrorKind::AlreadyExists,
            Code::FailedPrecondition => crate::error::ErrorKind::FailedPrecondition,
            Code::NotFound => crate::error::ErrorKind::NotFound,
            Code::Unauthenticated => crate::error::ErrorKind::Unauthenticated,
            Code::PermissionDenied => crate::error::ErrorKind::PermissionDenied,
            Code::Unavailable => crate::error::ErrorKind::Unavailable,
            Code::DeadlineExceeded => crate::error::ErrorKind::DeadlineExceeded,
            Code::ResourceExhausted => crate::error::ErrorKind::ResourceExhausted,
            _ => crate::error::ErrorKind::InvalidArgument,
        };
        Error::new(kind, status.message())
    }

    pub async fn cancel(&mut self, run: RunId, reason: &str) -> Result<()> {
        self.cancel_with_command(run, reason, CommandId::generate())
            .await
    }

    pub async fn cancel_with_command(
        &mut self,
        run: RunId,
        reason: &str,
        id: CommandId,
    ) -> Result<()> {
        let request = CancelRequest {
            command_id: id.to_hex(),
            run_id: run.to_hex(),
            reason: reason.to_owned(),
        };
        let resp = self
            .retry(id, |mut client| {
                let request = request.clone();
                async move { client.cancel(request).await }
            })
            .await?
            .into_inner();
        ack(&resp.error)
    }

    pub async fn inspect(&mut self, run: RunId) -> Result<serde_json::Value> {
        let resp = self
            .inner
            .inspect(InspectRequest {
                run_id: run.to_hex(),
            })
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(Error::invalid(resp.error));
        }
        serde_json::from_slice(&resp.view_json).map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn list(&mut self) -> Result<serde_json::Value> {
        let resp = self
            .inner
            .list(ListRequest {})
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
            .into_inner();
        serde_json::from_slice(&resp.runs_json).map_err(|err| Error::invalid(err.to_string()))
    }

    pub async fn history(&mut self, run: RunId) -> Result<serde_json::Value> {
        let resp = self
            .inner
            .history(HistoryRequest {
                run_id: run.to_hex(),
            })
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(Error::invalid(resp.error));
        }
        serde_json::from_slice(&resp.events_json).map_err(|err| Error::invalid(err.to_string()))
    }
}

fn ack(error: &str) -> Result<()> {
    if error.is_empty() {
        Ok(())
    } else {
        Err(Error::invalid(error.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transient_retries_are_bounded_and_keep_command_identity() {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let client = GrpcClient {
            inner: ClientClient::new(channel),
            retry_budget: Duration::from_millis(300),
            command_principal: None,
        };
        let id = CommandId::generate();
        let mut attempts = 0;
        let error = client
            .retry::<crate::generated::StartResponse, _, _>(id, |_| {
                attempts += 1;
                async {
                    Err::<tonic::Response<crate::generated::StartResponse>, _>(Status::unavailable(
                        "transport",
                    ))
                }
            })
            .await
            .unwrap_err();
        assert!(attempts >= 1);
        assert_eq!(error.kind, crate::error::ErrorKind::DeadlineExceeded);
        assert!(error.message.contains(&id.to_hex()));
        assert!(error.message.contains("unknown outcome"));
    }
}
