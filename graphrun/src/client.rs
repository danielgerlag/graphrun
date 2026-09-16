use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::generated::client_client::ClientClient;
use crate::generated::{
    CancelRequest, HistoryRequest, InspectRequest, ListRequest, SignalRequest, StartRequest,
};
use crate::ids::{CommandId, EventId, RunId};
use crate::rpc::client_tls;
use crate::tls::TlsMaterial;
use crate::value::Value;
use tonic::transport::Channel;

pub struct GrpcClient {
    inner: ClientClient<Channel>,
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
        Ok(Self {
            inner: ClientClient::new(channel),
        })
    }

    pub async fn start(&mut self, yaml: &str, catalog: &Catalog, input: &Value) -> Result<RunId> {
        let resp = self
            .inner
            .start(StartRequest {
                command_id: CommandId::generate().to_hex(),
                yaml: yaml.to_owned(),
                catalog_json: serde_json::to_vec(catalog)
                    .map_err(|err| Error::invalid(err.to_string()))?,
                input_json: serde_json::to_vec(input)
                    .map_err(|err| Error::invalid(err.to_string()))?,
            })
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
            .into_inner();
        if !resp.error.is_empty() {
            return Err(Error::invalid(resp.error));
        }
        RunId::from_hex(&resp.run_id).map_err(Error::invalid)
    }

    pub async fn signal(
        &mut self,
        run: RunId,
        event_id: EventId,
        name: &str,
        key: &str,
        payload: &Value,
    ) -> Result<()> {
        let resp = self
            .inner
            .signal(SignalRequest {
                command_id: CommandId::generate().to_hex(),
                run_id: run.to_hex(),
                event_id: event_id.to_hex(),
                name: name.to_owned(),
                key: key.to_owned(),
                payload_json: serde_json::to_vec(payload)
                    .map_err(|err| Error::invalid(err.to_string()))?,
            })
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
            .into_inner();
        ack(&resp.error)
    }

    pub async fn cancel(&mut self, run: RunId, reason: &str) -> Result<()> {
        let resp = self
            .inner
            .cancel(CancelRequest {
                command_id: CommandId::generate().to_hex(),
                run_id: run.to_hex(),
                reason: reason.to_owned(),
            })
            .await
            .map_err(|err| Error::invalid(err.to_string()))?
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
