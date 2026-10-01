use crate::error::{Error, ErrorKind, Result};
use crate::storage::StorageHandle;
use tonic::{Request, Status};

pub const WRITER_CAPABILITY: u16 = if cfg!(feature = "format-proof-new") {
    5
} else {
    4
};
pub const WRITER_HEADER: &str = "graphrun-proof-writer-format";

pub fn partitioned() -> bool {
    std::env::var_os("GRAPHRUN_FORMAT_PROOF_PARTITION")
        .is_some_and(|file| std::path::Path::new(&file).exists())
}

pub fn ensure_local_writer(active: u16) -> Result<()> {
    if active > WRITER_CAPABILITY {
        return Err(Error::new(
            ErrorKind::FailedPrecondition,
            format!(
                "proof writer capability {} is below committed writer format {active}",
                WRITER_CAPABILITY
            ),
        ));
    }
    Ok(())
}

pub async fn verify_member_writer<T>(
    storage: &StorageHandle,
    request: &Request<T>,
) -> std::result::Result<(), Status> {
    let active = storage
        .proof_writer_format()
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;
    if active < 5 {
        return Ok(());
    }
    let advertised = request
        .metadata()
        .get(WRITER_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| Status::failed_precondition("proof writer capability missing"))?;
    if advertised < active {
        return Err(Status::failed_precondition(format!(
            "member writer capability {advertised} is below committed format {active}"
        )));
    }
    Ok(())
}
