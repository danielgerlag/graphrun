use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

const MAGIC: &[u8] = b"graphrun.snapshot/v1\0";
const FRAME_BYTES: usize = 1024 * 1024;
const MAX_SNAPSHOT_BYTES: u64 = 128 * 1024 * 1024 * 1024;

pub(crate) struct RemoveOnDrop(pub PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.0.display(), %error, "snapshot staging cleanup failed");
        }
    }
}

pub struct SnapshotStream {
    pub path: PathBuf,
    file: tokio::fs::File,
    temporary: bool,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl SnapshotStream {
    pub fn open(path: PathBuf, temporary: bool) -> io::Result<Self> {
        let file = File::options().read(true).write(true).open(&path)?;
        Ok(Self {
            path,
            file: tokio::fs::File::from_std(file),
            temporary,
            _permit: None,
        })
    }

    pub fn with_permit(mut self, permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        self._permit = Some(permit);
        self
    }

    pub async fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all().await
    }
}

impl Drop for SnapshotStream {
    fn drop(&mut self) {
        if self.temporary {
            if let Err(error) = std::fs::remove_file(&self.path)
                && error.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!(path = %self.path.display(), %error, "snapshot staging cleanup failed");
            }
        }
    }
}

impl AsyncRead for SnapshotStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotStream {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.file).poll_complete(cx)
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct SnapshotManifest {
    #[prost(uint32, tag = "1")]
    pub framing_version: u32,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub applied_json: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub membership_json: Vec<u8>,
    #[prost(uint64, tag = "5")]
    pub payload_bytes: u64,
    #[prost(string, repeated, tag = "6")]
    pub record_formats: Vec<String>,
    #[prost(uint64, tag = "7")]
    pub record_count: u64,
    #[prost(string, repeated, tag = "8")]
    pub artifact_origin_ids: Vec<String>,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn read_manifest(input: &mut File) -> io::Result<(SnapshotManifest, Sha256)> {
    let mut magic = vec![0; MAGIC.len()];
    input.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(invalid("unknown snapshot framing version"));
    }
    let mut size = [0; 4];
    input.read_exact(&mut size)?;
    let length = u32::from_le_bytes(size) as usize;
    if length == 0 || length > FRAME_BYTES {
        return Err(invalid("snapshot manifest exceeds framing limit"));
    }
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes)?;
    let manifest =
        SnapshotManifest::decode(bytes.as_slice()).map_err(|err| invalid(err.to_string()))?;
    if manifest.framing_version != 1 || manifest.payload_bytes > MAX_SNAPSHOT_BYTES {
        return Err(invalid("unsupported or oversized snapshot"));
    }
    let mut digest = Sha256::new();
    digest.update(MAGIC);
    digest.update(size);
    digest.update(&bytes);
    Ok((manifest, digest))
}

pub(crate) fn write_snapshot(
    source: &mut impl Read,
    dest: &Path,
    manifest: &SnapshotManifest,
) -> io::Result<[u8; 32]> {
    if manifest.framing_version != 1 || manifest.payload_bytes > MAX_SNAPSHOT_BYTES {
        return Err(invalid("unsupported or oversized snapshot"));
    }
    let metadata = manifest.encode_to_vec();
    if metadata.is_empty() || metadata.len() > FRAME_BYTES {
        return Err(invalid("snapshot manifest exceeds framing limit"));
    }
    let length = (metadata.len() as u32).to_le_bytes();
    let mut options = File::options();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(dest)?;
    let mut digest = Sha256::new();
    file.write_all(MAGIC)?;
    file.write_all(&length)?;
    file.write_all(&metadata)?;
    digest.update(MAGIC);
    digest.update(length);
    digest.update(metadata);
    let mut transferred = 0u64;
    let mut buffer = vec![0; FRAME_BYTES];
    loop {
        let size = source.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        transferred = transferred
            .checked_add(size as u64)
            .ok_or_else(|| invalid("snapshot length overflow"))?;
        if transferred > manifest.payload_bytes {
            return Err(invalid("snapshot exceeds declared payload length"));
        }
        let length = (size as u32).to_le_bytes();
        let checksum = Sha256::digest(&buffer[..size]);
        file.write_all(&length)?;
        file.write_all(&buffer[..size])?;
        file.write_all(&checksum)?;
        digest.update(length);
        digest.update(&buffer[..size]);
        digest.update(checksum);
    }
    if transferred != manifest.payload_bytes {
        return Err(invalid("snapshot payload is shorter than its manifest"));
    }
    let end = 0u32.to_le_bytes();
    file.write_all(&end)?;
    digest.update(end);
    let footer: [u8; 32] = digest.finalize().into();
    file.write_all(&footer)?;
    file.sync_all()?;
    Ok(footer)
}

fn inspect_snapshot(
    path: &Path,
    mut output: Option<&mut dyn Write>,
) -> io::Result<(SnapshotManifest, [u8; 32])> {
    let mut input = File::open(path)?;
    let (manifest, mut digest) = read_manifest(&mut input)?;
    let mut transferred = 0u64;
    loop {
        let mut length = [0; 4];
        input.read_exact(&mut length)?;
        let size = u32::from_le_bytes(length) as usize;
        digest.update(length);
        if size == 0 {
            break;
        }
        if size > FRAME_BYTES {
            return Err(invalid("snapshot frame exceeds 1 MiB"));
        }
        let mut data = vec![0; size];
        input.read_exact(&mut data)?;
        let mut checksum = [0; 32];
        input.read_exact(&mut checksum)?;
        if Sha256::digest(&data).as_slice() != checksum {
            return Err(invalid("snapshot frame checksum mismatch"));
        }
        transferred = transferred
            .checked_add(size as u64)
            .ok_or_else(|| invalid("snapshot payload length overflow"))?;
        if transferred > manifest.payload_bytes {
            return Err(invalid("snapshot payload exceeds manifest"));
        }
        if let Some(output) = output.as_deref_mut() {
            output.write_all(&data)?;
        }
        digest.update(data);
        digest.update(checksum);
    }
    if transferred != manifest.payload_bytes {
        return Err(invalid("snapshot payload is incomplete"));
    }
    let mut footer = [0; 32];
    input.read_exact(&mut footer)?;
    if digest.finalize().as_slice() != footer {
        return Err(invalid("snapshot footer checksum mismatch"));
    }
    let mut trailing = [0; 1];
    if input.read(&mut trailing)? != 0 {
        return Err(invalid("snapshot has trailing bytes"));
    }
    Ok((manifest, footer))
}

pub(crate) fn verify_snapshot(path: &Path) -> io::Result<(SnapshotManifest, [u8; 32])> {
    inspect_snapshot(path, None)
}

pub(crate) fn copy_payload(path: &Path, output: &mut impl Write) -> io::Result<()> {
    inspect_snapshot(path, Some(output))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_large_payload_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot");
        let payload = vec![17u8; FRAME_BYTES + 513];
        let manifest = SnapshotManifest {
            framing_version: 1,
            generation: 2,
            applied_json: b"null".to_vec(),
            membership_json: b"{}".to_vec(),
            payload_bytes: payload.len() as u64,
            record_formats: vec!["graphrun.domain/v1".into()],
            record_count: 1,
            artifact_origin_ids: Vec::new(),
        };
        let digest = write_snapshot(&mut payload.as_slice(), &path, &manifest).unwrap();
        assert_eq!(verify_snapshot(&path).unwrap(), (manifest.clone(), digest));
        let mut copied = Vec::new();
        copy_payload(&path, &mut copied).unwrap();
        assert_eq!(copied, payload);
        let mut bytes = std::fs::read(&path).unwrap();
        let frame = MAGIC.len() + 4 + manifest.encoded_len() + 4;
        bytes[frame] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            verify_snapshot(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn snapshot_stream_seeks_and_cleans_temporary_file() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receiving.snap");
        std::fs::write(&path, b"abcd").unwrap();
        let mut stream = SnapshotStream::open(path.clone(), true).unwrap();
        stream.seek(io::SeekFrom::Start(2)).await.unwrap();
        let mut result = [0; 2];
        stream.read_exact(&mut result).await.unwrap();
        assert_eq!(&result, b"cd");
        drop(stream);
        assert!(!path.exists());
    }
}
