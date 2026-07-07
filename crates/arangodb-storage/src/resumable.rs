//! Restart-resumable, backend-agnostic chunked object uploads.
//!
//! `object_store`'s multipart upload (used inside
//! [`ObjectStore::put_stream`](crate::ObjectStore::put_stream)) is efficient
//! but *not* restart-resumable: if the process dies mid-upload, the parts are
//! abandoned and the next run starts from zero. That is acceptable for the
//! streaming dump/restore path, whose data source (a replication cursor) is not
//! seekable and which already resumes at collection/part granularity via
//! checkpoints.
//!
//! For a **seekable** source — a local file staged to the cloud, or an
//! in-memory buffer — this module provides a resumable alternative that works
//! identically across every backend (local FS, S3, GCS, Azure, SeaweedFS):
//!
//! * The logical object at `base` is stored as an ordered set of fixed-size
//!   part objects under `"<base>.upload/"`, plus a small `state.json` marker
//!   written last once every part is present.
//! * [`upload_resumable`] uploads only the parts that are missing (or present
//!   with the wrong size), so re-running after an interruption skips the work
//!   already done and finishes the tail. This is safe because a seekable
//!   [`PartSource`] can re-read any part by offset.
//! * [`read_resumable`] streams the parts back in order, reconstructing the
//!   original bytes, and [`open_resumable`] reports the completed size.
//!
//! Because the representation is plain part objects addressed by key, no
//! provider-specific multipart upload ID has to be persisted, which is what
//! makes the scheme portable across backends.

use std::path::PathBuf;
use std::sync::Arc;

use arangodb_tools_core::{Error, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::store::{ByteStream, ObjectPath, ObjectStore};

/// Default part size for resumable uploads (8 MiB; above S3's 5 MiB floor).
pub const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// A seekable byte source that can re-read any window on demand.
///
/// The ability to re-read an arbitrary `[offset, offset+len)` window is what
/// makes an upload resumable: after an interruption the uploader re-reads only
/// the parts it still needs. Implementations must be cheap to call repeatedly.
#[async_trait]
pub trait PartSource: Send + Sync {
    /// Total number of bytes the source will produce.
    fn len(&self) -> u64;

    /// Returns `true` if the source is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reads the window starting at `offset` for up to `len` bytes.
    ///
    /// The returned buffer is shorter than `len` only when `offset + len`
    /// exceeds [`PartSource::len`] (i.e. the final part).
    async fn read_part(&self, offset: u64, len: usize) -> Result<Bytes>;
}

/// An in-memory [`PartSource`] over a [`Bytes`] buffer.
#[derive(Debug, Clone)]
pub struct BytesPartSource {
    data: Bytes,
}

impl BytesPartSource {
    /// Wraps an in-memory buffer as a part source.
    #[must_use]
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}

#[async_trait]
impl PartSource for BytesPartSource {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    async fn read_part(&self, offset: u64, len: usize) -> Result<Bytes> {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.data.len());
        let end = start.saturating_add(len).min(self.data.len());
        Ok(self.data.slice(start..end))
    }
}

/// A [`PartSource`] backed by a local file, read with `seek` + `read`.
#[derive(Debug, Clone)]
pub struct FilePartSource {
    path: PathBuf,
    len: u64,
}

impl FilePartSource {
    /// Opens `path` and records its length.
    ///
    /// # Errors
    /// Returns [`Error::Storage`] if the file's metadata cannot be read.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let metadata = tokio::fs::metadata(&path).await?;
        Ok(Self {
            path,
            len: metadata.len(),
        })
    }
}

#[async_trait]
impl PartSource for FilePartSource {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_part(&self, offset: u64, len: usize) -> Result<Bytes> {
        let remaining = self.len.saturating_sub(offset);
        let want = (remaining.min(len as u64)) as usize;
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut buffer = vec![0u8; want];
        file.read_exact(&mut buffer).await?;
        Ok(Bytes::from(buffer))
    }
}

/// On-disk marker describing a completed resumable upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct UploadState {
    part_size: u64,
    total_size: u64,
    parts: u64,
    complete: bool,
}

/// Metadata for a resumable upload as seen by readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumableUpload {
    /// Fixed part size the object was chunked with.
    pub part_size: u64,
    /// Total reconstructed object size, in bytes.
    pub total_size: u64,
    /// Number of part objects.
    pub parts: u64,
}

/// Returns the `state.json` marker path for `base`.
fn state_object(base: &ObjectPath) -> ObjectPath {
    ObjectPath::new(format!("{}.upload/state.json", base.as_str()))
}

/// Returns the object path for part `index` of `base`.
fn part_object(base: &ObjectPath, index: u64) -> ObjectPath {
    ObjectPath::new(format!("{}.upload/{index:08}.part", base.as_str()))
}

/// Wraps bytes in a single-chunk stream.
fn once(bytes: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes) }))
}

/// Uploads `source` to `base` as resumable part objects, skipping any part
/// already present with the correct size.
///
/// Safe to call repeatedly: an interrupted upload resumes where it left off and
/// a completed upload is a cheap no-op (every part's size already matches). The
/// `state.json` completion marker is written only after every part is present,
/// so [`open_resumable`] never observes a torn upload.
///
/// # Errors
/// Returns [`Error::Storage`] if reading a part or writing to the store fails.
pub async fn upload_resumable(
    store: &dyn ObjectStore,
    base: &ObjectPath,
    source: &dyn PartSource,
    part_size: usize,
) -> Result<ResumableUpload> {
    let part_size = part_size.max(1);
    let part_size_u64 = part_size as u64;
    let total = source.len();
    let parts = total.div_ceil(part_size_u64);

    for index in 0..parts {
        let offset = index * part_size_u64;
        let this_len = (total - offset).min(part_size_u64) as usize;
        let path = part_object(base, index);
        // Resume: trust a part only when it is present at exactly the expected
        // size. A short/truncated part (e.g. from an interrupted write) is
        // re-uploaded.
        if let Some(meta) = store.head(&path).await? {
            if meta.size == this_len as u64 {
                continue;
            }
        }
        let bytes = source.read_part(offset, this_len).await?;
        if bytes.len() != this_len {
            return Err(Error::storage(format!(
                "part source returned {} bytes for part {index}, expected {this_len}",
                bytes.len()
            )));
        }
        store.put_stream(&path, once(bytes)).await?;
    }

    let state = UploadState {
        part_size: part_size_u64,
        total_size: total,
        parts,
        complete: true,
    };
    let json = serde_json::to_vec(&state)?;
    store
        .put_stream(&state_object(base), once(Bytes::from(json)))
        .await?;

    Ok(ResumableUpload {
        part_size: part_size_u64,
        total_size: total,
        parts,
    })
}

/// Opens a completed resumable upload at `base`.
///
/// # Errors
/// Returns [`Error::Storage`] if no completion marker exists, the marker is not
/// yet complete, or it cannot be parsed.
pub async fn open_resumable(store: &dyn ObjectStore, base: &ObjectPath) -> Result<ResumableUpload> {
    let marker = state_object(base);
    if store.head(&marker).await?.is_none() {
        return Err(Error::storage(format!(
            "no resumable upload at {}",
            base.as_str()
        )));
    }
    let bytes = read_all(store.get_stream(&marker, None).await?).await?;
    let state: UploadState = serde_json::from_slice(&bytes)
        .map_err(|err| Error::storage(format!("invalid resumable upload state: {err}")))?;
    if !state.complete {
        return Err(Error::storage(format!(
            "resumable upload at {} is incomplete",
            base.as_str()
        )));
    }
    Ok(ResumableUpload {
        part_size: state.part_size,
        total_size: state.total_size,
        parts: state.parts,
    })
}

/// Streams the reconstructed bytes of a completed resumable upload in order.
///
/// The parts are read sequentially so memory stays bounded regardless of object
/// size.
pub fn read_resumable(
    store: Arc<dyn ObjectStore>,
    base: &ObjectPath,
    upload: &ResumableUpload,
) -> ByteStream {
    let base = base.clone();
    let parts = upload.parts;
    Box::pin(async_stream::try_stream! {
        for index in 0..parts {
            let mut stream = store.get_stream(&part_object(&base, index), None).await?;
            while let Some(chunk) = stream.next().await {
                yield chunk?;
            }
        }
    })
}

/// Deletes every part object and the completion marker for `base`.
///
/// # Errors
/// Returns [`Error::Storage`] if a delete fails.
pub async fn delete_resumable(store: &dyn ObjectStore, base: &ObjectPath) -> Result<()> {
    let prefix = ObjectPath::new(format!("{}.upload/", base.as_str()));
    let mut listing = store.list(&prefix);
    let mut paths = Vec::new();
    while let Some(meta) = listing.next().await {
        paths.push(meta?.path);
    }
    for path in paths {
        store.delete(&path).await?;
    }
    Ok(())
}

/// Drains a byte stream into a contiguous buffer.
async fn read_all(mut stream: ByteStream) -> Result<Bytes> {
    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk?);
    }
    Ok(buffer.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::LocalFileSystem;

    /// A tempdir-backed local store plus a shareable `Arc` handle to it.
    struct TestStore {
        _dir: tempfile::TempDir,
        store: LocalFileSystem,
        root: PathBuf,
    }

    impl TestStore {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().to_path_buf();
            let store = LocalFileSystem::new(&root);
            Self {
                _dir: dir,
                store,
                root,
            }
        }

        fn arc(&self) -> Arc<dyn ObjectStore> {
            Arc::new(LocalFileSystem::new(&self.root))
        }
    }

    async fn read_back(ts: &TestStore, base: &ObjectPath) -> Vec<u8> {
        let upload = open_resumable(&ts.store, base).await.unwrap();
        let mut stream = read_resumable(ts.arc(), base, &upload);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn round_trips_across_multiple_parts() {
        let ts = TestStore::new();
        let base = ObjectPath::new("dir/object.bin");
        let data: Vec<u8> = (0..20_000u32).map(|i| i as u8).collect();
        let source = BytesPartSource::new(data.clone());

        let info = upload_resumable(&ts.store, &base, &source, 8192)
            .await
            .unwrap();
        assert_eq!(info.total_size, 20_000);
        assert_eq!(info.parts, 3); // ceil(20000 / 8192)

        assert_eq!(read_back(&ts, &base).await, data);
    }

    #[tokio::test]
    async fn empty_source_produces_zero_parts() {
        let ts = TestStore::new();
        let base = ObjectPath::new("empty.bin");
        let info = upload_resumable(&ts.store, &base, &BytesPartSource::new(Bytes::new()), 4096)
            .await
            .unwrap();
        assert_eq!(info.parts, 0);
        assert_eq!(info.total_size, 0);
        assert!(read_back(&ts, &base).await.is_empty());
    }

    #[tokio::test]
    async fn resumes_after_a_truncated_tail() {
        let ts = TestStore::new();
        let base = ObjectPath::new("resume.bin");
        let data: Vec<u8> = (0..20_000u32).map(|i| (i * 7) as u8).collect();
        let source = BytesPartSource::new(data.clone());

        // Full upload, then simulate an interruption: drop the last part and
        // the completion marker (as if the process died before finishing).
        upload_resumable(&ts.store, &base, &source, 8192)
            .await
            .unwrap();
        ts.store.delete(&state_object(&base)).await.unwrap();
        ts.store.delete(&part_object(&base, 2)).await.unwrap();
        assert!(open_resumable(&ts.store, &base).await.is_err());

        // Re-running finishes the tail and re-writes the marker.
        upload_resumable(&ts.store, &base, &source, 8192)
            .await
            .unwrap();
        assert_eq!(read_back(&ts, &base).await, data);
    }

    #[tokio::test]
    async fn re_uploads_a_wrong_sized_part() {
        let ts = TestStore::new();
        let base = ObjectPath::new("corrupt.bin");
        let data: Vec<u8> = (0..20_000u32).map(|i| i as u8).collect();
        let source = BytesPartSource::new(data.clone());
        upload_resumable(&ts.store, &base, &source, 8192)
            .await
            .unwrap();

        // Corrupt the first part so its size no longer matches; the next run
        // must detect the mismatch and re-upload it.
        ts.store
            .put_stream(&part_object(&base, 0), once(Bytes::from_static(b"short")))
            .await
            .unwrap();
        upload_resumable(&ts.store, &base, &source, 8192)
            .await
            .unwrap();
        assert_eq!(read_back(&ts, &base).await, data);
    }

    #[tokio::test]
    async fn file_part_source_round_trips() {
        let ts = TestStore::new();
        let base = ObjectPath::new("from-file.bin");
        let data: Vec<u8> = (0..12_345u32).map(|i| (i % 251) as u8).collect();

        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), &data).unwrap();
        let source = FilePartSource::open(file.path()).await.unwrap();
        assert_eq!(source.len(), data.len() as u64);

        upload_resumable(&ts.store, &base, &source, 4096)
            .await
            .unwrap();
        assert_eq!(read_back(&ts, &base).await, data);
    }

    #[tokio::test]
    async fn delete_removes_parts_and_marker() {
        let ts = TestStore::new();
        let base = ObjectPath::new("cleanup.bin");
        let source = BytesPartSource::new(vec![7u8; 10_000]);
        upload_resumable(&ts.store, &base, &source, 4096)
            .await
            .unwrap();

        delete_resumable(&ts.store, &base).await.unwrap();
        assert!(open_resumable(&ts.store, &base).await.is_err());
        assert!(!ts.store.exists(&part_object(&base, 0)).await.unwrap());
    }
}
