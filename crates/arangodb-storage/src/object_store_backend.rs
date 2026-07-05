//! An [`ObjectStore`] backed by the `object_store` crate.
//!
//! This adapter wraps any `object_store::ObjectStore` (S3, GCS, Azure, …)
//! behind our own trait, so the rest of the workspace never depends on
//! `object_store` types directly (see the Phase 2 spike outcome in
//! `docs/IMPLEMENTATION_PLAN.md`). Phase 2 wires the S3-compatible backend
//! (also covering MinIO/LocalStack); other clouds reuse the same adapter.

use std::sync::Arc;

use arangodb_tools_core::{Error, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as OsPath;
use object_store::{
    buffered::BufWriter, GetOptions, GetRange, ObjectStore as OsObjectStore, ObjectStoreExt,
    PutMode, PutOptions, PutPayload,
};
use tokio::io::AsyncWriteExt;

use crate::store::{
    ByteRange, ByteStream, MetadataStream, ObjectMetadata, ObjectPath, ObjectStore,
};
use crate::uri::{StorageScheme, StorageUri};

/// Part size for streaming multipart uploads (8 MiB; above S3's 5 MiB floor).
const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

/// An [`ObjectStore`] implemented over an `object_store` backend.
#[derive(Clone)]
pub struct ObjectStoreBackend {
    inner: Arc<dyn OsObjectStore>,
    /// Optional key prefix prepended to every path (no trailing slash).
    prefix: Option<String>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wraps an existing `object_store` backend, optionally scoped to `prefix`.
    #[must_use]
    pub fn new(inner: Arc<dyn OsObjectStore>, prefix: Option<String>) -> Self {
        let prefix = prefix
            .map(|p| p.trim_matches('/').to_string())
            .filter(|p| !p.is_empty());
        Self { inner, prefix }
    }

    /// Builds an S3-compatible backend for `bucket`, optionally scoped to a key
    /// `prefix`.
    ///
    /// Connection settings (region, endpoint, credentials, `allow_http`) are
    /// read from the standard `AWS_*` environment variables, so the same code
    /// targets real S3 and MinIO/LocalStack (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`).
    ///
    /// # Errors
    /// Returns [`Error::Storage`] if the backend cannot be constructed.
    pub fn s3(bucket: &str, prefix: Option<String>) -> Result<Self> {
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(map_os_error)?;
        Ok(Self::new(Arc::new(s3), prefix))
    }

    /// Builds a Google Cloud Storage backend for `bucket`, optionally scoped to
    /// a key `prefix`.
    ///
    /// Connection settings are read from the standard `GOOGLE_*` environment
    /// variables (e.g. `GOOGLE_SERVICE_ACCOUNT` / `GOOGLE_SERVICE_ACCOUNT_KEY`,
    /// or `GOOGLE_APPLICATION_CREDENTIALS`).
    ///
    /// # Errors
    /// Returns [`Error::Storage`] if the backend cannot be constructed.
    pub fn gcs(bucket: &str, prefix: Option<String>) -> Result<Self> {
        let gcs = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(map_os_error)?;
        Ok(Self::new(Arc::new(gcs), prefix))
    }

    /// Builds a Microsoft Azure Blob Storage backend for `container`, optionally
    /// scoped to a key `prefix`.
    ///
    /// Connection settings are read from the standard `AZURE_*` environment
    /// variables (e.g. `AZURE_STORAGE_ACCOUNT_NAME` with
    /// `AZURE_STORAGE_ACCOUNT_KEY`, a SAS token, or
    /// `AZURE_STORAGE_USE_EMULATOR` for Azurite).
    ///
    /// # Errors
    /// Returns [`Error::Storage`] if the backend cannot be constructed.
    pub fn azure(container: &str, prefix: Option<String>) -> Result<Self> {
        let azure = MicrosoftAzureBuilder::from_env()
            .with_container_name(container)
            .build()
            .map_err(map_os_error)?;
        Ok(Self::new(Arc::new(azure), prefix))
    }

    /// Builds a cloud backend for a parsed object-storage URI, rooted at the
    /// URI's bucket/container with **no** key prefix (callers address objects by
    /// their full key). Use this for single-object locations.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for `file://` (not an object store) or a URI
    /// without a bucket, or [`Error::Storage`] if the backend cannot be built.
    pub fn for_bucket(uri: &StorageUri) -> Result<Self> {
        Self::build(uri, None)
    }

    /// Builds a cloud backend for a parsed object-storage URI, scoped to the
    /// URI's path as a key `prefix`. Use this for artifact *roots* that hold
    /// many objects (dump/restore).
    ///
    /// # Errors
    /// See [`ObjectStoreBackend::for_bucket`].
    pub fn for_prefix(uri: &StorageUri) -> Result<Self> {
        let prefix = (!uri.path.is_empty()).then(|| uri.path.clone());
        Self::build(uri, prefix)
    }

    /// Dispatches URI scheme to the matching backend builder.
    fn build(uri: &StorageUri, prefix: Option<String>) -> Result<Self> {
        let bucket = uri
            .bucket
            .as_deref()
            .ok_or_else(|| Error::config("object-storage URI is missing a bucket/container"))?;
        match uri.scheme {
            // SeaweedFS is reached through its S3-compatible gateway; point the
            // AWS_* env (AWS_ENDPOINT, AWS_ALLOW_HTTP) at the gateway.
            StorageScheme::S3 | StorageScheme::SeaweedS3 => Self::s3(bucket, prefix),
            StorageScheme::Gcs => Self::gcs(bucket, prefix),
            StorageScheme::Azure => Self::azure(bucket, prefix),
            StorageScheme::File => Err(Error::config(
                "file:// is a local path, not an object store",
            )),
        }
    }

    /// Resolves a backend-relative path to an `object_store` key, applying the
    /// prefix and rejecting traversal segments (PRD §17).
    fn location(&self, path: &ObjectPath) -> Result<OsPath> {
        for segment in path.as_str().split('/') {
            if segment == ".." || segment == "." {
                return Err(Error::storage(format!(
                    "object path escapes storage prefix: {path}"
                )));
            }
        }
        let key = match &self.prefix {
            Some(prefix) => format!("{prefix}/{}", path.as_str().trim_start_matches('/')),
            None => path.as_str().to_string(),
        };
        Ok(OsPath::from(key))
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreBackend {
    async fn put_stream(&self, path: &ObjectPath, mut input: ByteStream) -> Result<ObjectMetadata> {
        let location = self.location(path)?;
        // BufWriter coalesces writes into multipart parts transparently, so
        // arbitrarily large objects upload with bounded memory.
        let mut writer = BufWriter::with_capacity(self.inner.clone(), location, UPLOAD_PART_SIZE);
        let mut size: u64 = 0;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            size += chunk.len() as u64;
            writer.write_all(&chunk).await?;
        }
        writer.shutdown().await?;
        Ok(ObjectMetadata {
            path: path.clone(),
            size,
        })
    }

    async fn put_if_absent(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata> {
        let location = self.location(path)?;
        let payload = collect(input).await?;
        let size = payload.content_length() as u64;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self.inner.put_opts(&location, payload, options).await {
            Ok(_) => Ok(ObjectMetadata {
                path: path.clone(),
                size,
            }),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(Error::already_exists(path.to_string()))
            }
            Err(err) => Err(map_os_error(err)),
        }
    }

    async fn get_stream(&self, path: &ObjectPath, range: Option<ByteRange>) -> Result<ByteStream> {
        let location = self.location(path)?;
        let options = GetOptions {
            range: range.map(|r| match r.end {
                Some(end) => GetRange::Bounded(r.start..end),
                None => GetRange::Offset(r.start),
            }),
            ..GetOptions::default()
        };
        let result = self
            .inner
            .get_opts(&location, options)
            .await
            .map_err(map_os_error)?;
        let stream = result
            .into_stream()
            .map(|chunk| chunk.map_err(map_os_error));
        Ok(Box::pin(stream))
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<ObjectMetadata>> {
        let location = self.location(path)?;
        match self.inner.head(&location).await {
            Ok(meta) => Ok(Some(ObjectMetadata {
                path: path.clone(),
                size: meta.size,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(map_os_error(err)),
        }
    }

    fn list(&self, prefix: &ObjectPath) -> MetadataStream {
        let location = match self.location(prefix) {
            Ok(location) => location,
            Err(err) => return Box::pin(futures::stream::once(async move { Err(err) })),
        };
        // Strip our prefix back off listed keys so callers see backend-relative
        // paths, matching what they passed in.
        let strip = self.prefix.clone();
        let stream = self.inner.list(Some(&location)).map(move |result| {
            result.map_err(map_os_error).map(|meta| {
                let key = meta.location.as_ref();
                let relative = match &strip {
                    Some(prefix) => key
                        .strip_prefix(prefix)
                        .map(|rest| rest.trim_start_matches('/'))
                        .unwrap_or(key),
                    None => key,
                };
                ObjectMetadata {
                    path: ObjectPath::new(relative.to_string()),
                    size: meta.size,
                }
            })
        });
        Box::pin(stream)
    }

    async fn delete(&self, path: &ObjectPath) -> Result<()> {
        let location = self.location(path)?;
        match self.inner.delete(&location).await {
            Ok(()) => Ok(()),
            // Deleting a missing object is not an error.
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(map_os_error(err)),
        }
    }
}

/// Drains a byte stream into a single [`PutPayload`].
async fn collect(mut input: ByteStream) -> Result<PutPayload> {
    let mut buffer = BytesMut::new();
    while let Some(chunk) = input.next().await {
        buffer.extend_from_slice(&chunk?);
    }
    Ok(PutPayload::from_bytes(Bytes::from(buffer)))
}

/// Maps an `object_store` error into the shared error taxonomy.
fn map_os_error(err: object_store::Error) -> Error {
    match err {
        object_store::Error::AlreadyExists { .. } => Error::already_exists(err.to_string()),
        other => Error::storage(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_bucket_and_prefix_reject_file_uri() {
        let uri = StorageUri::parse("file:///tmp/x").unwrap();
        assert!(ObjectStoreBackend::for_bucket(&uri).is_err());
        assert!(ObjectStoreBackend::for_prefix(&uri).is_err());
    }

    #[test]
    fn for_prefix_scopes_backend_to_uri_path() {
        let uri = StorageUri::parse("s3://bucket/backups/db").unwrap();
        let backend = ObjectStoreBackend::for_prefix(&uri).expect("s3 backend builds");
        assert_eq!(backend.prefix.as_deref(), Some("backups/db"));
    }

    #[test]
    fn for_bucket_has_no_prefix() {
        let uri = StorageUri::parse("s3://bucket/key.json").unwrap();
        let backend = ObjectStoreBackend::for_bucket(&uri).expect("s3 backend builds");
        assert_eq!(backend.prefix, None);
    }

    #[test]
    fn seaweed_uses_the_s3_gateway() {
        // SeaweedFS is reached through the S3-compatible backend.
        let uri = StorageUri::parse("seaweed+s3://bucket/prefix").unwrap();
        assert!(ObjectStoreBackend::for_prefix(&uri).is_ok());
    }
}
