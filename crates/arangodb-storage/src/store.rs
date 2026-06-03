//! The [`ObjectStore`] trait and supporting types.

use std::fmt;
use std::pin::Pin;

use arangodb_tools_core::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

/// A streaming sequence of byte chunks, used for both reads and writes.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// A backend-relative object path (always uses `/` separators).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath(String);

impl ObjectPath {
    /// Creates an object path from a string.
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// Returns the path as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ObjectPath {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ObjectPath {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// A half-open byte range `[start, end)`; `end` of `None` means "to the end".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// Inclusive start offset.
    pub start: u64,
    /// Exclusive end offset, or `None` for "until end of object".
    pub end: Option<u64>,
}

impl ByteRange {
    /// A range starting at `start` and continuing to the end of the object.
    #[must_use]
    pub fn starting_at(start: u64) -> Self {
        Self { start, end: None }
    }

    /// A bounded range `[start, end)`.
    #[must_use]
    pub fn bounded(start: u64, end: u64) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }
}

/// Metadata describing a stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// The object's path.
    pub path: ObjectPath,
    /// The object's size in bytes.
    pub size: u64,
}

/// A pluggable object/file store with streaming I/O.
#[async_trait]
pub trait ObjectStore: Send + Sync + fmt::Debug {
    /// Writes a stream of bytes to `path`, returning the resulting metadata.
    async fn put_stream(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata>;

    /// Opens a (optionally ranged) read stream for `path`.
    async fn get_stream(&self, path: &ObjectPath, range: Option<ByteRange>) -> Result<ByteStream>;

    /// Lists all objects under `prefix`.
    async fn list(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMetadata>>;

    /// Deletes `path`. Deleting a missing object is not an error.
    async fn delete(&self, path: &ObjectPath) -> Result<()>;

    /// Returns `true` if `path` exists.
    async fn exists(&self, path: &ObjectPath) -> Result<bool>;
}
