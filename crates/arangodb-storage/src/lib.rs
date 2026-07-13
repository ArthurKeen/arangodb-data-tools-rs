//! Storage abstraction for the ArangoDB data tools.
//!
//! Nothing above this crate should care whether bytes live on a local disk or
//! in an object store. The [`ObjectStore`] trait defines streaming
//! read/write/list/head/delete operations; [`LocalFileSystem`] is the local
//! backend and [`ObjectStoreBackend`] adapts the `object_store` crate for
//! S3-compatible storage (GCS/Azure reuse the same adapter in later phases).

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod compression;
pub mod local;
pub mod object_store_backend;
pub mod resumable;
pub mod store;
pub mod uri;

pub use compression::{compress, decompress, Compression};
pub use local::LocalFileSystem;
pub use object_store_backend::ObjectStoreBackend;
pub use resumable::{
    delete_resumable, open_resumable, read_resumable, upload_resumable, BytesPartSource,
    FilePartSource, PartSource, ResumableUpload, DEFAULT_PART_SIZE,
};
pub use store::{ByteRange, ByteStream, MetadataStream, ObjectMetadata, ObjectPath, ObjectStore};
pub use uri::{StorageScheme, StorageUri};
