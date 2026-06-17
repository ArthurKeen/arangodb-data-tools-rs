//! Storage abstraction for the ArangoDB data tools.
//!
//! Nothing above this crate should care whether bytes live on a local disk or
//! in an object store. The [`ObjectStore`] trait defines streaming
//! read/write/list/head/delete operations; [`LocalFileSystem`] is the local
//! backend and [`ObjectStoreBackend`] adapts the `object_store` crate for
//! S3-compatible storage (GCS/Azure reuse the same adapter in later phases).

pub mod local;
pub mod object_store_backend;
pub mod store;
pub mod uri;

pub use local::LocalFileSystem;
pub use object_store_backend::ObjectStoreBackend;
pub use store::{ByteRange, ByteStream, MetadataStream, ObjectMetadata, ObjectPath, ObjectStore};
pub use uri::{StorageScheme, StorageUri};
