//! Storage abstraction for the ArangoDB data tools.
//!
//! Nothing above this crate should care whether bytes live on a local disk or
//! in an object store. The [`ObjectStore`] trait defines streaming
//! read/write/list/delete operations; [`LocalFileSystem`] is the first
//! backend. Object-storage backends (S3, GCS, Azure, SeaweedFS) are added in
//! later phases per `docs/IMPLEMENTATION_PLAN.md`.

pub mod local;
pub mod store;
pub mod uri;

pub use local::LocalFileSystem;
pub use store::{ByteRange, ByteStream, ObjectMetadata, ObjectPath, ObjectStore};
pub use uri::{StorageScheme, StorageUri};
