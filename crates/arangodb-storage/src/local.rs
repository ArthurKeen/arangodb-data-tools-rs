//! Local filesystem [`ObjectStore`] backend.

use std::path::{Component, Path, PathBuf};

use arangodb_tools_core::{Error, Result};
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::store::{
    ByteRange, ByteStream, MetadataStream, ObjectMetadata, ObjectPath, ObjectStore,
};

/// An [`ObjectStore`] rooted at a local directory.
///
/// Object paths are interpreted relative to the root. Paths containing `..` or
/// absolute components are rejected to prevent writes outside the root.
#[derive(Debug, Clone)]
pub struct LocalFileSystem {
    root: PathBuf,
}

impl LocalFileSystem {
    /// Creates a backend rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolves an object path to an absolute filesystem path, rejecting any
    /// path that would escape the root.
    fn resolve(&self, path: &ObjectPath) -> Result<PathBuf> {
        let relative = Path::new(path.as_str());
        for component in relative.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                _ => {
                    return Err(Error::storage(format!(
                        "object path escapes storage root: {path}"
                    )));
                }
            }
        }
        Ok(self.root.join(relative))
    }
}

/// Converts an absolute path back into a backend-relative [`ObjectPath`].
fn relativize(root: &Path, full: &Path) -> ObjectPath {
    let relative = full.strip_prefix(root).unwrap_or(full);
    let key = relative
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    ObjectPath::new(key)
}

/// Writes a byte stream to an open file, returning the number of bytes written.
async fn write_all(file: &mut tokio::fs::File, mut input: ByteStream) -> Result<u64> {
    let mut size: u64 = 0;
    while let Some(chunk) = input.next().await {
        let chunk = chunk?;
        size += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(size)
}

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn put_stream(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata> {
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(&full).await?;
        let size = write_all(&mut file, input).await?;
        Ok(ObjectMetadata {
            path: path.clone(),
            size,
        })
    }

    async fn put_if_absent(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata> {
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // `create_new` makes the existence check and creation atomic.
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full)
            .await
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(Error::already_exists(path.to_string()));
            }
            Err(err) => return Err(err.into()),
        };
        let size = write_all(&mut file, input).await?;
        Ok(ObjectMetadata {
            path: path.clone(),
            size,
        })
    }

    async fn get_stream(&self, path: &ObjectPath, range: Option<ByteRange>) -> Result<ByteStream> {
        let full = self.resolve(path)?;
        let mut file = tokio::fs::File::open(&full).await?;
        if let Some(range) = range {
            file.seek(std::io::SeekFrom::Start(range.start)).await?;
            if let Some(end) = range.end {
                let limit = end.saturating_sub(range.start);
                let reader = file.take(limit);
                let stream: ByteStream =
                    Box::pin(ReaderStream::new(reader).map(|chunk| chunk.map_err(Error::from)));
                return Ok(stream);
            }
        }
        let stream: ByteStream =
            Box::pin(ReaderStream::new(file).map(|chunk| chunk.map_err(Error::from)));
        Ok(stream)
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<ObjectMetadata>> {
        let full = self.resolve(path)?;
        match tokio::fs::metadata(&full).await {
            Ok(metadata) if metadata.is_file() => Ok(Some(ObjectMetadata {
                path: path.clone(),
                size: metadata.len(),
            })),
            // A directory is not an object.
            Ok(_) => Ok(None),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn list(&self, prefix: &ObjectPath) -> MetadataStream {
        let base = match self.resolve(prefix) {
            Ok(base) => base,
            Err(err) => return Box::pin(futures::stream::once(async move { Err(err) })),
        };
        let root = self.root.clone();
        Box::pin(try_stream! {
            // Walk the subtree, then yield in a stable (sorted) order. Local
            // listings are small enough to order in memory; the streaming
            // contract matters for object stores with large prefixes.
            let mut out = Vec::new();
            let mut stack = vec![base];
            while let Some(current) = stack.pop() {
                let metadata = match tokio::fs::metadata(&current).await {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => Err(Error::from(err))?,
                };
                if metadata.is_file() {
                    out.push(ObjectMetadata {
                        path: relativize(&root, &current),
                        size: metadata.len(),
                    });
                } else if metadata.is_dir() {
                    let mut entries = tokio::fs::read_dir(&current).await?;
                    while let Some(entry) = entries.next_entry().await? {
                        stack.push(entry.path());
                    }
                }
            }
            out.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
            for metadata in out {
                yield metadata;
            }
        })
    }

    async fn delete(&self, path: &ObjectPath) -> Result<()> {
        let full = self.resolve(path)?;
        match tokio::fs::remove_file(&full).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn once_stream(data: &'static [u8]) -> ByteStream {
        Box::pin(futures::stream::once(async move {
            Ok::<_, arangodb_tools_core::Error>(Bytes::from_static(data))
        }))
    }

    async fn read_all(mut stream: ByteStream) -> Vec<u8> {
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk.unwrap());
        }
        buffer
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let path = ObjectPath::new("nested/dir/object.txt");

        let meta = store
            .put_stream(&path, once_stream(b"hello world"))
            .await
            .unwrap();
        assert_eq!(meta.size, 11);
        assert!(store.exists(&path).await.unwrap());

        let contents = read_all(store.get_stream(&path, None).await.unwrap()).await;
        assert_eq!(contents, b"hello world");
    }

    #[tokio::test]
    async fn ranged_read_returns_subrange() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let path = ObjectPath::new("data.bin");
        store
            .put_stream(&path, once_stream(b"0123456789"))
            .await
            .unwrap();

        let contents = read_all(
            store
                .get_stream(&path, Some(ByteRange::bounded(2, 5)))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(contents, b"234");

        let tail = read_all(
            store
                .get_stream(&path, Some(ByteRange::starting_at(7)))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(tail, b"789");
    }

    #[tokio::test]
    async fn list_returns_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        store
            .put_stream(&ObjectPath::new("a/one.txt"), once_stream(b"1"))
            .await
            .unwrap();
        store
            .put_stream(&ObjectPath::new("a/b/two.txt"), once_stream(b"22"))
            .await
            .unwrap();

        let listed: Vec<ObjectMetadata> = store
            .list(&ObjectPath::new("a"))
            .map(Result::unwrap)
            .collect()
            .await;
        let paths: Vec<&str> = listed.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(paths, vec!["a/b/two.txt", "a/one.txt"]);
    }

    #[tokio::test]
    async fn put_if_absent_rejects_existing() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let path = ObjectPath::new("manifest.json");

        store
            .put_if_absent(&path, once_stream(b"first"))
            .await
            .unwrap();
        let conflict = store.put_if_absent(&path, once_stream(b"second")).await;
        assert!(matches!(conflict, Err(Error::AlreadyExists(_))));

        // The original content is untouched.
        let contents = read_all(store.get_stream(&path, None).await.unwrap()).await;
        assert_eq!(contents, b"first");
    }

    #[tokio::test]
    async fn head_reports_size_and_absence() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let path = ObjectPath::new("obj.bin");
        store
            .put_stream(&path, once_stream(b"12345"))
            .await
            .unwrap();
        assert_eq!(store.head(&path).await.unwrap().map(|m| m.size), Some(5));
        assert!(store
            .head(&ObjectPath::new("missing"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let path = ObjectPath::new("gone.txt");
        store.put_stream(&path, once_stream(b"x")).await.unwrap();
        store.delete(&path).await.unwrap();
        store.delete(&path).await.unwrap();
        assert!(!store.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        assert!(store.exists(&ObjectPath::new("../escape")).await.is_err());
        assert!(store
            .put_stream(&ObjectPath::new("../escape"), once_stream(b"x"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn missing_object_does_not_exist() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        assert!(!store.exists(&ObjectPath::new("nope.txt")).await.unwrap());
    }
}
