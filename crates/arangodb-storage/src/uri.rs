//! Storage-URI parsing.
//!
//! Supported schemes:
//!
//! ```text
//! file:///data/dump
//! s3://bucket/prefix
//! gs://bucket/prefix
//! az://container/prefix
//! seaweed+s3://bucket/prefix
//! ```

use arangodb_tools_core::{Error, Result};

/// A recognized storage-URI scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageScheme {
    /// Local filesystem (`file://`).
    File,
    /// S3-compatible (`s3://`).
    S3,
    /// Google Cloud Storage (`gs://`).
    Gcs,
    /// Azure Blob/Data Lake (`az://`).
    Azure,
    /// SeaweedFS via its S3-compatible gateway (`seaweed+s3://`).
    SeaweedS3,
}

/// A parsed storage URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageUri {
    /// The URI scheme.
    pub scheme: StorageScheme,
    /// The bucket/container, or `None` for `file://`.
    pub bucket: Option<String>,
    /// The path/prefix. For `file://` this is the (absolute) filesystem path;
    /// for object stores it is the key prefix with no leading `/`.
    pub path: String,
}

impl StorageUri {
    /// Parses a storage URI.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the URI is malformed, uses an unsupported
    /// scheme, or omits a required bucket.
    pub fn parse(input: &str) -> Result<Self> {
        let (scheme_str, rest) = input
            .split_once("://")
            .ok_or_else(|| Error::config(format!("invalid storage URI: {input}")))?;

        let scheme = match scheme_str {
            "file" => StorageScheme::File,
            "s3" => StorageScheme::S3,
            "gs" => StorageScheme::Gcs,
            "az" => StorageScheme::Azure,
            "seaweed+s3" => StorageScheme::SeaweedS3,
            other => {
                return Err(Error::config(format!(
                    "unsupported storage scheme '{other}' in URI: {input}"
                )));
            }
        };

        if scheme == StorageScheme::File {
            // `file://<host>/path` — we ignore any host component and keep the
            // absolute path beginning at the first `/`.
            let path = match rest.find('/') {
                Some(idx) => rest[idx..].to_owned(),
                None => rest.to_owned(),
            };
            if path.is_empty() {
                return Err(Error::config(format!("file URI missing path: {input}")));
            }
            return Ok(Self {
                scheme,
                bucket: None,
                path,
            });
        }

        let (bucket, path) = match rest.split_once('/') {
            Some((bucket, path)) => (bucket.to_owned(), path.to_owned()),
            None => (rest.to_owned(), String::new()),
        };
        if bucket.is_empty() {
            return Err(Error::config(format!(
                "storage URI missing bucket: {input}"
            )));
        }
        Ok(Self {
            scheme,
            bucket: Some(bucket),
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_file_uri() {
        let uri = StorageUri::parse("file:///data/dump").unwrap();
        assert_eq!(uri.scheme, StorageScheme::File);
        assert_eq!(uri.bucket, None);
        assert_eq!(uri.path, "/data/dump");
    }

    #[test]
    fn parses_s3_uri_with_prefix() {
        let uri = StorageUri::parse("s3://my-bucket/backups/mydb").unwrap();
        assert_eq!(uri.scheme, StorageScheme::S3);
        assert_eq!(uri.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(uri.path, "backups/mydb");
    }

    #[test]
    fn parses_s3_uri_without_prefix() {
        let uri = StorageUri::parse("s3://my-bucket").unwrap();
        assert_eq!(uri.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(uri.path, "");
    }

    #[test]
    fn parses_gcs_azure_and_seaweed() {
        assert_eq!(
            StorageUri::parse("gs://b/p").unwrap().scheme,
            StorageScheme::Gcs
        );
        assert_eq!(
            StorageUri::parse("az://c/p").unwrap().scheme,
            StorageScheme::Azure
        );
        assert_eq!(
            StorageUri::parse("seaweed+s3://b/p").unwrap().scheme,
            StorageScheme::SeaweedS3
        );
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(StorageUri::parse("ftp://host/path").is_err());
    }

    #[test]
    fn rejects_missing_separator() {
        assert!(StorageUri::parse("not-a-uri").is_err());
    }

    #[test]
    fn rejects_missing_bucket() {
        assert!(StorageUri::parse("s3:///key").is_err());
    }
}
