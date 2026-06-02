//! Shared configuration types for connections, TLS, batching, and concurrency.

use std::path::PathBuf;
use std::time::Duration;

use crate::redact::Secret;

/// Authentication strategy for connecting to ArangoDB.
///
/// Credentials are wrapped in [`Secret`] so they are never accidentally
/// printed via `Debug`.
#[derive(Debug, Clone, Default)]
pub enum AuthConfig {
    /// No authentication.
    #[default]
    None,
    /// HTTP basic authentication.
    Basic {
        /// The username.
        username: String,
        /// The password.
        password: Secret,
    },
    /// JWT/bearer-token authentication.
    Bearer {
        /// The bearer token.
        token: Secret,
    },
}

/// TLS configuration.
///
/// Certificate verification is **on by default**, a deliberate departure from
/// the reference C++ client tools (which default to no verification).
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Whether to verify the server certificate chain and hostname.
    pub verify_certificates: bool,
    /// Optional path to a custom CA bundle (PEM).
    pub ca_file: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            verify_certificates: true,
            ca_file: None,
        }
    }
}

/// Connection configuration for an ArangoDB endpoint.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// The base endpoint URL, e.g. `http://localhost:8529`.
    pub endpoint: String,
    /// The target database name.
    pub database: String,
    /// Authentication strategy.
    pub auth: AuthConfig,
    /// TLS settings.
    pub tls: TlsConfig,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Connection-establishment timeout.
    pub connect_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8529".to_owned(),
            database: "_system".to_owned(),
            auth: AuthConfig::None,
            tls: TlsConfig::default(),
            request_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Batching limits. A batch is flushed when *either* bound is reached.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size in bytes.
    pub max_bytes: usize,
    /// Maximum number of documents per batch.
    pub max_docs: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_bytes: 4 * 1024 * 1024,
            max_docs: 100_000,
        }
    }
}

/// Concurrency limits for pipelines.
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Number of concurrent sender/worker tasks.
    pub workers: usize,
    /// Global cap on bytes buffered in flight across all workers.
    pub max_in_flight_bytes: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            max_in_flight_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Default worker count derived from available parallelism (at least 2).
#[must_use]
pub fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map_or(2, std::num::NonZeroUsize::get)
        .max(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_verifies_by_default() {
        assert!(TlsConfig::default().verify_certificates);
    }

    #[test]
    fn auth_password_is_redacted_in_debug() {
        let auth = AuthConfig::Basic {
            username: "root".to_owned(),
            password: Secret::new("hunter2"),
        };
        let rendered = format!("{auth:?}");
        assert!(rendered.contains("root"));
        assert!(!rendered.contains("hunter2"));
    }

    #[test]
    fn default_worker_count_is_at_least_two() {
        assert!(default_workers() >= 2);
    }
}
