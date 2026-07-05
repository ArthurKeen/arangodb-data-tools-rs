//! Shared connection/authentication CLI arguments.

use std::path::PathBuf;
use std::time::Duration;

use arangodb_client::{ArangoClient, ArangoClientBuilder};
use arangodb_tools_core::{Error, Result, RetryPolicy};
use clap::Args;

/// Connection and authentication options common to all subcommands.
#[derive(Debug, Args)]
pub(crate) struct ConnectionArgs {
    /// ArangoDB endpoint URL.
    #[arg(long, default_value = "http://localhost:8529")]
    pub endpoint: String,

    /// Target database.
    #[arg(long, default_value = "_system")]
    pub database: String,

    /// Username for basic authentication.
    #[arg(long)]
    pub username: Option<String>,

    /// Name of the environment variable holding the password (the password
    /// itself is never passed on the command line).
    #[arg(long, value_name = "VAR")]
    pub password_env: Option<String>,

    /// Name of the environment variable holding a JWT/bearer token.
    #[arg(long, value_name = "VAR")]
    pub auth_token_env: Option<String>,

    /// Path to a custom CA certificate bundle (PEM).
    #[arg(long, value_name = "FILE")]
    pub tls_ca: Option<PathBuf>,

    /// Disable TLS certificate verification (development only).
    #[arg(long)]
    pub insecure: bool,

    /// Per-request timeout, in seconds.
    #[arg(long, default_value_t = 120)]
    pub request_timeout_secs: u64,

    /// Maximum attempts (including the first) for each retryable request.
    #[arg(long, default_value_t = 5)]
    pub max_retries: u32,

    /// Upper bound, in seconds, on any single retry backoff interval.
    #[arg(long, default_value_t = 30)]
    pub max_retry_delay_secs: u64,
}

impl ConnectionArgs {
    /// Builds an [`ArangoClient`] from these options, resolving credentials
    /// from the named environment variables.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if a named credential variable is unset or the
    /// client cannot be constructed.
    pub(crate) fn build_client(&self) -> Result<ArangoClient> {
        let mut builder: ArangoClientBuilder = ArangoClient::builder()
            .endpoint(&self.endpoint)
            .database(&self.database)
            .insecure(self.insecure)
            .request_timeout(Duration::from_secs(self.request_timeout_secs))
            .retry_policy(RetryPolicy {
                max_attempts: self.max_retries.max(1),
                max_delay: Duration::from_secs(self.max_retry_delay_secs.max(1)),
                ..RetryPolicy::default()
            });

        if let Some(var) = &self.auth_token_env {
            builder = builder.bearer_auth(read_env(var)?);
        } else if let Some(username) = &self.username {
            let password = match &self.password_env {
                Some(var) => read_env(var)?,
                None => String::new(),
            };
            builder = builder.basic_auth(username, password);
        }

        if let Some(ca) = &self.tls_ca {
            builder = builder.tls(arangodb_tools_core::config::TlsConfig {
                verify_certificates: !self.insecure,
                ca_file: Some(ca.clone()),
            });
        }

        builder.build()
    }
}

/// Reads an environment variable by name, erroring clearly if it is unset.
fn read_env(var: &str) -> Result<String> {
    std::env::var(var)
        .map_err(|_| Error::config(format!("environment variable '{var}' is not set")))
}
