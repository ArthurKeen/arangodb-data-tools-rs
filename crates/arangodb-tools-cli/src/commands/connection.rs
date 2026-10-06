//! Shared connection/authentication CLI arguments.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arangodb_client::{ArangoClient, ArangoClientBuilder};
use arangodb_tools_core::{Error, Result, RetryPolicy, RetryStats};
use clap::{Args, ValueEnum};

/// How username/password credentials are presented to the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum AuthMode {
    /// HTTP basic authentication on every request.
    Basic,
    /// Log in at `POST /_open/auth` and use the returned user JWT.
    Jwt,
}

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

    /// Name of the environment variable holding a ready-made JWT/bearer token.
    ///
    /// The token is sent as-is and cannot be refreshed, so if it expires
    /// mid-run the operation fails. Prefer --jwt-secret-file or --auth jwt for
    /// long operations.
    #[arg(long, value_name = "VAR")]
    pub auth_token_env: Option<String>,

    /// Path to the server's JWT secret file (`--server.jwt-secret-keyfile`).
    ///
    /// The client mints and refreshes a short-lived **superuser** JWT from it,
    /// which bypasses user permissions. This is the JWT mode that matches
    /// ArangoDB's own client tools.
    #[arg(long, value_name = "FILE")]
    pub jwt_secret_file: Option<PathBuf>,

    /// Name of the environment variable holding the server's JWT secret.
    ///
    /// Same as --jwt-secret-file, for environments that inject secrets as
    /// variables rather than files.
    #[arg(long, value_name = "VAR")]
    pub jwt_secret_env: Option<String>,

    /// How to present the username/password credentials.
    ///
    /// `basic` (default) sends HTTP basic auth on every request. `jwt` logs in
    /// once at POST /_open/auth and uses the returned token, refreshing it
    /// before it expires. Both authenticate as the same user with the same
    /// permissions; `jwt` avoids resending the password on every request.
    #[arg(long, value_name = "MODE", default_value = "basic")]
    pub auth: AuthMode,

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
        self.build_client_with_stats(&Arc::new(RetryStats::new()))
    }

    /// Builds a client whose retry policy records into `stats`, so a command
    /// can report the retries and server errors its run actually incurred
    /// rather than a hard-coded zero.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if a named credential variable is unset or the
    /// client cannot be constructed.
    pub(crate) fn build_client_with_stats(&self, stats: &Arc<RetryStats>) -> Result<ArangoClient> {
        let mut builder: ArangoClientBuilder = ArangoClient::builder()
            .endpoint(&self.endpoint)
            .database(&self.database)
            .insecure(self.insecure)
            .request_timeout(Duration::from_secs(self.request_timeout_secs))
            .retry_policy(RetryPolicy {
                max_attempts: self.max_retries.max(1),
                max_delay: Duration::from_secs(self.max_retry_delay_secs.max(1)),
                stats: Some(Arc::clone(stats)),
                ..RetryPolicy::default()
            });

        builder = self.apply_auth(builder)?;

        if let Some(ca) = &self.tls_ca {
            builder = builder.tls(arangodb_tools_core::config::TlsConfig {
                verify_certificates: !self.insecure,
                ca_file: Some(ca.clone()),
            });
        }

        builder.build()
    }

    /// Resolves exactly one authentication method, or explains the conflict.
    ///
    /// ArangoDB accepts several credential shapes and its own tools disagree
    /// about how to spell them, so this refuses ambiguity outright rather than
    /// silently preferring one flag over another: giving two credential sources
    /// is an error naming both, not a coin flip.
    fn apply_auth(&self, builder: ArangoClientBuilder) -> Result<ArangoClientBuilder> {
        // Collect every credential source the user actually supplied.
        let mut sources: Vec<&str> = Vec::new();
        if self.auth_token_env.is_some() {
            sources.push("--auth-token-env");
        }
        if self.jwt_secret_file.is_some() {
            sources.push("--jwt-secret-file");
        }
        if self.jwt_secret_env.is_some() {
            sources.push("--jwt-secret-env");
        }
        if self.username.is_some() {
            sources.push("--username");
        }

        if sources.len() > 1 {
            return Err(Error::config(format!(
                "conflicting authentication options: {}. Choose exactly one of: \
                 --username (with --password-env, optionally --auth jwt), \
                 --jwt-secret-file / --jwt-secret-env (superuser JWT), or \
                 --auth-token-env (a token you already hold).",
                sources.join(", ")
            )));
        }

        // --auth only modifies username/password handling; flagging it against
        // a token or secret catches a real misunderstanding rather than
        // ignoring the flag.
        if self.auth == AuthMode::Jwt && self.username.is_none() {
            return Err(Error::config(
                "--auth jwt requires --username (it logs in at POST /_open/auth). \
                 To use the server's JWT secret instead, pass --jwt-secret-file or \
                 --jwt-secret-env; to use a token you already hold, pass --auth-token-env."
                    .to_string(),
            ));
        }

        if let Some(var) = &self.auth_token_env {
            return Ok(builder.bearer_auth(read_env(var)?));
        }

        if let Some(path) = &self.jwt_secret_file {
            let secret = std::fs::read_to_string(path).map_err(|err| {
                Error::config(format!(
                    "cannot read JWT secret file '{}': {err}",
                    path.display()
                ))
            })?;
            // ArangoDB treats the keyfile's contents verbatim, but a trailing
            // newline from an editor or `echo` is almost never intended and
            // produces a signature the server rejects with an opaque 401.
            return Ok(builder.jwt_secret(secret.trim_end_matches(['\n', '\r'])));
        }

        if let Some(var) = &self.jwt_secret_env {
            return Ok(builder.jwt_secret(read_env(var)?));
        }

        if let Some(username) = &self.username {
            let password = match &self.password_env {
                Some(var) => read_env(var)?,
                None => String::new(),
            };
            return Ok(match self.auth {
                AuthMode::Basic => builder.basic_auth(username, password),
                AuthMode::Jwt => builder.jwt_login(username, password),
            });
        }

        Ok(builder)
    }
}

/// Reads an environment variable by name, erroring clearly if it is unset.
fn read_env(var: &str) -> Result<String> {
    std::env::var(var)
        .map_err(|_| Error::config(format!("environment variable '{var}' is not set")))
}
