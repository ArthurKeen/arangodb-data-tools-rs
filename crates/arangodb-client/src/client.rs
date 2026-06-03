//! The [`ArangoClient`] and its builder.

use std::time::Duration;

use arangodb_tools_core::config::{AuthConfig, ConnectionConfig, TlsConfig};
use arangodb_tools_core::{retry, Error, ErrorContext, Result, RetryPolicy, Secret};
use reqwest::{Method, RequestBuilder};

use crate::version::VersionInfo;

/// An HTTP client for a single ArangoDB endpoint and database.
///
/// All requests are routed through a shared [`RetryPolicy`]. Construct one via
/// [`ArangoClient::builder`].
#[derive(Debug, Clone)]
pub struct ArangoClient {
    http: reqwest::Client,
    config: ConnectionConfig,
    retry: RetryPolicy,
    base: reqwest::Url,
}

impl ArangoClient {
    /// Starts building a client.
    #[must_use]
    pub fn builder() -> ArangoClientBuilder {
        ArangoClientBuilder::new()
    }

    /// The target database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.config.database
    }

    /// Fetches server version information via `/_api/version`.
    ///
    /// # Errors
    /// Returns an error if the request fails after retries or the response
    /// cannot be parsed.
    pub async fn version(&self) -> Result<VersionInfo> {
        let body = self.execute(Method::GET, "/_api/version", None).await?;
        Ok(serde_json::from_slice::<VersionInfo>(&body)?)
    }

    /// Builds the absolute, database-scoped URL for an API path.
    fn url_for(&self, path: &str) -> Result<reqwest::Url> {
        let scoped = format!("/_db/{}{}", self.config.database, path);
        self.base
            .join(&scoped)
            .map_err(|err| Error::config(format!("invalid request URL '{scoped}': {err}")))
    }

    /// Applies the configured authentication to a request.
    fn apply_auth(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.config.auth {
            AuthConfig::None => request,
            AuthConfig::Basic { username, password } => {
                request.basic_auth(username, Some(password.expose()))
            }
            AuthConfig::Bearer { token } => request.bearer_auth(token.expose()),
        }
    }

    /// Executes a request with retries, returning the response body on success.
    async fn execute(&self, method: Method, path: &str, body: Option<&[u8]>) -> Result<Vec<u8>> {
        retry(&self.retry, || self.send_request(&method, path, body)).await
    }

    /// Performs a single HTTP request attempt.
    async fn send_request(
        &self,
        method: &Method,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let url = self.url_for(path)?;
        let mut request = self.apply_auth(self.http.request(method.clone(), url));
        if let Some(payload) = body {
            request = request.header(reqwest::header::CONTENT_TYPE, "application/json");
            request = request.body(payload.to_vec());
        }

        let response = request.send().await.map_err(map_reqwest_error)?;
        let status = response.status();
        let payload = response.bytes().await.map_err(map_reqwest_error)?;

        if status.is_success() {
            return Ok(payload.to_vec());
        }

        let message = match arango_error_message(payload.as_ref()) {
            Some(message) => message,
            None => status.to_string(),
        };
        Err(Error::http(status.as_u16(), message, ErrorContext::new()))
    }
}

/// A builder for [`ArangoClient`].
#[derive(Debug, Clone)]
pub struct ArangoClientBuilder {
    config: ConnectionConfig,
    retry: RetryPolicy,
}

impl ArangoClientBuilder {
    fn new() -> Self {
        Self {
            config: ConnectionConfig::default(),
            retry: RetryPolicy::default(),
        }
    }

    /// Sets the base endpoint URL (e.g. `http://localhost:8529`).
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoint = endpoint.into();
        self
    }

    /// Sets the target database.
    #[must_use]
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = database.into();
        self
    }

    /// Uses HTTP basic authentication.
    #[must_use]
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = AuthConfig::Basic {
            username: username.into(),
            password: Secret::new(password.into()),
        };
        self
    }

    /// Uses JWT/bearer-token authentication.
    #[must_use]
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.config.auth = AuthConfig::Bearer {
            token: Secret::new(token.into()),
        };
        self
    }

    /// Replaces the TLS configuration.
    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.config.tls = tls;
        self
    }

    /// Disables TLS certificate verification (development only).
    #[must_use]
    pub fn insecure(mut self, insecure: bool) -> Self {
        self.config.tls.verify_certificates = !insecure;
        self
    }

    /// Sets the per-request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Sets the connection-establishment timeout.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Replaces the retry policy.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the endpoint URL is invalid, the CA file
    /// cannot be read, or the HTTP client cannot be constructed.
    pub fn build(self) -> Result<ArangoClient> {
        let endpoint = &self.config.endpoint;
        let base = reqwest::Url::parse(endpoint)
            .map_err(|err| Error::config(format!("invalid endpoint '{endpoint}': {err}")))?;

        let mut builder = reqwest::Client::builder()
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        if !self.config.tls.verify_certificates {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca_file) = &self.config.tls.ca_file {
            let pem = std::fs::read(ca_file)
                .map_err(|err| Error::config(format!("cannot read CA file: {err}")))?;
            let certificate = reqwest::Certificate::from_pem(&pem)
                .map_err(|err| Error::config(format!("invalid CA certificate: {err}")))?;
            builder = builder.add_root_certificate(certificate);
        }

        let http = builder
            .build()
            .map_err(|err| Error::config(format!("failed to build HTTP client: {err}")))?;

        Ok(ArangoClient {
            http,
            config: self.config,
            retry: self.retry,
            base,
        })
    }
}

/// Maps a `reqwest` transport error into the shared error type.
///
/// Transport-level failures are reported as [`Error::Connection`] so the retry
/// policy treats them as retryable.
fn map_reqwest_error(err: reqwest::Error) -> Error {
    if err.is_timeout() {
        Error::connection(format!("request timed out: {err}"))
    } else if err.is_connect() {
        Error::connection(format!("could not connect: {err}"))
    } else {
        Error::connection(err.to_string())
    }
}

/// Extracts ArangoDB's `errorMessage` field from a JSON error body, if present.
fn arango_error_message(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    value
        .get("errorMessage")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_client_with_valid_endpoint() {
        let client = ArangoClient::builder()
            .endpoint("http://localhost:8529")
            .database("mydb")
            .basic_auth("root", "secret")
            .build()
            .unwrap();
        assert_eq!(client.database(), "mydb");
    }

    #[test]
    fn rejects_invalid_endpoint() {
        let result = ArangoClient::builder().endpoint("not a url").build();
        assert!(result.is_err());
    }

    #[test]
    fn scopes_url_to_database() {
        let client = ArangoClient::builder()
            .endpoint("http://localhost:8529")
            .database("mydb")
            .build()
            .unwrap();
        let url = client.url_for("/_api/version").unwrap();
        assert_eq!(url.as_str(), "http://localhost:8529/_db/mydb/_api/version");
    }

    #[test]
    fn insecure_disables_verification() {
        let builder = ArangoClient::builder().insecure(true);
        assert!(!builder.config.tls.verify_certificates);
    }

    #[test]
    fn debug_output_does_not_leak_password() {
        let client = ArangoClient::builder()
            .endpoint("http://localhost:8529")
            .basic_auth("root", "hunter2")
            .build()
            .unwrap();
        assert!(!format!("{client:?}").contains("hunter2"));
    }

    /// Live check against an ArangoDB server. Runs only when `ARANGO_ENDPOINT`
    /// is set (the CI `test` job provides one); otherwise it is a no-op.
    #[tokio::test]
    async fn version_against_live_server() {
        let Ok(endpoint) = std::env::var("ARANGO_ENDPOINT") else {
            return;
        };
        let password = std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default();
        let client = ArangoClient::builder()
            .endpoint(endpoint)
            .database("_system")
            .basic_auth("root", password)
            .build()
            .unwrap();

        let info = client.version().await.unwrap();
        assert_eq!(info.server, "arango");
        assert!(!info.version.is_empty());
    }
}
