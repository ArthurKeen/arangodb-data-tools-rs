//! The [`ArangoClient`] and its builder.

use std::time::Duration;

use arangodb_tools_core::config::{AuthConfig, ConnectionConfig, TlsConfig};
use arangodb_tools_core::{retry, Error, ErrorContext, Result, RetryPolicy, Secret};
use bytes::Bytes;
use reqwest::{Method, RequestBuilder};

use crate::collection::{CollectionCount, CollectionInfo, CollectionKind};
use crate::cursor::{CursorBatch, CursorRequest};
use crate::import::{ImportOptions, ImportResult};
use crate::replication::{DumpChunk, Inventory};
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

    /// Imports a newline-delimited JSON (JSONL) batch via `POST /_api/import`.
    ///
    /// `body` must contain one JSON document per line (`type=documents`). The
    /// request goes through the client retry policy; a retried send may
    /// re-import documents from a partially applied attempt, so delivery is
    /// at-least-once (PRD §8.2).
    ///
    /// # Errors
    /// Returns [`Error::Config`] if `options.collection` is empty, or an error
    /// if the request fails after retries or the response cannot be parsed.
    pub async fn import_documents(
        &self,
        options: &ImportOptions,
        body: Bytes,
    ) -> Result<ImportResult> {
        if options.collection.is_empty() {
            return Err(Error::config("import requires a collection name"));
        }

        let mut url = self.url_for("/_api/import")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("collection", &options.collection);
            query.append_pair("type", "documents");
            query.append_pair("onDuplicate", options.on_duplicate.as_query_value());
            if options.wait_for_sync {
                query.append_pair("waitForSync", "true");
            }
            if options.complete {
                query.append_pair("complete", "true");
            }
            if options.details {
                query.append_pair("details", "true");
            }
            if options.overwrite {
                query.append_pair("overwrite", "true");
            }
            if let Some(prefix) = &options.from_prefix {
                query.append_pair("fromPrefix", prefix);
            }
            if let Some(prefix) = &options.to_prefix {
                query.append_pair("toPrefix", prefix);
            }
        }

        let payload = retry(&self.retry, || {
            self.send_body(
                Method::POST,
                url.clone(),
                "application/x-ndjson",
                body.clone(),
            )
        })
        .await?;
        Ok(serde_json::from_slice::<ImportResult>(&payload)?)
    }

    /// Fetches collection metadata, or `None` if the collection does not exist.
    ///
    /// # Errors
    /// Returns an error for failures other than a 404 (which maps to `None`).
    pub async fn collection_info(&self, name: &str) -> Result<Option<CollectionInfo>> {
        let path = format!("/_api/collection/{name}");
        match self.execute(Method::GET, &path, None).await {
            Ok(body) => Ok(Some(serde_json::from_slice::<CollectionInfo>(&body)?)),
            Err(Error::Http { status: 404, .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Opens an AQL cursor, returning the first batch.
    ///
    /// # Errors
    /// Returns an error if the query is rejected or the request fails.
    pub async fn cursor_open(&self, request: &CursorRequest) -> Result<CursorBatch> {
        let payload = serde_json::to_vec(request)?;
        let body = self
            .execute(Method::POST, "/_api/cursor", Some(&payload))
            .await?;
        Ok(serde_json::from_slice::<CursorBatch>(&body)?)
    }

    /// Fetches the next batch from an open cursor.
    ///
    /// # Errors
    /// Returns an error if the cursor is gone or the request fails.
    pub async fn cursor_next(&self, id: &str) -> Result<CursorBatch> {
        let path = format!("/_api/cursor/{id}");
        let body = self.execute(Method::PUT, &path, None).await?;
        Ok(serde_json::from_slice::<CursorBatch>(&body)?)
    }

    /// Disposes of an open cursor. Deleting an already-gone cursor is ignored.
    ///
    /// # Errors
    /// Returns an error for failures other than a 404.
    pub async fn cursor_delete(&self, id: &str) -> Result<()> {
        let path = format!("/_api/cursor/{id}");
        match self.execute(Method::DELETE, &path, None).await {
            Ok(_) => Ok(()),
            Err(Error::Http { status: 404, .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Creates a replication batch (a pinned snapshot) with the given TTL in
    /// seconds, returning its id. Keep it alive with
    /// [`ArangoClient::replication_batch_extend`] and release it with
    /// [`ArangoClient::replication_batch_delete`].
    ///
    /// # Errors
    /// Returns an error if the request fails or the response lacks an id.
    pub async fn replication_batch_create(&self, ttl_secs: u32) -> Result<String> {
        let payload = serde_json::to_vec(&serde_json::json!({ "ttl": ttl_secs }))?;
        let body = self
            .execute(Method::POST, "/_api/replication/batch", Some(&payload))
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::config("replication batch response missing id"))
    }

    /// Extends a replication batch's TTL (in seconds), keeping the snapshot
    /// alive for a long-running transfer.
    ///
    /// # Errors
    /// Returns an error if the batch is gone or the request fails.
    pub async fn replication_batch_extend(&self, id: &str, ttl_secs: u32) -> Result<()> {
        let payload = serde_json::to_vec(&serde_json::json!({ "ttl": ttl_secs }))?;
        let path = format!("/_api/replication/batch/{id}");
        self.execute(Method::PUT, &path, Some(&payload)).await?;
        Ok(())
    }

    /// Deletes a replication batch, releasing the snapshot. A missing batch is
    /// not an error.
    ///
    /// # Errors
    /// Returns an error for failures other than a 404.
    pub async fn replication_batch_delete(&self, id: &str) -> Result<()> {
        let path = format!("/_api/replication/batch/{id}");
        match self.execute(Method::DELETE, &path, None).await {
            Ok(_) => Ok(()),
            Err(Error::Http { status: 404, .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Retrieves the replication inventory for the connected database, pinned
    /// to `batch_id`.
    ///
    /// # Errors
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn replication_inventory(
        &self,
        batch_id: &str,
        include_system: bool,
    ) -> Result<Inventory> {
        let path = format!(
            "/_api/replication/inventory?batchId={batch_id}&includeSystem={include_system}"
        );
        let body = self.execute(Method::GET, &path, None).await?;
        Ok(serde_json::from_slice::<Inventory>(&body)?)
    }

    /// Fetches one chunk of a collection's replication dump, starting after
    /// `from_tick` and returning up to roughly `chunk_size` bytes.
    ///
    /// Callers page by passing the previous chunk's `last_included_tick` as the
    /// next `from_tick` until `has_more` is false.
    ///
    /// # Errors
    /// Returns an error if the request fails after retries.
    pub async fn replication_dump_chunk(
        &self,
        collection: &str,
        batch_id: &str,
        from_tick: u64,
        chunk_size: u64,
    ) -> Result<DumpChunk> {
        let mut url = self.url_for("/_api/replication/dump")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("collection", collection);
            query.append_pair("batchId", batch_id);
            query.append_pair("from", &from_tick.to_string());
            query.append_pair("chunkSize", &chunk_size.to_string());
        }

        retry(&self.retry, || {
            let url = url.clone();
            async move {
                let request = self.apply_auth(self.http.request(Method::GET, url));
                let response = request.send().await.map_err(map_reqwest_error)?;
                let status = response.status();
                let last_included_tick = header_u64(&response, "x-arango-replication-lastincluded");
                let has_more = header_bool(&response, "x-arango-replication-checkmore");
                let body = response.bytes().await.map_err(map_reqwest_error)?;
                if status.is_success() {
                    Ok(DumpChunk {
                        body,
                        last_included_tick,
                        has_more,
                    })
                } else {
                    let message = match arango_error_message(body.as_ref()) {
                        Some(message) => message,
                        None => status.to_string(),
                    };
                    Err(Error::http(status.as_u16(), message, ErrorContext::new()))
                }
            }
        })
        .await
    }

    /// Recreates a collection from its replication `parameters` and `indexes`
    /// (as returned by [`ArangoClient::replication_inventory`]). With
    /// `overwrite`, an existing collection of the same name is replaced.
    ///
    /// # Errors
    /// Returns an error if the request fails.
    pub async fn restore_collection(
        &self,
        parameters: &serde_json::Value,
        indexes: &[serde_json::Value],
        overwrite: bool,
    ) -> Result<()> {
        let body = serde_json::to_vec(&serde_json::json!({
            "parameters": parameters,
            "indexes": indexes,
        }))?;
        let path = format!("/_api/replication/restore-collection?overwrite={overwrite}");
        self.execute(Method::PUT, &path, Some(&body)).await?;
        Ok(())
    }

    /// Restores documents into `collection` from a replication dump body
    /// (`{"type":2300,"data":…}` JSONL, as produced by
    /// [`ArangoClient::replication_dump_chunk`]).
    ///
    /// # Errors
    /// Returns an error if the request fails after retries.
    pub async fn restore_data(&self, collection: &str, body: Bytes) -> Result<()> {
        let mut url = self.url_for("/_api/replication/restore-data")?;
        url.query_pairs_mut().append_pair("collection", collection);
        retry(&self.retry, || {
            self.send_body(Method::PUT, url.clone(), "application/json", body.clone())
        })
        .await?;
        Ok(())
    }

    /// Creates a database (idempotent: an existing database is not an error).
    /// Always targets `_system`, regardless of the client's configured
    /// database.
    ///
    /// # Errors
    /// Returns an error for failures other than a 409 (already exists).
    pub async fn create_database(&self, name: &str) -> Result<()> {
        let body = serde_json::to_vec(&serde_json::json!({ "name": name }))?;
        let url = self
            .base
            .join("/_db/_system/_api/database")
            .map_err(|err| Error::config(format!("invalid database URL: {err}")))?;
        match retry(&self.retry, || {
            self.send_body(
                Method::POST,
                url.clone(),
                "application/json",
                Bytes::from(body.clone()),
            )
        })
        .await
        {
            Ok(_) => Ok(()),
            Err(Error::Http { status: 409, .. }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Returns the number of documents in a collection.
    ///
    /// # Errors
    /// Returns an error if the collection does not exist or the request fails.
    pub async fn collection_count(&self, name: &str) -> Result<u64> {
        let path = format!("/_api/collection/{name}/count");
        let body = self.execute(Method::GET, &path, None).await?;
        let parsed: CollectionCount = serde_json::from_slice(&body)?;
        Ok(parsed.count)
    }

    /// Drops a collection.
    ///
    /// # Errors
    /// Returns an error if the collection does not exist or the request fails.
    pub async fn drop_collection(&self, name: &str) -> Result<()> {
        let path = format!("/_api/collection/{name}");
        self.execute(Method::DELETE, &path, None).await?;
        Ok(())
    }

    /// Creates a collection of the given `kind`.
    ///
    /// # Errors
    /// Returns an error if the request fails (including a 409 if the collection
    /// already exists).
    pub async fn create_collection(&self, name: &str, kind: CollectionKind) -> Result<()> {
        let body = serde_json::json!({ "name": name, "type": kind.type_id() });
        let payload = serde_json::to_vec(&body)?;
        self.execute(Method::POST, "/_api/collection", Some(&payload))
            .await?;
        Ok(())
    }

    /// Ensures a collection named `name` of `kind` exists, creating it if
    /// absent. If it already exists with a different kind, returns an error
    /// rather than silently importing into the wrong collection type.
    ///
    /// # Errors
    /// Returns [`Error::Config`] on a kind mismatch, or a transport/HTTP error.
    pub async fn ensure_collection(&self, name: &str, kind: CollectionKind) -> Result<()> {
        if let Some(info) = self.collection_info(name).await? {
            return match info.kind() {
                Some(existing) if existing == kind => Ok(()),
                Some(existing) => Err(Error::config(format!(
                    "collection '{name}' already exists as {existing:?}, but {kind:?} was requested"
                ))),
                None => Err(Error::config(format!(
                    "collection '{name}' already exists with an unsupported type"
                ))),
            };
        }
        self.create_collection(name, kind).await
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

    /// Performs a single request attempt with a body against a prebuilt URL.
    async fn send_body(
        &self,
        method: Method,
        url: reqwest::Url,
        content_type: &'static str,
        body: Bytes,
    ) -> Result<Vec<u8>> {
        let mut request = self.apply_auth(self.http.request(method, url));
        request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        request = request.body(body);

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

/// Reads a `u64` response header, defaulting to `0` when absent or unparsable.
fn header_u64(response: &reqwest::Response, name: &str) -> u64 {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// Reads a boolean response header (`"true"`), defaulting to `false`.
fn header_bool(response: &reqwest::Response, name: &str) -> bool {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
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
