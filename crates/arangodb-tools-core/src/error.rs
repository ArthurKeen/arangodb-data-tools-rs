//! Error taxonomy shared across the workspace.
//!
//! Errors carry optional [`ErrorContext`] so failures can report the
//! collection, object path, byte range, batch number, and server response
//! that produced them.

/// Convenience alias defaulting the error type to [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Optional context attached to an error to aid diagnosis.
///
/// All fields are optional; populate whichever are known at the failure site.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ErrorContext {
    /// The collection involved, if any.
    pub collection: Option<String>,
    /// The storage object path involved, if any.
    pub object_path: Option<String>,
    /// The byte range (inclusive start, exclusive end) involved, if any.
    pub byte_range: Option<(u64, u64)>,
    /// The batch number involved, if any.
    pub batch: Option<u64>,
    /// The raw server response body, if any.
    pub server_response: Option<String>,
}

impl ErrorContext {
    /// Creates an empty context.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the collection name.
    #[must_use]
    pub fn collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    /// Sets the object path.
    #[must_use]
    pub fn object_path(mut self, path: impl Into<String>) -> Self {
        self.object_path = Some(path.into());
        self
    }

    /// Sets the byte range.
    #[must_use]
    pub fn byte_range(mut self, start: u64, end: u64) -> Self {
        self.byte_range = Some((start, end));
        self
    }

    /// Sets the batch number.
    #[must_use]
    pub fn batch(mut self, batch: u64) -> Self {
        self.batch = Some(batch);
        self
    }

    /// Sets the server response body.
    #[must_use]
    pub fn server_response(mut self, response: impl Into<String>) -> Self {
        self.server_response = Some(response.into());
        self
    }
}

/// The unified error type for the ArangoDB data tools.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid or inconsistent configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// A transport-level connection failure.
    #[error("connection error: {0}")]
    Connection(String),

    /// A non-success HTTP response from ArangoDB.
    #[error("HTTP {status}: {message}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Human-readable message.
        message: String,
        /// Additional context (boxed to keep the error type small).
        context: Box<ErrorContext>,
    },

    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A storage-backend error.
    #[error("storage error: {0}")]
    Storage(String),

    /// A parse error in input data.
    #[error("parse error: {message}")]
    Parse {
        /// Human-readable message.
        message: String,
        /// 1-based line number, if known.
        line: Option<u64>,
        /// 1-based column number, if known.
        column: Option<u64>,
    },

    /// A (de)serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A checkpoint/resume error.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,
}

impl Error {
    /// Builds a [`Error::Config`].
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config(message.into())
    }

    /// Builds a [`Error::Connection`].
    pub fn connection(message: impl Into<String>) -> Self {
        Error::Connection(message.into())
    }

    /// Builds a [`Error::Storage`].
    pub fn storage(message: impl Into<String>) -> Self {
        Error::Storage(message.into())
    }

    /// Builds a [`Error::Checkpoint`].
    pub fn checkpoint(message: impl Into<String>) -> Self {
        Error::Checkpoint(message.into())
    }

    /// Builds a [`Error::Http`] with the given context.
    pub fn http(status: u16, message: impl Into<String>, context: ErrorContext) -> Self {
        Error::Http {
            status,
            message: message.into(),
            context: Box::new(context),
        }
    }

    /// Builds a [`Error::Parse`] without position information.
    pub fn parse(message: impl Into<String>) -> Self {
        Error::Parse {
            message: message.into(),
            line: None,
            column: None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_builder_sets_fields() {
        let ctx = ErrorContext::new()
            .collection("users")
            .object_path("dump/users.data.jsonl")
            .byte_range(0, 1024)
            .batch(3)
            .server_response("{\"error\":true}");
        assert_eq!(ctx.collection.as_deref(), Some("users"));
        assert_eq!(ctx.byte_range, Some((0, 1024)));
        assert_eq!(ctx.batch, Some(3));
    }

    #[test]
    fn error_type_stays_small() {
        // Guards against accidentally bloating the error type. clippy's
        // `result_large_err` lint fires above 128 bytes; stay well under it.
        assert!(std::mem::size_of::<Error>() <= 128);
    }

    #[test]
    fn http_error_displays_status() {
        let err = Error::http(503, "service unavailable", ErrorContext::new());
        assert_eq!(err.to_string(), "HTTP 503: service unavailable");
    }
}
