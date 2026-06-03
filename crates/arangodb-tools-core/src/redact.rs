//! Credential redaction utilities.

use std::fmt;

/// A wrapper that holds a secret string and refuses to reveal it via `Debug`
/// or `Display`. Use [`Secret::expose`] when the raw value is genuinely
/// needed (for example, when setting an HTTP header).
#[derive(Clone, Default)]
pub struct Secret(String);

impl Secret {
    /// Wraps a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying secret. Handle the result carefully and never
    /// log it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Returns `true` if the secret is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// The placeholder used when redacting a sensitive value in logs.
pub const REDACTED: &str = "***redacted***";

/// Returns a fixed redaction placeholder regardless of input, for use when
/// formatting potentially sensitive values (queries, bind variables, tokens).
#[must_use]
pub fn redacted() -> &'static str {
    REDACTED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_leak() {
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret:?}"), "Secret(***)");
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn empty_detection() {
        assert!(Secret::default().is_empty());
        assert!(!Secret::new("x").is_empty());
    }
}
