//! Types for the `/_api/version` endpoint.

use serde::Deserialize;

/// The response from ArangoDB's `/_api/version` endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct VersionInfo {
    /// The server identifier, normally `"arango"`.
    pub server: String,
    /// The server version string, e.g. `"3.12.0"`.
    pub version: String,
    /// The license type (`"community"` or `"enterprise"`), if reported.
    #[serde(default)]
    pub license: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_version_response() {
        let json = r#"{"server":"arango","version":"3.12.0","license":"community"}"#;
        let info: VersionInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.server, "arango");
        assert_eq!(info.version, "3.12.0");
        assert_eq!(info.license.as_deref(), Some("community"));
    }

    #[test]
    fn license_is_optional() {
        let json = r#"{"server":"arango","version":"3.11.5"}"#;
        let info: VersionInfo = serde_json::from_str(json).unwrap();
        assert!(info.license.is_none());
    }
}
