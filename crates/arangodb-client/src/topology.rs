//! Deployment-topology detection via `/_admin/server/role`.
//!
//! MVP dump and restore are designed and tested against single servers
//! (PRD §3, §8.4). Cluster-aware dump — `/_api/replication/clusterInventory`
//! and shard-level parallelism across DB-Servers — is post-MVP, so the tools
//! must *detect* a cluster and fail with a clear error rather than run the
//! single-server replication path against a coordinator and emit a dump whose
//! completeness across shards was never verified.
//!
//! [`ServerRole::is_cluster`] is that check. It is deliberately conservative:
//! only roles the server itself reports as cluster members count, so a single
//! server (including an active-failover leader/follower, which reports
//! `SINGLE`) is never refused.

use serde::Deserialize;

/// The raw `/_admin/server/role` response body.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RoleResponse {
    /// The reported role name, e.g. `"SINGLE"` or `"COORDINATOR"`.
    pub(crate) role: String,
}

/// The deployment role a server reports for itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    /// A single server, or an active-failover leader/follower. The deployment
    /// dump and restore are tested against.
    Single,
    /// A cluster coordinator.
    Coordinator,
    /// A cluster DB-Server. Older servers report this as `PRIMARY`.
    DbServer,
    /// An agency member.
    Agent,
    /// A role this build does not recognize, carried verbatim so it can be
    /// reported to the user instead of being silently treated as safe.
    Unknown(String),
}

impl ServerRole {
    /// Parses a role name as reported by `/_admin/server/role`.
    ///
    /// Matching is case-insensitive. `PRIMARY` is accepted as a synonym for
    /// [`ServerRole::DbServer`] (the name older servers use).
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "SINGLE" => Self::Single,
            "COORDINATOR" => Self::Coordinator,
            "DBSERVER" | "PRIMARY" => Self::DbServer,
            "AGENT" => Self::Agent,
            _ => Self::Unknown(raw.trim().to_owned()),
        }
    }

    /// Whether this role belongs to a cluster deployment.
    ///
    /// `UNDEFINED` and any unrecognized role return `false`: an inconclusive
    /// probe is not proof of a cluster, and callers surface it as a warning
    /// rather than refusing the operation outright.
    #[must_use]
    pub fn is_cluster(&self) -> bool {
        matches!(self, Self::Coordinator | Self::DbServer | Self::Agent)
    }

    /// The role name for use in messages.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Single => "SINGLE",
            Self::Coordinator => "COORDINATOR",
            Self::DbServer => "DBSERVER",
            Self::Agent => "AGENT",
            Self::Unknown(raw) => raw,
        }
    }
}

impl std::fmt::Display for ServerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_roles_case_insensitively() {
        assert_eq!(ServerRole::parse("SINGLE"), ServerRole::Single);
        assert_eq!(ServerRole::parse("single"), ServerRole::Single);
        assert_eq!(ServerRole::parse("COORDINATOR"), ServerRole::Coordinator);
        assert_eq!(ServerRole::parse("DBSERVER"), ServerRole::DbServer);
        assert_eq!(ServerRole::parse("AGENT"), ServerRole::Agent);
    }

    #[test]
    fn primary_is_a_dbserver_synonym() {
        // Older servers report DB-Servers as PRIMARY; treating it as unknown
        // would let a DB-Server dump through the single-server path.
        assert_eq!(ServerRole::parse("PRIMARY"), ServerRole::DbServer);
        assert!(ServerRole::parse("PRIMARY").is_cluster());
    }

    #[test]
    fn cluster_roles_are_flagged_and_single_is_not() {
        assert!(ServerRole::Coordinator.is_cluster());
        assert!(ServerRole::DbServer.is_cluster());
        assert!(ServerRole::Agent.is_cluster());
        assert!(!ServerRole::Single.is_cluster());
    }

    #[test]
    fn undefined_and_unknown_roles_are_not_treated_as_cluster() {
        // An inconclusive probe must not refuse a dump; callers warn instead.
        let undefined = ServerRole::parse("UNDEFINED");
        assert_eq!(undefined, ServerRole::Unknown("UNDEFINED".to_owned()));
        assert!(!undefined.is_cluster());
        assert!(!ServerRole::parse("something-new").is_cluster());
    }

    #[test]
    fn unknown_role_keeps_the_raw_name_for_reporting() {
        assert_eq!(ServerRole::parse("  WeIrD  ").as_str(), "WeIrD");
    }

    #[test]
    fn displays_as_the_role_name() {
        assert_eq!(ServerRole::Coordinator.to_string(), "COORDINATOR");
        assert_eq!(ServerRole::parse("UNDEFINED").to_string(), "UNDEFINED");
    }

    #[test]
    fn deserializes_role_response() {
        let parsed: RoleResponse =
            serde_json::from_str(r#"{"error":false,"code":200,"role":"COORDINATOR"}"#).unwrap();
        assert_eq!(ServerRole::parse(&parsed.role), ServerRole::Coordinator);
    }
}
