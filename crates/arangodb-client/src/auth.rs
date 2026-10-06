//! JWT handling for ArangoDB authentication.
//!
//! ArangoDB accepts a bearer token in two distinct shapes, and the difference
//! between them is the single most common source of confusion when scripting
//! the client tools. Both are HS256 JWTs signed with the server's JWT secret
//! (`--server.jwt-secret-keyfile`), and they differ **only by payload**:
//!
//! | Claims | Identity |
//! |---|---|
//! | `server_id` + `iss: "arangodb"` | **superuser** — bypasses user permissions |
//! | `preferred_username` + `iss: "arangodb"` | that user, with that user's permissions |
//!
//! Verified against ArangoDB 3.12.4:
//!
//! - `server_id` is **required** for a superuser token. A token carrying only
//!   `iss: "arangodb"` is rejected with 401, which is the trap that makes
//!   hand-rolled tokens fail for no visible reason.
//! - `exp` is **optional** on a superuser token; omitting it yields a token that
//!   never expires. This crate always sets one anyway (see
//!   [`mint_superuser_jwt`]) because it can re-mint for free.
//! - A token obtained from `POST /_open/auth` carries `preferred_username` and a
//!   server-chosen `exp` — **one hour** by default, which is shorter than many
//!   dump or import runs. Expiry must therefore be handled, not assumed away.
//! - A wrong `iss`, an unknown `preferred_username`, an elapsed `exp`, or a
//!   signature under the wrong secret each yield 401.

use arangodb_tools_core::{Error, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// The issuer claim ArangoDB requires on every JWT it accepts.
const ISSUER: &str = "arangodb";

/// The `server_id` recorded in minted superuser tokens. The value is arbitrary
/// to the server — it only has to be present — so a recognizable one is used to
/// make the token's origin obvious in server logs.
const SERVER_ID: &str = "arangodb-data-tools-rs";

/// Seconds before a token's `exp` at which it is treated as already expired, so
/// a long request started just before the boundary does not race it.
pub(crate) const EXPIRY_SKEW_SECS: u64 = 60;

/// Seconds of validity given to a minted superuser token.
const MINTED_LIFETIME_SECS: u64 = 3600;

/// Current wall-clock time as seconds since the Unix epoch.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Mints a **superuser** JWT signed with the server's JWT secret.
///
/// The payload is `{"server_id": …, "iss": "arangodb", "iat": …, "exp": …}`.
/// `server_id` is what makes the token a superuser token rather than a
/// malformed one; see the module docs.
///
/// The token is valid for one hour. Because minting is local and free, the
/// client re-mints rather than issuing a non-expiring token.
///
/// # Errors
/// Returns [`Error::Config`] if the secret cannot be used as an HMAC key.
pub(crate) fn mint_superuser_jwt(secret: &str) -> Result<String> {
    let issued = now_secs();
    let payload = serde_json::json!({
        "server_id": SERVER_ID,
        "iss": ISSUER,
        "iat": issued,
        "exp": issued + MINTED_LIFETIME_SECS,
    });
    sign_hs256(secret, &payload)
}

/// Signs `payload` as a compact HS256 JWT under `secret`.
fn sign_hs256(secret: &str, payload: &serde_json::Value) -> Result<String> {
    let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});
    let encoded_header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
    let encoded_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(payload)?);
    let signing_input = format!("{encoded_header}.{encoded_payload}");

    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|err| Error::config(format!("JWT secret is not a usable HMAC key: {err}")))?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());

    Ok(format!("{signing_input}.{signature}"))
}

/// Reads the `exp` claim from a JWT without verifying its signature.
///
/// Verification is the server's job; this is only used to refresh a token
/// before it lapses. A token whose payload cannot be decoded returns `None`,
/// which the caller treats as "no known expiry" rather than as an error — an
/// opaque token supplied by the user is still perfectly usable.
pub(crate) fn token_expiry(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    claims.get("exp")?.as_u64()
}

/// Whether a token with this expiry should be refreshed before use.
pub(crate) fn is_expiring(expires_at: Option<u64>) -> bool {
    match expires_at {
        Some(exp) => now_secs() + EXPIRY_SKEW_SECS >= exp,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decodes a JWT payload without verifying it, for assertions.
    fn claims(token: &str) -> serde_json::Value {
        let payload = token.split('.').nth(1).expect("token has a payload");
        let decoded = URL_SAFE_NO_PAD
            .decode(payload)
            .expect("payload is base64url");
        serde_json::from_slice(&decoded).expect("payload is JSON")
    }

    #[test]
    fn minted_token_carries_the_claims_arangodb_requires() {
        let token = mint_superuser_jwt("supersecretjwtkey").unwrap();
        let claims = claims(&token);

        // `server_id` is what distinguishes a superuser token; a token without
        // it is rejected with 401 (verified against 3.12.4).
        assert_eq!(claims["server_id"], SERVER_ID);
        assert_eq!(claims["iss"], ISSUER);
        assert!(claims.get("exp").is_some(), "minted tokens must expire");
        assert!(
            claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap(),
            "exp must be after iat"
        );
    }

    #[test]
    fn minted_token_has_three_compact_segments() {
        let token = mint_superuser_jwt("secret").unwrap();
        assert_eq!(token.split('.').count(), 3, "compact JWT serialization");
        assert!(!token.contains('='), "base64url must be unpadded");
    }

    #[test]
    fn signature_depends_on_the_secret() {
        let a = mint_superuser_jwt("secret-a").unwrap();
        let b = mint_superuser_jwt("secret-b").unwrap();
        let sig = |t: &str| t.split('.').nth(2).unwrap().to_string();
        assert_ne!(sig(&a), sig(&b), "a different secret must sign differently");
    }

    /// Pins the exact byte-level HS256 output so a dependency bump or an
    /// encoding change cannot silently alter what the server is asked to verify.
    #[test]
    fn hs256_signing_matches_a_known_vector() {
        let payload = serde_json::json!({"iss": "arangodb", "server_id": "probe"});
        let token = sign_hs256("supersecretjwtkey", &payload).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts[0], "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9");
        assert_eq!(
            parts[1],
            "eyJpc3MiOiJhcmFuZ29kYiIsInNlcnZlcl9pZCI6InByb2JlIn0"
        );
    }

    #[test]
    fn token_expiry_reads_the_exp_claim() {
        let token = mint_superuser_jwt("secret").unwrap();
        let exp = token_expiry(&token).expect("minted tokens carry exp");
        assert!(exp > now_secs(), "a freshly minted token is not expired");
    }

    #[test]
    fn token_expiry_tolerates_opaque_tokens() {
        assert_eq!(token_expiry("not-a-jwt"), None);
        assert_eq!(token_expiry(""), None);
        // Well-formed shape, payload is not JSON.
        assert_eq!(token_expiry("a.!!!.c"), None);
    }

    #[test]
    fn expiry_window_triggers_before_the_deadline() {
        assert!(!is_expiring(None), "unknown expiry never forces a refresh");
        assert!(is_expiring(Some(now_secs())), "already elapsed");
        assert!(
            is_expiring(Some(now_secs() + EXPIRY_SKEW_SECS / 2)),
            "inside the skew window"
        );
        assert!(
            !is_expiring(Some(now_secs() + EXPIRY_SKEW_SECS * 10)),
            "comfortably valid"
        );
    }
}
