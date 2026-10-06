//! Live checks for the JWT authentication modes.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise every test is a no-op.
//! The superuser-JWT test additionally needs `ARANGO_JWT_SECRET` to match the
//! server's `--server.jwt-secret-keyfile`, and is skipped without it.
//!
//! Spin up a suitable server with:
//!
//! ```bash
//! printf 'supersecretjwtkey' > /tmp/jwtsecret
//! docker run -d -p 8529:8529 -e ARANGO_ROOT_PASSWORD=probe \
//!   -v /tmp:/jwt:ro arangodb/arangodb:3.12 \
//!   arangod --server.jwt-secret-keyfile=/jwt/jwtsecret
//! export ARANGO_ENDPOINT=http://localhost:8529 \
//!        ARANGO_ROOT_PASSWORD=probe ARANGO_JWT_SECRET=supersecretjwtkey
//! ```

use arangodb_client::ArangoClient;

fn endpoint() -> Option<String> {
    std::env::var("ARANGO_ENDPOINT").ok()
}

fn jwt_secret() -> Option<String> {
    std::env::var("ARANGO_JWT_SECRET").ok()
}

fn root_password() -> String {
    std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default()
}

/// A superuser JWT minted from the server's secret is accepted, and carries
/// superuser rights — `/_admin/server/role` is not readable otherwise.
#[tokio::test]
async fn superuser_jwt_from_secret_is_accepted() {
    let (Some(endpoint), Some(secret)) = (endpoint(), jwt_secret()) else {
        eprintln!("ARANGO_ENDPOINT/ARANGO_JWT_SECRET not set; skipping");
        return;
    };
    let client = ArangoClient::builder()
        .endpoint(endpoint)
        .database("_system")
        .jwt_secret(secret)
        .build()
        .expect("client builds");

    client
        .version()
        .await
        .expect("a minted superuser JWT authenticates");
    client
        .server_role()
        .await
        .expect("a superuser JWT may read /_admin/server/role");
}

/// A secret that does not match the server's is refused, and the error explains
/// what to check rather than relaying a bare "not authorized".
#[tokio::test]
async fn a_wrong_jwt_secret_is_refused_with_an_actionable_error() {
    let Some(endpoint) = endpoint() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let client = ArangoClient::builder()
        .endpoint(endpoint)
        .database("_system")
        .jwt_secret("definitely-not-the-servers-secret")
        .build()
        .expect("client builds");

    let err = client
        .version()
        .await
        .expect_err("a wrong secret must not authenticate");
    let text = err.to_string();
    assert!(
        text.contains("jwt-secret-keyfile"),
        "the 401 must point at the secret to check, got: {text}"
    );
}

/// Logging in at `POST /_open/auth` yields a usable user token.
#[tokio::test]
async fn jwt_login_exchanges_credentials_for_a_token() {
    let Some(endpoint) = endpoint() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let client = ArangoClient::builder()
        .endpoint(endpoint)
        .database("_system")
        .jwt_login("root", root_password())
        .build()
        .expect("client builds");

    client
        .version()
        .await
        .expect("login at /_open/auth authenticates");

    // A second call exercises the cached-token path rather than logging in again.
    client.version().await.expect("cached token is reused");
}

/// A bad password fails at login, and the message names the endpoint that
/// rejected it — the ambiguity that otherwise sends people to curl.
#[tokio::test]
async fn jwt_login_with_a_bad_password_names_the_login_endpoint() {
    let Some(endpoint) = endpoint() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let client = ArangoClient::builder()
        .endpoint(endpoint)
        .database("_system")
        .jwt_login("root", "certainly-the-wrong-password")
        .build()
        .expect("client builds");

    let err = client
        .version()
        .await
        .expect_err("a wrong password must not authenticate");
    let text = err.to_string();
    assert!(
        text.contains("/_open/auth"),
        "the error must name the login endpoint, got: {text}"
    );
}
