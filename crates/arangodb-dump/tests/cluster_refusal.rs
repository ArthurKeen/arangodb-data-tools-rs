//! Cluster detection for dump (PRD §8.4).
//!
//! Cluster-aware dump is post-MVP, so a dump pointed at a cluster must fail
//! with a clear error rather than run the single-server replication path and
//! emit a dump whose completeness across shards was never verified.
//!
//! These tests need a server that *reports a role*, not a working ArangoDB, so
//! they stand up a minimal in-process HTTP responder instead of requiring a
//! live cluster (which CI cannot provide). That responder answers every request
//! with the same role document, which is exactly what makes the negative test
//! below meaningful: once the preflight passes, the dump proceeds and fails on
//! the *next* call for an unrelated reason.

use arangodb_client::ArangoClient;
use arangodb_dump::{run_dump, DumpOptions};
use arangodb_storage::LocalFileSystem;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Starts an HTTP responder that answers every request with `role`, and
/// returns its endpoint URL.
async fn spawn_role_server(role: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binds an ephemeral port");
    let addr = listener.local_addr().expect("has a local address");

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                // Read the request head; the body is irrelevant here.
                let mut buffer = vec![0u8; 4096];
                let _ = socket.read(&mut buffer).await;

                let body = format!(r#"{{"error":false,"code":200,"role":"{role}"}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    format!("http://{addr}")
}

/// Builds a client pointed at `endpoint`.
fn client_for(endpoint: String) -> ArangoClient {
    ArangoClient::builder()
        .endpoint(endpoint)
        .database("_system")
        .build()
        .expect("client builds")
}

/// Runs a dump against a server reporting `role`, returning the error message
/// and whether the destination stayed empty.
async fn dump_against_role(role: &'static str) -> (String, bool) {
    let client = client_for(spawn_role_server(role).await);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalFileSystem::new(dir.path());

    let error = run_dump(&client, &store, &DumpOptions::default())
        .await
        .expect_err("a dump against this fake server cannot succeed");

    let empty = std::fs::read_dir(dir.path())
        .expect("destination is readable")
        .next()
        .is_none();
    (error.to_string(), empty)
}

#[tokio::test]
async fn refuses_a_coordinator_and_writes_nothing() {
    let (message, destination_empty) = dump_against_role("COORDINATOR").await;

    assert!(
        message.contains("cluster"),
        "error must name the cluster as the reason: {message}"
    );
    assert!(
        message.contains("COORDINATOR"),
        "error must report the detected role: {message}"
    );
    assert!(
        destination_empty,
        "refusal must happen before any artifact is written"
    );
}

#[tokio::test]
async fn refuses_a_dbserver_reported_as_primary() {
    // Older servers report DB-Servers as PRIMARY; treating that as unrecognized
    // would let a DB-Server through the single-server path.
    let (message, destination_empty) = dump_against_role("PRIMARY").await;

    assert!(
        message.contains("cluster"),
        "PRIMARY must be recognized as a cluster role: {message}"
    );
    assert!(destination_empty);
}

#[tokio::test]
async fn refuses_an_agent() {
    let (message, _) = dump_against_role("AGENT").await;
    assert!(message.contains("cluster"), "{message}");
}

#[tokio::test]
async fn a_single_server_passes_the_preflight() {
    // The responder replies with the role document to every request, so the
    // dump gets past the preflight and then fails creating the replication
    // batch (that response has no `id`). Any error *other* than the cluster
    // refusal proves the preflight let a single server through.
    let (message, _) = dump_against_role("SINGLE").await;

    assert!(
        !message.contains("refusing to dump from a cluster"),
        "a single server must not be refused: {message}"
    );
    assert!(
        message.contains("replication batch response missing id"),
        "expected the dump to proceed to the replication batch call: {message}"
    );
}

#[tokio::test]
async fn an_unreadable_role_warns_but_does_not_refuse() {
    // An inconclusive probe is not proof of a cluster: refusing here would
    // break single-server users on any server that cannot answer the probe.
    // The responder is never started, so the connection is refused outright.
    let client = client_for("http://127.0.0.1:1".to_owned());
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LocalFileSystem::new(dir.path());

    let message = run_dump(&client, &store, &DumpOptions::default())
        .await
        .expect_err("an unreachable server cannot produce a dump")
        .to_string();

    assert!(
        !message.contains("refusing to dump from a cluster"),
        "a failed probe must not be reported as a cluster: {message}"
    );
}
