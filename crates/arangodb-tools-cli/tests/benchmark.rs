//! Throughput benchmark: official `arangoimport` vs `arangox import`.
//!
//! Satisfies the PRD §11.1 requirement that the import throughput target be
//! *measured*, not assumed. Both tools import the same JSONL fixture into the
//! same server with out-of-the-box settings; the test reports each tool's
//! docs/sec and the ratio.
//!
//! It runs only when `ARANGO_ENDPOINT` is set AND `arangoimport` is on `PATH`;
//! otherwise it no-ops (so plain `cargo test` and the standard CI job skip it).
//! It reports rather than gates on the ratio — a hard throughput assertion is
//! too sensitive to shared CI hardware — but it does assert that both tools
//! imported every document, so a broken arangox import still fails the test.
//!
//! Tunables (env): `ARANGO_BENCH_DOCS` (default 200_000).

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use arangodb_client::ArangoClient;

/// Path to the freshly built `arangox` binary under test.
const ARANGOX: &str = env!("CARGO_BIN_EXE_arangox");

/// Env var name passed to `arangox --password-env`.
const PW_VAR: &str = "ARANGOX_BENCH_PW";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn benchmark_import_throughput() {
    let Ok(endpoint) = std::env::var("ARANGO_ENDPOINT") else {
        eprintln!("ARANGO_ENDPOINT not set; skipping import benchmark");
        return;
    };
    let password = std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default();
    if !arangoimport_available() {
        eprintln!("arangoimport not found on PATH; skipping import benchmark");
        return;
    }

    let docs: u64 = std::env::var("ARANGO_BENCH_DOCS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);

    let client = ArangoClient::builder()
        .endpoint(&endpoint)
        .database("_system")
        .basic_auth("root", &password)
        .build()
        .expect("client builds");

    let fixture = std::env::temp_dir().join("arangox_bench.jsonl");
    write_fixture(&fixture, docs);

    // --- arangox (the tool under test) ---
    let coll_x = "arangox_bench_arangox";
    let _ = client.drop_collection(coll_x).await;
    let arangox_elapsed = time_arangox(&endpoint, &password, coll_x, &fixture);
    let count_x = client
        .collection_count(coll_x)
        .await
        .expect("count arangox");

    // --- official arangoimport ---
    let coll_ai = "arangox_bench_arangoimport";
    let _ = client.drop_collection(coll_ai).await;
    let arangoimport_elapsed = time_arangoimport(&endpoint, &password, coll_ai, &fixture);
    let count_ai = client
        .collection_count(coll_ai)
        .await
        .expect("count arangoimport");

    // Both tools must have imported every document.
    assert_eq!(count_x, docs, "arangox imported all documents");
    assert_eq!(count_ai, docs, "arangoimport imported all documents");

    let x_rate = docs as f64 / arangox_elapsed.as_secs_f64();
    let ai_rate = docs as f64 / arangoimport_elapsed.as_secs_f64();
    let ratio = x_rate / ai_rate;

    println!("\n=== import throughput benchmark ({docs} docs) ===");
    println!(
        "  arangox import : {:>8.2}s  ({:>10.0} docs/s)",
        arangox_elapsed.as_secs_f64(),
        x_rate
    );
    println!(
        "  arangoimport   : {:>8.2}s  ({:>10.0} docs/s)",
        arangoimport_elapsed.as_secs_f64(),
        ai_rate
    );
    println!(
        "  arangox / arangoimport throughput ratio: {ratio:.2}x  (PRD §11.1 floor: >= 0.50x => {})",
        if ratio >= 0.50 { "MEETS" } else { "BELOW" }
    );

    // Cleanup.
    let _ = client.drop_collection(coll_x).await;
    let _ = client.drop_collection(coll_ai).await;
    let _ = std::fs::remove_file(&fixture);
}

/// Whether `arangoimport --version` runs successfully.
fn arangoimport_available() -> bool {
    Command::new("arangoimport")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Times an `arangox import` of `fixture` into `collection`.
fn time_arangox(endpoint: &str, password: &str, collection: &str, fixture: &Path) -> Duration {
    let start = Instant::now();
    let output = Command::new(ARANGOX)
        .args(["import", "--endpoint", endpoint])
        .args(["--database", "_system", "--username", "root"])
        .args(["--password-env", PW_VAR])
        .args([
            "--collection",
            collection,
            "--create-collection",
            "--overwrite",
        ])
        .args(["--input", fixture.to_str().unwrap(), "--format", "jsonl"])
        .env(PW_VAR, password)
        .output()
        .expect("run arangox");
    let elapsed = start.elapsed();
    assert!(
        output.status.success(),
        "arangox import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    elapsed
}

/// Times an official `arangoimport` of `fixture` into `collection`.
fn time_arangoimport(endpoint: &str, password: &str, collection: &str, fixture: &Path) -> Duration {
    // The client tools speak `tcp://`/`ssl://`, not `http://`/`https://`.
    let tcp_endpoint = endpoint
        .replacen("https://", "ssl://", 1)
        .replacen("http://", "tcp://", 1);
    let start = Instant::now();
    let output = Command::new("arangoimport")
        .args(["--server.endpoint", &tcp_endpoint])
        .args(["--server.database", "_system", "--server.username", "root"])
        .args(["--server.password", password])
        .args(["--collection", collection, "--create-collection", "true"])
        .args(["--type", "jsonl", "--file", fixture.to_str().unwrap()])
        .args(["--overwrite", "true", "--progress", "false"])
        .output()
        .expect("run arangoimport");
    let elapsed = start.elapsed();
    assert!(
        output.status.success(),
        "arangoimport failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    elapsed
}

/// Writes `docs` JSONL records with mixed-type fields to `path`.
fn write_fixture(path: &Path, docs: u64) {
    use std::io::Write as _;

    let file = std::fs::File::create(path).expect("create fixture");
    let mut writer = std::io::BufWriter::new(file);
    for i in 0..docs {
        writeln!(
            writer,
            "{{\"_key\":\"k{i}\",\"name\":\"user {i}\",\"age\":{},\"active\":{}}}",
            i % 100,
            i % 2 == 0
        )
        .expect("write fixture line");
    }
    writer.flush().expect("flush fixture");
}
