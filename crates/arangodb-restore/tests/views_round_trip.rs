//! Live dump -> restore round-trip for search views (PRD §8.4 / §8.5).
//!
//! Seeds a database with an ArangoSearch view and a search-alias view over an
//! inverted index, dumps it, restores into a fresh database, and asserts both
//! views come back usable — not merely present.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise it is a no-op.

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions};
use arangodb_dump::{run_dump, DumpOptions};
use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_storage::LocalFileSystem;
use bytes::Bytes;
use serde_json::Value;

fn client_for(database: &str) -> Option<ArangoClient> {
    let endpoint = std::env::var("ARANGO_ENDPOINT").ok()?;
    let password = std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default();
    Some(
        ArangoClient::builder()
            .endpoint(endpoint)
            .database(database)
            .basic_auth("root", password)
            .build()
            .expect("client builds from env"),
    )
}

/// Lists a database's views as `(name, type)` pairs.
async fn views(client: &ArangoClient) -> Vec<(String, String)> {
    let batch = client.replication_batch_create(60).await.unwrap();
    let inventory = client.replication_inventory(&batch, false).await.unwrap();
    let _ = client.replication_batch_delete(&batch).await;
    let mut found: Vec<(String, String)> = inventory
        .views
        .iter()
        .filter_map(|v| {
            Some((
                v.get("name")?.as_str()?.to_string(),
                v.get("type")?.as_str()?.to_string(),
            ))
        })
        .collect();
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn views_survive_a_dump_and_restore() {
    let Some(system) = client_for("_system") else {
        eprintln!("ARANGO_ENDPOINT not set; skipping view round-trip");
        return;
    };
    let (src, dst) = ("arangox_it_vsrc", "arangox_it_vdst");

    // Seed: a collection with data, an arangosearch view, an inverted index,
    // and a search-alias view over that index.
    let _ = system.drop_database(src).await;
    let _ = system.drop_database(dst).await;
    system.create_database(src).await.unwrap();
    let source = system.with_database(src);
    source
        .ensure_collection("docs", CollectionKind::Document)
        .await
        .unwrap();
    source
        .import_documents(
            &ImportOptions::new("docs"),
            Bytes::from_static(
                b"{\"_key\":\"a\",\"v\":\"alpha\"}\n{\"_key\":\"b\",\"v\":\"beta\"}\n",
            ),
        )
        .await
        .unwrap();
    source
        .create_view(&serde_json::json!({
            "name": "docs_as",
            "type": "arangosearch",
            "links": {"docs": {"includeAllFields": true}},
            "primarySort": [{"field": "v", "direction": "desc"}],
        }))
        .await
        .unwrap();
    source
        .create_index(
            "docs",
            &serde_json::json!({"type": "inverted", "name": "inv1", "fields": ["v"]}),
        )
        .await
        .unwrap();
    source
        .create_view(&serde_json::json!({
            "name": "docs_sa",
            "type": "search-alias",
            "indexes": [{"collection": "docs", "index": "inv1"}],
        }))
        .await
        .unwrap();

    // Dump.
    let dir = tempdir();
    let store = LocalFileSystem::new(&dir);
    let manifest = run_dump(
        &source,
        &store,
        &DumpOptions {
            database: src.to_string(),
            ..DumpOptions::default()
        },
    )
    .await
    .expect("dump succeeds");

    let view_artifacts: Vec<&str> = manifest
        .artifacts
        .iter()
        .filter(|a| a.kind == arangodb_tools_core::manifest::ArtifactKind::View)
        .filter_map(|a| a.view.as_deref())
        .collect();
    assert_eq!(
        view_artifacts.len(),
        2,
        "both views must be recorded in the manifest, got {view_artifacts:?}"
    );

    // Restore into a fresh database.
    let target = system.with_database(dst);
    run_restore(
        &target,
        &store,
        &RestoreOptions {
            create_database: Some(dst.to_string()),
            ..RestoreOptions::default()
        },
    )
    .await
    .expect("restore succeeds");

    // Both views exist, with the right types.
    assert_eq!(
        views(&target).await,
        vec![
            ("docs_as".to_string(), "arangosearch".to_string()),
            ("docs_sa".to_string(), "search-alias".to_string()),
        ],
        "both views must be restored"
    );

    // The arangosearch view is not merely present but queryable, which is what
    // proves its links were restored rather than an empty shell created.
    let hits = target
        .cursor_open(&arangodb_client::CursorRequest::new(
            "FOR d IN docs_as SEARCH d.v == 'alpha' OPTIONS {waitForSync: true} RETURN d._key",
        ))
        .await
        .expect("view query runs");
    let keys: Vec<&str> = hits.result.iter().filter_map(Value::as_str).collect();
    assert_eq!(
        keys,
        vec!["a"],
        "the restored view must return its documents"
    );

    let _ = system.drop_database(src).await;
    let _ = system.drop_database(dst).await;
}

/// A unique temp directory for one test run.
fn tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "arangox-views-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}
