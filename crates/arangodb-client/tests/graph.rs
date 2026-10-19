//! Live checks for named-graph management.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise every test is a no-op.

use arangodb_client::ArangoClient;

fn client() -> Option<ArangoClient> {
    let endpoint = std::env::var("ARANGO_ENDPOINT").ok()?;
    let password = std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default();
    Some(
        ArangoClient::builder()
            .endpoint(endpoint)
            .database("_system")
            .basic_auth("root", password)
            .build()
            .expect("client builds from env"),
    )
}

fn definition(name: &str, edge: &str, from: &str, to: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "edgeDefinitions": [{"collection": edge, "from": [from], "to": [to]}],
        "orphanCollections": [],
    })
}

/// Creating a graph also creates the collections it names, so a graph can be
/// declared before any data is loaded.
#[tokio::test]
async fn creating_a_graph_creates_its_collections() {
    let Some(client) = client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let (graph, edge, from, to) = ("it_g", "it_g_edge", "it_g_from", "it_g_to");
    let _ = client.drop_graph(graph, true).await;

    client
        .create_graph(&definition(graph, edge, from, to))
        .await
        .expect("graph is created");

    for collection in [edge, from, to] {
        assert!(
            client
                .collection_info(collection)
                .await
                .expect("collection lookup")
                .is_some(),
            "{collection} should have been created with the graph"
        );
    }
    let _ = client.drop_graph(graph, true).await;
}

/// A second create is refused rather than silently replacing the definition.
#[tokio::test]
async fn creating_an_existing_graph_is_refused() {
    let Some(client) = client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let (graph, edge, from, to) = ("it_g2", "it_g2_edge", "it_g2_from", "it_g2_to");
    let _ = client.drop_graph(graph, true).await;
    client
        .create_graph(&definition(graph, edge, from, to))
        .await
        .expect("first create succeeds");

    let err = client
        .create_graph(&definition(graph, edge, from, to))
        .await
        .expect_err("a duplicate graph must be refused");
    assert!(
        err.to_string().contains("exists"),
        "the error should say the graph exists, got: {err}"
    );
    let _ = client.drop_graph(graph, true).await;
}

/// Dropping a graph leaves its collections and their documents alone, which is
/// what makes `--overwrite` safe to re-run after a load.
#[tokio::test]
async fn dropping_a_graph_keeps_its_collections_by_default() {
    let Some(client) = client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let (graph, edge, from, to) = ("it_g3", "it_g3_edge", "it_g3_from", "it_g3_to");
    let _ = client.drop_graph(graph, true).await;
    client
        .create_graph(&definition(graph, edge, from, to))
        .await
        .expect("graph is created");

    client
        .drop_graph(graph, false)
        .await
        .expect("graph is dropped");

    assert!(
        client
            .collection_info(from)
            .await
            .expect("collection lookup")
            .is_some(),
        "collections must survive a graph drop"
    );
    let _ = client.drop_graph(graph, true).await;
    for collection in [edge, from, to] {
        let _ = client.drop_collection(collection).await;
    }
}

/// Dropping a graph that is not there is not an error, so a teardown step can
/// run unconditionally.
#[tokio::test]
async fn dropping_a_missing_graph_succeeds() {
    let Some(client) = client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    client
        .drop_graph("it_graph_that_does_not_exist", false)
        .await
        .expect("dropping a missing graph is a no-op");
}
