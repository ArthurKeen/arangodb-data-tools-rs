//! Live integration tests for RDF bulk import.
//!
//! These need a running ArangoDB and run only when `ARANGO_ENDPOINT` is set
//! (the CI test job provides it); otherwise each test no-ops. They exercise the
//! full path: parse -> build graph model -> load vertex + edge collections.

use arangodb_client::ArangoClient;
use arangodb_rdf::{import_rdf, GraphModel, RdfFormat, RdfOptions};

/// Builds a client from the CI/integration environment, or `None` when no
/// server is configured (so the test no-ops in plain `cargo test`).
fn live_client() -> Option<ArangoClient> {
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

async fn reset(client: &ArangoClient, vertex: &str, edge: &str) {
    let _ = client.drop_collection(vertex).await;
    let _ = client.drop_collection(edge).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imports_ntriples_into_a_property_graph_idempotently() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping live RDF N-Triples test");
        return;
    };
    let vertex = "arangox_it_rdf_nt_nodes";
    let edge = "arangox_it_rdf_nt_links";
    reset(&client, vertex, edge).await;

    // 3 triples over 3 distinct IRI resources; the literal object is dropped
    // by the default NoLiterals policy, so it yields no edge or vertex.
    let ntriples = concat!(
        "<http://ex/alice> <http://ex/knows> <http://ex/bob> .\n",
        "<http://ex/bob> <http://ex/knows> <http://ex/carol> .\n",
        "<http://ex/alice> <http://ex/name> \"Alice\" .\n",
    );
    let options = RdfOptions::new(vertex, edge);

    let summary = import_rdf(
        &client,
        std::io::Cursor::new(ntriples.as_bytes().to_vec()),
        RdfFormat::NTriples,
        &options,
        Default::default(),
        Default::default(),
    )
    .await
    .expect("first import succeeds");

    assert_eq!(summary.triples_read, 3);
    assert_eq!(summary.vertices_created, 3, "alice, bob, carol");
    assert_eq!(summary.edges_created, 2, "two knows edges");
    assert_eq!(
        client.collection_count(vertex).await.unwrap(),
        3,
        "vertex count"
    );
    assert_eq!(
        client.collection_count(edge).await.unwrap(),
        2,
        "edge count"
    );

    // Re-importing the same data is idempotent: deterministic keys + ignore.
    let again = import_rdf(
        &client,
        std::io::Cursor::new(ntriples.as_bytes().to_vec()),
        RdfFormat::NTriples,
        &options,
        Default::default(),
        Default::default(),
    )
    .await
    .expect("second import succeeds");
    assert_eq!(again.vertices_created, 0, "no new vertices on re-import");
    assert_eq!(again.edges_created, 0, "no new edges on re-import");
    assert_eq!(client.collection_count(vertex).await.unwrap(), 3);
    assert_eq!(client.collection_count(edge).await.unwrap(), 2);

    reset(&client, vertex, edge).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imports_turtle_with_prefixes_and_a_keyword() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping live RDF Turtle test");
        return;
    };
    let vertex = "arangox_it_rdf_ttl_nodes";
    let edge = "arangox_it_rdf_ttl_links";
    reset(&client, vertex, edge).await;

    // (alice a Person), (alice knows bob), (bob a Person) => 3 triples,
    // vertices {alice, Person, bob}, 3 edges (all objects are IRIs).
    let turtle = concat!(
        "@prefix ex: <http://ex/> .\n",
        "ex:alice a ex:Person ; ex:knows ex:bob .\n",
        "ex:bob a ex:Person .\n",
    );
    let options = RdfOptions::new(vertex, edge);

    let summary = import_rdf(
        &client,
        std::io::Cursor::new(turtle.as_bytes().to_vec()),
        RdfFormat::Turtle,
        &options,
        Default::default(),
        Default::default(),
    )
    .await
    .expect("turtle import succeeds");

    assert_eq!(summary.triples_read, 3);
    assert_eq!(summary.vertices_created, 3, "alice, Person, bob");
    assert_eq!(summary.edges_created, 3);
    assert_eq!(client.collection_count(vertex).await.unwrap(), 3);
    assert_eq!(client.collection_count(edge).await.unwrap(), 3);

    reset(&client, vertex, edge).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpt_routes_terms_into_typed_collections() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping live RDF RPT test");
        return;
    };
    let base = "arangox_it_rdf_rpt";
    let edge = "arangox_it_rdf_rpt_stmt";
    let uriref = format!("{base}_URIRef");
    let bnode = format!("{base}_BNode");
    let literal = format!("{base}_Literal");
    for c in [&uriref, &bnode, &literal, &edge.to_string()] {
        let _ = client.drop_collection(c).await;
    }

    // 2 IRIs + 1 blank + 1 literal, 3 statements. Under RPT the literal policy
    // is ignored and the literal becomes its own vertex.
    let ntriples = concat!(
        "<http://ex/alice> <http://ex/knows> <http://ex/bob> .\n",
        "<http://ex/alice> <http://ex/homepage> _:site .\n",
        "<http://ex/alice> <http://ex/name> \"Alice\" .\n",
    );
    let mut options = RdfOptions::new(base, edge);
    options.graph_model = GraphModel::Rpt;

    let summary = import_rdf(
        &client,
        std::io::Cursor::new(ntriples.as_bytes().to_vec()),
        RdfFormat::NTriples,
        &options,
        Default::default(),
        Default::default(),
    )
    .await
    .expect("rpt import succeeds");

    assert_eq!(summary.triples_read, 3);
    assert_eq!(summary.vertices_created, 4, "alice, bob, blank, literal");
    assert_eq!(summary.edges_created, 3, "one edge per statement");
    assert_eq!(
        client.collection_count(&uriref).await.unwrap(),
        2,
        "alice+bob"
    );
    assert_eq!(client.collection_count(&bnode).await.unwrap(), 1);
    assert_eq!(client.collection_count(&literal).await.unwrap(), 1);
    assert_eq!(client.collection_count(edge).await.unwrap(), 3);

    for c in [&uriref, &bnode, &literal, &edge.to_string()] {
        let _ = client.drop_collection(c).await;
    }
}
