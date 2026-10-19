//! The `graph` subcommand: create and drop named graphs.
//!
//! A bulk load usually ends by declaring the named graph over the collections
//! it just populated. Without this, that last step needs `arangosh` — pulling
//! in the whole ArangoDB client package for one API call, which defeats the
//! point of shipping a single binary.

use arangodb_tools_core::{Error, Result};
use clap::{Args, Subcommand};

use crate::commands::connection::ConnectionArgs;
use crate::output::Reporter;

/// Named-graph management.
#[derive(Debug, Args)]
pub(crate) struct GraphArgs {
    #[command(subcommand)]
    pub action: GraphAction,
}

/// What to do with the graph.
#[derive(Debug, Subcommand)]
pub(crate) enum GraphAction {
    /// Create a named graph from its edge definitions.
    Create(CreateArgs),
    /// Drop a named graph.
    Drop(DropArgs),
}

/// Options for `graph create`.
#[derive(Debug, Args)]
pub(crate) struct CreateArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Graph name.
    #[arg(long)]
    pub name: String,

    /// An edge definition: `EDGE_COLLECTION:FROM[,FROM...]->TO[,TO...]`.
    ///
    /// Repeatable, and at least one is required unless the graph is made up
    /// only of orphan collections. Example:
    /// `--edge-definition 'orders_to_customers:orders->customers'`.
    #[arg(long, value_name = "SPEC")]
    pub edge_definition: Vec<String>,

    /// A vertex collection belonging to the graph but to no edge definition.
    #[arg(long, value_name = "COLLECTION")]
    pub orphan_collection: Vec<String>,

    /// Replace an existing graph of the same name.
    ///
    /// Drops the graph first, leaving its collections and their documents
    /// intact; only the graph definition is replaced.
    #[arg(long)]
    pub overwrite: bool,
}

/// Options for `graph drop`.
#[derive(Debug, Args)]
pub(crate) struct DropArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Graph name.
    #[arg(long)]
    pub name: String,

    /// Also drop the collections the graph referenced, and their documents.
    ///
    /// Off by default: the collections are usually loaded separately and
    /// outlive the graph definition.
    #[arg(long)]
    pub drop_collections: bool,
}

/// Runs the `graph` subcommand.
pub(crate) async fn run(args: GraphArgs, reporter: Reporter) -> Result<()> {
    match &args.action {
        GraphAction::Create(create) => {
            let client = create.connection.build_client()?;
            let definitions = create
                .edge_definition
                .iter()
                .map(|spec| parse_edge_definition(spec))
                .collect::<Result<Vec<_>>>()?;
            if definitions.is_empty() && create.orphan_collection.is_empty() {
                return Err(Error::config(
                    "a graph needs at least one --edge-definition or --orphan-collection",
                ));
            }

            if create.overwrite {
                // Only the definition is replaced; the data stays.
                client.drop_graph(&create.name, false).await?;
            }
            let body = serde_json::json!({
                "name": create.name,
                "edgeDefinitions": definitions,
                "orphanCollections": create.orphan_collection,
            });
            client.create_graph(&body).await.map_err(|err| {
                Error::config(format!(
                    "failed to create graph '{}': {err}. Pass --overwrite to replace an \
                     existing graph of the same name; collections named in the definitions \
                     are created automatically if missing.",
                    create.name
                ))
            })?;

            let edges = definitions.len();
            let orphans = create.orphan_collection.len();
            reporter.result(
                || {
                    format!(
                        "created graph '{}' with {edges} edge definition(s) and \
                         {orphans} orphan collection(s)",
                        create.name
                    )
                },
                || {
                    serde_json::json!({
                        "operation": "graph-create",
                        "status": "ok",
                        "graph": create.name,
                        "edge_definitions": edges,
                        "orphan_collections": orphans,
                    })
                },
            );
        }
        GraphAction::Drop(drop) => {
            let client = drop.connection.build_client()?;
            client.drop_graph(&drop.name, drop.drop_collections).await?;
            reporter.result(
                || {
                    let suffix = if drop.drop_collections {
                        " and its collections"
                    } else {
                        " (collections kept)"
                    };
                    format!("dropped graph '{}'{suffix}", drop.name)
                },
                || {
                    serde_json::json!({
                        "operation": "graph-drop",
                        "status": "ok",
                        "graph": drop.name,
                        "dropped_collections": drop.drop_collections,
                    })
                },
            );
        }
    }
    Ok(())
}

/// Parses `EDGE_COLLECTION:FROM[,FROM...]->TO[,TO...]`.
///
/// The shape mirrors ArangoDB's own `graph._relation(edge, from, to)`, so a
/// definition reads the same way whichever tool declares it.
fn parse_edge_definition(spec: &str) -> Result<serde_json::Value> {
    let bad = |reason: &str| {
        Error::config(format!(
            "invalid --edge-definition '{spec}': {reason}. Expected \
             'EDGE_COLLECTION:FROM[,FROM...]->TO[,TO...]', for example \
             'orders_to_customers:orders->customers'"
        ))
    };

    let (collection, rest) = spec
        .split_once(':')
        .ok_or_else(|| bad("no ':' separator"))?;
    let (from, to) = rest
        .split_once("->")
        .ok_or_else(|| bad("no '->' separator"))?;

    let names = |list: &str, side: &str| -> Result<Vec<String>> {
        let parsed: Vec<String> = list
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect();
        if parsed.is_empty() {
            return Err(bad(&format!("the {side} side names no collection")));
        }
        Ok(parsed)
    };

    let collection = collection.trim();
    if collection.is_empty() {
        return Err(bad("the edge collection name is empty"));
    }

    Ok(serde_json::json!({
        "collection": collection,
        "from": names(from, "'from'")?,
        "to": names(to, "'to'")?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_simple_definition() {
        let parsed = parse_edge_definition("knows:people->people").unwrap();
        assert_eq!(parsed["collection"], "knows");
        assert_eq!(parsed["from"], serde_json::json!(["people"]));
        assert_eq!(parsed["to"], serde_json::json!(["people"]));
    }

    #[test]
    fn parses_multiple_endpoints_on_each_side() {
        let parsed = parse_edge_definition("rel:a,b->c,d").unwrap();
        assert_eq!(parsed["from"], serde_json::json!(["a", "b"]));
        assert_eq!(parsed["to"], serde_json::json!(["c", "d"]));
    }

    #[test]
    fn tolerates_surrounding_whitespace() {
        let parsed = parse_edge_definition(" rel : a , b -> c ").unwrap();
        assert_eq!(parsed["collection"], "rel");
        assert_eq!(parsed["from"], serde_json::json!(["a", "b"]));
        assert_eq!(parsed["to"], serde_json::json!(["c"]));
    }

    /// Every rejection names the offending spec and shows the expected form,
    /// so a malformed definition does not need the docs to decode.
    #[test]
    fn malformed_definitions_are_rejected_with_the_expected_form() {
        for (spec, reason) in [
            ("knows", "no ':' separator"),
            ("knows:people", "no '->' separator"),
            (":people->people", "edge collection name is empty"),
            ("knows:->people", "'from' side names no collection"),
            ("knows:people->", "'to' side names no collection"),
        ] {
            let err = parse_edge_definition(spec).unwrap_err().to_string();
            assert!(err.contains(spec), "names the spec: {err}");
            assert!(err.contains(reason), "explains '{reason}': {err}");
            assert!(
                err.contains("orders_to_customers:orders->customers"),
                "shows an example: {err}"
            );
        }
    }
}
