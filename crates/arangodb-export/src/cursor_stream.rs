//! Turns an AQL cursor into a stream of documents.
//!
//! A background task drives the cursor (`open` → `next` → …) and feeds batches
//! through a small bounded channel, so the server fetch of batch *N+1* overlaps
//! the encoding/writing of batch *N* (PRD §11.1: export should be bound by the
//! cursor or the storage write, not by client stalls). If the consumer drops
//! early, the cursor is disposed server-side.

use arangodb_client::{ArangoClient, CursorRequest};
use arangodb_tools_core::Result;
use async_stream::try_stream;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::DocumentStream;

/// Number of batches buffered between the cursor task and the consumer.
const PREFETCH: usize = 2;

/// Streams the documents produced by `request`, fetching ahead as the consumer
/// drains the stream.
pub fn document_stream(client: ArangoClient, request: CursorRequest) -> DocumentStream {
    let (tx, mut rx) = mpsc::channel::<Result<Vec<Value>>>(PREFETCH);

    tokio::spawn(async move {
        let mut batch = match client.cursor_open(&request).await {
            Ok(batch) => batch,
            Err(err) => {
                let _ = tx.send(Err(err)).await;
                return;
            }
        };
        loop {
            let has_more = batch.has_more;
            let id = batch.id.clone();
            let documents = std::mem::take(&mut batch.result);
            if tx.send(Ok(documents)).await.is_err() {
                // Consumer dropped: release the server-side cursor and stop.
                if let Some(id) = id {
                    let _ = client.cursor_delete(&id).await;
                }
                return;
            }
            if !has_more {
                return;
            }
            let Some(id) = id else { return };
            batch = match client.cursor_next(&id).await {
                Ok(batch) => batch,
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            };
        }
    });

    Box::pin(try_stream! {
        while let Some(item) = rx.recv().await {
            let documents = item?;
            for document in documents {
                yield document;
            }
        }
    })
}
