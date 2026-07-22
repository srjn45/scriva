//! test_watch — watch example for the ScrivaDB Rust client.
//!
//! Subscribes to change events on a collection while inserting records in a
//! background task, then prints each event received.
//!
//! Prerequisites:
//!   - ScrivaDB server running: `make run` from the repo root.
//!   - Run: `cargo run --example test_watch` from clients/rust/.

use futures::StreamExt;
use scriva::{ScrivaDB, FilterInput, FilterOp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure the collection exists before watching.
    let mut setup = ScrivaDB::connect("localhost", 5433, "dev-key").await?;
    // Drop from any previous run.
    let _ = setup.drop_collection("watch_rs").await;
    setup.create_collection("watch_rs").await?;
    drop(setup);

    // Spawn a background task that inserts records after a short delay.
    let insert_handle = tokio::spawn(async {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let mut db = ScrivaDB::connect("localhost", 5433, "dev-key").await.unwrap();
        db.insert("watch_rs", serde_json::json!({"name": "Alice", "role": "admin"})).await.unwrap();
        db.insert("watch_rs", serde_json::json!({"name": "Bob",   "role": "user"})).await.unwrap();
        db.insert("watch_rs", serde_json::json!({"name": "Carol", "role": "admin"})).await.unwrap();

        let id = db.insert("watch_rs", serde_json::json!({"name": "Dave"})).await.unwrap();
        db.update("watch_rs", id, serde_json::json!({"name": "Dave", "role": "user"})).await.unwrap();
        db.delete("watch_rs", id).await.unwrap();

        println!("[inserter] done — closing connection");
    });

    // Watch only admin events using a filter.
    let mut watcher = ScrivaDB::connect("localhost", 5433, "dev-key").await?;
    let filter = FilterInput::field("role", FilterOp::Eq, "admin");
    let mut stream = watcher.watch("watch_rs", Some(filter)).await?;

    println!("[watcher] subscribed to watch_rs (role=admin only)");
    println!("[watcher] waiting for events — press Ctrl-C to stop");

    let mut count = 0usize;
    loop {
        tokio::select! {
            maybe_event = stream.next() => {
                match maybe_event {
                    Some(Ok(event)) => {
                        println!(
                            "[watcher] {:?} collection={} id={} data={}{}",
                            event.op,
                            event.collection,
                            event.record.id,
                            event.record.data,
                            event.ts.as_deref().map(|t| format!(" ts={}", t)).unwrap_or_default(),
                        );
                        count += 1;
                        if count >= 2 {
                            // Received both admin inserts — exit gracefully.
                            break;
                        }
                    }
                    Some(Err(status)) => {
                        eprintln!("[watcher] error: {}", status);
                        break;
                    }
                    None => {
                        println!("[watcher] stream ended");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                println!("[watcher] timed out waiting for events");
                break;
            }
        }
    }

    insert_handle.await?;

    // Cleanup
    let mut cleanup = ScrivaDB::connect("localhost", 5433, "dev-key").await?;
    cleanup.drop_collection("watch_rs").await?;
    println!("[watcher] collection dropped, exiting");
    Ok(())
}
