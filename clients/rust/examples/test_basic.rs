//! test_basic — end-to-end example for the FileDB v2 Rust client.
//!
//! Prerequisites:
//!   - FileDB server running: `make run` from the repo root.
//!   - Run: `cargo run --example test_basic` from clients/rust/.

use filedbv2::{
    AggregateOp, AggregateOptions, FileDB, FileDbError, FilterInput, FilterOp, FindOptions, OrderBy,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut db = FileDB::connect("localhost", 5433, "dev-key").await?;

    // --- Collection management ---
    println!("=== Collections ===");
    db.create_collection("test_rs").await?;
    let cols = db.list_collections().await?;
    println!("Created collection. All collections: {:?}", cols);

    // --- Insert ---
    println!("\n=== Insert ===");
    let id1 = db.insert("test_rs", serde_json::json!({"name": "Alice", "age": 30, "role": "admin"})).await?;
    let id2 = db.insert("test_rs", serde_json::json!({"name": "Bob",   "age": 25, "role": "user"})).await?;
    let id3 = db.insert("test_rs", serde_json::json!({"name": "Carol", "age": 35, "role": "admin"})).await?;
    println!("Inserted IDs: {}, {}, {}", id1, id2, id3);

    let ids = db.insert_many("test_rs", vec![
        serde_json::json!({"name": "Dave", "age": 28, "role": "user"}),
        serde_json::json!({"name": "Eve",  "age": 22, "role": "user"}),
    ]).await?;
    println!("InsertMany IDs: {:?}", ids);

    // --- Find by ID ---
    println!("\n=== FindById ===");
    let record = db.find_by_id("test_rs", id1).await?;
    println!("Record: id={} data={}", record.id, record.data);

    // --- Find with filter ---
    println!("\n=== Find (filter: role=admin) ===");
    let admins = db.find("test_rs", FindOptions {
        filter: Some(FilterInput::field("role", FilterOp::Eq, "admin")),
        order_by: "name".to_owned(),
        ..Default::default()
    }).await?;
    println!("Admins: {}", admins.iter()
        .map(|r| format!("{}: {}", r.id, r.data))
        .collect::<Vec<_>>()
        .join(", "));

    // --- AND filter ---
    println!("\n=== Find (AND: role=user AND age>=25) ===");
    let filtered = db.find("test_rs", FindOptions {
        filter: Some(FilterInput::and(vec![
            FilterInput::field("role", FilterOp::Eq, "user"),
            FilterInput::field("age",  FilterOp::Gte, "25"),
        ])),
        ..Default::default()
    }).await?;
    println!("Filtered: {:?}", filtered.iter().map(|r| &r.data).collect::<Vec<_>>());

    // --- Find with limit ---
    println!("\n=== Find (limit 2) ===");
    let limited = db.find("test_rs", FindOptions {
        limit: 2,
        ..Default::default()
    }).await?;
    for r in &limited {
        println!(" - {} {}", r.id, r.data);
    }

    // --- Update ---
    println!("\n=== Update ===");
    db.update("test_rs", id1, serde_json::json!({"name": "Alice", "age": 31, "role": "superadmin"})).await?;
    let updated = db.find_by_id("test_rs", id1).await?;
    println!("Updated: {:?}", updated.data);

    // --- Delete ---
    println!("\n=== Delete ===");
    let deleted = db.delete("test_rs", id2).await?;
    println!("Deleted id2: {}", deleted);

    // --- Field projection (N2) ---
    println!("\n=== Projection (fields: name) ===");
    let projected = db.find_by_id_with_fields("test_rs", id1, &["name"]).await?;
    println!("Projected data (name only): {}", projected.data);
    let projected_list = db.find("test_rs", FindOptions {
        fields: vec!["name".to_owned()],
        ..Default::default()
    }).await?;
    println!("Projected list: {:?}", projected_list.iter().map(|r| &r.data).collect::<Vec<_>>());

    // --- Keyset pagination + multi-field order_by (N3) ---
    println!("\n=== Pagination (order_by name asc, limit 2) ===");
    let mut page_token = String::new();
    let mut page = 1;
    loop {
        let (records, next) = db.find_page("test_rs", FindOptions {
            order_by_fields: vec![OrderBy::asc("role"), OrderBy::desc("age")],
            limit: 2,
            page_token: page_token.clone(),
            ..Default::default()
        }).await?;
        println!("Page {}: {:?}", page, records.iter()
            .map(|r| format!("{}={}", r.id, r.data)).collect::<Vec<_>>());
        if next.is_empty() {
            break;
        }
        page_token = next;
        page += 1;
    }

    // --- Keyed CRUD, upsert & compare-and-swap (N1) ---
    println!("\n=== Keyed CRUD (N1) ===");
    let upserted = db.upsert("test_rs", "alice", serde_json::json!({"name": "Alice", "age": 30})).await?;
    println!("Upsert inserted key={} rev={}", upserted.key, upserted.rev);
    let replaced = db.upsert("test_rs", "alice", serde_json::json!({"name": "Alice", "age": 31})).await?;
    println!("Upsert replaced key={} rev={}", replaced.key, replaced.rev);

    let by_key = db.find_by_key("test_rs", "alice").await?;
    println!("FindByKey: id={} rev={} data={}", by_key.id, by_key.rev, by_key.data);

    // NotFound is a typed error.
    match db.find_by_key("test_rs", "nope").await {
        Err(FileDbError::NotFound(_)) => println!("FindByKey('nope') -> NotFound (expected)"),
        other => println!("Unexpected: {:?}", other),
    }

    // Keyed insert; a duplicate key is AlreadyExists.
    let bob_id = db.insert_with_key("test_rs", serde_json::json!({"name": "Bob"}), "bob").await?;
    println!("Keyed insert 'bob' -> id {}", bob_id);
    match db.insert_with_key("test_rs", serde_json::json!({"name": "Bob2"}), "bob").await {
        Err(FileDbError::AlreadyExists(_)) => println!("Re-insert 'bob' -> AlreadyExists (expected)"),
        other => println!("Unexpected: {:?}", other),
    }

    let updated_key = db.update_by_key("test_rs", "alice", serde_json::json!({"name": "Alice", "age": 32})).await?;
    println!("UpdateByKey: id={} key={} rev={}", updated_key.id, updated_key.key, updated_key.rev);

    // Compare-and-swap: stale rev is a clean no-op (swapped=false).
    let cas_stale = db.update_if_rev("test_rs", "alice", 1, serde_json::json!({"name": "Alice", "age": 99})).await?;
    println!("CAS with stale rev 1: swapped={}", cas_stale.swapped);
    let cas_ok = db.update_if_rev("test_rs", "alice", updated_key.rev, serde_json::json!({"name": "Alice", "age": 33})).await?;
    println!("CAS with rev {}: swapped={} new_rev={:?}",
        updated_key.rev, cas_ok.swapped, cas_ok.record.map(|r| r.rev));

    let del_key = db.delete_by_key("test_rs", "bob").await?;
    println!("DeleteByKey('bob'): {}", del_key);

    // --- Aggregations (N4) ---
    println!("\n=== Aggregations (N4) ===");
    let total = db.count("test_rs", None).await?;
    println!("count(all) = {}", total);

    let agg = db.aggregate("test_rs", AggregateOptions {
        aggregations: vec![AggregateOp::Sum, AggregateOp::Avg, AggregateOp::Min, AggregateOp::Max],
        field: "age".to_owned(),
        ..Default::default()
    }).await?;
    if let Some(g) = agg.first() {
        println!("age: count={} sum={} avg={} min={} max={}", g.count, g.sum, g.avg, g.min, g.max);
    }

    let grouped = db.group_by(
        "test_rs", "role",
        vec![AggregateOp::Avg],
        "age",
        None,
    ).await?;
    for g in &grouped {
        println!("group {:?}: count={} avg_age={}", g.group, g.count, g.avg);
    }

    // --- Indexes ---
    println!("\n=== Indexes ===");
    db.ensure_index("test_rs", "role").await?;
    let indexes = db.list_indexes("test_rs").await?;
    println!("Indexes: {:?}", indexes);

    let users = db.find("test_rs", FindOptions {
        filter: Some(FilterInput::field("role", FilterOp::Eq, "user")),
        ..Default::default()
    }).await?;
    println!("Users (via index): {:?}", users.iter().map(|r| &r.data).collect::<Vec<_>>());

    db.drop_index("test_rs", "role").await?;
    let indexes_after = db.list_indexes("test_rs").await?;
    println!("Indexes after drop: {:?}", indexes_after);

    // --- Transactions ---
    println!("\n=== Transactions ===");
    let tx_id = db.begin_tx("test_rs").await?;
    println!("TX ID: {}", tx_id);
    let committed = db.commit_tx(&tx_id).await?;
    println!("Committed: {}", committed);

    let tx_id2 = db.begin_tx("test_rs").await?;
    let rolled_back = db.rollback_tx(&tx_id2).await?;
    println!("Rolled back: {}", rolled_back);

    // --- Stats ---
    println!("\n=== Stats ===");
    let stats = db.stats("test_rs").await?;
    println!(
        "Stats: collection={} records={} segments={} dirty={} size_bytes={}",
        stats.collection, stats.record_count, stats.segment_count,
        stats.dirty_entries, stats.size_bytes,
    );

    // --- Compaction ---
    println!("\n=== Compact ===");
    let compacted = db.compact("test_rs").await?;
    println!("Compacted: {}", compacted);

    // --- Per-record TTL ---
    println!("\n=== Per-record TTL ===");
    let ttl_id = db
        .insert_with_ttl("test_rs", serde_json::json!({"name": "Ephemeral", "role": "temp"}), 3600)
        .await?;
    println!("Inserted {} with a 3600s TTL", ttl_id);
    // ttl_seconds 0 is sticky — it keeps the existing deadline.
    db.update("test_rs", ttl_id, serde_json::json!({"name": "Ephemeral", "role": "temp", "touched": true})).await?;
    println!("Updated the TTL record (deadline preserved)");

    // --- Snapshot (whole-database backup) ---
    println!("\n=== Snapshot ===");
    let backup = std::env::temp_dir().join("filedb_rust_snapshot.tar.gz");
    let bytes = db.snapshot_to_file(&backup).await?;
    println!("Snapshot: wrote {} bytes to {}", bytes, backup.display());
    let _ = std::fs::remove_file(&backup);

    // --- Cleanup ---
    println!("\n=== Cleanup ===");
    db.drop_collection("test_rs").await?;
    let cols_after = db.list_collections().await?;
    println!("Collections after drop: {:?}", cols_after);

    println!("\nAll done!");
    Ok(())
}
