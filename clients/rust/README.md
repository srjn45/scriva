# filedbv2 — Rust client

Async Rust gRPC client for [FileDB v2](https://github.com/srjn45/filedbv2).

Built on [tonic](https://github.com/hyperium/tonic) + [prost](https://github.com/tokio-rs/prost), using Tokio for async I/O.

## Prerequisites

- Rust 1.75+ (2021 edition)
- `protoc` (Protocol Buffers compiler) — required at build time for code generation  
  Install: https://grpc.io/docs/protoc-installation/

## Install

Add to `Cargo.toml`:

```toml
[dependencies]
filedbv2 = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

## Connect

```rust
use filedbv2::FileDB;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Plaintext (default for local development)
    let mut db = FileDB::connect("localhost", 5433, "dev-key").await?;

    // With TLS — pass PEM bytes of the CA certificate
    let ca_pem = std::fs::read("/path/to/ca.pem")?;
    let mut db = FileDB::connect_tls("myserver.example.com", 5433, "api-key", &ca_pem).await?;

    Ok(())
}
```

The API key is sent as `x-api-key` gRPC metadata on every call.

## Collection management

```rust
db.create_collection("users").await?;
let names: Vec<String> = db.list_collections().await?;
db.drop_collection("users").await?;

// Give the collection a default per-record TTL (seconds). Records inserted
// without an explicit TTL then expire this long after being written.
db.create_collection_with_ttl("sessions", 3600).await?;
```

## CRUD

```rust
use serde_json::json;

// Insert one record — returns the assigned u64 ID.
let id = db.insert("users", json!({"name": "Alice", "age": 30, "role": "admin"})).await?;

// Insert many — returns Vec<u64> of IDs.
let ids = db.insert_many("users", vec![
    json!({"name": "Bob",   "age": 25}),
    json!({"name": "Carol", "age": 35}),
]).await?;

// Fetch by ID.
let record = db.find_by_id("users", id).await?;
println!("{} — {}", record.id, record.data);

// Update.
db.update("users", id, json!({"name": "Alice", "age": 31})).await?;

// Delete.
let existed: bool = db.delete("users", id).await?;
```

Every record also carries a caller-supplied `key` (empty for records inserted
without one) and a monotonic `rev` (starts at 1, bumped on every write):

```rust
let r = db.find_by_id("users", id).await?;
println!("id={} key={:?} rev={}", r.id, r.key, r.rev);
```

## Keyed CRUD, upsert & compare-and-swap

Records may carry a caller-supplied string primary **key**, giving natural upsert
and optimistic-concurrency (compare-and-swap on `rev`) semantics.

```rust
use filedbv2::FileDbError;

// Insert under a key. A key already held by a live record is AlreadyExists.
let id = db.insert_with_key("users", json!({"name": "Alice"}), "alice").await?;

// Upsert: insert under the key, or replace the existing keyed record — atomic.
// Returns the resulting Record (rev is incremented on replace).
let rec = db.upsert("users", "alice", json!({"name": "Alice", "age": 31})).await?;
println!("key={} rev={}", rec.key, rec.rev);

// Fetch / overwrite / delete by key. A missing key is a typed NotFound error.
let rec = db.find_by_key("users", "alice").await?;
let upd = db.update_by_key("users", "alice", json!({"name": "Alice", "age": 32})).await?;
println!("rev after update = {}", upd.rev);
let existed = db.delete_by_key("users", "alice").await?;

// Compare-and-swap: applies only if the current rev matches expected_rev.
// A stale rev (or missing key) is a clean no-op — swapped = false, never an error.
let cas = db.update_if_rev("users", "alice", upd.rev, json!({"name": "Alice", "age": 33})).await?;
if cas.swapped {
    println!("swapped; new rev = {}", cas.record.unwrap().rev);
}

// NotFound / AlreadyExists are dedicated error variants you can match on.
match db.find_by_key("users", "ghost").await {
    Err(FileDbError::NotFound(_)) => println!("no such key"),
    Ok(rec) => println!("{}", rec.data),
    Err(e) => return Err(e.into()),
}
```

### Per-record TTL

`insert`, `insert_many`, and `update` each have a `*_with_ttl` variant taking a
`ttl_seconds: i64`:

```rust
// Expire this record 60 seconds from now, regardless of the collection default.
db.insert_with_ttl("sessions", json!({"token": "abc"}), 60).await?;

// Same TTL applied to every record in the batch.
db.insert_many_with_ttl("sessions", vec![
    json!({"token": "a"}),
    json!({"token": "b"}),
], 60).await?;

// On update, ttl_seconds > 0 resets the deadline; the plain `update` (ttl 0) is
// sticky and leaves the existing deadline untouched.
db.update_with_ttl("sessions", id, json!({"token": "abc", "seen": true}), 120).await?;
```

A `ttl_seconds` of `0` (what the plain `insert`/`insert_many`/`update` methods
use) inherits the collection's default TTL on insert; a value greater than 0
overrides it. Negative values are rejected by the server.

## Find (querying)

```rust
use filedbv2::{FilterInput, FilterOp, FindOptions};

// Collect all results into a Vec<Record>.
let admins = db.find("users", FindOptions {
    filter: Some(FilterInput::field("role", FilterOp::Eq, "admin")),
    order_by: "name".to_owned(),
    ..Default::default()
}).await?;

// AND composite filter.
let records = db.find("users", FindOptions {
    filter: Some(FilterInput::and(vec![
        FilterInput::field("age",  FilterOp::Gte, "18"),
        FilterInput::field("role", FilterOp::Eq,  "admin"),
    ])),
    limit: 10,
    ..Default::default()
}).await?;

// OR composite filter.
let records = db.find("users", FindOptions {
    filter: Some(FilterInput::or(vec![
        FilterInput::field("role", FilterOp::Eq, "admin"),
        FilterInput::field("role", FilterOp::Eq, "superuser"),
    ])),
    ..Default::default()
}).await?;

// Streaming variant — more memory-efficient for large result sets.
use futures::StreamExt;
let mut stream = db.find_stream("users", FindOptions::default()).await?;
while let Some(record) = stream.next().await {
    let record = record?;
    println!("{}", record.data);
}
```

### Field projection

Set `fields` to project each record's `data` down to those top-level fields
(`id`, `key` and `rev` are always returned; unknown fields are silently omitted):

```rust
let slim = db.find("users", FindOptions {
    fields: vec!["name".into(), "email".into()],
    ..Default::default()
}).await?;

// Also on single-record fetches:
let r = db.find_by_id_with_fields("users", id, &["name"]).await?;
let r = db.find_by_key_with_fields("users", "alice", &["name"]).await?;
```

### Multi-field sort & keyset pagination

`order_by_fields` gives a multi-field, per-field-directional sort (the record id
is always the final tiebreaker, so the order is total). Use `find_page` to walk
results page by page with a keyset cursor — O(page), not O(offset):

```rust
use filedbv2::OrderBy;

let mut page_token = String::new();
loop {
    let (records, next) = db.find_page("users", FindOptions {
        order_by_fields: vec![OrderBy::asc("role"), OrderBy::desc("age")],
        limit: 100,
        page_token: page_token.clone(),
        ..Default::default()
    }).await?;

    for r in &records {
        println!("{}", r.data);
    }
    if next.is_empty() {
        break;   // last page reached
    }
    page_token = next;   // feed the cursor back for the next page
}
```

Keep the same filter, ordering and limit on every page. The deprecated scalar
`order_by` / `descending` fields are still honoured when `order_by_fields` is empty.

### Filter operators

| `FilterOp` | Description |
|---|---|
| `Eq` | Equal |
| `Neq` | Not equal |
| `Gt` | Greater than |
| `Gte` | Greater than or equal |
| `Lt` | Less than |
| `Lte` | Less than or equal |
| `Contains` | String contains (substring) |
| `Regex` | Regular expression match |

### `FindOptions`

| Field | Type | Default | Description |
|---|---|---|---|
| `filter` | `Option<FilterInput>` | `None` | Query filter |
| `limit` | `u32` | `0` | Max results (0 = no limit) |
| `offset` | `u32` | `0` | Skip first N results |
| `order_by` | `String` | `""` | *Deprecated* single-field sort — use `order_by_fields` |
| `descending` | `bool` | `false` | *Deprecated* direction for `order_by` |
| `order_by_fields` | `Vec<OrderBy>` | `[]` | Multi-field sort (supersedes `order_by`) |
| `fields` | `Vec<String>` | `[]` | Project `data` to these top-level fields |
| `page_token` | `String` | `""` | Keyset cursor from a previous `find_page` |

## Secondary indexes

```rust
// Create an index on a field (no-op if already exists).
db.ensure_index("users", "role").await?;

// List indexes.
let fields: Vec<String> = db.list_indexes("users").await?;

// Drop an index.
db.drop_index("users", "role").await?;
```

Once an index exists, `find` with a single `Eq` filter on that field uses
the index automatically (O(1) lookup instead of full scan).

## Transactions

```rust
let tx_id = db.begin_tx("orders").await?;

// Stage writes... (the server records them under tx_id)
db.insert("orders", serde_json::json!({"item": "widget", "qty": 1})).await?;

// Commit or roll back.
db.commit_tx(&tx_id).await?;
// db.rollback_tx(&tx_id).await?;
```

## Watch (change feed)

`watch` returns an async `Stream` of `WatchEvent`s. Drop the stream to cancel.

```rust
use futures::StreamExt;
use filedbv2::WatchOp;

let mut events = db.watch("users", None).await?;
while let Some(event) = events.next().await {
    let event = event?;
    match event.op {
        WatchOp::Inserted => println!("INSERT id={} data={}", event.record.id, event.record.data),
        WatchOp::Updated  => println!("UPDATE id={} data={}", event.record.id, event.record.data),
        WatchOp::Deleted  => println!("DELETE id={}", event.record.id),
        // The server dropped events because this subscriber fell behind —
        // resync from a fresh `find`. No record accompanies an overflow.
        WatchOp::Overflow => println!("OVERFLOW — missed events, resync needed"),
        WatchOp::Unspecified => {}
    }
}

// With a filter — only receive events matching the filter.
let filter = FilterInput::field("role", FilterOp::Eq, "admin");
let mut events = db.watch("users", Some(filter)).await?;
```

## Aggregations

Compute `count` and numeric aggregations (`sum`/`avg`/`min`/`max`) entirely in the
engine — optionally grouped by a field, honouring the same filter as `find`.

```rust
use filedbv2::{AggregateOp, AggregateOptions, FilterInput, FilterOp};

// Count matching records.
let n = db.count("users", Some(FilterInput::field("role", FilterOp::Eq, "admin"))).await?;

// Whole-collection numeric aggregation over a field.
let groups = db.aggregate("users", AggregateOptions {
    aggregations: vec![AggregateOp::Sum, AggregateOp::Avg, AggregateOp::Min, AggregateOp::Max],
    field: "age".into(),
    ..Default::default()
}).await?;
let g = &groups[0];   // ungrouped => exactly one group, with `group == Null`
println!("count={} sum={} avg={} min={} max={}", g.count, g.sum, g.avg, g.min, g.max);

// Group by a field — one AggregateGroup per distinct value.
let by_role = db.group_by(
    "users",
    "role",                    // group-by field
    vec![AggregateOp::Avg],    // aggregations
    "age",                     // numeric metric field
    None,                      // optional filter
).await?;
for g in &by_role {
    // `numeric` is false for groups with no numeric metric value; then
    // sum/avg/min/max are meaningless.
    println!("{:?}: count={} avg_age={}", g.group, g.count, if g.numeric { g.avg } else { 0.0 });
}
```

`count` is always returned; `sum`/`avg`/`min`/`max` require `field` to be set.
`Aggregate` is server-streaming (one message per group); these helpers collect it
into a `Vec<AggregateGroup>`.

## Stats

```rust
let s = db.stats("users").await?;
println!(
    "collection={} records={} segments={} dirty={} size_bytes={}",
    s.collection, s.record_count, s.segment_count, s.dirty_entries, s.size_bytes,
);
```

## Maintenance

```rust
// Force a synchronous compaction of a collection — merges dirty segments and
// reclaims space from deleted/overwritten records. Returns true on success.
db.compact("users").await?;

// Stream a consistent gzip-compressed tar snapshot of the whole database
// straight to a file. Returns the number of bytes written; restore with
// `tar xzf backup.tar.gz`.
let bytes = db.snapshot_to_file("backup.tar.gz").await?;

// Or consume the raw archive chunks yourself (Snapshot is server-streaming):
use futures::StreamExt;
let mut chunks = db.snapshot().await?;
while let Some(chunk) = chunks.next().await {
    let chunk: Vec<u8> = chunk?;   // raw .tar.gz bytes
    // out.write_all(&chunk)?;
}
```

## TLS

```rust
let ca_pem = std::fs::read("/path/to/ca.crt").expect("ca cert");
let mut db = FileDB::connect_tls("myserver.example.com", 5433, "api-key", &ca_pem).await?;
```

Generate a self-signed CA for local testing:

```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout key.pem -out cert.pem \
  -days 365 -subj "/CN=localhost"
```

Then start the server with `--tls-cert cert.pem --tls-key key.pem` and connect
with `FileDB::connect_tls("localhost", 5433, "api-key", &std::fs::read("cert.pem")?).await?`.

## Examples

Run the end-to-end examples against a live server (`make run` from repo root):

```bash
# Basic CRUD, filters, indexes, transactions, stats
cargo run --example test_basic

# Watch change feed with concurrent inserts
cargo run --example test_watch
```

## Building from source

```bash
cd clients/rust
cargo build
```

`tonic-build` calls `protoc` during `cargo build`. If `protoc` is not on your
`PATH`, set `PROTOC=/path/to/protoc` before building.

## Crates.io

```bash
cargo publish
```
