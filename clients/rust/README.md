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
| `order_by` | `String` | `""` | Field name to sort by |
| `descending` | `bool` | `false` | Reverse sort order |

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
        WatchOp::Unspecified => {}
    }
}

// With a filter — only receive events matching the filter.
let filter = FilterInput::field("role", FilterOp::Eq, "admin");
let mut events = db.watch("users", Some(filter)).await?;
```

## Stats

```rust
let s = db.stats("users").await?;
println!(
    "collection={} records={} segments={} dirty={} size_bytes={}",
    s.collection, s.record_count, s.segment_count, s.dirty_entries, s.size_bytes,
);
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
