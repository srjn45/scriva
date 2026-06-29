//! FileDB v2 Rust client — async gRPC wrapper over `proto/filedb.proto`.
//!
//! All methods are `async` and require a Tokio runtime.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use filedbv2::{FileDB, FilterInput, FilterOp, FindOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let mut db = FileDB::connect("localhost", 5433, "dev-key").await?;
//!
//!     db.create_collection("users").await?;
//!
//!     let id = db.insert("users", serde_json::json!({"name": "Alice", "age": 30})).await?;
//!     let record = db.find_by_id("users", id).await?;
//!     println!("{:?}", record);
//!
//!     let admins = db.find("users", FindOptions {
//!         filter: Some(FilterInput::field("role", FilterOp::Eq, "admin")),
//!         ..Default::default()
//!     }).await?;
//!     println!("{} admins", admins.len());
//!
//!     db.drop_collection("users").await?;
//!     Ok(())
//! }
//! ```

pub(crate) mod pb {
    tonic::include_proto!("filedb.v1");
}

mod client;

pub use client::{
    CollectionStats, FileDB, FilterInput, FilterOp, FindOptions, Record, WatchEvent, WatchOp,
};
