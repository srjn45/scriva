use futures::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Certificate};
use tonic::Request;

use crate::pb;
use crate::pb::file_db_client::FileDbClient;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A record returned by the server.
#[derive(Debug, Clone)]
pub struct Record {
    pub id: u64,
    pub data: serde_json::Value,
    pub date_added: Option<String>,
    pub date_modified: Option<String>,
}

/// Options for the `find` method.
#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    pub filter: Option<FilterInput>,
    pub limit: u32,
    pub offset: u32,
    pub order_by: String,
    pub descending: bool,
}

/// Collection statistics returned by `stats`.
#[derive(Debug, Clone)]
pub struct CollectionStats {
    pub collection: String,
    pub record_count: u64,
    pub segment_count: u64,
    pub dirty_entries: u64,
    pub size_bytes: u64,
}

/// A change event delivered by the `watch` stream.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub op: WatchOp,
    pub collection: String,
    pub record: Record,
    pub ts: Option<String>,
}

/// The operation type in a `WatchEvent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchOp {
    Inserted,
    Updated,
    Deleted,
    Unspecified,
}

/// Comparison operator for a `FilterInput::Field`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    Regex,
}

/// A composable filter for `find` and `watch` queries.
///
/// # Examples
///
/// ```rust
/// use filedbv2::{FilterInput, FilterOp};
///
/// // Field filter
/// let f = FilterInput::field("age", FilterOp::Gt, "18");
///
/// // AND composite
/// let f = FilterInput::and(vec![
///     FilterInput::field("age",  FilterOp::Gte, "18"),
///     FilterInput::field("role", FilterOp::Eq,  "admin"),
/// ]);
///
/// // OR composite
/// let f = FilterInput::or(vec![
///     FilterInput::field("status", FilterOp::Eq, "active"),
///     FilterInput::field("role",   FilterOp::Eq, "admin"),
/// ]);
/// ```
#[derive(Debug, Clone)]
pub enum FilterInput {
    Field {
        field: String,
        op: FilterOp,
        value: String,
    },
    And(Vec<FilterInput>),
    Or(Vec<FilterInput>),
}

impl FilterInput {
    /// Construct a leaf field filter.
    pub fn field(field: impl Into<String>, op: FilterOp, value: impl ToString) -> Self {
        FilterInput::Field {
            field: field.into(),
            op,
            value: value.to_string(),
        }
    }

    /// Construct an AND composite filter.
    pub fn and(filters: Vec<FilterInput>) -> Self {
        FilterInput::And(filters)
    }

    /// Construct an OR composite filter.
    pub fn or(filters: Vec<FilterInput>) -> Self {
        FilterInput::Or(filters)
    }
}

// ---------------------------------------------------------------------------
// Internal conversion helpers
// ---------------------------------------------------------------------------

fn filter_op_to_proto(op: FilterOp) -> i32 {
    match op {
        FilterOp::Eq       => pb::FilterOp::Eq as i32,
        FilterOp::Neq      => pb::FilterOp::Neq as i32,
        FilterOp::Gt       => pb::FilterOp::Gt as i32,
        FilterOp::Gte      => pb::FilterOp::Gte as i32,
        FilterOp::Lt       => pb::FilterOp::Lt as i32,
        FilterOp::Lte      => pb::FilterOp::Lte as i32,
        FilterOp::Contains => pb::FilterOp::Contains as i32,
        FilterOp::Regex    => pb::FilterOp::Regex as i32,
    }
}

fn filter_to_proto(f: &FilterInput) -> pb::Filter {
    let kind = match f {
        FilterInput::Field { field, op, value } => {
            pb::filter::Kind::Field(pb::FieldFilter {
                field: field.clone(),
                op: filter_op_to_proto(*op),
                value: value.clone(),
            })
        }
        FilterInput::And(children) => {
            pb::filter::Kind::And(pb::AndFilter {
                filters: children.iter().map(filter_to_proto).collect(),
            })
        }
        FilterInput::Or(children) => {
            pb::filter::Kind::Or(pb::OrFilter {
                filters: children.iter().map(filter_to_proto).collect(),
            })
        }
    };
    pb::Filter { kind: Some(kind) }
}

fn json_to_value(val: serde_json::Value) -> prost_types::Value {
    let kind = match val {
        serde_json::Value::Null => prost_types::value::Kind::NullValue(0),
        serde_json::Value::Bool(b) => prost_types::value::Kind::BoolValue(b),
        serde_json::Value::Number(n) => {
            prost_types::value::Kind::NumberValue(n.as_f64().unwrap_or(0.0))
        }
        serde_json::Value::String(s) => prost_types::value::Kind::StringValue(s),
        serde_json::Value::Array(arr) => {
            prost_types::value::Kind::ListValue(prost_types::ListValue {
                values: arr.into_iter().map(json_to_value).collect(),
            })
        }
        serde_json::Value::Object(map) => {
            let fields = map
                .into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect();
            prost_types::value::Kind::StructValue(prost_types::Struct { fields })
        }
    };
    prost_types::Value { kind: Some(kind) }
}

fn json_to_struct(val: serde_json::Value) -> prost_types::Struct {
    if let serde_json::Value::Object(map) = val {
        prost_types::Struct {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
        }
    } else {
        prost_types::Struct::default()
    }
}

fn value_to_json(v: prost_types::Value) -> serde_json::Value {
    match v.kind {
        None => serde_json::Value::Null,
        Some(prost_types::value::Kind::NullValue(_)) => serde_json::Value::Null,
        Some(prost_types::value::Kind::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(prost_types::value::Kind::NumberValue(n)) => serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(prost_types::value::Kind::StringValue(s)) => serde_json::Value::String(s),
        Some(prost_types::value::Kind::ListValue(l)) => {
            serde_json::Value::Array(l.values.into_iter().map(value_to_json).collect())
        }
        Some(prost_types::value::Kind::StructValue(s)) => struct_to_json(s),
    }
}

fn struct_to_json(s: prost_types::Struct) -> serde_json::Value {
    serde_json::Value::Object(
        s.fields
            .into_iter()
            .map(|(k, v)| (k, value_to_json(v)))
            .collect(),
    )
}

fn proto_record_to_record(r: pb::Record) -> Record {
    Record {
        id: r.id,
        data: r.data.map(struct_to_json).unwrap_or(serde_json::Value::Object(Default::default())),
        date_added: r.date_added.map(timestamp_to_rfc3339),
        date_modified: r.date_modified.map(timestamp_to_rfc3339),
    }
}

fn timestamp_to_rfc3339(ts: prost_types::Timestamp) -> String {
    let secs = ts.seconds;
    let nanos = ts.nanos as u64;
    let (year, month, day, hour, min, sec) = epoch_secs_to_parts(secs);
    if nanos == 0 {
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", year, month, day, hour, min, sec)
    } else {
        let ms = nanos / 1_000_000;
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", year, month, day, hour, min, sec, ms)
    }
}

fn epoch_secs_to_parts(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let sec = secs.rem_euclid(60) as u32;
    let mins = secs.div_euclid(60);
    let min = mins.rem_euclid(60) as u32;
    let hours = mins.div_euclid(60);
    let hour = hours.rem_euclid(24) as u32;
    let mut days = hours.div_euclid(24);

    let mut year = 1970i32;
    loop {
        let diy = if is_leap_year(year) { 366 } else { 365 };
        if days < diy {
            break;
        }
        days -= diy;
        year += 1;
    }

    let month_days: [i64; 12] = [
        31, if is_leap_year(year) { 29 } else { 28 }, 31, 30, 31, 30,
        31, 31, 30, 31, 30, 31,
    ];
    let mut month = 1u32;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }

    (year, month, days as u32 + 1, hour, min, sec)
}

fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn watch_op_from_proto(op: i32) -> WatchOp {
    match pb::WatchOp::try_from(op) {
        Ok(pb::WatchOp::Inserted) => WatchOp::Inserted,
        Ok(pb::WatchOp::Updated) => WatchOp::Updated,
        Ok(pb::WatchOp::Deleted) => WatchOp::Deleted,
        _ => WatchOp::Unspecified,
    }
}

// ---------------------------------------------------------------------------
// FileDB client
// ---------------------------------------------------------------------------

/// Async gRPC client for FileDB v2.
///
/// Obtain an instance via [`FileDB::connect`] (plaintext) or
/// [`FileDB::connect_tls`] (TLS with CA certificate verification).
///
/// All RPC methods require `&mut self` because the underlying tonic channel
/// is driven through a mutable reference to the generated stub.
pub struct FileDB {
    client: FileDbClient<Channel>,
    api_key: String,
}

impl FileDB {
    /// Connect to a FileDB server over plaintext gRPC.
    pub async fn connect(host: &str, port: u16, api_key: &str) -> Result<Self, Error> {
        let endpoint = format!("http://{}:{}", host, port);
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        Ok(Self {
            client: FileDbClient::new(channel),
            api_key: api_key.to_owned(),
        })
    }

    /// Connect over TLS, verifying the server against the provided PEM CA certificate.
    pub async fn connect_tls(
        host: &str,
        port: u16,
        api_key: &str,
        ca_cert_pem: &[u8],
    ) -> Result<Self, Error> {
        let ca = Certificate::from_pem(ca_cert_pem);
        let tls = ClientTlsConfig::new()
            .ca_certificate(ca)
            .domain_name(host);
        let endpoint = format!("https://{}:{}", host, port);
        let channel = Channel::from_shared(endpoint)?
            .tls_config(tls)?
            .connect()
            .await?;
        Ok(Self {
            client: FileDbClient::new(channel),
            api_key: api_key.to_owned(),
        })
    }

    fn req<T>(&self, body: T) -> Request<T> {
        let mut req = Request::new(body);
        let val: MetadataValue<_> = self.api_key.parse().expect("api_key is a valid header value");
        req.metadata_mut().insert("x-api-key", val);
        req
    }

    // -------------------------------------------------------------------------
    // Collection management
    // -------------------------------------------------------------------------

    /// Create a new collection. Returns the collection name.
    pub async fn create_collection(&mut self, name: &str) -> Result<String, tonic::Status> {
        let resp = self
            .client
            .create_collection(self.req(pb::CreateCollectionRequest {
                name: name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.name)
    }

    /// Drop a collection and all its data. Returns `true` on success.
    pub async fn drop_collection(&mut self, name: &str) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .drop_collection(self.req(pb::DropCollectionRequest {
                name: name.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    /// List all collection names.
    pub async fn list_collections(&mut self) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .client
            .list_collections(self.req(pb::ListCollectionsRequest {}))
            .await?
            .into_inner();
        Ok(resp.names)
    }

    // -------------------------------------------------------------------------
    // CRUD
    // -------------------------------------------------------------------------

    /// Insert one record. Returns the assigned ID.
    pub async fn insert(
        &mut self,
        collection: &str,
        data: serde_json::Value,
    ) -> Result<u64, tonic::Status> {
        let resp = self
            .client
            .insert(self.req(pb::InsertRequest {
                collection: collection.to_owned(),
                data: Some(json_to_struct(data)),
            }))
            .await?
            .into_inner();
        Ok(resp.id)
    }

    /// Insert multiple records. Returns the assigned IDs in insertion order.
    pub async fn insert_many(
        &mut self,
        collection: &str,
        records: Vec<serde_json::Value>,
    ) -> Result<Vec<u64>, tonic::Status> {
        let resp = self
            .client
            .insert_many(self.req(pb::InsertManyRequest {
                collection: collection.to_owned(),
                records: records.into_iter().map(json_to_struct).collect(),
            }))
            .await?
            .into_inner();
        Ok(resp.ids)
    }

    /// Fetch a single record by ID.
    pub async fn find_by_id(
        &mut self,
        collection: &str,
        id: u64,
    ) -> Result<Record, tonic::Status> {
        let resp = self
            .client
            .find_by_id(self.req(pb::FindByIdRequest {
                collection: collection.to_owned(),
                id,
            }))
            .await?
            .into_inner();
        Ok(proto_record_to_record(resp.record.unwrap_or_default()))
    }

    /// Query records and collect them into a `Vec`.
    ///
    /// The server streams results one by one; this method buffers the full
    /// stream for convenience. Use [`FileDB::find_stream`] for large datasets.
    pub async fn find(
        &mut self,
        collection: &str,
        opts: FindOptions,
    ) -> Result<Vec<Record>, tonic::Status> {
        let mut stream = self.find_stream(collection, opts).await?;
        let mut records = Vec::new();
        while let Some(item) = stream.next().await {
            records.push(item?);
        }
        Ok(records)
    }

    /// Query records and return a server-side streaming [`Stream`].
    ///
    /// Prefer this over [`FileDB::find`] when the result set may be very large.
    pub async fn find_stream(
        &mut self,
        collection: &str,
        opts: FindOptions,
    ) -> Result<impl Stream<Item = Result<Record, tonic::Status>>, tonic::Status> {
        let streaming = self
            .client
            .find(self.req(pb::FindRequest {
                collection: collection.to_owned(),
                filter: opts.filter.as_ref().map(filter_to_proto),
                limit: opts.limit,
                offset: opts.offset,
                order_by: opts.order_by,
                descending: opts.descending,
            }))
            .await?
            .into_inner();

        Ok(streaming.map(|result| {
            result.map(|resp| proto_record_to_record(resp.record.unwrap_or_default()))
        }))
    }

    /// Update a record by ID. Returns the updated ID.
    pub async fn update(
        &mut self,
        collection: &str,
        id: u64,
        data: serde_json::Value,
    ) -> Result<u64, tonic::Status> {
        let resp = self
            .client
            .update(self.req(pb::UpdateRequest {
                collection: collection.to_owned(),
                id,
                data: Some(json_to_struct(data)),
            }))
            .await?
            .into_inner();
        Ok(resp.id)
    }

    /// Delete a record by ID. Returns `true` if the record existed.
    pub async fn delete(&mut self, collection: &str, id: u64) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .delete(self.req(pb::DeleteRequest {
                collection: collection.to_owned(),
                id,
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    // -------------------------------------------------------------------------
    // Secondary indexes
    // -------------------------------------------------------------------------

    /// Create a secondary index on a field (no-op if it already exists).
    pub async fn ensure_index(
        &mut self,
        collection: &str,
        field: &str,
    ) -> Result<(), tonic::Status> {
        self.client
            .ensure_index(self.req(pb::EnsureIndexRequest {
                collection: collection.to_owned(),
                field: field.to_owned(),
            }))
            .await?;
        Ok(())
    }

    /// Drop a secondary index. Returns `true` if the index existed.
    pub async fn drop_index(
        &mut self,
        collection: &str,
        field: &str,
    ) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .drop_index(self.req(pb::DropIndexRequest {
                collection: collection.to_owned(),
                field: field.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    /// List all indexed field names for a collection.
    pub async fn list_indexes(&mut self, collection: &str) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .client
            .list_indexes(self.req(pb::ListIndexesRequest {
                collection: collection.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.fields)
    }

    // -------------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------------

    /// Begin a transaction on a collection. Returns the transaction ID.
    pub async fn begin_tx(&mut self, collection: &str) -> Result<String, tonic::Status> {
        let resp = self
            .client
            .begin_tx(self.req(pb::BeginTxRequest {
                collection: collection.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.tx_id)
    }

    /// Commit a transaction. Returns `true` on success.
    pub async fn commit_tx(&mut self, tx_id: &str) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .commit_tx(self.req(pb::CommitTxRequest {
                tx_id: tx_id.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    /// Roll back a transaction. Returns `true` on success.
    pub async fn rollback_tx(&mut self, tx_id: &str) -> Result<bool, tonic::Status> {
        let resp = self
            .client
            .rollback_tx(self.req(pb::RollbackTxRequest {
                tx_id: tx_id.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    // -------------------------------------------------------------------------
    // Watch (server-streaming change feed)
    // -------------------------------------------------------------------------

    /// Subscribe to change events on a collection.
    ///
    /// Returns an async `Stream` of [`WatchEvent`]s. Iterate with
    /// `while let Some(event) = stream.next().await`.  Dropping the stream
    /// cancels the server-side subscription.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use futures::StreamExt;
    ///
    /// # async fn example(mut db: filedbv2::FileDB) -> Result<(), tonic::Status> {
    /// let mut events = db.watch("users", None).await?;
    /// while let Some(event) = events.next().await {
    ///     let event = event?;
    ///     println!("{:?} {:?}", event.op, event.record.id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn watch(
        &mut self,
        collection: &str,
        filter: Option<FilterInput>,
    ) -> Result<impl Stream<Item = Result<WatchEvent, tonic::Status>>, tonic::Status> {
        let streaming = self
            .client
            .watch(self.req(pb::WatchRequest {
                collection: collection.to_owned(),
                filter: filter.as_ref().map(filter_to_proto),
            }))
            .await?
            .into_inner();

        Ok(streaming.map(|result| {
            result.map(|event| WatchEvent {
                op: watch_op_from_proto(event.op),
                collection: event.collection,
                record: proto_record_to_record(event.record.unwrap_or_default()),
                ts: event.ts.map(timestamp_to_rfc3339),
            })
        }))
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    /// Return statistics for a collection.
    pub async fn stats(&mut self, collection: &str) -> Result<CollectionStats, tonic::Status> {
        let resp = self
            .client
            .collection_stats(self.req(pb::CollectionStatsRequest {
                collection: collection.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(CollectionStats {
            collection: resp.collection,
            record_count: resp.record_count,
            segment_count: resp.segment_count,
            dirty_entries: resp.dirty_entries,
            size_bytes: resp.size_bytes,
        })
    }
}
