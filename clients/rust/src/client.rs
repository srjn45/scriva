use futures::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Certificate};
use tonic::Request;

use crate::pb;
use crate::pb::scriva_client::ScrivaClient;

/// Transport / setup errors from [`ScrivaDB::connect`], [`ScrivaDB::connect_tls`]
/// and the file-writing snapshot helper. RPC methods return [`ScrivaDbError`].
pub type Error = Box<dyn std::error::Error + Send + Sync>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by ScrivaDB RPC methods.
///
/// [`NotFound`](ScrivaDbError::NotFound) and
/// [`AlreadyExists`](ScrivaDbError::AlreadyExists) are surfaced as dedicated
/// variants so keyed operations can be matched idiomatically: a keyed
/// lookup/update/delete against a key no live record holds yields `NotFound`,
/// and a keyed insert on a key already held by a live record yields
/// `AlreadyExists`. Every other gRPC status propagates as
/// [`Rpc`](ScrivaDbError::Rpc).
#[derive(Debug)]
pub enum ScrivaDbError {
    /// A keyed lookup/update/delete referenced a key no live record holds
    /// (gRPC `NOT_FOUND`).
    NotFound(tonic::Status),
    /// A keyed insert used a key already held by a live record (gRPC
    /// `ALREADY_EXISTS`).
    AlreadyExists(tonic::Status),
    /// Any other gRPC status.
    Rpc(tonic::Status),
}

impl ScrivaDbError {
    /// The underlying gRPC status.
    pub fn status(&self) -> &tonic::Status {
        match self {
            ScrivaDbError::NotFound(s)
            | ScrivaDbError::AlreadyExists(s)
            | ScrivaDbError::Rpc(s) => s,
        }
    }
}

impl From<tonic::Status> for ScrivaDbError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::NotFound => ScrivaDbError::NotFound(status),
            tonic::Code::AlreadyExists => ScrivaDbError::AlreadyExists(status),
            _ => ScrivaDbError::Rpc(status),
        }
    }
}

impl std::fmt::Display for ScrivaDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScrivaDbError::NotFound(s) => write!(f, "not found: {}", s.message()),
            ScrivaDbError::AlreadyExists(s) => write!(f, "already exists: {}", s.message()),
            ScrivaDbError::Rpc(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for ScrivaDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.status())
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A record returned by the server.
#[derive(Debug, Clone)]
pub struct Record {
    pub id: u64,
    /// Caller-supplied string primary key; empty for records inserted without one.
    pub key: String,
    /// Monotonic per-record revision, bumped on every write. Fresh records start
    /// at 1. Feed it to [`ScrivaDB::update_if_rev`] for optimistic-concurrency updates.
    pub rev: u64,
    pub data: serde_json::Value,
    pub date_added: Option<String>,
    pub date_modified: Option<String>,
}

/// One sort key and its direction, for the multi-field ordering of a `find`.
///
/// Several may be supplied via [`FindOptions::order_by_fields`]; they are applied
/// in order as a lexicographic sort. The record id is always the final tiebreaker,
/// so the ordering is total and a keyset cursor ([`FindOptions::page_token`]) is
/// stable.
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub field: String,
    /// `false` = ascending, `true` = descending.
    pub desc: bool,
}

impl OrderBy {
    /// Sort ascending by `field`.
    pub fn asc(field: impl Into<String>) -> Self {
        OrderBy { field: field.into(), desc: false }
    }

    /// Sort descending by `field`.
    pub fn desc(field: impl Into<String>) -> Self {
        OrderBy { field: field.into(), desc: true }
    }
}

/// Options for the `find` methods.
#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    pub filter: Option<FilterInput>,
    pub limit: u32,
    pub offset: u32,
    /// Deprecated single-field sort. Superseded by [`order_by_fields`](Self::order_by_fields):
    /// honoured only when `order_by_fields` is empty.
    pub order_by: String,
    /// Deprecated: direction for the single-field [`order_by`](Self::order_by).
    pub descending: bool,
    /// Multi-field, per-field-directional sort (N3). When non-empty it supersedes
    /// the deprecated scalar `order_by`/`descending`.
    pub order_by_fields: Vec<OrderBy>,
    /// Optional field projection (N2): when non-empty, only these top-level fields
    /// are returned in each record's `data` (`id`, `key` and `rev` are always
    /// included). Empty returns full records; an unknown field is silently omitted.
    pub fields: Vec<String>,
    /// Opaque keyset pagination token (N3). Empty requests the first page;
    /// otherwise it must be a token returned by a previous [`ScrivaDB::find_page`].
    /// Only meaningful with an ordering — pass the same ordering, filter and limit
    /// on every page.
    pub page_token: String,
}

/// Result of a keyed update ([`ScrivaDB::update_by_key`]).
#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub id: u64,
    /// Caller-supplied string key preserved by the update (empty for a keyless one).
    pub key: String,
    /// The record's revision after the write.
    pub rev: u64,
    pub date_modified: Option<String>,
}

/// Result of a compare-and-swap update ([`ScrivaDB::update_if_rev`]).
#[derive(Debug, Clone)]
pub struct CasResult {
    /// `true` when `expected_rev` matched and the update applied; `false` — never
    /// an error — when the revision was stale or no live record carries the key.
    pub swapped: bool,
    /// The resulting record when `swapped` is `true`; `None` otherwise.
    pub record: Option<Record>,
}

/// A numeric aggregation to compute per group over a request's `field`.
///
/// `Count` (the per-group record count) is always returned and need not be listed.
/// Any of `Sum`/`Avg`/`Min`/`Max` requires [`AggregateOptions::field`] to be set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Options for [`ScrivaDB::aggregate`].
#[derive(Debug, Clone, Default)]
pub struct AggregateOptions {
    /// Which aggregations to compute. `Count` is always returned even if omitted;
    /// an empty list yields count-only. `Sum`/`Avg`/`Min`/`Max` require `field`.
    pub aggregations: Vec<AggregateOp>,
    /// Numeric field for `Sum`/`Avg`/`Min`/`Max`; ignored for a pure count.
    pub field: String,
    /// Optional group-by field — one result per distinct value. Empty aggregates
    /// the whole filtered set into a single group whose `group` is `Null`.
    pub group_by: String,
    /// The same composable filter as `find`. Empty aggregates the whole collection.
    pub filter: Option<FilterInput>,
}

/// One group's result from [`ScrivaDB::aggregate`].
#[derive(Debug, Clone)]
pub struct AggregateGroup {
    /// The group-by field's value for this group, type-preserved. `Null` for the
    /// whole-set group and for records missing the group field.
    pub group: serde_json::Value,
    /// Number of records in this group (post-filter).
    pub count: u64,
    /// `true` when at least one record in the group carried a numeric `field`.
    /// When `false`, `sum`/`avg`/`min`/`max` are zero and meaningless.
    pub numeric: bool,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
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
    /// The server dropped events because this subscriber fell behind; the
    /// client missed writes and should resync. No record accompanies it.
    Overflow,
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
/// use scriva::{FilterInput, FilterOp};
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

fn agg_op_to_proto(op: AggregateOp) -> i32 {
    match op {
        AggregateOp::Count => pb::AggregateOp::AggCount as i32,
        AggregateOp::Sum   => pb::AggregateOp::AggSum as i32,
        AggregateOp::Avg   => pb::AggregateOp::AggAvg as i32,
        AggregateOp::Min   => pb::AggregateOp::AggMin as i32,
        AggregateOp::Max   => pb::AggregateOp::AggMax as i32,
    }
}

fn order_by_to_proto(o: &OrderBy) -> pb::OrderBy {
    pb::OrderBy { field: o.field.clone(), desc: o.desc }
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
        key: r.key,
        rev: r.rev,
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
        Ok(pb::WatchOp::Overflow) => WatchOp::Overflow,
        _ => WatchOp::Unspecified,
    }
}

fn opt_string(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

// ---------------------------------------------------------------------------
// ScrivaDB client
// ---------------------------------------------------------------------------

/// Async gRPC client for ScrivaDB.
///
/// Obtain an instance via [`ScrivaDB::connect`] (plaintext) or
/// [`ScrivaDB::connect_tls`] (TLS with CA certificate verification).
///
/// All RPC methods require `&mut self` because the underlying tonic channel
/// is driven through a mutable reference to the generated stub, and return
/// [`ScrivaDbError`] (with dedicated `NotFound` / `AlreadyExists` variants for
/// keyed operations).
pub struct ScrivaDB {
    client: ScrivaClient<Channel>,
    api_key: String,
}

impl ScrivaDB {
    /// Connect to a ScrivaDB server over plaintext gRPC.
    pub async fn connect(host: &str, port: u16, api_key: &str) -> Result<Self, Error> {
        let endpoint = format!("http://{}:{}", host, port);
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        Ok(Self {
            client: ScrivaClient::new(channel),
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
            client: ScrivaClient::new(channel),
            api_key: api_key.to_owned(),
        })
    }

    fn req<T>(&self, body: T) -> Request<T> {
        let mut req = Request::new(body);
        let val: MetadataValue<_> = self.api_key.parse().expect("api_key is a valid header value");
        req.metadata_mut().insert("x-api-key", val);
        req
    }

    fn build_find_request(&self, collection: &str, opts: FindOptions) -> pb::FindRequest {
        // order_by / descending are the deprecated single-field sort, still
        // supported for back-compat (honoured only when order_by_fields is empty).
        #[allow(deprecated)]
        pb::FindRequest {
            collection: collection.to_owned(),
            filter: opts.filter.as_ref().map(filter_to_proto),
            limit: opts.limit,
            offset: opts.offset,
            order_by: opts.order_by,
            descending: opts.descending,
            fields: opts.fields,
            page_token: opts.page_token,
            order_by_fields: opts.order_by_fields.iter().map(order_by_to_proto).collect(),
        }
    }

    // -------------------------------------------------------------------------
    // Collection management
    // -------------------------------------------------------------------------

    /// Create a new collection. Returns the collection name.
    pub async fn create_collection(&mut self, name: &str) -> Result<String, ScrivaDbError> {
        self.create_collection_with_ttl(name, 0).await
    }

    /// Create a new collection with a default per-record TTL, in seconds.
    ///
    /// When `default_ttl_seconds > 0`, records inserted into this collection
    /// without an explicit TTL expire that long after being written; the value
    /// is persisted per-collection and overrides the server-wide default. `0`
    /// inherits the server-wide default. Returns the collection name.
    pub async fn create_collection_with_ttl(
        &mut self,
        name: &str,
        default_ttl_seconds: i64,
    ) -> Result<String, ScrivaDbError> {
        let resp = self
            .client
            .create_collection(self.req(pb::CreateCollectionRequest {
                name: name.to_owned(),
                default_ttl_seconds,
            }))
            .await?
            .into_inner();
        Ok(resp.name)
    }

    /// Drop a collection and all its data. Returns `true` on success.
    pub async fn drop_collection(&mut self, name: &str) -> Result<bool, ScrivaDbError> {
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
    pub async fn list_collections(&mut self) -> Result<Vec<String>, ScrivaDbError> {
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
    ) -> Result<u64, ScrivaDbError> {
        self.insert_inner(collection, data, 0, "").await
    }

    /// Insert one record with a per-record TTL, in seconds.
    ///
    /// When `ttl_seconds > 0`, the record expires that long after insertion,
    /// overriding the collection default. `0` applies the collection's default
    /// TTL, if any. Returns the assigned ID.
    pub async fn insert_with_ttl(
        &mut self,
        collection: &str,
        data: serde_json::Value,
        ttl_seconds: i64,
    ) -> Result<u64, ScrivaDbError> {
        self.insert_inner(collection, data, ttl_seconds, "").await
    }

    /// Insert one record under a caller-supplied string primary `key` (keyed
    /// create). Returns the assigned ID.
    ///
    /// A key already held by a live record is rejected with
    /// [`ScrivaDbError::AlreadyExists`]. A keyed insert does not participate in
    /// transactions or per-record TTL.
    pub async fn insert_with_key(
        &mut self,
        collection: &str,
        data: serde_json::Value,
        key: &str,
    ) -> Result<u64, ScrivaDbError> {
        self.insert_inner(collection, data, 0, key).await
    }

    async fn insert_inner(
        &mut self,
        collection: &str,
        data: serde_json::Value,
        ttl_seconds: i64,
        key: &str,
    ) -> Result<u64, ScrivaDbError> {
        let resp = self
            .client
            .insert(self.req(pb::InsertRequest {
                collection: collection.to_owned(),
                data: Some(json_to_struct(data)),
                ttl_seconds,
                key: key.to_owned(),
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
    ) -> Result<Vec<u64>, ScrivaDbError> {
        self.insert_many_with_ttl(collection, records, 0).await
    }

    /// Insert multiple records with a per-record TTL applied to every record in
    /// the batch. Same semantics as [`ScrivaDB::insert_with_ttl`]. Returns the
    /// assigned IDs in insertion order.
    pub async fn insert_many_with_ttl(
        &mut self,
        collection: &str,
        records: Vec<serde_json::Value>,
        ttl_seconds: i64,
    ) -> Result<Vec<u64>, ScrivaDbError> {
        let resp = self
            .client
            .insert_many(self.req(pb::InsertManyRequest {
                collection: collection.to_owned(),
                records: records.into_iter().map(json_to_struct).collect(),
                ttl_seconds,
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
    ) -> Result<Record, ScrivaDbError> {
        self.find_by_id_with_fields(collection, id, &[]).await
    }

    /// Fetch a single record by ID, projecting `data` to `fields` (N2).
    ///
    /// When `fields` is non-empty, only those top-level fields are returned in
    /// the record's `data`; `id`, `key` and `rev` are always included. An empty
    /// slice returns the full record.
    pub async fn find_by_id_with_fields(
        &mut self,
        collection: &str,
        id: u64,
        fields: &[&str],
    ) -> Result<Record, ScrivaDbError> {
        let resp = self
            .client
            .find_by_id(self.req(pb::FindByIdRequest {
                collection: collection.to_owned(),
                id,
                fields: fields.iter().map(|s| s.to_string()).collect(),
            }))
            .await?
            .into_inner();
        Ok(proto_record_to_record(resp.record.unwrap_or_default()))
    }

    /// Query records and collect them into a `Vec`.
    ///
    /// The server streams results one by one; this method buffers the full
    /// stream for convenience. Use [`ScrivaDB::find_stream`] for large datasets,
    /// or [`ScrivaDB::find_page`] to also receive the next-page cursor.
    pub async fn find(
        &mut self,
        collection: &str,
        opts: FindOptions,
    ) -> Result<Vec<Record>, ScrivaDbError> {
        let (records, _) = self.find_page(collection, opts).await?;
        Ok(records)
    }

    /// Query one keyset page, returning `(records, next_page_token)` (N3).
    ///
    /// Pass an ordering ([`FindOptions::order_by_fields`]) and a `limit`, then
    /// feed the returned token back as [`FindOptions::page_token`] on the next
    /// call to walk the collection page by page in O(page) time. An empty
    /// returned token means the last page was reached. Keep the same filter,
    /// ordering and limit on every page.
    pub async fn find_page(
        &mut self,
        collection: &str,
        opts: FindOptions,
    ) -> Result<(Vec<Record>, String), ScrivaDbError> {
        let request = self.req(self.build_find_request(collection, opts));
        let mut streaming = self.client.find(request).await?.into_inner();
        let mut records = Vec::new();
        let mut next_token = String::new();
        while let Some(resp) = streaming.message().await? {
            records.push(proto_record_to_record(resp.record.unwrap_or_default()));
            if !resp.page_token.is_empty() {
                next_token = resp.page_token;
            }
        }
        Ok((records, next_token))
    }

    /// Query records and return a server-side streaming [`Stream`].
    ///
    /// Prefer this over [`ScrivaDB::find`] when the result set may be very large.
    /// The keyset page token is not surfaced by the streaming variant — use
    /// [`ScrivaDB::find_page`] when you need to paginate.
    pub async fn find_stream(
        &mut self,
        collection: &str,
        opts: FindOptions,
    ) -> Result<impl Stream<Item = Result<Record, ScrivaDbError>>, ScrivaDbError> {
        let request = self.req(self.build_find_request(collection, opts));
        let streaming = self.client.find(request).await?.into_inner();

        Ok(streaming.map(|result| {
            result
                .map(|resp| proto_record_to_record(resp.record.unwrap_or_default()))
                .map_err(ScrivaDbError::from)
        }))
    }

    /// Update a record by ID. Returns the updated ID.
    pub async fn update(
        &mut self,
        collection: &str,
        id: u64,
        data: serde_json::Value,
    ) -> Result<u64, ScrivaDbError> {
        self.update_with_ttl(collection, id, data, 0).await
    }

    /// Update a record by ID, resetting its TTL, in seconds.
    ///
    /// When `ttl_seconds > 0`, the record's deadline is reset to that long from
    /// now, overriding the collection default. `0` is sticky — it re-applies
    /// the collection default TTL and leaves an existing deadline untouched.
    /// Returns the updated ID.
    pub async fn update_with_ttl(
        &mut self,
        collection: &str,
        id: u64,
        data: serde_json::Value,
        ttl_seconds: i64,
    ) -> Result<u64, ScrivaDbError> {
        let resp = self
            .client
            .update(self.req(pb::UpdateRequest {
                collection: collection.to_owned(),
                id,
                data: Some(json_to_struct(data)),
                ttl_seconds,
            }))
            .await?
            .into_inner();
        Ok(resp.id)
    }

    /// Delete a record by ID. Returns `true` if the record existed.
    pub async fn delete(&mut self, collection: &str, id: u64) -> Result<bool, ScrivaDbError> {
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
    // Keyed CRUD, upsert & compare-and-swap (N1)
    // -------------------------------------------------------------------------

    /// Insert `data` under `key`, or replace the existing keyed record — atomically.
    ///
    /// If no live record carries `key` it is inserted; otherwise the existing
    /// record's data is replaced and its `rev` incremented. Returns the resulting
    /// [`Record`] (including its `key` and `rev`).
    pub async fn upsert(
        &mut self,
        collection: &str,
        key: &str,
        data: serde_json::Value,
    ) -> Result<Record, ScrivaDbError> {
        let resp = self
            .client
            .upsert(self.req(pb::UpsertRequest {
                collection: collection.to_owned(),
                key: key.to_owned(),
                data: Some(json_to_struct(data)),
            }))
            .await?
            .into_inner();
        Ok(proto_record_to_record(resp.record.unwrap_or_default()))
    }

    /// Fetch the record carrying `key`. Returns [`ScrivaDbError::NotFound`] if none.
    pub async fn find_by_key(
        &mut self,
        collection: &str,
        key: &str,
    ) -> Result<Record, ScrivaDbError> {
        self.find_by_key_with_fields(collection, key, &[]).await
    }

    /// Fetch the record carrying `key`, projecting `data` to `fields` (N2).
    ///
    /// `id`, `key` and `rev` are always included. An empty slice returns the full
    /// record. Returns [`ScrivaDbError::NotFound`] if no live record carries `key`.
    pub async fn find_by_key_with_fields(
        &mut self,
        collection: &str,
        key: &str,
        fields: &[&str],
    ) -> Result<Record, ScrivaDbError> {
        let resp = self
            .client
            .find_by_key(self.req(pb::FindByKeyRequest {
                collection: collection.to_owned(),
                key: key.to_owned(),
                fields: fields.iter().map(|s| s.to_string()).collect(),
            }))
            .await?
            .into_inner();
        Ok(proto_record_to_record(resp.record.unwrap_or_default()))
    }

    /// Overwrite the record carrying `key`, preserving the key itself.
    ///
    /// Returns [`ScrivaDbError::NotFound`] if no live record carries `key`.
    pub async fn update_by_key(
        &mut self,
        collection: &str,
        key: &str,
        data: serde_json::Value,
    ) -> Result<UpdateResult, ScrivaDbError> {
        let resp = self
            .client
            .update_by_key(self.req(pb::UpdateByKeyRequest {
                collection: collection.to_owned(),
                key: key.to_owned(),
                data: Some(json_to_struct(data)),
            }))
            .await?
            .into_inner();
        Ok(UpdateResult {
            id: resp.id,
            key: resp.key,
            rev: resp.rev,
            date_modified: opt_string(resp.date_modified),
        })
    }

    /// Delete the record carrying `key`. Returns `true` on success.
    ///
    /// Returns [`ScrivaDbError::NotFound`] if no live record carries `key`.
    pub async fn delete_by_key(
        &mut self,
        collection: &str,
        key: &str,
    ) -> Result<bool, ScrivaDbError> {
        let resp = self
            .client
            .delete_by_key(self.req(pb::DeleteByKeyRequest {
                collection: collection.to_owned(),
                key: key.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    /// Compare-and-swap update on `key`, conditional on `expected_rev`.
    ///
    /// The write is applied only if the record's current `rev` equals
    /// `expected_rev`. A stale revision (or a missing key) is a clean no-op —
    /// never an error — reported as [`CasResult::swapped`] `= false`. The
    /// resulting record is returned only when the swap applied.
    pub async fn update_if_rev(
        &mut self,
        collection: &str,
        key: &str,
        expected_rev: u64,
        data: serde_json::Value,
    ) -> Result<CasResult, ScrivaDbError> {
        let resp = self
            .client
            .update_if_rev(self.req(pb::UpdateIfRevRequest {
                collection: collection.to_owned(),
                key: key.to_owned(),
                expected_rev,
                data: Some(json_to_struct(data)),
            }))
            .await?
            .into_inner();
        let record = if resp.swapped {
            resp.record.map(proto_record_to_record)
        } else {
            None
        };
        Ok(CasResult {
            swapped: resp.swapped,
            record,
        })
    }

    // -------------------------------------------------------------------------
    // Secondary indexes
    // -------------------------------------------------------------------------

    /// Create a secondary index on a field (no-op if it already exists).
    pub async fn ensure_index(
        &mut self,
        collection: &str,
        field: &str,
    ) -> Result<(), ScrivaDbError> {
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
    ) -> Result<bool, ScrivaDbError> {
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
    pub async fn list_indexes(&mut self, collection: &str) -> Result<Vec<String>, ScrivaDbError> {
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
    pub async fn begin_tx(&mut self, collection: &str) -> Result<String, ScrivaDbError> {
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
    pub async fn commit_tx(&mut self, tx_id: &str) -> Result<bool, ScrivaDbError> {
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
    pub async fn rollback_tx(&mut self, tx_id: &str) -> Result<bool, ScrivaDbError> {
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
    /// # async fn example(mut db: scriva::ScrivaDB) -> Result<(), scriva::ScrivaDbError> {
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
    ) -> Result<impl Stream<Item = Result<WatchEvent, ScrivaDbError>>, ScrivaDbError> {
        let streaming = self
            .client
            .watch(self.req(pb::WatchRequest {
                collection: collection.to_owned(),
                filter: filter.as_ref().map(filter_to_proto),
            }))
            .await?
            .into_inner();

        Ok(streaming.map(|result| {
            result
                .map(|event| WatchEvent {
                    op: watch_op_from_proto(event.op),
                    collection: event.collection,
                    record: proto_record_to_record(event.record.unwrap_or_default()),
                    ts: event.ts.map(timestamp_to_rfc3339),
                })
                .map_err(ScrivaDbError::from)
        }))
    }

    // -------------------------------------------------------------------------
    // Aggregations (N4)
    // -------------------------------------------------------------------------

    /// Compute count + numeric aggregations over the filtered live records.
    ///
    /// The `Aggregate` RPC is server-streaming — one message per group; this
    /// collects them into a `Vec`. Each [`AggregateGroup`] carries its `group`
    /// value (`Null` for the whole-set group), `count`, and — when the group had
    /// at least one numeric `field` value ([`AggregateGroup::numeric`]) — the
    /// `sum`/`avg`/`min`/`max`.
    pub async fn aggregate(
        &mut self,
        collection: &str,
        opts: AggregateOptions,
    ) -> Result<Vec<AggregateGroup>, ScrivaDbError> {
        let request = self.req(pb::AggregateRequest {
            collection: collection.to_owned(),
            filter: opts.filter.as_ref().map(filter_to_proto),
            group_by: opts.group_by,
            field: opts.field,
            aggregations: opts.aggregations.iter().map(|o| agg_op_to_proto(*o)).collect(),
        });
        let mut streaming = self.client.aggregate(request).await?.into_inner();
        let mut out = Vec::new();
        while let Some(resp) = streaming.message().await? {
            out.push(AggregateGroup {
                group: resp.group_value.map(value_to_json).unwrap_or(serde_json::Value::Null),
                count: resp.count,
                numeric: resp.numeric,
                sum: resp.sum,
                avg: resp.avg,
                min: resp.min,
                max: resp.max,
            });
        }
        Ok(out)
    }

    /// Count the live records matching `filter` (or all records when `None`).
    pub async fn count(
        &mut self,
        collection: &str,
        filter: Option<FilterInput>,
    ) -> Result<u64, ScrivaDbError> {
        let groups = self
            .aggregate(collection, AggregateOptions { filter, ..Default::default() })
            .await?;
        Ok(groups.first().map(|g| g.count).unwrap_or(0))
    }

    /// Group live records by `field` and aggregate each group.
    ///
    /// Convenience wrapper over [`ScrivaDB::aggregate`]: `field` is the group-by
    /// field, `metric` the numeric field for `sum`/`avg`/`min`/`max` (request
    /// those via `aggregations`). Returns one [`AggregateGroup`] per distinct
    /// group value.
    pub async fn group_by(
        &mut self,
        collection: &str,
        field: &str,
        aggregations: Vec<AggregateOp>,
        metric: &str,
        filter: Option<FilterInput>,
    ) -> Result<Vec<AggregateGroup>, ScrivaDbError> {
        self.aggregate(
            collection,
            AggregateOptions {
                aggregations,
                field: metric.to_owned(),
                group_by: field.to_owned(),
                filter,
            },
        )
        .await
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    /// Return statistics for a collection.
    pub async fn stats(&mut self, collection: &str) -> Result<CollectionStats, ScrivaDbError> {
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

    // -------------------------------------------------------------------------
    // Maintenance
    // -------------------------------------------------------------------------

    /// Run a forced, synchronous compaction pass on a collection.
    ///
    /// Merges dirty segments and reclaims space from deleted or overwritten
    /// records. Returns only after the pass completes; `true` on success.
    pub async fn compact(&mut self, collection: &str) -> Result<bool, ScrivaDbError> {
        let resp = self
            .client
            .compact(self.req(pb::CompactRequest {
                collection: collection.to_owned(),
            }))
            .await?
            .into_inner();
        Ok(resp.ok)
    }

    /// Stream a consistent, gzip-compressed tar snapshot of the whole database.
    ///
    /// Returns an async `Stream` yielding the raw archive bytes chunk by chunk.
    /// Concatenate the chunks to reconstruct the `.tar.gz`; restore by
    /// extracting it into a data directory. See [`ScrivaDB::snapshot_to_file`] for
    /// a convenience wrapper that writes straight to disk.
    pub async fn snapshot(
        &mut self,
    ) -> Result<impl Stream<Item = Result<Vec<u8>, ScrivaDbError>>, ScrivaDbError> {
        let streaming = self
            .client
            .snapshot(self.req(pb::SnapshotRequest {}))
            .await?
            .into_inner();

        Ok(streaming.map(|result| result.map(|chunk| chunk.data).map_err(ScrivaDbError::from)))
    }

    /// Stream a database snapshot straight to a file at `path`.
    ///
    /// Writes the gzip-compressed tar archive chunk by chunk and returns the
    /// total number of bytes written. Restore with `tar xzf <path>`.
    pub async fn snapshot_to_file(
        &mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<u64, Error> {
        use tokio::io::AsyncWriteExt;

        let mut stream = self.snapshot().await?;
        let mut file = tokio::fs::File::create(path).await?;
        let mut total: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            total += chunk.len() as u64;
        }
        file.flush().await?;
        Ok(total)
    }
}
