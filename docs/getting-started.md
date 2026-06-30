# Getting Started

## Install

### Option 1: Download binary

```bash
# Linux amd64
curl -L https://github.com/srjn45/filedbv2/releases/latest/download/filedbv2_linux_amd64.tar.gz | tar xz
sudo mv filedb filedb-cli /usr/local/bin/
```

### Option 2: Docker

```bash
docker run -d \
  -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e FILEDB_API_KEY=my-secret-key \
  ghcr.io/srjn45/filedbv2:latest
```

### Option 3: Build from source

```bash
git clone https://github.com/srjn45/filedbv2
cd filedbv2
make build
# binaries: bin/filedb and bin/filedb-cli
```

---

## Start the server

```bash
filedb serve \
  --data ./data \
  --api-key my-secret-key \
  --grpc-addr :5433 \
  --rest-addr :8080
```

Environment variable alternative:

```bash
export FILEDB_API_KEY=my-secret-key
filedb serve --data ./data
```

All flags and their defaults:

| Flag | Default | Description |
|---|---|---|
| `--data` | `./data` | Data directory |
| `--grpc-addr` | `:5433` | TCP gRPC listen address |
| `--rest-addr` | `:8080` | REST gateway listen address |
| `--socket` | `/tmp/filedb.sock` | Unix domain socket path |
| `--api-key` | `$FILEDB_API_KEY` | API key (empty = no auth) |
| `--metrics-addr` | `:9090` | Prometheus metrics address (empty = disabled) |
| `--tls-cert` | *(none)* | Path to TLS certificate PEM file |
| `--tls-key` | *(none)* | Path to TLS private key PEM file |
| `--segment-size` | `4194304` | Max segment file size in bytes (4 MiB) |
| `--compact-interval` | `5m` | Compaction interval |
| `--compact-dirty` | `0.30` | Dirty-ratio threshold to trigger compaction |
| `--sync` | `none` | Durability mode: `none`, `always`, or `interval` |
| `--sync-interval` | `1s` | Flush cadence when `--sync=interval` |
| `--tx-timeout` | `5m` | Idle timeout before an open transaction is reaped (`0` = disabled) |
| `--config` | *(none)* | Path to YAML config file |

---

## YAML config file

You can put all server options in a YAML file and load it with `--config`:

```yaml
# filedb.yaml
data_dir: ./data
grpc_addr: :5433
rest_addr: :8080
unix_socket: /tmp/filedb.sock
api_key: my-secret-key
metrics_addr: :9090
segment_max_size: 4194304   # 4 MiB
compact_interval: 5m
compact_dirty_pct: 0.30
sync_mode: none             # none | always | interval
sync_interval: 1s           # used when sync_mode: interval
tx_timeout: 5m              # reap transactions idle longer than this (0 = disabled)
# tls_cert: /etc/filedb/cert.pem
# tls_key:  /etc/filedb/key.pem
```

```bash
filedb serve --config filedb.yaml
```

CLI flags always override the config file. Omitted keys fall back to defaults.

---

## Durability

FileDB lets you trade write throughput against how much you can lose on a crash:

```bash
filedb serve --data ./data --sync none        # fastest; OS decides when to flush
filedb serve --data ./data --sync interval --sync-interval 1s  # lose ≤ 1s on power loss
filedb serve --data ./data --sync always       # fsync every write; lose nothing acknowledged
```

| Mode | What it does | You can lose | Speed |
|---|---|---|---|
| `none` (default) | No explicit fsync; relies on OS page-cache flush | All un-flushed writes on power loss | Fastest |
| `interval` | Background fsync every `--sync-interval` | At most one interval | Fast |
| `always` | fsync before acknowledging each write | Nothing acknowledged | Slowest |

Note: `none` is **not** lossless — partial-line recovery on restart fixes torn
writes, but a write acknowledged under `none` can still vanish if power is lost
before the OS flushes. Use `interval` or `always` when that matters. See
[architecture.md](architecture.md#durability) for details. Benchmark the
trade-off on your own hardware with `make bench`.

---

## Use the CLI

### Interactive REPL

```bash
export FILEDB_API_KEY=my-secret-key
filedb-cli repl

filedb> create-collection users
filedb> use users
filedb:users> insert {"name":"alice","age":30}
→ inserted id:1

filedb:users> find
→ id:1  {"name":"alice","age":30}

filedb:users> find {"field":"name","op":"eq","value":"alice"}
→ id:1  {"name":"alice","age":30}

filedb:users> update 1 {"name":"alice","age":31}
→ updated id:1

filedb:users> stats
→ collection:users  records:1  segments:1  dirty:0  size:89 bytes

filedb:users> delete 1
→ deleted id:1
```

### One-shot commands

```bash
filedb-cli create-collection products
filedb-cli insert products '{"name":"widget","price":9.99}'
filedb-cli find products '{"field":"price","op":"lte","value":"10.00"}'
filedb-cli get products 1
filedb-cli update products 1 '{"name":"widget","price":8.99}'
filedb-cli delete products 1
filedb-cli stats products
```

### Batch script (.fql)

```bash
# seed.fql
# Create users collection and seed data
create-collection users
use users
insert {"name":"alice","email":"alice@example.com"}
insert {"name":"bob","email":"bob@example.com"}
insert {"name":"carol","email":"carol@example.com"}
```

```bash
filedb-cli run seed.fql
# or via pipe:
cat seed.fql | filedb-cli run
```

### Export / Import

```bash
# Export to NDJSON
filedb-cli export users > users_backup.ndjson

# Import from NDJSON
cat users_backup.ndjson | filedb-cli import users
```

### CLI connection flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `localhost:5433` | gRPC server address |
| `--socket` | `/tmp/filedb.sock` | Unix socket path (used automatically when the file exists) |
| `--api-key` | `$FILEDB_API_KEY` | API key |
| `--tls-ca` | *(none)* | Path to CA certificate PEM for TLS (enables TLS on TCP) |

---

## Use via REST API

```bash
# Create collection
curl -X POST http://localhost:8080/v1/collections \
  -H "x-api-key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"name":"users"}'

# Insert
curl -X POST http://localhost:8080/v1/users/records \
  -H "x-api-key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"data":{"name":"alice","age":30}}'

# Get by id
curl http://localhost:8080/v1/users/records/1 \
  -H "x-api-key: my-secret-key"

# Update
curl -X PUT http://localhost:8080/v1/users/records/1 \
  -H "x-api-key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"data":{"name":"alice","age":31}}'

# Delete
curl -X DELETE http://localhost:8080/v1/users/records/1 \
  -H "x-api-key: my-secret-key"
```

---

## OpenAPI spec (use from any language)

The REST gateway is described by an OpenAPI (Swagger 2.0) spec generated from the
proto, checked in at [`docs/openapi/filedb.swagger.json`](openapi/filedb.swagger.json).
Regenerate it after changing `proto/filedb.proto`:

```bash
make openapi   # requires the buf CLI
```

Because every operation is in the spec, you can generate a typed client for almost
any language without hand-writing one — e.g. with
[openapi-generator](https://openapi-generator.tech/):

```bash
openapi-generator generate \
  -i docs/openapi/filedb.swagger.json \
  -g python \
  -o clients/python-generated
```

This is the quickest path to language coverage; the hand-written SDKs under
`clients/` (Python, JavaScript/TypeScript, PHP, Java, Ruby, Rust, and C#) exist
where a more ergonomic, idiomatic wrapper is worth the maintenance — see the
[Client SDKs](#client-sdks) reference below.

---

## Filter syntax

Filters are JSON objects passed to `find` commands or the `POST /v1/{collection}/records/find` endpoint.

### Field filter

```json
{"field": "name", "op": "eq",       "value": "alice"}
{"field": "age",  "op": "gte",      "value": 18}
{"field": "bio",  "op": "contains", "value": "engineer"}
{"field": "email","op": "regex",    "value": ".*@gmail\\.com"}
```

Supported operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `regex`

### Comparison semantics: numeric vs. string

The comparison operators (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`) are
**type-aware**. The JSON type of `value` — and the type of the stored field —
decides how two values are ordered:

| Stored field | `value`              | Comparison    | Example                                    |
|--------------|----------------------|---------------|--------------------------------------------|
| number       | number (e.g. `9`)    | numeric       | `age gt 9` matches `age = 10` (10 > 9)     |
| string       | string (e.g. `"m"`)  | lexicographic | `name gt "m"` matches `name = "n"`         |
| number       | string, or vice-versa| lexicographic on the string forms (cross-type comparisons are a query mistake; the result is deterministic but rarely meaningful) |

Key points:

- **Pass numbers as JSON numbers, not strings.** Write `"value": 9`, not
  `"value": "9"`. A number compared against the JSON number `9` is ordered
  numerically, so `gt 9` correctly matches `10`. If you quote it as `"9"`,
  it becomes a string and the comparison falls back to lexicographic ordering,
  where `"10" < "9"`.
- **Numeric-looking strings stay strings.** A field stored as the string
  `"10"` is *not* coerced to a number; it keeps lexicographic ordering, so
  `"10" < "9"`. This keeps string fields (zero-padded codes, IDs, versions)
  predictable.
- `contains` and `regex` always operate on the string form of the field value.

### Composite filters

```json
{
  "and": [
    {"field": "age",  "op": "gte", "value": 18},
    {"field": "city", "op": "eq",  "value": "New York"}
  ]
}
```

```json
{
  "or": [
    {"field": "role", "op": "eq", "value": "admin"},
    {"field": "role", "op": "eq", "value": "superuser"}
  ]
}
```

---

## Secondary indexes

Secondary indexes accelerate equality lookups on any field from O(n) full scan to O(1).

```bash
# Create an index on the "email" field
filedb-cli ensure-index users email

# List indexes on a collection
filedb-cli indexes users

# Drop an index
filedb-cli drop-index users email
```

Once an index exists, `find` with a single `eq` filter on that field uses the index automatically — no query hint needed.

Indexes are:
- Persisted to `sidx_<field>.json` (SHA-256 checksummed) and reloaded on startup
- Maintained automatically on every insert, update, and delete
- Rebuilt transparently after compaction

Via REST:

```bash
# Ensure index
curl -X POST http://localhost:8080/v1/users/indexes \
  -H "x-api-key: my-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"field":"email"}'

# List indexes
curl http://localhost:8080/v1/users/indexes \
  -H "x-api-key: my-secret-key"

# Drop index
curl -X DELETE http://localhost:8080/v1/users/indexes/email \
  -H "x-api-key: my-secret-key"
```

---

## Transactions

Group multiple operations into an atomic unit. All staged writes are applied on commit, or discarded on rollback.

```bash
# Begin a transaction (prints tx_id)
TX=$(filedb-cli begin-tx orders)

# Stage writes inside the transaction
filedb-cli insert orders '{"item":"widget","qty":1}' --tx-id "$TX"
filedb-cli insert orders '{"item":"gadget","qty":2}' --tx-id "$TX"

# Commit
filedb-cli commit-tx "$TX"

# Or rollback
filedb-cli rollback-tx "$TX"
```

Open transactions are held in server memory. If a client disconnects without
committing or rolling back, the server reaps the transaction once it has been
idle longer than `--tx-timeout` (default `5m`); a later commit on a reaped
transaction returns a not-found error. Set `--tx-timeout 0` to keep
transactions indefinitely.

---

## TLS

TLS secures the TCP gRPC listener. The Unix socket always uses plaintext (local-only transport).

### Server

Generate a self-signed cert (development only):

```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout key.pem -out cert.pem \
  -days 365 -subj "/CN=localhost"
```

Start with TLS:

```bash
filedb serve --data ./data --api-key my-secret-key \
  --tls-cert cert.pem --tls-key key.pem
```

Or in `filedb.yaml`:

```yaml
tls_cert: /etc/filedb/cert.pem
tls_key:  /etc/filedb/key.pem
```

### CLI with TLS

```bash
filedb-cli --tls-ca cert.pem --host localhost:5433 collections
```

When `--tls-ca` is given, the CLI dials TCP with TLS, verifying the server certificate against the provided CA. Without `--tls-ca`, the CLI connects insecurely (or uses the Unix socket if available).

---

## Web UI

FileDB v2 ships a browser-based admin UI built with React 18, TypeScript, Vite, and Tailwind CSS (dark theme). It connects to the existing REST gateway at `:8080`.

**Features:** browse and manage collections (create, drop), full CRUD on records with filter/order/pagination, secondary index management, collection stats (auto-refreshes every 30 s), live Watch event feed via streaming, and connection settings (URL + API key) saved to `localStorage`.

### Development server

```bash
cd clients/web
npm install
npm run dev
# Open http://localhost:5173
```

The Vite dev server proxies all `/v1` requests to `http://localhost:8080`, so the FileDB server must be running (`make run`).

### Production build

```bash
cd clients/web
npm run build
# Output in clients/web/dist/
```

Serve `dist/` with any static file server; point it at a running FileDB REST gateway.

---

## Client SDKs

FileDB ships hand-written, idiomatic client libraries for seven languages. Every
SDK wraps the same gRPC API (`proto/filedb.proto`), takes the same connection
config (`host`, `port`, `api_key`, optional TLS CA cert), and exposes every RPC —
including the streaming `Find` and `Watch` calls in each language's native
iterator/stream style.

| Language | Install | Reference |
|---|---|---|
| Python | `pip install filedbv2` | [clients/python](../clients/python/README.md) |
| JavaScript / TypeScript | `npm install filedbv2` | [clients/js](../clients/js/README.md) |
| PHP | `composer require srjn45/filedbv2` | [clients/php](../clients/php/README.md) |
| Java | `com.srjn45:filedbv2-client` (Maven Central) | [clients/java](../clients/java/README.md) |
| Ruby | `gem install filedbv2` | [clients/ruby](../clients/ruby/README.md) |
| Rust | `cargo add filedbv2` | [clients/rust](../clients/rust/README.md) |
| C# / .NET | `dotnet add package FileDBv2.Client` | [clients/csharp](../clients/csharp/README.md) |

The per-language sections below cover install and basic usage; each client's
`README.md` has the full API reference, filter syntax, watch streaming, and
transaction examples.

---

## Python SDK

Install:

```bash
pip install filedbv2
```

```python
from filedbv2 import FileDB

db = FileDB("localhost", 5433, "dev-key")

db.create_collection("users")

rid = db.insert("users", {"name": "Alice", "age": 30})

record = db.find_by_id("users", rid)

# find collects the server stream into a list of record dicts
admins = db.find("users", {"field": "role", "op": "eq", "value": "admin"}, order_by="name")

db.update("users", rid, {"name": "Alice", "age": 31})
db.delete("users", rid)
db.drop_collection("users")
db.close()
```

`FileDB` is also a context manager (`with FileDB(...) as db:`). Watch returns an
iterator of event dicts:

```python
for event in db.watch("users"):
    print(event["op"], event["record"]["id"], event["record"]["data"])
```

With TLS:

```python
db = FileDB("myserver.example.com", 5433, "api-key", tls_ca_cert="/path/to/ca.crt")
```

See [clients/python/README.md](../clients/python/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## JavaScript / TypeScript SDK

Install:

```bash
npm install filedbv2
```

```typescript
import { FileDB } from 'filedbv2';

const db = new FileDB('localhost', 5433, 'dev-key');

await db.createCollection('users');

const id = await db.insert('users', { name: 'Alice', age: 30 });

const record = await db.findById('users', id);

// Streaming find — use `for await`
for await (const r of db.find('users', { filter: { field: 'role', op: 'eq', value: 'admin' } })) {
  console.log(r);
}

// Or collect all results at once
const admins = await db.findAll('users', {
  filter: { field: 'role', op: 'eq', value: 'admin' },
  orderBy: 'name',
});

await db.update('users', id, { name: 'Alice', age: 31 });
await db.delete('users', id);
await db.dropCollection('users');
db.close();
```

CommonJS works too: `const { FileDB } = require('filedbv2')`.

With TLS:

```typescript
const db = FileDB.fromTlsCertPath('myserver.example.com', 5433, 'api-key', '/path/to/ca.crt');
```

See [clients/js/README.md](../clients/js/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## PHP SDK

Install:

```bash
composer require srjn45/filedbv2
```

```php
<?php
require 'vendor/autoload.php';

use FileDBv2\FileDB;

$db = new FileDB('localhost', 5433, 'dev-key');

$db->createCollection('users');

$id = $db->insert('users', ['name' => 'Alice', 'age' => 30]);

$record = $db->findById('users', $id);

// find() collects the server stream into an array of record arrays
$admins = $db->find('users', ['field' => 'role', 'op' => 'eq', 'value' => 'admin'],
                    orderBy: 'name');

$db->update('users', $id, ['name' => 'Alice', 'age' => 31]);
$db->delete('users', $id);
$db->dropCollection('users');
```

Watch returns a PHP Generator of event arrays:

```php
foreach ($db->watch('users') as $event) {
    echo $event['op'] . ' id=' . $event['record']['id'] . "\n";
    // $event['op'] is 'INSERTED' | 'UPDATED' | 'DELETED'
}
```

With TLS:

```php
$db = new FileDB('myserver.example.com', 5433, 'api-key', '/path/to/ca.crt');
```

See [clients/php/README.md](../clients/php/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## Java SDK

Add the dependency to your Gradle or Maven project:

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.srjn45:filedbv2-client:0.1.0")
}
```

```java
import com.srjn45.filedbv2.FileDBClient;
import java.util.List;
import java.util.Map;

try (FileDBClient db = new FileDBClient("localhost", 5433, "dev-key")) {
    db.createCollection("users");

    long id = db.insert("users", Map.of("name", "Alice", "age", 30));

    Map<String, Object> record = db.findById("users", id);

    List<Map<String, Object>> admins = db.find("users",
            Map.of("field", "role", "op", "eq", "value", "admin"),
            0, 0, "name", false);

    db.update("users", id, Map.of("name", "Alice", "age", 31));
    db.delete("users", id);
    db.dropCollection("users");
}
```

With TLS:

```java
FileDBClient db = new FileDBClient("myserver.example.com", 5433, "api-key",
        new java.io.File("/path/to/ca.crt"));
```

See [clients/java/README.md](../clients/java/README.md) for the full API reference, filter syntax, and transaction usage.

---

## Ruby SDK

Install:

```bash
gem install filedbv2
```

Or add to your `Gemfile`:

```ruby
gem "filedbv2", "~> 0.1"
```

```ruby
require "filedbv2"

db = FileDBv2::Client.new(host: "localhost", port: 5433, api_key: "dev-key")

db.create_collection("users")

id = db.insert("users", { name: "Alice", age: 30 })

record = db.find_by_id("users", id)

# find collects the server stream into an Array of Hashes
admins = db.find("users", filter: { field: "role", op: "eq", value: "admin" },
                           order_by: "name")

# Or stream results one by one with a block
db.find("users") { |r| puts r["data"]["name"] }

db.update("users", id, { name: "Alice", age: 31 })
db.delete("users", id)
db.drop_collection("users")
db.close
```

Use `.open` for automatic close:

```ruby
FileDBv2::Client.open(host: "localhost", port: 5433, api_key: "dev-key") do |db|
  db.create_collection("orders")
  # ...
end
```

Watch returns an `Enumerator` of event Hashes:

```ruby
db.watch("users") do |event|
  puts "#{event[:op]}: #{event[:record]["data"].inspect}"
end
```

With TLS:

```ruby
db = FileDBv2::Client.new(
  host: "myserver.example.com",
  port: 5433,
  api_key: "api-key",
  tls_ca_cert: "/path/to/ca.crt"
)
```

See [clients/ruby/README.md](../clients/ruby/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## Rust SDK

Add to `Cargo.toml`:

```toml
[dependencies]
filedbv2 = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

**Requires:** `protoc` on `PATH` at build time (used by `tonic-build` for code generation).

```rust
use filedbv2::{FileDB, FilterInput, FilterOp, FindOptions};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut db = FileDB::connect("localhost", 5433, "dev-key").await?;

    db.create_collection("users").await?;

    let id = db.insert("users", serde_json::json!({"name": "Alice", "age": 30})).await?;

    let record = db.find_by_id("users", id).await?;
    println!("{} — {}", record.id, record.data);

    // Streaming find — collect into Vec or iterate lazily with find_stream.
    let admins = db.find("users", FindOptions {
        filter: Some(FilterInput::field("role", FilterOp::Eq, "admin")),
        order_by: "name".to_owned(),
        ..Default::default()
    }).await?;

    db.update("users", id, serde_json::json!({"name": "Alice", "age": 31})).await?;
    db.delete("users", id).await?;
    db.drop_collection("users").await?;
    Ok(())
}
```

Watch (change feed) returns an async `Stream`:

```rust
let mut events = db.watch("users", None).await?;
while let Some(event) = events.next().await {
    let event = event?;
    println!("{:?} id={} data={}", event.op, event.record.id, event.record.data);
}
```

With TLS:

```rust
let ca_pem = std::fs::read("/path/to/ca.pem")?;
let mut db = FileDB::connect_tls("myserver.example.com", 5433, "api-key", &ca_pem).await?;
```

See [clients/rust/README.md](../clients/rust/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## C# / .NET SDK

Install:

```bash
dotnet add package FileDBv2.Client --version 0.1.0
```

```csharp
using FileDBv2.Client;

await using var db = new FileDB("localhost", 5433, "dev-key");

await db.CreateCollectionAsync("users");

ulong id = await db.InsertAsync("users", new()
{
    ["name"] = "Alice",
    ["age"]  = 30,
});

var record = await db.FindByIdAsync("users", id);

// Streaming find — results arrive one by one via IAsyncEnumerable
await foreach (var r in db.FindAsync("users",
    filter:  new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" },
    orderBy: "name"))
{
    Console.WriteLine(r["name"]);
}

// Or collect all results at once
var admins = await db.FindAllAsync("users",
    filter: new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" });

await db.UpdateAsync("users", id, new() { ["name"] = "Alice", ["age"] = 31 });
await db.DeleteAsync("users", id);
await db.DropCollectionAsync("users");
```

`FileDB` implements both `IAsyncDisposable` (`await using`) and `IDisposable` (`using`). Watch returns an `IAsyncEnumerable<WatchEventResult>`:

```csharp
using var cts = new CancellationTokenSource();
await foreach (var evt in db.WatchAsync("users", ct: cts.Token))
{
    Console.WriteLine($"{evt.Op} id={evt.RecordId}");
}
cts.Cancel(); // stop the stream
```

With TLS:

```csharp
var db = new FileDB("myserver.example.com", 5433, "api-key", "/path/to/ca.crt");
```

See [clients/csharp/README.md](../clients/csharp/README.md) for the full API reference, filter syntax, watch streaming, and transaction usage.

---

## Prometheus metrics

When `--metrics-addr` is set (default `:9090`), FileDB exposes a `/metrics` endpoint in Prometheus format.

```bash
curl http://localhost:9090/metrics
```

Available metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `filedb_collection_records_total` | Gauge | `collection` | Live record count per collection |
| `filedb_collection_segments_total` | Gauge | `collection` | Segment file count per collection |
| `filedb_compaction_runs_total` | Counter | `collection` | Total compaction runs per collection |
| `filedb_compaction_duration_seconds` | Histogram | `collection` | Compaction run duration |
| `filedb_grpc_request_duration_seconds` | Histogram | `method`, `code` | gRPC unary request duration by method and status code |

Disable metrics by setting `--metrics-addr ""` (or `metrics_addr: ""` in the config file).

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: filedb
    static_configs:
      - targets: ['localhost:9090']
```
