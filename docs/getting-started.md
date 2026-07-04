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
| `--default-ttl` | `0` | Default expiry applied to inserted records (`0` = never expire), e.g. `24h` |
| `--watch-buffer` | `64` | Per-subscriber Watch event buffer; a slow subscriber gets an `OVERFLOW` signal once full |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `--log-format` | `text` | Log output format: `json` or `text` |
| `--max-concurrent-streams` | `0` | Max concurrent HTTP/2 streams per gRPC connection (`0` = gRPC library default) |
| `--max-inflight` | `0` | Server-wide concurrent in-flight RPC ceiling; excess calls get `RESOURCE_EXHAUSTED` (`0` = unlimited) |
| `--rate-limit` | `0` | Per-API-key rate limit in requests/sec; over-budget calls get `RESOURCE_EXHAUSTED` (`0` = disabled) |
| `--slow-query-ms` | `0` | Log any `Find` slower than this many milliseconds at `WARN`, with scan stats (`0` = disabled) |
| `--otlp-endpoint` | *(none)* | OTLP/gRPC collector address for OpenTelemetry tracing, e.g. `localhost:4317` (empty = tracing disabled) |
| `--otlp-sample-ratio` | `1.0` | Fraction of traces to sample when tracing is enabled (`0`–`1`) |
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
default_ttl: 0              # expire inserted records after this long (0 = never), e.g. 24h
watch_buffer_size: 64       # per-subscriber Watch buffer before an OVERFLOW signal
log_level: info             # debug | info | warn | error
log_format: text            # json | text
max_concurrent_streams: 0   # per-connection HTTP/2 stream cap (0 = gRPC default)
max_inflight: 0             # server-wide in-flight RPC ceiling (0 = unlimited)
rate_limit: 0               # per-API-key requests/sec (0 = disabled)
slow_query_ms: 0            # log Find slower than this many ms at WARN (0 = disabled)
otlp_endpoint: ""           # OTLP/gRPC collector address (empty = tracing disabled)
otlp_sample_ratio: 1.0      # fraction of traces sampled when tracing is enabled
# tls_cert: /etc/filedb/cert.pem
# tls_key:  /etc/filedb/key.pem
```

```bash
filedb serve --config filedb.yaml
```

CLI flags always override the config file. Omitted keys fall back to defaults.

---

## Authentication & scoped API keys

The simplest setup is a single key via `--api-key` (or `$FILEDB_API_KEY`);
clients send it in the `x-api-key` gRPC metadata header or HTTP header. An empty
key disables authentication entirely.

For multi-client deployments you can define a list of **scoped keys** in the
config file. Each key has a `name` (used in logs) and a `scope`:

- `read` — non-mutating RPCs only (`find`, `find-by-id`, `list`, index listing,
  stats, watch, backup).
- `read-write` — everything, including inserts, updates, deletes, index
  management, and compaction.

```yaml
# filedb.yaml
keys:
  - key: reader-secret
    name: analytics
    scope: read
  - key: writer-secret
    name: backend
    scope: read-write
```

A read-scoped key presenting on a write RPC is rejected with `PermissionDenied`
(a missing or unknown key returns `Unauthenticated`). The legacy `api_key` /
`--api-key` still works alongside `keys:` and is registered as a `read-write`
key named `default`.

```bash
# analytics can read…
filedb-cli find users '{"field":"name","op":"eq","value":"alice"}' --api-key reader-secret
# …but not write:
filedb-cli insert users '{"name":"bob"}' --api-key reader-secret
# Error: rpc error: code = PermissionDenied ...
```

**Rotation without downtime.** Edit the `keys:` list in the config file and send
the server `SIGHUP` — it re-reads the file and swaps the key set atomically, so
you can add, remove, or re-scope keys without dropping connections or
restarting:

```bash
kill -HUP $(pgrep -f 'filedb serve')
```

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

## Backpressure & limits

By default FileDB accepts unbounded concurrent work — fine for a trusted
embedded or single-tenant deployment, risky behind a public load balancer where
a greedy or buggy client can exhaust goroutines and file descriptors. Three
**opt-in, off-by-default** controls let the server shed load with a typed
`RESOURCE_EXHAUSTED` error instead of growing without bound. Setting any of them
to `0` (the default) leaves that control disabled, so existing deployments are
unaffected.

| Flag | Protects against | When to use it |
|---|---|---|
| `--max-concurrent-streams` | One connection multiplexing unbounded HTTP/2 streams | Cap per-connection fan-out; leave at `0` to accept the gRPC library default |
| `--max-inflight` | Too many RPCs executing at once (goroutine/FD/memory growth) | Set to a ceiling your hardware can comfortably serve; calls above it are rejected immediately rather than queued |
| `--rate-limit` | A single API key monopolising the server | Give each principal a steady requests/sec budget; bursts up to one second's worth are absorbed before throttling |

```bash
# Accept at most 512 concurrent in-flight RPCs and 100 req/s per API key.
filedb serve --data ./data --max-inflight 512 --rate-limit 100
```

- **`--max-inflight`** installs a server-wide semaphore. Once the ceiling is
  saturated, further calls fail fast with `RESOURCE_EXHAUSTED` rather than
  queueing — the server sheds load instead of accumulating it. A streaming RPC
  (`Find`, `Watch`, `Snapshot`) holds a slot for its whole lifetime.
- **`--rate-limit`** is a token bucket **per API-key principal** (the `name`
  from your scoped keys). Each principal gets an independent bucket, so one
  client being throttled never affects another. The burst size is one second's
  worth of budget. Unauthenticated deployments share a single bucket.
- **`--max-concurrent-streams`** maps straight to gRPC's per-connection HTTP/2
  stream cap.

Over-budget calls return gRPC `RESOURCE_EXHAUSTED` (HTTP `429` via the REST
gateway); clients should back off and retry. See
[architecture.md](architecture.md#backpressure--rate-limiting) for how the
interceptors are ordered.

---

## Slow-query log

A query that scans the whole collection because no index can serve its filter is
the classic operability trap — it works fine on a small dataset and degrades
silently as data grows. Turn on the slow-query log to catch these:

```bash
# Log any Find that takes 50ms or longer, at WARN, with scan stats.
filedb serve --data ./data --slow-query-ms 50 --log-format json
```

`--slow-query-ms` (default `0` = disabled) sets a duration threshold. Any `Find`
whose server-side wall-clock time reaches it is logged once at `WARN`. In JSON
format a line looks like:

```json
{"time":"2026-07-03T10:15:04Z","level":"WARN","msg":"slow query",
 "collection":"users","filter":"role EQ","rows_scanned":250000,
 "rows_returned":12,"index_used":false,"duration":"82ms"}
```

Read it as follows:

- **`filter`** — the *shape* of the filter (fields and operators only, never the
  compared values), e.g. `role EQ` or `and(status EQ, age GTE)`. Safe to log and
  aggregate: it identifies the query pattern without leaking record data.
- **`rows_scanned` vs `rows_returned`** — records examined versus emitted. A
  large ratio (250000 scanned to return 12) is the signature of a full scan doing
  far more work than the result justifies.
- **`index_used`** — whether a secondary index produced the candidate set. When
  this is `false` on a hot query, adding an index on the filtered field
  (`ensureindex`) is usually the fix — re-run and it flips to `true` with a much
  smaller `rows_scanned`.

The same cost is exported as the `filedb_scan_rows_scanned` Prometheus histogram
(labelled by `collection`), so you can alert on scan cost without scraping logs.
See [architecture.md](architecture.md#slow-query-log--scan-stats) for how the
stats flow from the engine to the log and the metric.

---

## Distributed tracing (OpenTelemetry)

FileDB can emit [OpenTelemetry](https://opentelemetry.io/) traces so you can see
where a slow request spends its time — across the gateway → gRPC → engine-scan
hops. Tracing is **opt-in and off by default**: nothing is wired unless you set
`--otlp-endpoint`, so there is zero overhead on the default path.

| Flag | Default | Meaning |
|---|---|---|
| `--otlp-endpoint` | *(none)* | Address of an OTLP/gRPC collector (e.g. `localhost:4317`). Empty = tracing disabled. |
| `--otlp-sample-ratio` | `1.0` | Fraction of traces to sample at the root: `1.0` traces everything, `0.1` one in ten, `0` none. |

Point the server at any OTLP-compatible collector — the
[OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), Jaeger
(`4317`), Tempo, or a vendor endpoint:

```bash
# Run a local collector, then:
filedb serve --data ./data \
  --otlp-endpoint localhost:4317 \
  --otlp-sample-ratio 1.0
```

What you get per traced request:

- One **server span per RPC**, named after the method (e.g.
  `/filedb.v1.FileDB/Find`) and tagged with `rpc.method` and the returned
  `rpc.grpc.status_code`. A failed RPC marks the span as errored.
- A child **`engine.scan`** span for every `Find`/`Scan`, so you can see which
  query drove a long collection scan, and a **`engine.compaction`** span for
  each compaction run.

The connection to the collector is made in-the-clear (`insecure`) — run the
collector locally or as a sidecar, or front it with a mesh that provides
transport security. Sampling is parent-based: if an upstream caller already made
a sampling decision (via propagated trace context), FileDB honours it, and the
ratio applies only to traces it roots. See
[architecture.md](architecture.md#tracing-opentelemetry) for how the interceptor
and the engine hook fit together.

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
filedb-cli compact products
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

## Keyed records, upsert & optimistic concurrency

Alongside server-assigned `uint64` ids, records can carry a **caller-supplied
string key** (a natural key such as an email or SKU) and a monotonic **revision
(`rev`)** that increments on every write. These unlock natural-key CRUD, upsert,
and compare-and-swap (optimistic-concurrency) updates directly over the wire —
the same operations the embedded engine has always had.

- **Keyed insert** — `insert --key`: creates a record under a string key; a key
  already held by a live record is rejected with `AlreadyExists`.
- **`upsert`** — insert under a key, or replace the existing record if the key is
  already present. Returns the resulting record with its (incremented on replace)
  `rev`.
- **`find-by-key` / `update-by-key` / `delete-by-key`** — natural-key CRUD; a
  missing key returns `NotFound`.
- **`update-if-rev`** — compare-and-swap: applies the update only if the record's
  current `rev` matches the one you pass. A stale `rev` (or a missing key) is a
  clean no-op — reported as *not swapped*, never an error — so a client can retry.

Every record-bearing response (`insert`, `get`/`find`, `find-by-key`, `upsert`,
`update-by-key`, `update-if-rev`) now includes `key` and `rev`.

```bash
# Keyed insert (duplicate key → AlreadyExists)
filedb-cli insert users --key alice '{"name":"Alice","age":30}'

# Upsert: insert-or-replace by key, returns the new rev
filedb-cli upsert users alice '{"name":"Alice","age":31}'

# Read / update / delete by key
filedb-cli find-by-key users alice
filedb-cli update-by-key users alice '{"name":"Alice","age":32}'
filedb-cli delete-by-key users alice

# Compare-and-swap: only applies if the record is still at rev 2
filedb-cli update-if-rev users alice 2 '{"name":"Alice","age":33}'
```

Over REST:

```bash
# Keyed insert
curl -X POST http://localhost:8080/v1/users/records \
  -H "x-api-key: my-secret-key" -H "Content-Type: application/json" \
  -d '{"data":{"name":"Alice"},"key":"alice"}'

# Upsert (custom verb)
curl -X POST "http://localhost:8080/v1/users/records:upsert" \
  -H "x-api-key: my-secret-key" -H "Content-Type: application/json" \
  -d '{"key":"alice","data":{"name":"Alice","age":31}}'

# Find / update / delete by key
curl http://localhost:8080/v1/users/keys/alice -H "x-api-key: my-secret-key"
curl -X PUT http://localhost:8080/v1/users/keys/alice \
  -H "x-api-key: my-secret-key" -H "Content-Type: application/json" \
  -d '{"data":{"name":"Alice","age":32}}'
curl -X DELETE http://localhost:8080/v1/users/keys/alice -H "x-api-key: my-secret-key"

# Compare-and-swap
curl -X POST "http://localhost:8080/v1/users/keys/alice:cas" \
  -H "x-api-key: my-secret-key" -H "Content-Type: application/json" \
  -d '{"expected_rev":2,"data":{"name":"Alice","age":33}}'
```

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

### Ordering & pagination

`Find` accepts `order_by`, `descending`, `limit`, and `offset`. These are pushed
into the storage engine and applied *as it reads*, so a limited query never
loads the whole collection into memory:

- **`limit` / `offset` without `order_by`** — results stream in insertion (id)
  order and the scan stops after `offset + limit` matches. `Find … limit 10`
  over a huge collection reads about ten rows, not all of them.
- **`order_by`** — results are sorted by that field using the same type-aware
  comparison as the `gt`/`lt` filter operators: numerically when both values are
  numbers (so `2` sorts before `10`, not the lexical `"10"` before `"2"`),
  otherwise by their string form. Set `descending` to reverse. Ties break on
  ascending `id` so pages are stable. With a `limit`, only a bounded
  top-`(offset+limit)` buffer is kept rather than sorting every row.

Cancelling the request (client disconnect or context cancellation) stops the
scan promptly instead of running to completion.

### Field projection

`find`, `get`, and `find-by-key` accept a `--fields` flag (a comma-separated
list; the wire field is a repeated `fields`) that narrows each returned record's
data to just those top-level fields. Wide documents then transmit only what the
caller asked for:

```bash
# Return only name and email, not the whole user document
filedb-cli find users '{"field":"role","op":"eq","value":"admin"}' --fields name,email

# Projection also works on point lookups
filedb-cli get users 42 --fields name,email
filedb-cli find-by-key users alice --fields name,email
```

Rules:

- **`id`, `key`, and `rev` are always included**, regardless of the projection —
  they identify the record and drive optimistic-concurrency updates, so a
  projection never strips them.
- **An empty `--fields` (the default) returns the full record**, so existing
  reads are unchanged.
- **An unknown or absent field is silently omitted** — projecting to a field a
  record doesn't have is not an error; that field is simply not present.

Projection is applied in the engine after filtering and ordering, so an
`order_by` field need not be listed in `--fields`.

---

## Secondary indexes

Secondary indexes accelerate equality lookups on any field from O(n) full scan to O(1), and **range** queries (`gt`/`gte`/`lt`/`lte`) from O(n) to O(matches).

```bash
# Create an index on the "email" field
filedb-cli ensure-index users email

# List indexes on a collection
filedb-cli indexes users

# Drop an index
filedb-cli drop-index users email
```

Once an index exists, `find` with a single `eq` **or a single range** (`gt`/`gte`/`lt`/`lte`) filter on that field uses the index automatically — no query hint needed. Range ordering is type-aware: a numeric field compares numerically (`age > 9` matches `10`), a string field lexically.

```bash
# Uses the "age" index to read only the matching rows, not the whole collection
filedb-cli ensure-index users age
filedb-cli find users '{"field":"age","op":"gte","value":21}'
```

Indexes are:
- Persisted to `sidx_<field>.json` (SHA-256 checksummed) and reloaded on startup
- Maintained automatically on every insert, update, and delete
- Rebuilt transparently after compaction

A field that mixes numbers and strings can't define a range order, so range queries on it fall back to a full scan (equality lookups still use the index).

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

## On-demand compaction

Compaction normally runs on its own — triggered when a collection's sealed
segments cross the dirty-ratio threshold or on the compaction timer. You can
also force a pass immediately, for example to reclaim space after a bulk delete
or to shrink a collection before backing it up:

```bash
filedb-cli compact products
```

The command runs a **synchronous, forced** compaction: it ignores the
dirty-ratio gate and returns only once the pass has finished, so when the prompt
comes back the collection is fully compacted. It maps to the `Compact` RPC
(`POST /v1/{collection}/compact`), so you can trigger it over REST too:

```bash
curl -H "x-api-key: dev-key" -X POST \
  http://localhost:8080/v1/products/compact
```

---

## Backup & restore

`filedb-cli backup` streams a consistent, gzip-compressed snapshot of the entire
database from a running server to a local file:

```bash
filedb-cli backup db-$(date +%F).tar.gz
```

The snapshot is taken without stopping the server: each collection is captured at
a point in time (writes are briefly held only while that collection's files are
copied), and because segments are append-only a backup taken under concurrent
writes always restores to a consistent state.

Restore is a plain extract into a data directory — no special import step:

```bash
tar xzf db-2026-07-03.tar.gz -C ./data
filedb serve --data ./data --api-key dev-key
```

The archive lays files out as `<collection>/<file>`, so it also doubles as a
human-inspectable copy of your data. The primary index is rebuilt from the
segments the first time the restored server opens each collection.

> The snapshot is exposed as the gRPC-only streaming `Snapshot` RPC. It is not
> available on the REST gateway (binary streaming does not map cleanly onto HTTP
> JSON) — use the CLI or a gRPC client.

---

## TTL / expiring records

Records can be given an expiry **deadline**, after which they vanish from reads
and are reclaimed by compaction — a natural fit for caches, sessions, and IoT
telemetry.

Set a **server-wide default** with `--default-ttl` (or `default_ttl` in the
config file). Every inserted record that doesn't carry its own deadline expires
that long after it was written:

```bash
filedb serve --data ./data --default-ttl 24h   # inserts expire after a day
```

A default of `0` (the default) means records never expire.

**Per-collection default.** A single collection can pin its own default TTL at
creation time, overriding the server-wide default and surviving restarts:

```bash
filedb-cli create-collection sessions --default-ttl 30m
```

**Per-record TTL.** Individual writes can set (or reset) a record's deadline
with `--ttl`, overriding any collection default:

```bash
filedb-cli insert sessions '{"user":"alice"}' --ttl 15m   # expires in 15 min
filedb-cli update sessions 7 '{"user":"alice"}' --ttl 15m # slide the deadline
```

Over the API these map to `ttl_seconds` on the `Insert`/`InsertMany`/`Update`
RPCs and `default_ttl_seconds` on `CreateCollection`. A plain `update` with no
`--ttl` keeps the record's existing deadline; passing `--ttl` moves it. Setting
a per-record TTL inside a transaction is not supported and is rejected.

Expiry semantics:

- An expired record is invisible to **every** read the moment its deadline
  passes — `find-id`, filtered `find`, and key lookups all skip it — even before
  the background reaper reclaims the space.
- A reaper on the compaction cadence tombstones expired records, and compaction
  drops them, so on-disk space is reclaimed.
- Deadlines are **durable**: they survive server restarts.

**Embedded engine.** Finer-grained, per-record deadlines are available through
the embeddable Go engine (`import "github.com/srjn45/filedbv2/engine"`):

```go
// Explicit per-record deadline, overriding any collection default.
id, _, _ := col.InsertWithExpiry(map[string]any{"session": "abc"}, time.Now().Add(30*time.Minute))

// A plain Update keeps the record's existing deadline (sticky);
// UpdateWithExpiry moves it.
col.Update(id, map[string]any{"session": "abc", "hits": 1})            // deadline unchanged
col.UpdateWithExpiry(id, map[string]any{"session": "abc"}, later)      // deadline extended

// A collection-level default (server maps --default-ttl to this):
db, _ := engine.Open("./data", engine.CollectionConfig{DefaultTTL: 24 * time.Hour})
```

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
---

## Structured logging

FileDB logs through the standard library [`log/slog`](https://pkg.go.dev/log/slog).
Every gRPC request produces exactly one structured record once it returns,
carrying the method, the authenticated principal, the wall-clock duration, and
the gRPC status code. Successful calls log at `info`; failed calls at `error`.

Two flags control output:

| Flag | Default | Values | Description |
|---|---|---|---|
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` | Minimum level emitted |
| `--log-format` | `text` | `json`, `text` | Handler format (JSON for machines, text for humans) |

```bash
# machine-parseable JSON logs at info and above
filedb serve --data ./data --log-format json --log-level info
```

A JSON request record looks like:

```json
{"time":"2026-07-03T12:00:00Z","level":"INFO","msg":"grpc request","method":"/filedb.v1.FileDB/Insert","principal":"default","duration":"412.7µs","code":"OK"}
```

The `principal` is the `name` of the API key that authenticated the call (or
`anonymous` when authentication is disabled). Logs are written to standard error.

---

## Health & readiness probes

FileDB exposes both a standard gRPC health service and two HTTP probes so load
balancers and orchestrators (e.g. Kubernetes) can gate traffic.

### gRPC health

The standard [`grpc.health.v1.Health`](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)
service is registered on both the TCP and Unix gRPC servers. It reports
`SERVING` once the listeners are up and flips to `NOT_SERVING` at the start of
graceful shutdown so in-flight RPCs drain before the process exits.

```bash
grpc_health_probe -addr localhost:5433   # NOT_SERVING during shutdown
```

### HTTP probes

Two routes are served on the REST gateway (default `:8080`):

| Route | Meaning | Response |
|---|---|---|
| `GET /healthz` | **Liveness** — the process is running | `200 ok` always |
| `GET /readyz` | **Readiness** — the DB is open and the data directory is writable | `200 ready`, or `503` with the reason |

```bash
curl -i http://localhost:8080/healthz   # 200 ok
curl -i http://localhost:8080/readyz    # 200 ready  (503 if the data dir is unwritable)
```

A Kubernetes deployment typically wires `/healthz` to `livenessProbe` and
`/readyz` to `readinessProbe`, so a node with a full or read-only data volume is
pulled out of rotation without being killed.
