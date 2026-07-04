# FileDB v2

A lightweight, append-only, file-based document database — single binary, zero dependencies, human-readable storage.

[![CI](https://github.com/srjn45/filedbv2/actions/workflows/ci.yml/badge.svg)](https://github.com/srjn45/filedbv2/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/srjn45/filedbv2)](https://goreportcard.com/report/github.com/srjn45/filedbv2)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Quick start

```bash
# 1. Start the server
filedb serve --data ./data --api-key dev-key

# 2. Insert a record
filedb-cli insert users '{"name":"alice","age":30}' --api-key dev-key

# 3. Query it
filedb-cli find users '{"field":"name","op":"eq","value":"alice"}' --api-key dev-key
```

Or use the interactive REPL:

```bash
filedb-cli repl --api-key dev-key
```

---

## Run with Docker Compose

```bash
# Start the server (builds the image from the local Dockerfile on first run)
FILEDB_API_KEY=dev-key docker compose up -d

# Insert a record against the REST gateway on :8080
curl -H "x-api-key: dev-key" \
  -d '{"data":{"name":"alice","age":30}}' \
  http://localhost:8080/v1/users/records
```

gRPC is exposed on `:5433` and REST on `:8080`; data persists in the named
`filedb-data` volume. See [`docker-compose.yml`](docker-compose.yml) and the
fully commented [`filedb.example.yaml`](filedb.example.yaml) for configuration.

---

## What it is

FileDB v2 stores each collection as a set of **NDJSON segment files** — one JSON object per line, appended in order. There is no binary format, no external runtime, and no hidden state. You can inspect, backup, or migrate your data with any text editor or Unix tool.

Key properties:

- **Append-only writes** — inserts, updates, and deletes are always new lines; no in-place modification
- **Configurable durability** — choose `none` (OS flush), `always` (fsync per write), or `interval` (fsync on a timer) to trade throughput against crash-loss window
- **End-to-end integrity** — every segment entry carries a CRC32C checksum, so silent on-disk bit-rot is caught on read instead of returning wrong data
- **Background compaction** — a goroutine per collection merges and deduplicates sealed segments; operators can also force a synchronous pass on demand (`filedb-cli compact`)
- **Online backup** — `filedb-cli backup` streams a consistent gzip snapshot of the live database; restore is a plain `tar xzf` into a data directory
- **Leader→follower replication** — start a hot standby with `--replicate-from <leader>`: the follower bootstraps from a snapshot then tails the leader's committed writes (each tagged with a monotonic global LSN), applying them through the normal write path so its indexes match exactly. Async (bounded lag), resumes after a disconnect from its persisted applied-LSN with no gaps or duplicates, and exposes `ReplicationStatus` (leader LSN, per-follower lag).
- **Read replicas** — a follower serves reads (`Find`/`FindById`/`FindByKey`/`Aggregate`) from its applied state and refuses writes with a typed `FAILED_PRECONDITION`, so read traffic scales horizontally across followers. Bound staleness by diffing a follower's `applied_lsn` against the leader's `leader_lsn`
- **Manual failover** — after a leader loss, promote a caught-up follower to leader with the admin `Promote` RPC (`filedb-cli promote`): it stops replicating, lifts the read-only guard, and accepts writes. A lag guard refuses promoting a stale replica (override with `--force`); promotion is one-way and automatic election is out of scope. See the [operator runbook](docs/operations.md)
- **In-memory index** — O(1) lookup by id, persisted with a checksum for fast restarts
- **Secondary indexes** — per-field indexes for O(1) equality lookups and O(matches) range queries (`gt`/`lt`/…); automatically maintained and persisted
- **Streaming queries** — `Find` pushes `limit`/`offset`, a multi-field `order_by_fields` sort, and a keyset `page_token` cursor into the engine and streams results as it reads; a limited query is bounded by the page size, not the collection size, and honours client cancellation. Cursor pagination seeks past already-returned rows in O(page) — concatenated pages cover every row exactly once, no dupes or gaps. Optional field projection (`--fields`) returns only the requested fields (`id`/`key`/`rev` always included)
- **Aggregations** — `Aggregate` computes a `count` and numeric `sum`/`avg`/`min`/`max` over the same filter as `Find`, optionally grouped by a field; it streams one result per group and folds records in the engine (memory bounded by distinct groups, not rows), so clients count and total server-side instead of pulling the whole collection. CLI: `aggregate --group-by --field --aggs count,sum,avg,min,max`
- **Transactions** — optimistic multi-operation transactions via `BeginTx` / `CommitTx` / `RollbackTx`
- **Keyed CRUD, upsert & CAS over the wire** — caller-supplied string keys, insert-or-replace `upsert`, natural-key find/update/delete, and revision-checked compare-and-swap (`update-if-rev`) are now exposed over gRPC/REST (not just the embedded engine); every record carries a `key` and a monotonic `rev`, with duplicate/missing keys mapped to `AlreadyExists`/`NotFound`
- **TTL / expiring records** — per-record deadlines (`--ttl` / `ttl_seconds` on Insert & Update), a per-collection default (`create-collection --default-ttl`, persisted), and a server-wide default (`--default-ttl`); expired records are hidden from reads immediately and reclaimed by compaction
- **gRPC + REST** — dual API served from one binary; CLI uses the Unix socket when local
- **OpenAPI spec** — `docs/openapi/filedb.swagger.json` generated from the proto; generate clients for any language with [openapi-generator](https://openapi-generator.tech/)
- **Official client SDKs** — idiomatic, hand-written libraries for 7 languages (Python, JavaScript/TypeScript, PHP, Java, Ruby, Rust, C#/.NET) under `clients/`
- **Scoped API keys** — multiple named keys with `read` or `read-write` scope; a read-only key is refused on writes, and keys hot-reload on `SIGHUP` for rotation without a restart
- **Optional TLS** — TCP gRPC listener can be secured with a cert/key pair; CLI verifies via `--tls-ca`
- **YAML config file** — `--config filedb.yaml` with CLI flag overrides always winning
- **Prometheus metrics** — per-collection gauges, compaction histograms, gRPC request duration, and per-query rows-scanned at `--metrics-addr`
- **Structured logging** — leveled `log/slog` output (`--log-level`, `--log-format json|text`); one record per RPC with method, principal, duration, and status code
- **Slow-query log** — opt-in `--slow-query-ms` logs any `Find` over the threshold at `WARN` with filter shape, rows scanned vs returned, and whether an index was used, so unindexed hot queries surface from logs and metrics (off by default)
- **Health & readiness** — standard `grpc.health.v1.Health` service (SERVING → NOT_SERVING on graceful shutdown) plus HTTP `/healthz` (liveness) and `/readyz` (DB open + data dir writable) probes
- **Backpressure & limits** — opt-in `--max-inflight` in-flight ceiling and per-key `--rate-limit` token bucket shed load with `RESOURCE_EXHAUSTED` instead of unbounded resource growth; `--max-concurrent-streams` caps per-connection HTTP/2 streams (all off by default)
- **Distributed tracing** — opt-in OpenTelemetry spans to an OTLP collector (`--otlp-endpoint`, `--otlp-sample-ratio`); one span per RPC plus child `engine.scan`/`engine.compaction` spans, so a slow `Find` is traceable gateway → gRPC → engine (off by default; the embeddable engine gains no OTel dependency)
- **Single binary** — no JVM, no Python, no config files required to get started
- **Web admin UI** — browser-based collection and record manager at `clients/web/` (React + Vite, talks to the REST gateway)

---

## When to use it

FileDB v2 is the right tool when:

- You need persistence without standing up PostgreSQL or MongoDB
- Your data fits on one machine and you want human-readable files
- You're building CLI tools, local services, IoT daemons, or small web apps
- You want a simple HTTP/gRPC API you can call from any language

It is **not** the right tool for multi-node replication, complex joins, or datasets too large to compact on a single machine.

---

## Embedding (use it as a Go library)

FileDB v2's storage engine is a plain Go library, so you can skip the server
entirely and run the database **in-process** — no gRPC, no network, no separate
daemon. This is the right choice when your program is the only writer and you
want FileDB's durability and query model directly inside your binary.

```bash
go get github.com/srjn45/filedbv2/engine   # the storage engine
go get github.com/srjn45/filedbv2/filedb   # the ergonomic façade (recommended)
```

```go
import "github.com/srjn45/filedbv2/filedb"

db, _ := filedb.Open("./data")            // embedded durability defaults (fsync ~1s)
defer db.Close()

sessions := db.MustCollection("sessions")
id, _, _ := sessions.InsertWithKey("sess-1", map[string]any{"status": "open"})
rec, _   := sessions.GetByKey("sess-1")   // caller-supplied string keys
_, _ = sessions.UpdateIfRev("sess-1", rec.Rev, map[string]any{"status": "closed"}) // CAS
```

The embedded surface includes caller-supplied string keys, per-record revisions
with compare-and-swap, upsert, count/exists, secondary indexes, in-process
`Watch` subscriptions, and a `LoadJSONL` bulk-import path. The engine pulls in
**no** gRPC/protobuf/Prometheus/cobra/OpenTelemetry dependencies — a CI gate
enforces that.

This is a distinct distribution channel: **`go get` for embedding**, while the
standalone server ships via Homebrew/apt/GHCR and tagged binary releases. Both
build from the same repo. See **[docs/embedding.md](docs/embedding.md)** for the
full API reference, durability modes, the Watch overflow contract, the
versioning/stability policy, and a migration guide.

---

## Client SDKs

Idiomatic, hand-written client libraries are available for seven languages. Each
wraps the same gRPC API, takes the same connection config (`host`, `port`,
`api_key`, optional TLS CA cert), and exposes every RPC including the streaming
`Find` and `Watch` calls.

| Language | Install | Reference |
|---|---|---|
| Python | `pip install filedbv2` | [clients/python](clients/python/README.md) |
| JavaScript / TypeScript | `npm install filedbv2` | [clients/js](clients/js/README.md) |
| PHP | `composer require srjn45/filedbv2` | [clients/php](clients/php/README.md) |
| Java | `com.srjn45:filedbv2-client` (Maven Central) | [clients/java](clients/java/README.md) |
| Ruby | `gem install filedbv2` | [clients/ruby](clients/ruby/README.md) |
| Rust | `cargo add filedbv2` | [clients/rust](clients/rust/README.md) |
| C# / .NET | `dotnet add package FileDBv2.Client` | [clients/csharp](clients/csharp/README.md) |

Prefer to generate your own? The checked-in [OpenAPI spec](docs/openapi/filedb.swagger.json)
covers every RPC — see [Getting Started](docs/getting-started.md#client-sdks).

---

## Documentation

| Document | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Install, run, first queries, TLS, config file, secondary indexes, metrics, logging, health probes |
| [Architecture](docs/architecture.md) | Storage model, write/read paths, compaction, secondary indexes, crash safety |
| [Embedding](docs/embedding.md) | Use FileDB as an in-process Go library: `filedb`/`engine` API, keyed ops, CAS, Watch, migration, versioning policy |

---

## Build from source

```bash
git clone https://github.com/srjn45/filedbv2
cd filedbv2
make build          # produces bin/filedb and bin/filedb-cli
make test           # run tests with race detector
make proto          # regenerate gRPC code (requires buf)
```

---

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for how to
build, test, and submit changes.

---

## License

MIT — see [LICENSE](LICENSE).

---

*Rebuilt from [FileDB PHP](https://github.com/srjn45/FileDB-php) — original college project, now in Go.*
