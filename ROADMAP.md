# FileDBv2 — Project Roadmap & Status

## What This Is

A ground-up rebuild of [FileDB PHP](https://github.com/srjn45/FileDB-php) (a college-era flat-file JSON database) into a production-quality Go service. The goal is a **lightweight, embeddable, local-first database** that:

- Ships as a single binary with zero runtime dependencies
- Stores data as human-readable NDJSON files on disk
- Exposes a gRPC API (with REST gateway) over TCP and Unix socket
- Has a full CLI client with interactive REPL, one-shot commands, and batch scripting
- Auto-generates language-specific client SDKs from a `.proto` file

---

## Design Decisions (Why We Made These Choices)

| Decision | Choice | Reason |
|---|---|---|
| Language | Go | Single static binary, goroutines for concurrency, fast compile, easy cross-compile |
| Storage format | Append-only NDJSON | Human readable, crash-safe (no in-place writes), sequential append = fastest disk op |
| Segments | Multiple files per collection, capped by size (default 4MB) | Bounds RAM usage, enables background compaction without blocking writes |
| Concurrency model | Pessimistic locking (`sync.RWMutex` per collection) | Write lock held for microseconds (just append + index update), so reader starvation never happens in practice |
| API protocol | gRPC primary + REST via grpc-gateway | gRPC gives persistent dual-channel connections (HTTP/2 multiplexed), bidirectional streaming, auto-generated SDKs; REST for curl/browser |
| Local transport | Unix domain socket | Bypasses TCP stack entirely for CLI on same machine |
| Compaction trigger | Dirty ratio (>30%) OR time interval (5min) | Dirty ratio prevents wasted space; timer catches slow-write collections |
| Auth | API key via gRPC metadata (`x-api-key`) | Simple, stateless, good enough for local/trusted-network use |

---

## Architecture in One Page

```
data/
└── users/                         ← one dir per collection
    ├── seg_000001.ndjson           ← sealed (immutable, old entries)
    ├── seg_000002.ndjson           ← sealed
    ├── seg_000003.ndjson           ← active (current append target)
    ├── index.json                  ← id → {segment, byte_offset} + SHA-256 checksum
    └── meta.json                   ← id counter, created_at

Each line in a segment:
{"id":1,"op":"insert","ts":"2026-03-29T10:00:00Z","data":{"name":"alice"}}
{"id":1,"op":"update","ts":"2026-03-29T11:00:00Z","data":{"name":"alice2"}}
{"id":2,"op":"delete","ts":"2026-03-29T12:00:00Z"}

Write path:  append line → update in-memory index → (rotate if size ≥ limit)
Read path:   index lookup → seek to offset → read one line
Compaction:  resolve latest per id → write clean segments → atomic swap → rebuild index
```

---

## Release status

**v0.3.0 — released** (`v*` tag → goreleaser → GitHub Releases + GHCR). The core
is feature-complete and the entire post-v0.1.0 plan
([**docs/roadmap-v0.2.md**](docs/roadmap-v0.2.md), milestones v0.2.0–v0.5.0) has
shipped, including per-record TTL over the wire and full API parity across all
seven client SDKs. See [CHANGELOG.md](CHANGELOG.md) for the per-release
breakdown. The next arc is planned in
[**docs/roadmap-v0.6.md**](docs/roadmap-v0.6.md).

The **v0.8.0 Replication & HA** arc is complete: **R1 — leader→follower log
shipping** (async replication with a monotonic global LSN, snapshot bootstrap +
stream catch-up, `--replicate-from` follower mode, and a `ReplicationStatus`
RPC), **R2 — read replicas** (followers serve reads and reject writes with a
typed `FailedPrecondition`, exposing their applied-LSN staleness bound), and
**R3 — manual failover** (an admin `Promote` RPC/CLI that flips a caught-up
follower to leader, guarded against promoting a lagging replica — see
[docs/operations.md](docs/operations.md)). Automatic leader election stays out
of scope.

---

## What Is Done ✅

### Durability, benchmarks & OpenAPI — shipped
- [x] Configurable durability policy (`--sync none|always|interval`, `--sync-interval`)
  - `engine.SyncMode` on `CollectionConfig`; `Segment.Sync()` (fsync); per-write fsync for `always`; background flush loop for `interval`
  - Wired through `server.Config`, YAML (`sync_mode` / `sync_interval`), and CLI flags with validation
  - Zero-value `CollectionConfig` is now normalized to safe defaults
  - Tests in `engine/durability_test.go` (CRUD + reopen under every mode)
- [x] Engine benchmark suite (`engine/bench_test.go`, `make bench`) — insert per sync mode, FindByID, full vs indexed scan
- [x] OpenAPI/Swagger spec generated from proto (`docs/openapi/filedb.swagger.json`, `make openapi`) — universal client-generation path
- [x] `LICENSE` file added (MIT)

### Web admin UI — shipped
- [x] `clients/web/` — React 18 + TypeScript + Vite + Tailwind CSS browser UI (dark theme)
  - Browse and manage collections (create, drop), full CRUD on records with filter/order/pagination
  - Secondary index management (ensure/drop), collection stats (auto-refresh every 30 s)
  - Live Watch event feed via ReadableStream streaming
  - Connection settings (URL + API key) persisted to `localStorage`
  - CORS middleware added to REST gateway (`server/rest.go`)
  - Custom Watch REST handler (`server/watch_rest.go`) — fills grpc-gateway gap for server-streaming RPCs
  - Vite dev proxy: `/v1` → `:8080` for seamless local development

### Phase 1 — Project Scaffold
- [x] Directory structure: `internal/`, `server/`, `cmd/`, `clients/`, `docs/`, `.github/`
- [x] Go module: `github.com/srjn45/filedbv2` (Go 1.22+)
- [x] `Makefile` with targets: `build`, `test`, `proto`, `lint`, `run`, `cli`, `release`, `clean`
- [x] `buf.yaml` + `buf.gen.yaml` for proto code generation via [buf](https://buf.build)

### Phase 2 — Proto API Contract
- [x] `proto/filedb.proto` — defines all 15+ RPCs
- [x] Generated: `internal/pb/proto/filedb.pb.go`, `filedb_grpc.pb.go`, `filedb.pb.gw.go`
- [x] Full REST annotations via `google/api/annotations.proto`

**RPCs implemented:**
```
CreateCollection  DropCollection  ListCollections
Insert  InsertMany  FindById  Find (streaming)  Update  Delete
Watch (server-streaming change feed)
CollectionStats
EnsureIndex  DropIndex  ListIndexes
BeginTx  CommitTx  RollbackTx
```

### Phase 3 — Storage Engine
- [x] `store/ndjson.go` — Entry struct, Encode/Decode, NewInsert/NewUpdate/NewDelete
- [x] `engine/segment.go` — Append, ReadAt, ScanAll, Seal, crash recovery (partial line truncation)
- [x] `engine/index.go` — In-memory `map[uint64]IndexEntry`, SHA-256 checksum persist/load, Rebuild from segments
- [x] `engine/collection.go` — RWMutex, Insert/Update/Delete/FindByID/Scan, segment rotation, Watch subscribers
- [x] `engine/secondary_index.go` — Field-value → ID set inverted index, EnsureIndex/DropIndex/ListIndexes/IndexLookup, persist/load/rebuild
- [x] `engine/compactor.go` — Background goroutine, dirty-ratio trigger, timer trigger, rebalancer (merge small segments)
- [x] `engine/db.go` — Collection registry, Open/CreateCollection/DropCollection/ListCollections/Close
- [x] `query/filter.go` — FieldFilter, AndFilter, OrFilter, ops: eq/neq/gt/gte/lt/lte/contains/regex

### Phase 4 — Server
- [x] `internal/auth/apikey.go` — gRPC unary + stream interceptors, `crypto/subtle.ConstantTimeCompare`
- [x] `server/config.go` — Config struct with defaults, `EngineConfig()` converter
- [x] `server/grpc.go` — Full `FileDBServer` implementation, proto↔engine mapping, filter conversion
- [x] `server/rest.go` — grpc-gateway bridge (TCP + Unix socket variants)
- [x] `cmd/filedb/main.go` — `cobra` CLI, `serve` subcommand, TCP + Unix socket + REST listeners, graceful shutdown

### Phase 5 — CLI Client
- [x] `cmd/filedb-cli/main.go` — Connection management (Unix socket auto-detect → TCP fallback), auth context
- [x] `cmd/filedb-cli/commands.go` — All commands: collections, create-collection, drop-collection, insert, find, get, update, delete, stats, export, import
- [x] `cmd/filedb-cli/repl.go` — Interactive REPL with readline history, tab-completion scaffold, `use <collection>` context
- [x] `cmd/filedb-cli/batch.go` — `.fql` script runner + stdin pipe support

### Phase 6 — Build Pipeline
- [x] `.github/workflows/ci.yml` — Lint + race tests + build on every push/PR
- [x] `.github/workflows/release.yml` — GoReleaser on `v*` tag push, publishes to GitHub Releases + GHCR
- [x] `.goreleaser.yml` — Cross-compile: linux/darwin/windows × amd64/arm64, Docker image to `ghcr.io/srjn45/filedbv2`
- [x] `Dockerfile` — Multi-stage, Alpine, non-root user

### Phase 7 — Documentation
- [x] `README.md` — Quick start, positioning, links
- [x] `docs/getting-started.md` — Install, server setup, CLI usage, REST examples, filter syntax
- [x] `docs/architecture.md` — Storage model, write/read paths, compaction, crash safety, network layer

### Tests
- [x] `store/ndjson_test.go` — encode/decode parity, delete entry
- [x] `engine/segment_test.go` — append + readAt, scanAll, crash recovery, seal
- [x] `engine/collection_test.go` — insert/findById, update, delete, scan, persist across reopen, concurrent writes (race detector), watcher
- [x] `engine/index_test.go` — Set/Get/Delete, Len, Persist+Load, checksum mismatch, Rebuild from segments
- [x] `engine/compactor_test.go` — isDirty threshold, compact reduces segments, records readable after compact, rebalancer merges tiny segments
- [x] `engine/secondary_index_test.go` — EnsureIndex/DropIndex/ListIndexes, insert/update/delete maintenance, Scan uses index, Scan falls back, Persist+Load, rebuild from existing data, survives compaction
- [x] `query/filter_test.go` — all 8 ops, And/Or/nested, MatchAll, missing field, invalid regex
- [x] `server/grpc_integration_test.go` — in-process gRPC server, CRUD, Find with filter/order/limit, transactions, error paths

**All 50+ tests pass with `go test ./... -race`**

---

## What Is NOT Done ❌

### High Priority

#### 1. Language clients
The proto file is ready. Two strategies, used together:

- **Universal (cheap):** generate clients from the checked-in OpenAPI spec
  (`docs/openapi/filedb.swagger.json`) with `openapi-generator` — covers nearly
  every language with zero hand-written code.
- **Ergonomic (curated):** hand-written SDKs for the highest-value languages
  where an idiomatic wrapper is worth the maintenance. Seven are scoped — see
  [`clients/PLAN.md`](clients/PLAN.md) — but the OpenAPI path means the long tail
  (Ruby, PHP, C#, …) no longer blocks "use from any language."

| Client | Package manager | Status |
|---|---|---|
| `clients/python/` | PyPI: `pip install filedbv2` | ✅ Done |
| `clients/js/` | npm: `npm install filedbv2` | ✅ Done |
| `clients/php/` | Packagist: `composer require srjn45/filedbv2` | ✅ Done |
| `clients/java/` | Maven Central: `com.srjn45:filedbv2-client` | ✅ Done |
| `clients/ruby/` | RubyGems: `gem install filedbv2` | ✅ Done |
| `clients/rust/` | crates.io: `filedbv2` | ✅ Done |
| `clients/csharp/` | NuGet: `FileDBv2.Client` | ✅ Done |

Each client needs:
1. Proto stub generation (language-specific `protoc` plugin or `buf` remote plugin)
2. Package scaffolding (manifest + directory structure)
3. `FileDB` class wrapper — all RPCs with ergonomic method names
4. Connection setup (host, port, API key, optional TLS CA cert; Unix socket for Python/Node)
5. Runnable example program in `examples/`
6. `README.md` + update to `docs/getting-started.md`
7. Publish config for the package registry

### Medium Priority

#### ~~2. `golangci-lint` — stricter rules~~ ✅ Done
`.golangci.yml` now explicitly enables `staticcheck`, `govet`, `nilerr`, and `misspell` in addition to `bodyclose`, `errorlint`, `copyloopvar`. Generated pb code is excluded. All existing violations were fixed.

### Low Priority / Future

#### ~~3. Secondary indexes~~ ✅ Done
`engine/secondary_index.go` — in-memory inverted index (field-value → ID set).
- `EnsureIndex(field)` / `DropIndex(field)` / `ListIndexes()` on `Collection`
- `Scan` uses the index for single eq-filters (O(1)), falls back to full scan otherwise
- Index maintained on Insert/Update/Delete and rebuilt after compaction
- Persisted to `sidx_<field>.json` with SHA-256 checksum, reloaded on startup
- gRPC: `EnsureIndex` / `DropIndex` / `ListIndexes` RPCs + REST via grpc-gateway
- CLI: `ensure-index`, `drop-index`, `indexes` commands

#### ~~4. TLS support~~ ✅ Done
Optional TLS on the TCP gRPC listener via `--tls-cert` / `--tls-key` server flags.
- `credentials.NewTLS()` used for the TCP gRPC server when both flags are set
- REST gateway dials gRPC with `InsecureSkipVerify` for the internal loopback hop
- Unix socket server always uses `insecure.NewCredentials()` (local-only transport)
- CLI `--tls-ca <pem>` builds a `x509.CertPool` and dials with `credentials.NewTLS()`; omit for insecure (or Unix socket auto-detect)

#### ~~5. Config file (`filedb.yaml`)~~ ✅ Done
`server/config.go` — `LoadConfigFile(path)` reads a YAML config file via `gopkg.in/yaml.v3`, falling back to defaults for omitted fields. `--config` flag on the `serve` command loads it before applying CLI flag overrides (CLI always wins).

#### ~~6. Metrics / observability~~ ✅ Done
`internal/metrics/metrics.go` — Prometheus instrumentation via `github.com/prometheus/client_golang`.
- `filedb_collection_records_total` / `filedb_collection_segments_total` — per-collection gauges via a custom `DBCollector` (sampled at scrape time)
- `filedb_compaction_runs_total` / `filedb_compaction_duration_seconds` — counter + histogram per collection (via `OnCompaction` hook in `CollectionConfig`)
- `filedb_grpc_request_duration_seconds` — histogram by method + status code (via unary interceptor)
- Served at `--metrics-addr` (default `:9090`) on `/metrics`; set to empty string to disable

---

## Post-v0.1.0 roadmap (Hardening & Scale)

Derived from the 2026-06-29 codebase review. Full plan with per-item approach,
files, tests, and acceptance criteria in
[**docs/roadmap-v0.2.md**](docs/roadmap-v0.2.md).

| Milestone | Theme | Key items |
|---|---|---|
| **v0.2.0** | Durability & correctness hardening | dir fsync, atomic/off-hot-path `meta.json`, per-record checksums, Watch overflow signal, propagate rotation errors, transaction GC |
| **v0.3.0** | Query at scale | streaming push-down `Find` (honor `limit` before materializing), typed/directional `order_by`, range-capable secondary indexes, context cancellation |
| **v0.4.0** | Feature breadth | TTL / expiring records, backup/snapshot, on-demand compaction |
| **v0.5.0** | Auth & multi-tenancy | multiple scoped, rotatable API keys |

---

## Key Files Reference

| File | Purpose |
|---|---|
| [proto/filedb.proto](proto/filedb.proto) | Single source of truth for all APIs — edit here first |
| [engine/collection.go](engine/collection.go) | Core read/write logic, RWMutex, Watch |
| [engine/compactor.go](engine/compactor.go) | Background compaction goroutine |
| [engine/index.go](engine/index.go) | In-memory index, checksum, rebuild |
| [engine/segment.go](engine/segment.go) | NDJSON file I/O, crash recovery |
| [server/grpc.go](server/grpc.go) | gRPC handlers — proto → engine mapping |
| [cmd/filedb/main.go](cmd/filedb/main.go) | Server binary, startup, graceful shutdown |
| [cmd/filedb-cli/repl.go](cmd/filedb-cli/repl.go) | Interactive REPL |
| [cmd/filedb-cli/commands.go](cmd/filedb-cli/commands.go) | All CLI subcommands |
| [Makefile](Makefile) | All dev tasks |

---

## How to Pick This Up

```bash
cd FileDBv2

# Build
make build

# Run tests
make test

# Start server
make run          # serves on :5433 (gRPC), :8080 (REST), /tmp/filedb.sock

# Use CLI
make cli          # connects to local socket automatically
```

Next logical steps are tracked in [docs/roadmap-v0.2.md](docs/roadmap-v0.2.md).
In order:
1. **v0.2.0 durability hardening** — directory fsync, atomic `meta.json`,
   propagate rotation errors (one PR); then transaction GC, Watch overflow
   signal, per-record checksums.
2. **v0.3.0 query at scale** — streaming push-down `Find`, range indexes.
3. **v0.4.0 features** — TTL records, backup/snapshot, on-demand compaction.
4. **v0.5.0 auth** — multiple scoped, rotatable API keys.
