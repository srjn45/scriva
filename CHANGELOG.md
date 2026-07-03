# Changelog

All notable changes to FileDB v2 are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Versioning & stability policy

FileDB is **pre-1.0**. Per semver, a `0.y.z` project makes no compatibility
promise across minor (`0.y`) bumps, and FileDB uses that latitude deliberately
while the API settles:

- **Minor bumps (`v0.(y+1).0`) may include breaking changes** — to the Go
  embedding API, the gRPC/REST surface, or the CLI. Every such change is called
  out in this file with a migration note.
- **Patch bumps (`v0.y.(z+1)`) are bug-fix only** and never change a documented
  API or the on-disk format.
- **Import paths for the embedding packages are stable.** `engine`, `store`,
  `query`, and `filedb` are public and will not move back under `internal/`.
- **The on-disk segment format is additive.** New fields are `omitempty`, so a
  newer engine reads older segments without migration.

Depend on a tagged version (`go get …@v0.y.0`), read this changelog before
upgrading, and expect the occasional mechanical migration on a minor bump. When
the surface has proven itself, it will be frozen under `v1.0.0`, after which the
usual "no breaking changes without a major bump" guarantee applies. See
[`docs/embedding.md`](docs/embedding.md#stability-and-versioning) for the
embedding-specific contract.

---

## [0.2.0] — unreleased

This release makes FileDB **embeddable as a Go library** for the first time, in
addition to the standalone server. The storage engine can now run entirely
in-process — no gRPC, no network, no daemon — via the new `filedb` façade and the
promoted public `engine` / `store` / `query` packages. It also folds in the
durability-hardening and query-at-scale work that landed since v0.1.0.

### Added — embedding surface

- **Public engine packages.** `internal/engine`, `internal/store`, and
  `internal/query` are promoted to public `engine/`, `store/`, and `query/`. You
  can now `go get github.com/srjn45/filedbv2/engine` and open a database
  in-process. A CI gate (`make deps-check`) keeps the engine free of
  gRPC/protobuf/Prometheus/cobra dependencies, and an `embeddemo/` module proves
  the engine builds standalone. (EMB-1, EMB-3)
- **`filedb` façade** — new top-level package. `filedb.Open(dir, opts…)` roots a
  store at a directory and lazily opens-or-creates named collections with
  per-collection options (`WithUniqueIndex`, `WithCollectionSyncMode`, …).
  Existing collections on disk are discovered automatically; `db.Engine()` drops
  to the underlying `*engine.DB`. (EMB-2)
- **Embedded durability default** — a DB opened via `filedb.Open` defaults every
  collection to `SyncModeInterval` at a 1s cadence (bounded crash-loss window
  without a per-write fsync); opt back into `SyncModeAlways` per collection. The
  raw engine default (`SyncModeNone`) is unchanged. (OPS-1)
- **Caller-supplied string primary keys** — `InsertWithKey`, `FindByKey`,
  `UpdateByKey`, `DeleteByKey` on `Collection`. Keys live in a reserved `_key`
  field enforced unique by a secondary index; O(1) lookup. New typed errors
  `engine.ErrKeyNotFound` and `engine.ErrReservedField`. (KEY-1)
- **Unique secondary indexes** — `EnsureUniqueIndex(field)` and the typed
  `engine.ErrDuplicateKey`, enforced on insert, update, and `CommitTx`; the
  unique flag survives persist/reopen/rebuild. (KEY-2)
- **Per-record revisions + compare-and-swap** — every record carries a monotonic
  `Rev` (`1` on insert, `+1` per update). New `engine.Record{ID,Key,Rev,Ts,Data}`
  and `Get`/`GetByKey` expose it without a body read; `UpdateIfRev` (optimistic
  concurrency) and `UpdateIfMatch` (predicate) apply a write only if a condition
  on the current record still holds, in a single critical section. A stale
  revision, false predicate, or missing key is a clean `(false, nil)` no-op.
  `store.Entry` and `ScanResult` gain `Rev`. (KEY-3)
- **Upsert** — `Upsert(key, data)` inserts-if-absent / replaces-if-present in one
  critical section, returning the resulting `Record` and emitting the matching
  Watch event. (KEY-4)
- **Count / exists helpers** — `Count(filter)` returns the number of matching
  live records without materializing them (O(1) for match-all and indexed
  equality); `Exists(key)` is an O(1) key-presence check. (QRY-3)
- **Bulk import** — `Collection.LoadJSONL(r, keyField)` streams NDJSON through the
  normal write path (indexes, uniqueness, durability, Watch all apply) as an
  **atomic, all-or-nothing** batch; errors name the 1-based line number and leave
  the collection untouched. (OPS-3)
- **`docs/embedding.md`** — full API reference for the embedding surface:
  façade, keyed ops, CAS, upsert, querying, the in-process Watch contract
  (including the `OpOverflow` resync sentinel), durability modes, `store.Entry`,
  the versioning/stability policy, and a JSON-store migration guide with a worked
  example. A runnable `examples/watch` program and an `Example_watch` test
  demonstrate in-process subscriptions. (EMB-4, OPS-2)
- **`engine.DB.CollectionWithConfig(name, cfg)`** — open-or-create a collection
  with an explicit per-collection config (lets the façade give each collection
  its own durability/compaction settings).

### Added — durability, correctness & query (since v0.1.0)

- **Durability hardening** — directory fsync on segment rotation, atomic
  off-hot-path `meta.json` writes, and propagated rotation errors.
- **Idle-transaction reaping** — `--tx-timeout` GCs abandoned `BeginTx` sessions.
- **Watch overflow signal** — a slow subscriber now receives an `OpOverflow`
  sentinel (resync cue) instead of silently dropping events.
- **Per-record CRC32C checksums** — every segment entry carries a CRC32C
  (Castagnoli) checksum, so silent on-disk bit-rot is caught on read. Legacy
  checksum-less lines still decode.
- **Streaming, push-down `Find`** — `limit`/`offset`/`order_by` are pushed into
  the engine and results stream as they are read, with context cancellation; a
  limited query is bounded by page size, not collection size.
- **Typed, directional `order_by`** — ordering uses the same type-aware
  comparison as the filter operators (numbers order numerically).
- **Range-capable secondary indexes** — indexed `gt`/`gte`/`lt`/`lte` lookups.
- **TTL / expiring records** — optional per-record deadlines and a
  collection-level default (`--default-ttl`); expired records are hidden from
  reads immediately and reclaimed by compaction.

### Notes

- No breaking changes to the v0.1.0 gRPC/REST API or CLI. The engine package
  move (`internal/…` → public) affects only code that imported the previously
  internal packages — external consumers could not, so this is additive in
  practice.

---

## [0.1.0] — 2026-06-29

Initial release: the feature-complete core.

- Append-only NDJSON storage engine with segments, in-memory index
  (SHA-256-checksummed), background compaction, and crash recovery.
- gRPC API with REST gateway (grpc-gateway) over TCP and Unix socket; API-key
  auth.
- Secondary indexes, transactions (`BeginTx`/`CommitTx`/`RollbackTx`), and a
  server-streaming `Watch` change feed.
- Configurable durability (`--sync none|always|interval`), optional TLS, YAML
  config file, and Prometheus metrics.
- Full CLI client with interactive REPL, one-shot commands, and `.fql` batch
  scripting.
- Cross-compiled release archives (linux/darwin/windows × amd64/arm64) and a
  Docker image published to GHCR via GoReleaser on `v*` tags.
- Idiomatic client SDKs for Python, JavaScript/TypeScript, PHP, Java, Ruby,
  Rust, and C#/.NET, plus a generated OpenAPI spec and a React web admin UI.

[0.2.0]: https://github.com/srjn45/filedbv2/releases/tag/v0.2.0
[0.1.0]: https://github.com/srjn45/filedbv2/releases/tag/v0.1.0
