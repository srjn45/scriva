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

## [Unreleased]

**Operability & observability (v0.6.0).** The server is no longer a black box:
it emits structured request logs (PR-A), exposes health/readiness probes for
load balancers and orchestrators (PR-A), can shed load under pressure with
opt-in concurrency, in-flight, and per-key rate limits (PR-B), can emit
OpenTelemetry traces to an OTLP collector (PR-C), and surfaces per-query cost
through a slow-query log and a rows-scanned metric (PR-D). No breaking changes —
logging defaults to human-readable text at `info`, the probes are additive, and
every limit, tracing, and the slow-query log are off by default.

### Added

- **Structured, leveled logging (O1).** The server layer now logs through the
  standard library `log/slog` (zero new dependencies). A unary **and** stream
  gRPC interceptor emits exactly one structured record per RPC — `method`,
  `principal` (the authenticated API-key name), `duration`, and status `code` —
  at `info` for success and `error` for failure. Two flags configure output:
  `--log-level` (`debug|info|warn|error`, default `info`) and `--log-format`
  (`json|text`, default `text`), also settable via `log_level` / `log_format`
  in the config file. The embeddable `engine`/`store`/`query` packages remain
  free of any logging dependency (enforced by `make deps-check`).
- **Health & readiness (O2).** The standard `grpc.health.v1.Health` service is
  registered on the TCP and Unix gRPC servers; it reports `SERVING` once
  listeners are up and flips to `NOT_SERVING` at the start of graceful shutdown
  so in-flight RPCs drain cleanly. The REST gateway gains `GET /healthz`
  (liveness) and `GET /readyz` (readiness — `200` when the DB is open and the
  data directory is writable, `503` otherwise).
- **Request backpressure & rate limiting (O3).** Three opt-in, off-by-default
  controls let the server shed load with a typed `RESOURCE_EXHAUSTED` instead of
  growing goroutines/FDs without bound. `--max-concurrent-streams` caps the
  HTTP/2 streams per gRPC connection (`0` = library default). `--max-inflight`
  installs a server-wide in-flight semaphore: once the ceiling is saturated,
  further calls are rejected immediately rather than queued (`0` = unlimited).
  `--rate-limit` throttles each API-key principal to a requests/sec budget via
  an independent token bucket, so one principal being throttled never affects
  another (`0` = disabled). All three are also settable via
  `max_concurrent_streams` / `max_inflight` / `rate_limit` in the config file.
  The limiter interceptors chain after auth (to read the resolved principal) and
  before logging (so a shed request is still logged); the embeddable engine
  keeps its zero transport dependencies (rate limiting lives entirely in the
  server layer via `golang.org/x/time/rate`).
- **OpenTelemetry tracing (O4).** Opt-in distributed tracing, off unless
  `--otlp-endpoint` is set. When enabled, a tracing interceptor (unary **and**
  stream) starts one span per RPC named after the method and tagged with
  `rpc.method` and the returned `rpc.grpc.status_code`; it is chained outermost
  so the span wraps the whole handler and its context flows into the engine.
  `--otlp-sample-ratio` (default `1.0`) sets the root sampling fraction. Spans
  export to an OTLP/gRPC collector. Engine scan and compaction cost surface as
  child `engine.scan` / `engine.compaction` spans through the existing
  `engine.CollectionConfig` hook pattern (new `OnScan` hook), so a slow `Find`
  shows which segment scan dominated — the embeddable engine gains **no** OTel
  dependency (the server owns the SDK; enforced by `make deps-check`). Both flags
  are also settable via `otlp_endpoint` / `otlp_sample_ratio` in the config file.
- **Slow-query log & scan stats (O5).** `Collection.ScanStream` now returns a
  `ScanStats` value alongside its error — `RowsScanned` (live records examined),
  `RowsReturned` (records emitted), and `IndexUsed` (whether a secondary index
  produced the candidate set). It is plain data, so the embeddable engine gains
  no dependency; the planner already knew index-vs-scan, and this simply surfaces
  it. The server turns it into two operator signals: a new `--slow-query-ms`
  flag (also `slow_query_ms` in the config file, `0` = disabled) logs any `Find`
  reaching that duration at **WARN** with the filter shape (fields/ops, never
  values), `rows_scanned` vs `rows_returned`, `index_used`, and `duration`; and
  a `filedb_scan_rows_scanned` Prometheus histogram records rows examined per
  query, labelled by collection. Together they let an operator find unindexed hot
  queries from logs and metrics. The scan cost reaches metrics via the same
  server-layer hook pattern as compaction — the engine never imports metrics
  (enforced by `make deps-check`).

**Network/engine API parity (v0.7.0).** The wire API is catching up to the
embedded engine: the richest engine operations — natural string keys, upsert, and
optimistic-concurrency updates — are now reachable from any gRPC/REST client, not
just an in-process Go program. Additive only: new field numbers and new RPCs, so
pre-existing clients are unaffected.

### Added

- **Keyed CRUD, Upsert, CAS & Rev over the wire (N1).** The keyed operations the
  embedded engine has had since v0.2.0 are now exposed over gRPC/REST, mapping
  straight onto the engine methods:
  - New RPCs **`Upsert`** (insert-or-replace by key), **`FindByKey`**,
    **`UpdateByKey`**, **`DeleteByKey`**, and **`UpdateIfRev`** (compare-and-swap
    on a record's revision). A **keyed insert** is expressed by setting the new
    optional `key` field on the existing `Insert` request (`POST
    /v1/{collection}/records` with a `key`, `.../records:upsert`,
    `.../keys/{key}`, and `.../keys/{key}:cas` on REST) — empty `key` preserves
    the unchanged server-assigned-id behaviour.
  - Every record-bearing response now carries **`key`** and **`rev`**: `Record`
    gained fields 5/6 and `InsertResponse`/`UpdateResponse` the same pair (all
    additive field numbers).
  - Typed engine errors map to stable gRPC codes: `engine.ErrDuplicateKey` →
    `AlreadyExists`, `engine.ErrKeyNotFound` → `NotFound`, and setting the
    reserved `_key` field via `data` → `InvalidArgument`. `UpdateIfRev` treats a
    stale revision or missing key as a clean `swapped=false` no-op, not an error.
  - New CLI commands: `upsert`, `find-by-key`, `update-by-key`, `delete-by-key`,
    `update-if-rev`, plus an `insert --key` flag for keyed create. The 7-language
    SDK parity sweep for these operations is a separate follow-on wave.
- **Field projection on reads (N2).** `Find`, `FindById`, and `FindByKey` gained
  an optional repeated **`fields`** projection: when non-empty, only those
  top-level fields are returned in each record's data, so wide documents transmit
  only what the caller asked for. `id`, `key`, and `rev` are **always** included;
  an empty `fields` returns the full record (backward compatible); an unknown or
  absent field is silently omitted (not an error). The engine applies the
  projection in `ScanStream` (via a new exported `engine.ProjectData` helper)
  **after** filtering and ordering, so an `order_by` field need not be projected
  and the embeddable engine keeps its zero transport dependencies. New CLI flag
  `--fields` (comma-separated) on `find`, `get`, and `find-by-key`. Additive
  field numbers only; the SDK parity sweep is a separate follow-on wave.
- **Keyset (cursor) pagination + multi-field order_by (N3).** `Find` can now sort
  by several fields and paginate deep result sets in O(page) instead of O(offset):
  - **Multi-field, directional sort.** New `repeated OrderBy { field, desc }
    order_by_fields` on `FindRequest` applies sort keys lexicographically (first
    dominant), each with its own direction, reusing the same type-aware
    `query.Compare` as the filter operators. The record `id` is always the implicit
    final tiebreaker, so the ordering is **total** and pages are stable.
  - **Opaque keyset cursor.** New `string page_token` on both `FindRequest` and
    `FindResponse`. A response carries the next `page_token` on its final streamed
    message when more rows remain (empty = last page); feed it back to seek strictly
    past the rows already returned rather than counting past them. Concatenated pages
    cover every matching row **exactly once — no duplicates, no gaps — even under
    concurrent inserts**. The cursor is a base64 of compact JSON encoding the last
    `(sort-key tuple, id)`; its codec lives entirely in the engine
    (`engine.ScanOptions.PageToken` / `ScanStats.NextPageToken`, new
    `engine.SortField`), so the embeddable engine keeps **zero** transport
    dependencies. A malformed token → `engine.ErrInvalidPageToken` → gRPC
    `InvalidArgument`. `offset` is retained for backward compatibility.
  - New CLI: repeatable `--order-by field[:asc|:desc]` (multi-field) and a
    `--page-token` passthrough on `find`; a full page prints its
    `next-page-token:` for the next fetch. The 7-language SDK parity sweep is a
    separate follow-on wave.
  - **⚠️ Deprecation / migration.** The scalar `FindRequest.order_by` and
    `descending` fields are now marked `[deprecated = true]`. They still work — and
    are honoured **only** when `order_by_fields` is empty — for one release, then
    will be removed (a minor-bump break, per the versioning policy above). **Migrate**
    `order_by:"f", descending:d` → `order_by_fields:[{field:"f", desc:d}]`. The CLI
    `--descending` flag likewise still applies to a single bare `--order-by`; prefer
    the `field:desc` form.

## [0.3.0] — 2026-07-03

**Client parity release.** Per-record TTL is now exposed over the wire, and all
seven client SDKs reach the full server API surface. No breaking changes — every
addition is optional and backward compatible.

### Added

- **TTL on the gRPC/REST API (F1 completion).** `ttl_seconds` on `Insert`,
  `InsertMany`, and `Update`, and `default_ttl_seconds` on `CreateCollection`.
  On insert, `0` inherits the collection default and a value `> 0` overrides it;
  on update, `0` is sticky (keeps the record's existing deadline) and `> 0`
  resets it; negative values are rejected. TTL was previously usable only via the
  embedded engine and the server-wide `--default-ttl`; it is now a first-class
  wire parameter. (#38)
- **All seven client SDKs at API parity.** Python (#39), JavaScript/TypeScript
  (#40), PHP (#41), Java (#42), Ruby (#43), Rust (#44), and C#/.NET (#45) each
  gain `compact`, `snapshot` / snapshot-to-file, and the per-record TTL
  parameters, and surface the `OVERFLOW` watch op.

### Fixed

- **JavaScript client repaired** (#40) — restored to a working state alongside
  the parity additions.
- **Stale vendored proto** refreshed in the Java, Rust, and C# clients. Each
  vendored its own copy of `proto/filedb.proto` that had drifted behind
  canonical — missing the `Compact`/`Snapshot` RPCs, the TTL fields, and the
  `OVERFLOW` enum value — so the generated stubs could not reach the new surface.

### Notes

- Backward compatible: all new fields are additive; omitting them preserves prior
  behavior. No migration needed.

---

## [0.2.1] — 2026-07-03

Maintenance release.

- **Module hygiene** — renamed the stray top-level `roadmap.md` to
  `docs/warden-roadmap.md` so it no longer ships in the module root. No API,
  on-disk-format, or behavior change.

---

## [0.2.0] — 2026-07-03

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

[0.3.0]: https://github.com/srjn45/filedbv2/releases/tag/v0.3.0
[0.2.1]: https://github.com/srjn45/filedbv2/releases/tag/v0.2.1
[0.2.0]: https://github.com/srjn45/filedbv2/releases/tag/v0.2.0
[0.1.0]: https://github.com/srjn45/filedbv2/releases/tag/v0.1.0
