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

### Added

- **S4 — per-tenant quotas & limits.** A collection can now carry an **optional
  write-path budget** so a single tenant cannot consume unbounded disk or record
  count. It is **opt-in** and fully backward compatible: a collection with no
  quota is unlimited, exactly as before.
  - New config-file `quotas:` section mapping a collection name to a
    `max_records` and/or `max_bytes` budget (either may be `0`/omitted for
    unlimited). It is **config-file only** — a per-collection map does not fit a
    flat flag. The embedded façade gets matching `filedb.WithMaxRecords` /
    `filedb.WithMaxBytes` collection options for parity.
  - **Enforcement lives in the engine, on the write path**, before the durable
    append — so a refused write persists nothing and mutates no index. A write
    that would push the collection past either budget is rejected with a new
    typed `engine.ErrResourceExhausted`, which the server maps to gRPC
    **`ResourceExhausted`**. `max_bytes` is measured against the collection's
    total on-disk segment size (the same figure `CollectionStats.SizeBytes`
    reports).
  - **Quotas gate the creation of new records only** — `Insert`, `InsertMany`,
    keyed insert, an *inserting* `Upsert`, and transaction inserts. An in-place
    `Update`/`UpdateByKey`, a compare-and-swap, an `Upsert` that *replaces*, and
    a `Delete` are **never refused**, so a tenant sitting at its limit can still
    edit or delete to recover. `InsertMany` and `CommitTx` are checked as a
    **whole batch atomically**: a batch that would breach the budget writes
    nothing. (`InsertMany` is now a genuinely atomic engine operation.)
  - **Observability:** a new `filedb_quota_rejected_total{collection}` counter
    tracks refused writes, and the per-collection gauges gain
    `filedb_collection_bytes{collection}` alongside the existing record/segment
    gauges, so consumption and the rejections it triggers are both visible. The
    metric is wired via a server-side observer hook — the engine still imports no
    metrics package. No proto/API change.
  - **Per-key quotas are deferred:** the engine has no key identity on the write
    path, so this ships per-collection only (which satisfies the tenancy budget
    goal). See [`docs/getting-started.md`](docs/getting-started.md#per-collection-quotas)
    for a YAML example and [`docs/architecture.md`](docs/architecture.md#quotas)
    for how write-path enforcement works.
- **S3 — per-collection ACLs on scoped keys.** An API key can now be **confined
  to a named set of collections** instead of reaching every collection. It is
  **opt-in** and fully backward compatible: a key with no allow-list keeps
  cluster-wide access.
  - New optional `collections:` list on a `keys:` entry: when present, the key
    may only act on the listed collections; an RPC targeting any other collection
    is rejected with **`PermissionDenied`**. Omitted/empty means **all
    collections** — the historical behaviour.
  - **Enforcement** happens in the auth interceptor, per RPC, *after* the
    principal is resolved and recorded (so an ACL denial is still attributed in
    the S2 audit log). The target collection is read from the request's
    `GetCollection()` accessor: unary RPCs are checked before the handler runs;
    collection-scoped **streams** (`Watch`, `Find`, `Aggregate`) are checked on
    the first client message. RPCs with **no** collection field (e.g.
    `ListCollections`) are not collection-scoped and remain callable by a
    restricted key.
  - **mTLS unchanged:** a certificate-authenticated principal carries no
    allow-list and reaches all collections (per-certificate ACLs remain out of
    scope). ACL changes are picked up on config **reload**. No proto/API change.
  - See [`docs/getting-started.md`](docs/getting-started.md#per-collection-acls)
    for a YAML example and [`docs/architecture.md`](docs/architecture.md#auth)
    for how enforcement works.
- **S2 — audit log.** A durable, append-only record of **who did what**: every
  state-mutating and admin RPC and every rejected authentication attempt. It is
  **off by default** and separate from the request log so it can have its own
  retention and be shipped to a tamper-evident store.
  - New server flag (config: `audit_log`): `--audit-log <path>` writes an
    append-only **NDJSON** stream — one self-contained JSON record per line —
    carrying the **principal** (API-key name, mTLS cert subject, `anonymous` when
    auth is off, or `unauthenticated` for a rejected call), the RPC **method**,
    the **target** (`collection`/`key`/`id` where applicable), the **outcome**
    (`ok` or the gRPC status code), and `auth_failure: true` on a rejected call.
  - **What is recorded:** all writes (incl. keyed, compare-and-swap, `InsertMany`,
    `Upsert`), schema changes (`CreateCollection`/`DropCollection`,
    `EnsureIndex`/`DropIndex`), transaction control, the admin `Compact` and
    `Promote`, and any RPC — read or write — rejected by the auth layer.
    Successful reads are **not** audited. Exactly one record is emitted per RPC.
  - Implemented as a single gRPC interceptor chained **outside** auth (so
    rejected-auth calls are still recorded), reusing the O1 structured-logging
    plumbing with a dedicated JSON audit logger. No proto/API change.
  - See [`docs/getting-started.md`](docs/getting-started.md#audit-log) for the
    format and [`docs/operations.md`](docs/operations.md#audit-log) for the
    retention/rotation runbook.
- **S1 — mutual TLS (client-certificate auth).** The server can now verify
  **client** certificates against a configured CA and authenticate a request by
  its certificate, giving callers a cryptographic identity independent of the
  `x-api-key` header. It is **off by default** and composes with API keys.
  - Two new server flags (config: `tls_client_ca`, `tls_client_auth`):
    `--tls-client-ca <bundle.pem>` sets the CA that signs trusted client certs,
    and `--tls-client-auth off|require|verify-if-given` chooses the policy —
    `require` mandates a valid client cert on every connection, `verify-if-given`
    accepts a cert when presented but does not require one. Both require server
    TLS (`--tls-cert`/`--tls-key`) to also be set.
  - **Composition:** a valid `x-api-key` always wins and its scope is enforced;
    a request with **no** API key but a **verified** client cert is authenticated
    as the certificate's principal (subject **CN**, else the first **SAN**) with
    **read-write** scope. Per-certificate scoping / ACLs are deferred to S3.
  - **Backward compatible:** server-only TLS and plain API-key auth are
    unchanged. Under `--tls-client-auth require`, the local REST gateway dials
    gRPC over the Unix socket so REST keeps working.
  - See [`docs/getting-started.md`](docs/getting-started.md#mutual-tls-client-certificate-auth)
    for a full CA/cert setup walkthrough.

## [0.8.0] — 2026-07-04

Replication & high availability — a node can follow a leader, serve reads from
its applied state, and be promoted on failover — plus a compaction/shutdown
crash-consistency fix.

### Added

- **R1 — leader→follower replication (log shipping).** A follower node tails a
  new server-streaming `Replicate` RPC that ships every committed segment entry
  (post-fsync) tagged with a monotonic **global LSN**, and applies it through the
  engine's normal write path so its primary and secondary indexes match the
  leader's exactly. Replication is **asynchronous** (bounded lag) with a
  per-follower shipped-LSN channel that lays the groundwork for R2.
  - New `ReplicationStatus` RPC (`GET /v1/replication/status`) reports the leader
    LSN and, per connected follower, its shipped LSN, lag, and connect time.
  - Run a node as a follower with `--replicate-from <leader-addr>` (config:
    `replicate_from`). A fresh follower **bootstraps from a `Snapshot`** and then
    catches up from the stream; a restarted follower **resumes from its persisted
    applied-LSN** with no gaps and no duplication (apply is idempotent by record
    revision). Tune the leader's in-memory resume buffer with
    `--replication-ring-size` (config: `replication_ring_size`, default 8192).
  - The embeddable `engine` package stays dependency-free: it exposes the LSN
    feed and apply as plain Go types (`engine.ReplicationEntry`,
    `DB.SubscribeReplication`, `DB.ApplyReplication`, `DB.ReplicationStatus`); the
    server maps them to proto. Leader-side sequencing is opt-in via
    `CollectionConfig.ReplicationRingSize`, so the default/embedded write path is
    unchanged.
  - New on-disk file `replication.json` at the data-dir root holds the leader LSN
    and follower applied-LSN watermarks (additive; absent on non-replicated DBs).

- **R2 — read replicas & follower reads.** A node started with
  `--replicate-from` now serves read RPCs (`Find`, `FindById`, `FindByKey`,
  `Aggregate`, plus `CollectionStats`/`ListCollections`/`ListIndexes`/`Watch`)
  from its applied state, so reads scale horizontally across followers.
  - **Writes are refused on a follower** with a typed `FAILED_PRECONDITION` and
    the message `read-only replica; write to the leader`. The guard covers every
    mutating RPC — `Insert`/`InsertMany`/`Update`/`Delete`, the keyed and
    compare-and-swap writes (`Upsert`/`UpdateByKey`/`DeleteByKey`/`UpdateIfRev`),
    `CreateCollection`/`DropCollection`, `EnsureIndex`/`DropIndex`, the
    transaction RPCs, and `Compact`. It is a single pair of gRPC interceptors
    installed only in follower mode, keyed on the generated method names, so the
    read-only contract lives in one auditable place.
  - **Observable staleness bound.** `ReplicationStatusResponse` gains an additive
    `applied_lsn` field: query a follower's `ReplicationStatus`
    (`GET /v1/replication/status`) for its applied LSN and diff it against the
    leader's `leader_lsn` to bound how stale a follower read may be. `applied_lsn`
    is 0 on a leader.
  - The embeddable `engine` package is unchanged — role/routing lives entirely in
    the server layer; the engine only exposes the existing `DB.AppliedLSN()`.

- **R3 — manual failover & role management.** A new admin **`Promote`** RPC
  (`POST /v1/replication/promote`) and `filedb-cli promote` command flip a
  caught-up follower into a leader: it stops replicating from its upstream, lifts
  the read-only guard, and begins accepting writes — the documented path for
  recovering from a leader loss without an external coordinator.
  - **Guard against silent divergence.** Promotion is refused with a typed
    `FAILED_PRECONDITION` when the follower's replication lag (last-known leader
    LSN minus applied LSN) exceeds a configurable threshold — `--promote-max-lag`
    (config: `promote_max_lag`, default 0 = must be fully caught up). Pass
    `--force` (proto `force`) to override the guard when the leader is
    unrecoverable and some divergence is acceptable. Promoting a node that is not
    a follower is refused (nothing to promote). The response reports the new
    `role`, the LSN the new leader continues from, and the `lag` at promotion.
  - **The read-only guard is now dynamic.** The R2 follower write-rejection
    interceptors consult the node's role on every call instead of relying on
    their mere presence, so a `Promote` lifts them live — no restart. A promoted
    leader reseeds its LSN counter above the replicated tail, so it never reuses
    an LSN the old leader assigned.
  - **`Promote` requires a read-write API key.** Finer-grained admin ACLs (an
    admin scope) are deferred to S3; until then a read-write key is the admin
    boundary.
  - The embeddable `engine` package stays proto-free: the role-flip and lag guard
    live in `engine` as plain Go (`DB.Promote`, `DB.IsFollower`,
    `DB.NoteLeaderLSN`, typed `engine.ErrReplicaLagExceeded`/`ErrNotFollower`);
    the server maps them to proto and enforces auth. Promotion is **one-way** —
    automatic leader election (consensus) remains out of scope. See
    [`docs/operations.md`](docs/operations.md) for the manual-failover runbook.

### Fixed

- **Compaction / shutdown crash consistency** (#68) — four stacked defects that
  could make a collection reopen empty (or, in one window, silently lose sealed
  data) even though the segments held every live record:
  - **`Close()` now serializes with an in-flight compaction pass** (via
    `compactMu`) instead of persisting the final index while the pass is
    mid-swap; a pass that was still blocked on the lock when Close finished
    aborts instead of mutating the layout afterwards.
  - **Open self-heals a dangling index.** A checksum-valid `index.json` whose
    entries reference segment files that no longer exist — or offsets past a
    segment's end — is treated as stale and rebuilt from the segments (the
    documented source of truth), along with the secondary indexes. Directories
    already corrupted by earlier builds heal on first open.
  - **The compaction swap is crash-atomic.** The pass now records a durable
    swap manifest (`compact.manifest`) before touching any segment file,
    renames temps over their final names first, deletes only old segments whose
    names were not reused, and retires the manifest after the post-swap index
    persist. A leftover manifest at open rolls the swap forward idempotently
    and forces an index rebuild; the old remove-then-rename order could strand
    the only copy of the sealed data in `.compact_*` temp files an open never
    discovers. Swap manifests are excluded from snapshots for the same reason
    `index.json` is.
  - **Segment rotation no longer reuses a live segment's name.** The new
    active segment is numbered one past the highest segment on disk instead of
    by segment count; after a compaction renumbered the sealed set, the old
    count-based name could collide with the just-sealed segment, and the next
    pass would then delete that file out from under the active writer.
  - On-disk format: unchanged (the manifest is transient); no migration needed.

## [0.7.0] — 2026-07-04

This release rolls up the operability/observability and network-API-parity work
accumulated since v0.3.0, plus the follow-on sweep that brought all seven client
SDKs up to the new wire surface. It collapses the interim internal milestone
labels (v0.6.0 operability, v0.7.0 API parity) into a single tagged release. No
breaking changes to existing behaviour; every addition is opt-in or additive.

**Operability & observability.** The server is no longer a black box:
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

**Network/engine API parity.** The wire API is catching up to the
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
- **Aggregations — count / group-by / numeric (N4).** A new server-streaming
  **`Aggregate`** RPC (`POST /v1/{collection}/aggregate`) computes a `count` and
  the numeric aggregations `sum`/`avg`/`min`/`max` over the live records matching
  the **same `Filter` as `Find`**, optionally grouped by a field — so clients no
  longer pull a whole collection just to count or total it.
  - **Ungrouped** (`group_by` empty) streams a single result over the whole
    filtered set; **grouped** streams one result per distinct `group_by` value, in
    ascending group order. Each result carries the type-preserved `group_value`
    (number/string/bool, or null for the whole-set group), the `count`, and — when
    a numeric `field` is named — `sum`/`avg`/`min`/`max` with a `numeric` flag.
    Only records whose `field` is numeric contribute (per the same `query.AsNumber`
    rules the filter/sort use); `avg` divides by that numeric count (SQL `AVG`
    semantics, ignoring absent/non-numeric values). `sum`/`avg`/`min`/`max` require
    a `field` — requesting one without it is `InvalidArgument`.
  - **Runs entirely in the engine, streaming — never materialised.** New
    `engine.Aggregate` (in `engine/aggregate.go`) folds each matching record into
    its group's accumulator, so memory is bounded by the number of **distinct
    groups**, not the collection size. A whole-set count reuses the existing
    `Collection.Count` fast path (answered from the primary/secondary index without
    reading segments where possible); grouped/filtered aggregations reuse the same
    index-aware `forEachMatch` scan as `Count`/`Find`. The engine returns plain Go
    structs (`AggregateSpec` / `GroupResult`) the server maps to proto, so the
    embeddable engine keeps **zero** transport dependencies (enforced by
    `make deps-check`). A new exported `query.AsNumber` gives the numeric reduction
    the exact type rules of `query.Compare`.
  - New CLI `aggregate <collection> [filter-json]` with `--group-by`, numeric
    `--field`, and comma-separated `--aggs count,sum,avg,min,max` (default
    `count`), reusing the existing filter-JSON style. The 7-language SDK parity
    sweep is a separate follow-on wave.

**All seven client SDKs brought to v0.7.0 wire parity.** The follow-on sweep
promised by N1–N4 landed: every SDK now covers the full new surface —
keyed CRUD / `upsert` / `find-by-key` / `update-by-key` / `delete-by-key` and
`update-if-rev` CAS with `key`/`rev` surfaced on records and typed
not-found / already-exists errors (N1); the `fields` read projection (N2);
multi-field `order_by` with opaque keyset `page_token` pagination (N3); and the
streaming `aggregate` / `count` / `group_by` RPC (N4). Each client refreshed its
vendored `filedb.proto` and regenerated stubs against the current API; examples
and READMEs demonstrate the new methods. Python (#56), JavaScript/TypeScript
(#58), PHP (#57), Rust (#59), Java (#60), Ruby (#62), and C#/.NET (#61). The Go
server CI is unchanged (it does not build the clients); each client was verified
against a locally built v0.7.0 server where a toolchain was available.

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
