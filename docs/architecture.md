# FileDB v2 — Architecture

## Overview

FileDB v2 is a lightweight, append-only, file-based document database written in Go. It exposes a gRPC API (with a REST gateway) and stores data as human-readable NDJSON files on disk.

---

## Storage Model

### One directory per collection

```
data/
└── users/
    ├── seg_000001.ndjson   # sealed (immutable)
    ├── seg_000002.ndjson   # sealed
    ├── seg_000003.ndjson   # active (append target)
    ├── index.json          # persisted id → {segment, offset} map
    ├── meta.json           # id counter, created_at
    └── sidx_email.json     # secondary index for "email" field (if created)
```

### Segment files (NDJSON)

Each line is one operation entry:

```json
{"id":1,"op":"insert","ts":"2026-03-29T10:00:00Z","data":{"userName":"admin"},"crc":2872375771}
{"id":1,"op":"update","ts":"2026-03-29T11:00:00Z","data":{"userName":"admin2"},"crc":1483902337}
{"id":2,"op":"delete","ts":"2026-03-29T12:00:00Z","crc":1032541209}
```

- `op` is one of `insert`, `update`, `delete`
- For `delete`, `data` is omitted (tombstone entry)
- The **latest entry for each id wins**

A segment is **sealed** (made immutable) when its file size exceeds `SegmentMaxSize` (default 4 MiB). After sealing a new active segment is created.

#### Per-entry checksums

Every entry carries a `crc` field: a **CRC32C (Castagnoli)** checksum computed over the entry's `id`, `op`, and canonical `data` (the timestamp and the `crc` field itself are excluded, so the value is stable across encode/decode). It is written on `Encode` and verified on `Decode`.

This guards against silent bit-rot in sealed segments: without it, a flipped byte that still parses as JSON would return wrong data with no error. A checksum mismatch instead surfaces as a typed `store.ErrCorruptEntry`, which propagates out of `ScanAll`/`ReadAt` with the segment path and offset.

The field is **backward-compatible**: an entry with no `crc` key (a line written before checksums existed) is decoded without verification. Because compaction rewrites entries through `Encode`, legacy lines gain a checksum the next time their segment is compacted.

---

## Write Path

```
client request
    │
    ▼
Collection.Insert / Update / Delete
    │
    ├── acquire write lock (sync.RWMutex)
    ├── append NDJSON entry to active segment (sequential write)
    ├── update in-memory primary index
    ├── update in-memory secondary indexes (all indexed fields)
    ├── release write lock
    │
    ├── if segment size ≥ limit → rotate (seal + new active)
    └── emit WatchEvent to subscribers
```

Writes are always sequential appends — the fastest possible disk operation.

---

## Read Path

```
FindById:
    acquire read lock → primary index lookup → seek to offset → read one line → decode

Find (scan) without index:
    stream segments in insertion order → skip stale/deleted versions via the
    primary index → apply filter → order/paginate → stream results

Find (scan) with secondary index (single eq or range filter on indexed field):
    secondary index lookup → fetch candidate ids via primary index → filter → stream
```

The in-memory primary index makes `FindById` an O(1) index lookup + one disk seek. A secondary index on a field reduces `Find` with a single equality filter from O(n) to O(1), and a single range filter (`gt`/`gte`/`lt`/`lte`) from O(n) to O(matches) via the index's ordered key view.

### Streaming, push-down `Find`

`Find` is served by the engine's `ScanStream`, which emits matches to the gRPC
stream *as it reads* rather than buffering the whole result set. It never builds
an in-memory `map[id]entry` of the collection: instead it streams each segment
sequentially and treats an entry as live only when the primary index still
points at exactly its `(segment, offset)`, skipping superseded versions and
tombstones. This keeps the deduplication cost off the heap.

`limit`, `offset`, `order_by_fields`, and the `page_token` cursor are pushed
**into** the engine so their cost is paid before materialization:

| Query shape | Rows examined | Memory held |
|---|---|---|
| unordered, `limit > 0` | stops after `offset+limit` matches | O(`offset+limit`) |
| ordered, `limit > 0` | all candidates (needs full comparison) | O(`offset+limit`) — bounded **top-K** buffer |
| ordered, no limit | all candidates | O(matches) — an inherent full sort |

So `Find … limit 10` over a million-row collection reads and holds ~10 rows, not
a million. A `gt`/`lt` predicate on an indexed field is likewise served from the
index's ordered key view (see [Secondary Indexes](#secondary-indexes)); ordering
by a non-indexed field still examines every candidate.

**Ordering guarantees.** `order_by_fields` is a list of `{field, desc}` sort keys
(`ScanOptions.Sort` in the engine) applied lexicographically — the first field is
dominant, and each field carries its own direction. Every comparison uses
`query.Compare` — the *same* type-aware comparison the `gt`/`gte`/`lt`/`lte`
filter operators use, so a sort and a filter never disagree about how two values
relate. Numbers order numerically (`2` before `10`, not the lexical `"10"` before
`"2"`) and strings order lexically; mixed types degrade to a deterministic string
comparison. The record **`id` is always the implicit final tiebreaker** (ascending),
so the ordering is *total*: results are deterministic, a bounded top-K agrees with a
full sort, and — crucially — a keyset cursor is unambiguous. The deprecated scalar
`order_by`/`descending` is promoted to a single-element `Sort` when
`order_by_fields` is empty, so the two share one code path. Without any ordering,
results are returned in insertion (id) order.

**Keyset (cursor) pagination.** `offset` skips rows by *counting* past them —
O(offset). A `page_token` instead lets the engine *seek* past the rows already
returned — O(page) regardless of depth. The mechanics:

- The token is an opaque, URL-safe **base64 of compact JSON** — `{"k":[…sort-key
  values…],"i":<id>}` — encoding the `(sort-key tuple, id)` of the last row emitted
  on the previous page. The codec (`encodeCursor`/`decodeCursor` in `engine/scan.go`)
  is defined entirely in the engine with **no grpc/proto dependency**, so the
  embeddable engine keeps its zero transport imports; JSON round-trips numbers as
  `float64`, which `query.Compare` treats identically to the numeric types decoded
  from a segment.
- On a paginated scan the engine rebuilds a synthetic boundary `ScanResult` from the
  token and keeps only rows that sort **strictly after** it under the very same
  `sortLess` used for ordering. Because that order is total (id tiebreak), "strictly
  after" excludes exactly the boundary row and nothing else — so no row is skipped or
  re-emitted, even across concurrent inserts. Concatenated pages therefore cover every
  matching row **exactly once, no duplicates and no gaps**.
- After emitting a page, the engine sets `ScanStats.NextPageToken` **only when the
  limit truncated the result** (there were more matching rows than `offset+limit`),
  encoding the last emitted row. The server rides this token on the **final streamed
  `FindResponse`** (buffering one record so the last message can carry it) — no extra
  record-less message that would break an older client. An empty token means the last
  page was reached. A malformed token, or one whose key count disagrees with the
  ordering, surfaces as `engine.ErrInvalidPageToken` → gRPC `InvalidArgument`.

A cursor requires an ordering (`order_by_fields` or the deprecated scalar); pass the
same ordering, filter, and limit on every page, with `offset = 0`.

**Cancellation.** `ScanStream` threads the request `context` and checks it
between segments and records, so a client that cancels a long `Find` (or
disconnects) stops server-side work promptly instead of scanning to completion.

**Field projection.** `FindRequest.fields` (and `FindByIdRequest` /
`FindByKeyRequest`) carry an optional projection: a list of top-level field
names to return. It is passed to the engine as `ScanOptions.Fields`, and
`ScanStream` applies it via the exported `engine.ProjectData` helper **after**
filtering and ordering and just before each record is yielded to the server —
so it narrows only what crosses the wire, never what the filter or `order_by`
see (an `order_by` field need not be projected). The point-lookup handlers
(`FindById` / `FindByKey`) call the same helper on the fetched record. The rules
live in one place, `engine.ProjectData`:

- An **empty** projection returns the record's data unchanged (full record —
  backward compatible), never copying the map.
- A non-empty projection builds a fresh map holding only the requested keys that
  exist; an unknown key is silently skipped, and the input map is never mutated.
- The reserved `_key` field is always retained so a record's caller-supplied
  string `key` survives projection. `id` and `rev` live outside the data map, so
  they are inherently unaffected — `id`, `key`, and `rev` are always returned.

### Slow-query log & scan stats

`ScanStream` returns a plain `engine.ScanStats` value alongside its error,
describing the cost of the scan it just ran:

| Field | Meaning |
|---|---|
| `RowsScanned` | live records examined against the filter |
| `RowsReturned` | records emitted to the caller |
| `IndexUsed` | whether a secondary index produced the candidate set |

The engine already decides index-vs-scan in `forEachMatch` — an eq or range
predicate on an indexed field walks the index's candidate ids (`IndexUsed =
true`, `RowsScanned` counts only those candidates), and everything else streams
the segments (`IndexUsed = false`, `RowsScanned` counts every live record the
filter is tested against). This change simply *surfaces* that decision as data;
`ScanStats` carries no `slog`, `grpc`, or `prometheus` types, so the embeddable
engine gains **no** dependency (`make deps-check` enforces it — the same
discipline as `OnCompaction` for metrics and the request logger).

The **server layer** turns the stats into two operator signals in
`GRPCServer.Find` (`server/grpc.go`), both injected at construction as optional
hooks so the default and embedded paths add nothing:

1. **Slow-query log.** When `--slow-query-ms > 0` and a `Find` reaches that
   wall-clock duration, one record is logged at `WARN` on the shared `slog`
   logger: the `collection`, the `filter` **shape** (fields and operators only,
   never the compared values — rendered by `filterShape`), `rows_scanned`,
   `rows_returned`, `index_used`, and `duration`. Logging the shape rather than
   the values makes the line safe to aggregate and keeps record data out of the
   logs.
2. **Rows-scanned metric.** A `filedb_scan_rows_scanned` Prometheus histogram
   (labelled by `collection`) records `RowsScanned` for every `Find`, via a
   `WithScanObserver` hook that calls `metrics.ObserveScan`. As with compaction,
   the engine never references the metrics package — the server owns the
   instrument and feeds it through the hook.

Together an operator can spot an unindexed hot query two ways: a `WARN` line
showing `index_used=false` with `rows_scanned ≫ rows_returned`, or a rising
`filedb_scan_rows_scanned` histogram for a collection. The fix — an index on the
filtered field — flips `index_used` to `true` and collapses `rows_scanned`.

---

## In-Memory Primary Index

```
map[uint64]IndexEntry{
    SegmentPath string
    Offset      int64
}
```

- Updated on every write (same write lock scope)
- Persisted to `index.json` with a SHA-256 checksum on every close
- Loaded on startup; rebuilt from segment scans if checksum fails
- Rebuilt after compaction (offsets change)

---

## Secondary Indexes

Secondary indexes are per-field inverted indexes stored in memory and on disk:

```
map[string]map[string][]uint64
// field → value → []id
```

### Lifecycle

- Created with `EnsureIndex(field)` — idempotent, builds from existing data on first call
- Dropped with `DropIndex(field)` — removes from memory and deletes `sidx_<field>.json`
- Listed with `ListIndexes()`
- Maintained automatically on every Insert / Update / Delete (same write lock scope)
- Persisted to `sidx_<field>.json` with a SHA-256 checksum
- Reloaded on startup; rebuilt from segments if the checksum fails
- Rebuilt transparently after each compaction run

### Range queries (ordered key view)

Alongside the hash buckets, each index keeps its **distinct keys in a sorted
slice** together with the field's value *kind* (numeric, string, or mixed),
maintained incrementally as records are inserted, updated, and deleted:

- A range predicate (`gt`/`gte`/`lt`/`lte`) binary-searches the sorted view for
  the matching key window and unions those buckets' ids — reading O(log k + matches)
  instead of scanning the whole collection.
- Ordering is the type-aware `query.Compare` (numbers numerically, strings
  lexically), so `age > 9` matches `10` rather than falling for the lexical
  `"10" < "9"`.
- The candidate ids are then re-validated against the primary index and the
  filter, so results are **identical** to a full scan.
- The sorted view is only kept while the field is homogeneous. If a field mixes
  numbers and strings its range ordering is undefined, so the index reports it
  cannot serve the range and the query falls back to a full scan (eq lookups
  still work). A range whose bound type differs from the indexed values falls
  back the same way.

The kind is persisted in `sidx_<field>.json` (outside the checksum, so old files
stay valid) and the sorted view is rebuilt from the buckets on load and after
compaction.

### Query acceleration

`Scan` uses the secondary index when the filter is a single `eq` **or a single
range** (`gt`/`gte`/`lt`/`lte`) on an indexed field. All other filter shapes
(composite filters, `contains`/`regex`, non-indexed fields) fall back to a full
segment scan.

### Unique indexes

`EnsureUniqueIndex(field)` creates a secondary index that additionally enforces
a uniqueness constraint: any insert or update that would map the indexed value
to a *different* live record is rejected with the typed `ErrDuplicateKey`
(wrapped with field/value context). The check is performed under the same write
lock as the index mutation, so it is atomic — a rejected write appends nothing
to the segment and mutates no index. `CommitTx` pre-validates every staged op
(against committed data and against other ops in the same batch) before applying
any of them.

The `unique` flag is persisted in `sidx_<field>.json` and restored on reload —
including when the file is stale and the buckets are rebuilt from segments.
Uniqueness is enforced on new writes going forward only; historical duplicates
already present in the data (resolved last-write-wins during a rebuild) are
tolerated, not rejected retroactively.

### Caller-supplied string keys

The engine assigns every record a monotonic `uint64` id, but callers often have
their own string identifier (a session id, a name, a context key). Rather than
generalise the primary index to strings — which would touch every offset and
tombstone path — string keys are layered on top of the unique-index machinery:

- A reserved data field, `_key` (`engine.KeyField`), holds the caller's key.
  Plain `Insert`/`Update` **reject** data that sets `_key` directly with the
  typed `ErrReservedField`; it is settable only through the keyed API.
- `InsertWithKey(key, data)` stamps `data["_key"] = key`, lazily ensures a
  **unique** secondary index on `_key` exists (created on the first keyed write
  to a collection), and inserts. A key already held by a live record is rejected
  with `ErrDuplicateKey`.
- `FindByKey`, `UpdateByKey`, and `DeleteByKey` resolve the key to its `uint64`
  id via `IndexLookup("_key", key)` — an O(1) index hit — and then reuse the
  existing id-based path. A key with no live record yields `ErrKeyNotFound`.
  `UpdateByKey` re-stamps `_key`, so a record's key is fixed for its lifetime.

Because `_key` is an ordinary field inside `data`, keyed records need no special
handling anywhere else: they survive segment rotation, compaction, index
rebuild, and reopen exactly like any other record, and their key is visible in
`WatchEvent.Data` for free. The `uint64` id, primary index, `WatchEvent`, and
`CommitTx` are all unchanged.

### Revisions and compare-and-swap

Each record carries an explicit monotonic revision, `rev`: `1` on insert, `+1`
on every update. It is stored on both the segment entry (`store.Entry.Rev`,
`json:"rev,omitempty"`) and the in-memory `IndexEntry`, so the current revision
is readable without a segment read. `rev` is a real field, deliberately **not**
derived from the timestamp (`Ts` is not collision-proof).

Backward compatibility is by construction: `rev` is omitted when zero, so a
segment line or `index.json` written before revisions existed decodes as rev 0
and still verifies its checksum (the CRC folds in `rev` only when non-zero).
Durability of the value across the engine's existing paths:

- **Update** reads the current `IndexEntry.Rev` under the write lock and writes
  `rev+1`; **insert** writes `rev 1`.
- **Rebuild** recomputes revisions by replay order — counting the surviving
  writes per id — but never below a revision already recorded in an entry, so a
  compacted record keeps its true rev instead of resetting to 1.
- **Compaction** preserves the latest entry's `rev` (the collapsed line carries
  it), and the post-compaction rebuild honours it via the rule above.

Two conditional-update primitives build on the revision, both executed under a
single `c.mu.Lock` critical section so the read-check-write is atomic against
every other writer — the direct, lock-free-to-the-caller CAS the embedded
consumer needs:

- `UpdateIfRev(key, expectedRev, data)` applies only if the record's current
  revision equals `expectedRev` (optimistic concurrency).
- `UpdateIfMatch(key, pred, data)` applies only if `pred(currentData)` holds
  (value-based CAS).

Both return `(applied bool, err error)`. A stale revision, a false predicate, or
a missing key is a clean `(false, nil)` no-op — never an error. On success the
revision bumps and a normal update `WatchEvent` is emitted; the string key is
preserved. Reads expose the revision through a `Record{ID, Key, Rev, Ts, Data}`
struct returned by `Get`/`GetByKey`, and through `ScanResult.Rev`.

### Key-based upsert

`Upsert(key, data)` is a create-or-replace on a string key, for the many
call sites that would otherwise do a get-then-branch (e.g. archiving a record to
its final state). It runs the whole decision in one `c.mu.Lock` critical
section, so concurrent upserts on the same key serialise cleanly with no lost
updates:

- The `_key` index is looked up under the write lock. If a live record carries
  the key, the upsert appends an **update** (`rev+1`, preserving the id); if not,
  it appends an **insert** (`rev 1`, a freshly assigned id) — the same revision
  convention `InsertWithKey`/`UpdateByKey` follow.
- The key is stamped into `_key` either way, so supplying `_key` inside `data` is
  rejected with `ErrReservedField`, and unique indexes on other fields are still
  enforced before the append.
- It returns the resulting `Record{ID, Key, Rev, Ts, Data}` and emits the
  matching `WatchEvent` — `OpInsert` for a create, `OpUpdate` for a replace.

Because a replace is an ordinary update entry, the stale versions collapse on
compaction to a single live line, exactly as with `UpdateByKey`.

### Count and existence checks

Dashboards and list views ask "how many?" and "does this key exist?" far more
often than they ask for the rows themselves. `Count` and `Exists` answer those
without materialising the collection:

- `Count(filter)` picks the cheapest path the filter allows:
  - a `nil` or match-all filter returns the primary index length in **O(1)** — no
    segment is read, because the index already tracks exactly the live records;
  - a single `eq` filter on an indexed field returns the size of that value's id
    set from the secondary index (**O(matches)**, still no segment read) — the
    bucket membership is exactly what the filter would accept, so the count is
    scan-identical;
  - any other filter streams live records through the same `forEachMatch` path
    `Scan` uses and increments a counter, so it never buffers a result slice or a
    whole-collection data map. `Count(f)` always equals `len(Scan(f))`.
- `Exists(key)` is a single `IndexLookup("_key", key)` — an **O(1)** in-memory
  hit with no segment read, so it stays flat regardless of collection size. A
  collection that has never taken a keyed write has no `_key` index and reports
  `false` for every key.

### TTL / expiring records

A record can carry an expiry **deadline**, after which it is invisible to reads
and reclaimed by compaction. The deadline is stored as a Unix-nanosecond
timestamp on the segment entry (`store.Entry.ExpiresAt`, `json:"expires_at,omitempty"`)
and mirrored onto the in-memory `IndexEntry`, so a read can drop an expired
record without touching disk. Like `rev`, it is folded into the entry CRC only
when non-zero, so a segment line or `index.json` written before TTLs existed
decodes as *never expires* and still verifies — fully backward compatible. A
Unix-nano `int64` (not a `time.Time`) is used precisely so `omitempty` drops it
when unset; a zero `time.Time` struct would still serialise on every line.

Deadlines are set three ways, in precedence order:

- **Explicit per-record** — `InsertWithExpiry(data, when)` /
  `UpdateWithExpiry(id, data, when)` stamp an exact instant. Over the wire these
  surface as `ttl_seconds` (relative) on the `Insert`/`InsertMany`/`Update`
  RPCs; the server converts `now + ttl_seconds` into the absolute deadline the
  engine stores.
- **Per-collection default** — `CreateCollectionWithDefaultTTL(name, ttl)` (RPC
  field `default_ttl_seconds`, CLI `create-collection --default-ttl`) pins a
  default for one collection. It is persisted in that collection's `meta.json`
  (`default_ttl_seconds`) and reloaded on open, so it survives restarts and
  **overrides** the server-wide default for that collection. It is stored
  separately from the inherited global default (a plain collection persists no
  value and keeps tracking the live `--default-ttl`), so changing the global
  later still affects collections that never set their own.
- **Server-wide default** — `CollectionConfig.DefaultTTL` (server
  `--default-ttl`) stamps `now + TTL` on every insert that carries no explicit
  deadline. Zero (the default) means records never expire.

Per-record `ttl_seconds` is rejected inside a transaction (the transaction
staging path does not yet carry a deadline); transaction inserts still honor the
collection/server default.

A plain `Update` keeps a record's existing deadline (**sticky**) — it is a
data-only write, not a TTL refresh; `UpdateWithExpiry` is the way to move the
deadline. Compare-and-swap and transaction updates likewise preserve the
deadline; transaction inserts honor the default TTL.

Two mechanisms make expiry effective:

1. **Defensive read filtering** — `Get` (hence `FindByID`/`GetByKey` and indexed
   scan candidates) and the streaming scan liveness check both drop any record
   whose deadline has passed. This makes a record invisible **the instant** it
   expires, before any background work runs, and independent of clock skew
   between the reaper and the reader.
2. **Reaping + reclamation** — a reaper runs on the compactor cadence
   (`reapExpired`), appends delete tombstones for expired ids, and removes them
   from the primary and secondary indexes. Compaction additionally drops expired
   entries during `resolveEntries`, so space is reclaimed even if the reaper has
   not yet run.

---

## Change Feed (Watch)

Every write emits a `WatchEvent` to all live `Watch` subscribers. Each
subscriber gets its own buffered channel (`--watch-buffer`, default 64).

### Overflow signal

Delivery is non-blocking: a write never waits on a slow consumer. If a
subscriber's buffer is full, the event is dropped and the watcher is marked
*overflowed*. Once that channel drains, the next emit delivers a single
sentinel `WatchEvent` with op `OVERFLOW` (no record) before normal events
resume — so a consumer that fell behind learns it missed writes and can resync
(re-read the affected records) instead of silently losing them. Exactly one
overflow sentinel is delivered per overflow episode.

Server-side `Watch` filters are applied *after* the buffer, so an `OVERFLOW`
sentinel always bypasses the filter and reaches the client regardless of
whether the dropped events would have matched.

---

## Concurrency Model

**Pessimistic locking per collection using `sync.RWMutex`:**

| Operation | Lock |
|---|---|
| Insert / Update / Delete | Write lock |
| FindById / Scan | Read lock |
| Compaction (rebuild phase) | Write lock (brief) |

Multiple concurrent reads proceed without blocking each other. The write lock is held only for the duration of the file append + in-memory index update, which is typically microseconds.

The compaction goroutine acquires the write lock only during the final atomic segment swap — reads and writes are unblocked for the entire resolve + rewrite phase.

---

## Background Compactor

Runs as a goroutine per collection. Two trigger conditions (whichever fires first):

1. **Dirty ratio**: >30% of entries in sealed segments are stale (overwritten or deleted)
2. **Time interval**: every 5 minutes (configurable)

A third, explicit trigger exists — see [On-demand compaction](#on-demand-compaction).

### Compaction algorithm

```
1. Snapshot sealed segment list (read lock, release)
2. Check dirty ratio — skip if below threshold
3. Scan all sealed segments, keep latest entry per id, drop deletes
4. Write resolved entries to new temp segments (no lock held)
5. Acquire write lock
6. Atomic rename: temp → final segment files
7. Delete old dirty segments
8. Rebuild primary index and all secondary indexes
9. Release write lock
10. Persist updated indexes to disk
11. Fire OnCompaction hook (used by Prometheus metrics)
```

### Rebalancer

After compaction, adjacent segments smaller than 10% of `SegmentMaxSize` are merged to prevent segment count bloat from many small leftover files.

### On-demand compaction

Operators can force a compaction pass without waiting for the dirty-ratio or
timer trigger — for example to reclaim space immediately or to quiesce a
collection before a backup. `Collection.CompactNow()` (exposed as the `Compact`
RPC and `filedb-cli compact <collection>`) runs the same algorithm as the
background pass with two differences:

- **The dirty-ratio gate is skipped** (`compact(force=true)`), so the merge runs
  even when stale entries are below the 30% threshold.
- **It is synchronous** — the call returns only after the pass (and the reaper
  that precedes it) has fully completed, so a client knows the collection is
  compacted when the RPC returns.

Background and on-demand passes are serialized by a dedicated `compactMu`, so a
timer-triggered run and a forced run can never concurrently snapshot, remove,
and rename the same sealed segments. A closed collection refuses to compact.

---

## Transactions

Transactions are optimistic and scoped to a single collection. Operations are staged in memory and applied atomically on commit:

```
BeginTx   → allocate tx_id, create in-memory staging buffer
Insert / Update / Delete (with tx_id) → append to staging buffer (no disk write)
CommitTx  → acquire write lock → apply all staged ops sequentially → release
RollbackTx → discard staging buffer
```

Staged operations bypass the normal single-operation write path; the write lock is held for the entire commit batch.

### Idle transaction expiry

Open transactions live only in server memory, so a client that calls `BeginTx`
and then disconnects without committing or rolling back would otherwise leak its
staging buffer and reserved id forever. A background sweeper in the `TxManager`
reaps any transaction whose last staged op is older than `--tx-timeout`
(default 5m; set `0` to disable). Reaping an abandoned transaction is equivalent
to a rollback — nothing was ever written to disk, so the staged buffer is simply
discarded. A subsequent `CommitTx`/`RollbackTx` on a reaped id returns
`NotFound`.

---

## Durability

Writes are appended to the active segment with a single `write(2)`. Whether that
write is flushed to stable storage (via `fsync(2)`) before the operation is
acknowledged is controlled by the **sync mode** (`--sync`):

| Mode | Behaviour | Crash-loss window | Throughput |
|---|---|---|---|
| `none` (default) | Never fsyncs explicitly; relies on the OS page-cache flush | All not-yet-flushed writes | Highest |
| `interval` | A per-collection goroutine fsyncs the active segment every `--sync-interval` (default 1s) | At most one interval | High |
| `always` | fsyncs after every write, before acknowledging it | Zero (for acknowledged writes) | Lowest |

`always` holds the collection write lock across the fsync, so it serializes
durable writes — correct, but the slowest option. `interval` is the recommended
middle ground for most workloads. Sealing a segment and `Close()` always fsync
regardless of mode.

> Pick the mode that matches your data's value. `none` is appropriate for caches
> and rebuildable data; `always` for data you cannot afford to lose on power loss.

## Crash Safety

- **Partial write recovery**: on segment open, the last line is validated. Any partial line (from a crash mid-write) is detected and truncated before the segment is used.
- **Bit-rot detection**: each segment entry carries a CRC32C checksum verified on read. A single flipped byte in a sealed segment that still parses as JSON is caught (`store.ErrCorruptEntry`) rather than silently returning wrong data. See *Per-entry checksums* above.
- **Index recovery**: on startup, both the primary index and each secondary index checksum are verified. A mismatch triggers a full rebuild by replaying all segment entries.
- **Atomic segment swap**: compaction uses `os.Rename` which is atomic on POSIX filesystems. The old segments are only deleted after the new ones are in place.
- **Durable metadata writes**: `index.json`, `sidx_*.json`, and `meta.json` are written with a write-temp → `fsync` → atomic `rename` → directory `fsync` sequence, so a crash can never leave a half-written or invisible file. Directory `fsync` after creating or rotating a segment (under `--sync=interval`/`always`) ensures the new segment file's directory entry survives a crash too. (Directory `fsync` is a no-op on Windows, which does not support it; the atomic rename still holds.)
- **Id-counter recovery**: `meta.json` is persisted on segment rotation and on `Close`, not on every write. On startup the counter is reconciled against the highest id present in the active segment (which always holds the most recently assigned id), so a crash that lost an unsynced `meta.json` can never cause id reuse.

Note that partial-line recovery protects against *torn* writes (an incomplete
final line), not against *lost* writes — a write acknowledged under `--sync=none`
can still be lost if the machine loses power before the OS flushes its page
cache. Use `--sync=interval` or `--sync=always` to bound or eliminate that window.

---

## Backup / snapshot

`DB.SnapshotTo(io.Writer)` (the `Snapshot` RPC and `filedb-cli backup`) writes a
**gzip-compressed tar** of the whole database — one entry per collection file,
named `<collection>/<file>`. Because the on-disk format is just append-only
NDJSON plus small sidecar files, a backup is a plain file copy; restore is a
plain extract:

```bash
filedb-cli backup db.tar.gz
tar xzf db.tar.gz -C ./data      # then start the server with --data ./data
```

**Consistency** is layered to match FileDB's guarantees without a global stop:

- The DB registry is held read-locked for the whole archive, so no collection is
  created, dropped, or reopened mid-snapshot.
- Each collection's files are copied while its **own read lock** is held, so no
  write, rotation, or compaction can mutate them during the copy — the archive
  captures a per-collection point in time. (FileDB has no cross-collection
  transactions, so per-collection consistency is the strongest meaningful
  guarantee.)
- Segments are append-only, so even the active segment is captured at a valid
  entry boundary — the copy simply ends at the current file size.

**What is and isn't archived:** segment files (`seg_*.ndjson`), `meta.json`, and
the secondary indexes (`sidx_*.json`, refreshed from memory just before the copy)
are included. The primary `index.json` is **deliberately excluded**: it stores
absolute segment paths and its checksum only guards its own contents, so a copied
index would reference the source directory and could be silently stale. The
restored collection rebuilds a correct primary index from its segments the first
time it is opened (the same [index recovery](#crash-safety) path used after a
crash), which is also why a backup taken under concurrent writes always restores
to a consistent state.

The RPC streams the archive in 64 KiB chunks (`SnapshotChunk`); it is gRPC-only
because binary streaming does not map cleanly onto the REST gateway.

---

## Network Layer

```
┌───────────────────────────────────────────────┐
│  filedb binary                                │
│                                               │
│  ┌────────────────┐   ┌──────────────────────┐│
│  │ gRPC/TCP :5433 │   │ REST gateway :8080   ││
│  │ (optional TLS) │   │ (grpc-gateway)       ││
│  └───────┬────────┘   └──────────┬───────────┘│
│          │                       │             │
│  ┌───────▼───────────────────────▼───────────┐ │
│  │ Unix socket /tmp/filedb.sock              │ │
│  │ (local connections, always insecure)      │ │
│  └───────────────────────┬───────────────────┘ │
│                          │                     │
│  ┌───────────────────────▼───────────────────┐ │
│  │ engine.DB                                 │ │
│  └───────────────────────────────────────────┘ │
│                                               │
│  ┌────────────────────────────────────────┐   │
│  │ Prometheus metrics :9090/metrics       │   │
│  └────────────────────────────────────────┘   │
└───────────────────────────────────────────────┘
```

- **TCP gRPC listener** — optional TLS via `--tls-cert` / `--tls-key`. When both flags are set, `credentials.NewTLS()` is used; otherwise `insecure.NewCredentials()`.
- **REST gateway** — dials the TCP gRPC server on the internal loopback. Uses `InsecureSkipVerify` for this internal hop (the cert may be self-signed).
- **Unix socket** — always uses `insecure.NewCredentials()`. The CLI auto-detects this socket and prefers it for zero-overhead local connections.
- **Metrics HTTP server** — serves Prometheus exposition format at `/metrics`. Disabled when `--metrics-addr` is empty.

### Auth

All gRPC calls (TCP and Unix socket) pass through unary and stream interceptors backed by an `auth.Authenticator`. Each request's `x-api-key` metadata header is matched against the configured key set using `crypto/subtle.ConstantTimeCompare` — the lookup compares against *every* key without short-circuiting, so response timing never reveals which (or whether a) key matched.

**Scoped keys.** A key resolves to a principal with a scope of either `read` or `read-write`. The interceptor classifies each RPC by its method name: mutating RPCs (`Insert`, `Update`, `Delete`, `CreateCollection`, `DropCollection`, `EnsureIndex`, `DropIndex`, `Compact`, and the transaction verbs) require `read-write`; the rest (`Find`, `FindById`, `ListCollections`, `ListIndexes`, `CollectionStats`, `Watch`, `Snapshot`) are reads. A read-scoped key presenting on a write RPC is rejected with `PermissionDenied` (distinct from the `Unauthenticated` returned for a missing/unknown key). Unknown method names are treated as writes, so a read-only key can never slip through a newly added RPC that predates its classification.

**Key sources.** Keys come from the config file's `keys:` list (`{key, name, scope}` entries). The legacy single `--api-key` / `FILEDB_API_KEY` still works and is registered as an additional `read-write` key named `default`, so existing single-key and no-auth (empty) deployments are unchanged.

**Rotation.** The active key set lives behind an `atomic.Pointer`; sending the server `SIGHUP` re-reads the config file and swaps in the new set atomically, with in-flight requests finishing against the set they started on. Keys can therefore be added, removed, or re-scoped without a restart.

### Interceptor pipeline

Both gRPC servers install the same interceptor chain, in this order:

```
[tracing] → auth → limiter → logging → metrics → handler
```

Auth runs first (of the always-present interceptors): on success it resolves the principal and attaches it to the request context (via a stream wrapper for streaming RPCs). The limiter runs next so it can read that principal — it applies the per-key rate limit and the in-flight semaphore, shedding over-budget calls before they reach the handler. Logging runs after the limiter so a shed call is still logged (with its `RESOURCE_EXHAUSTED` code). Metrics is innermost and records the Prometheus request histogram. Because logging and metrics sit *inside* auth, a call rejected by auth is not double-counted as a served request. **The limiter is chained only when at least one limit is configured**, so the default (unlimited) path keeps the exact `auth → logging → metrics` chain and adds no overhead. **Tracing, when enabled (`--otlp-endpoint`), is chained *outermost*** — before auth — so its per-RPC span wraps the whole handler (including the status of a call rejected by auth or the limiter) and its span-bearing context flows down through the chain into the engine scan hook; when tracing is off, it adds no interceptor at all.

### Request logging

The server owns a single `*slog.Logger` (`log/slog`, no third-party dependency), built from `--log-level` and `--log-format`. The logging interceptor (`server/logging.go`) writes exactly one record per RPC — `method`, `principal`, `duration`, `code` — at `info` for success and `error` for failure, letting an operator filter noise with the level while still capturing every error. The **engine package never imports the logger**: it stays embeddable and dependency-free, surfacing anything it needs to report through the existing `engine.CollectionConfig` hooks (the same rule metrics follows via `OnCompaction`), and `make deps-check` enforces this.

### Health & readiness

The standard `grpc.health.v1.Health` service (`server/health.go`) is registered on both the TCP and Unix gRPC servers via a shared `HealthService`. It starts `NOT_SERVING`, is marked `SERVING` once the listeners are accepting connections, and is flipped back to `NOT_SERVING` at the start of graceful shutdown — so a load balancer stops routing new work while `GracefulStop` drains the in-flight RPCs. Two HTTP probes are registered directly on the grpc-gateway mux: `GET /healthz` (liveness — `200` whenever the process can answer) and `GET /readyz` (readiness — `200` when the DB is open and the data directory accepts a probe write, else `503` with the reason). Readiness is deliberately data-plane aware: a full or read-only data volume makes the node *unready* without making it *dead*, so it is pulled from rotation rather than restarted.

### Backpressure & rate limiting

The `Limiter` (`server/limits.go`) provides two independent, opt-in defences against resource exhaustion, both surfaced through the unary and stream interceptors described above. Like metrics and logging, this is a **server-layer** concern — the limiter reaches for `golang.org/x/time/rate`, which the embeddable `engine`/`store`/`query` packages must never import (`make deps-check` enforces it).

- **In-flight semaphore (`--max-inflight`).** A counting semaphore of fixed capacity is acquired at the start of every RPC and released when it returns. Acquisition is *non-blocking*: when the ceiling is saturated the interceptor returns `RESOURCE_EXHAUSTED` immediately rather than queueing, so the server sheds load instead of accumulating goroutines, file descriptors, and memory behind a saturated CPU. A streaming RPC holds its slot for the whole stream lifetime, which correctly counts a long-lived `Watch` or `Snapshot` against the ceiling.

- **Per-principal token bucket (`--rate-limit`).** Each API-key principal (the resolved `name` the auth interceptor put on the context) gets its **own** `rate.Limiter`, created lazily on first request and stored in a mutex-guarded map. The rate is the configured requests/sec and the burst is one second's worth of budget (rounded up). Because the buckets are keyed by principal, throttling one key can never consume another key's budget. An unauthenticated deployment funnels every call into a single shared `"anonymous"` bucket.

Both controls default to their zero value (unlimited / disabled). `NewLimiter` reports `Enabled()` only when at least one is active, and `cmd/filedb` chains the limiter interceptors solely in that case — so the common, un-limited deployment pays nothing. `grpc.MaxConcurrentStreams` (`--max-concurrent-streams`) is set directly as a `grpc.ServerOption` on both servers, capping the HTTP/2 streams a single connection may multiplex; it is orthogonal to the server-wide in-flight ceiling.

### Tracing (OpenTelemetry)

Distributed tracing (`server/tracing.go`) is **opt-in and off by default**: `cmd/filedb` builds an OTel SDK `TracerProvider` and chains the tracing interceptors only when `--otlp-endpoint` is set. The provider batches spans to an OTLP/gRPC collector, tags them with a `service.name=filedb` resource, and samples with a **parent-based** sampler over `TraceIDRatioBased(--otlp-sample-ratio)` — so an upstream sampling decision propagated on the trace context is honoured, and the ratio governs only the traces FileDB roots. On graceful shutdown the provider is `Shutdown` (with a bounded timeout) to flush any spans still buffered.

**Interceptor span.** `TracingInterceptors` (unary **and** stream) starts one span per RPC, named after the full method (`/filedb.v1.FileDB/Find`) with span kind *server*, tagged `rpc.method` and — once the call returns — `rpc.grpc.status_code`; a non-OK result additionally marks the span errored and records the error. For streaming RPCs the interceptor wraps the `ServerStream` so its `Context()` carries the span, exactly as the auth interceptor does for the principal. Chained outermost, the span becomes the parent of everything downstream.

**Engine hook.** The rule that keeps the engine embeddable applies here too: **the `engine`/`store`/`query` packages import no OpenTelemetry code** (`make deps-check` enforces it). Instead, the engine exposes timing through the same `engine.CollectionConfig` hook pattern used for metrics — a new `OnScan(ctx, collection, dur)` hook fired by `ScanStream`, alongside the existing `OnCompaction(collection, dur)`. The **server** owns the SDK and turns those callbacks into spans: `ScanTraceHook` starts an `engine.scan` span parented on the scan's context (which, because the span-bearing context threads down from the interceptor, nests it under the RPC span), and `CompactionTraceHook` records a root `engine.compaction` span (compaction is a background task with no request context). The scan hook receives the scan's `context.Context` precisely so its span can attach to the caller's; the compaction hook takes none, so its span stands alone. Both reconstruct their start/end timestamps from the reported duration so the span's extent matches the real work. The metrics `OnCompaction` hook and the tracing one are **composed** in `cmd/filedb` (metrics first, then tracing) so enabling tracing never displaces Prometheus compaction timing.

The net effect: a slow `Find` produces a trace spanning gateway → gRPC (`/filedb.v1.FileDB/Find`) → `engine.scan`, making it obvious whether the cost was in transport, a saturated limiter, or a large collection scan.

### Keyed CRUD, Upsert & CAS on the wire (N1)

The [caller-supplied string keys](#caller-supplied-string-keys),
[revisions/compare-and-swap](#revisions-and-compare-and-swap), and
[key-based upsert](#key-based-upsert) the engine has always had are surfaced over
gRPC/REST by a thin handler layer in `server/grpc.go` — the handlers **map
straight onto the engine methods and add no logic of their own**:

| RPC | REST | Engine method |
|---|---|---|
| `Insert` (with `key`) | `POST /v1/{collection}/records` | `InsertWithKey` |
| `Upsert` | `POST /v1/{collection}/records:upsert` | `Upsert` |
| `FindByKey` | `GET /v1/{collection}/keys/{key}` | `GetByKey` |
| `UpdateByKey` | `PUT /v1/{collection}/keys/{key}` | `UpdateByKey` |
| `DeleteByKey` | `DELETE /v1/{collection}/keys/{key}` | `DeleteByKey` |
| `UpdateIfRev` | `POST /v1/{collection}/keys/{key}:cas` | `UpdateIfRev` |

There is deliberately **no `InsertWithKey` RPC**: a keyed create is expressed by
setting the additive `key` field on the existing `Insert` request (empty = the
unchanged server-assigned-id behaviour). A keyed insert bypasses the transaction
and per-record-TTL paths (the engine's keyed insert supports neither), which the
handler rejects with `InvalidArgument`.

**Error-code mapping.** The handlers translate the engine's *typed* errors into
gRPC status codes, so a client sees a stable code rather than an opaque string:

| Engine error | gRPC code |
|---|---|
| `engine.ErrKeyNotFound` | `NotFound` |
| `engine.ErrDuplicateKey` | `AlreadyExists` |
| `engine.ErrReservedField` (data sets `_key` directly) | `InvalidArgument` |

`UpdateIfRev` is **not** an error path: a stale revision or a missing key returns
`swapped=false` with no error (mirroring the engine's `(false, nil)` no-op), so a
client distinguishes "someone else won the race, retry" from "the call failed".

**`key`/`rev` on responses.** `Record` gained `key` (field 5) and `rev` (field 6),
and `InsertResponse`/`UpdateResponse` gained the same pair — all additive field
numbers, so pre-N1 clients are unaffected. The read handlers populate them from
the engine's `Record{ID, Key, Rev, …}` (via `Get`/`GetByKey`) and, for streaming
`Find`, from `ScanResult.Rev` plus the `_key` field carried in `data`. `Watch`
events carry the key but no revision (the change feed does not track it), so their
`rev` is `0`.

---

## Web Admin UI

A browser-based admin UI lives at `clients/web/` (React 18 + TypeScript + Vite + Tailwind CSS, dark theme). It communicates exclusively with the REST gateway at `:8080` — no direct gRPC.

**CORS** — `server/rest.go` includes a CORS middleware that adds the necessary `Access-Control-Allow-*` headers so the browser can reach the gateway from a different origin (e.g., the Vite dev server at `localhost:5173`).

**Watch streaming** — grpc-gateway does not support the server-streaming shape used by the `Watch` RPC. A custom HTTP handler in `server/watch_rest.go` fills this gap: it opens a gRPC `Watch` stream internally and forwards each event to the browser as a `text/event-stream` (ReadableStream).

**Vite dev proxy** — during local development, Vite proxies all `/v1` requests from `localhost:5173` to `localhost:8080`, so no CORS issue arises in the dev workflow. The proxy is configured in `clients/web/vite.config.ts`.

---

## Observability

FileDB exposes Prometheus metrics via a dedicated HTTP server (default `:9090/metrics`):

| Metric | Type | Labels |
|---|---|---|
| `filedb_collection_records_total` | Gauge | `collection` |
| `filedb_collection_segments_total` | Gauge | `collection` |
| `filedb_compaction_runs_total` | Counter | `collection` |
| `filedb_compaction_duration_seconds` | Histogram | `collection` |
| `filedb_grpc_request_duration_seconds` | Histogram | `method`, `code` |
| `filedb_scan_rows_scanned` | Histogram | `collection` |

Per-collection gauges are sampled at scrape time via a custom `DBCollector`. Compaction metrics are recorded via an `OnCompaction` hook injected into `CollectionConfig` at startup. gRPC request duration is recorded by a unary interceptor chained after the auth interceptor. `filedb_scan_rows_scanned` records the rows examined by each `Find`, fed from the engine's `ScanStats` through a server-layer scan-observer hook (never from inside the engine) — see [Slow-query log & scan stats](#slow-query-log--scan-stats).

For **distributed tracing** (opt-in OpenTelemetry, `--otlp-endpoint`), which complements these pull-based metrics with per-request spans across the gateway → gRPC → engine-scan hops, see [Tracing (OpenTelemetry)](#tracing-opentelemetry) above.
