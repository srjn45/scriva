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

Find (scan) with secondary index (single eq filter on indexed field):
    secondary index lookup (O(1)) → fetch candidate ids via primary index → filter → stream
```

The in-memory primary index makes `FindById` an O(1) index lookup + one disk seek. A secondary index on a field reduces `Find` with a single equality filter from O(n) to O(1).

### Streaming, push-down `Find`

`Find` is served by the engine's `ScanStream`, which emits matches to the gRPC
stream *as it reads* rather than buffering the whole result set. It never builds
an in-memory `map[id]entry` of the collection: instead it streams each segment
sequentially and treats an entry as live only when the primary index still
points at exactly its `(segment, offset)`, skipping superseded versions and
tombstones. This keeps the deduplication cost off the heap.

`limit`, `offset`, and `order_by` are pushed **into** the engine so their cost is
paid before materialization:

| Query shape | Rows examined | Memory held |
|---|---|---|
| unordered, `limit > 0` | stops after `offset+limit` matches | O(`offset+limit`) |
| ordered, `limit > 0` | all candidates (needs full comparison) | O(`offset+limit`) — bounded **top-K** buffer |
| ordered, no limit | all candidates | O(matches) — an inherent full sort |

So `Find … limit 10` over a million-row collection reads and holds ~10 rows, not
a million. (Range/`gt`/`lt` predicates and ordering by a non-indexed field still
examine every candidate — ordered indexes to fix that are tracked as Q3.)

**Ordering guarantees.** With `order_by`, results are sorted by that field using
`query.Compare` — the *same* type-aware comparison the `gt`/`gte`/`lt`/`lte`
filter operators use, so a sort and a filter never disagree about how two values
relate. Numbers order numerically (`2` before `10`, not the lexical `"10"` before
`"2"`) and strings order lexically; mixed types degrade to a deterministic string
comparison. `descending` reverses the order. Ties are broken by ascending `id` so
pages are deterministic and a bounded top-K agrees with a full sort. Without
`order_by`, results are returned in insertion (id) order.

**Cancellation.** `ScanStream` threads the request `context` and checks it
between segments and records, so a client that cancels a long `Find` (or
disconnects) stops server-side work promptly instead of scanning to completion.

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

### Query acceleration

`Scan` uses the secondary index when the filter is a single `eq` on an indexed field. All other filter shapes (composite filters, non-eq ops, non-indexed fields) fall back to a full segment scan.

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

All gRPC calls (TCP and Unix socket) pass through unary and stream interceptors that validate the `x-api-key` metadata header using `crypto/subtle.ConstantTimeCompare`. Set `--api-key ""` to disable auth entirely.

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

Per-collection gauges are sampled at scrape time via a custom `DBCollector`. Compaction metrics are recorded via an `OnCompaction` hook injected into `CollectionConfig` at startup. gRPC request duration is recorded by a unary interceptor chained after the auth interceptor.
