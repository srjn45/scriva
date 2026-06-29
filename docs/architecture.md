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
{"id":1,"op":"insert","ts":"2026-03-29T10:00:00Z","data":{"userName":"admin"}}
{"id":1,"op":"update","ts":"2026-03-29T11:00:00Z","data":{"userName":"admin2"}}
{"id":2,"op":"delete","ts":"2026-03-29T12:00:00Z"}
```

- `op` is one of `insert`, `update`, `delete`
- For `delete`, `data` is omitted (tombstone entry)
- The **latest entry for each id wins**

A segment is **sealed** (made immutable) when its file size exceeds `SegmentMaxSize` (default 4 MiB). After sealing a new active segment is created.

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
    acquire read lock → iterate all segments → apply filter → stream results

Find (scan) with secondary index (single eq filter on indexed field):
    acquire read lock → secondary index lookup (O(1)) → fetch matching ids via primary index
```

The in-memory primary index makes `FindById` an O(1) index lookup + one disk seek. A secondary index on a field reduces `Find` with a single equality filter from O(n) to O(1).

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
