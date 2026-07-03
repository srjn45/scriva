# FileDB — Post-v0.1.0 Roadmap & Implementation Plan

v0.1.0 shipped the feature-complete core (storage engine, gRPC+REST, 7 client
SDKs, web UI, durability modes, metrics, TLS). This document plans the next arc:
**making the durability promise real, making queries scale, and rounding out the
feature set.**

It is derived from the codebase review of 2026-06-29. Each item lists the
*problem* (with `file:line` evidence), the *approach*, *proto/API* impact, the
*files* touched, *tests*, and an *acceptance bar*. Items are grouped into
release milestones and sized **S** (≤½ day), **M** (1–2 days), **L** (3+ days).

> **Workflow:** one PR per task. Small correctness fixes are batched into a
> single hardening PR; each large feature is its own branch (or warden agent in
> an isolated worktree). CI must be green before merge; every feature updates
> `docs/`, `README.md`, and ticks its box here.

---

## Milestone summary

| Milestone | Theme | Items | Risk | Headline |
|---|---|---|---|---|
| **v0.2.0** | Durability & correctness hardening | D1–D6 | Low | Back the durability promise; no silent data/edge bugs |
| **v0.3.0** | Query at scale | Q1–Q4 | Medium | Stop materializing whole collections; range indexes |
| **v0.4.0** | Feature breadth | F1–F3 | Medium | TTL, backup/snapshot, on-demand compaction |
| **v0.5.0** | Auth & multi-tenancy | A1 | Medium | Multiple scoped, rotatable API keys |

---

## v0.2.0 — Durability & correctness hardening

Small, high-trust-impact fixes. Ship D1–D2 + D5 first as one PR (they directly
back the `--sync` feature), then D3/D4/D6 as follow-ups.

### D1 — fsync the directory after segment create/rotate/rename — **S**
- **Problem:** `segment.go` fsyncs file *contents* (`Sync`, `Seal`) but never
  the parent directory. A new `seg_NNNNNN.ndjson` created in `rotateSegment`
  (`collection.go:531`) or `openActiveSegment` (`segment.go:30`) is not durable
  on crash even under `SyncModeAlways`. Same for the atomic-rename of
  `index.json` / `sidx_*.json` (`index.go:98`, `secondary_index.go:182`) —
  rename is atomic for *visibility*, not *durability*, without a dir fsync.
- **Approach:** add `fsyncDir(path string) error` helper (open dir, `Sync`,
  close). Call it after: first active-segment create, every rotation, and every
  tmp→final rename of index/sidx/meta files. Gate the per-rotation dir-fsync on
  `SyncMode != none` to preserve fast-mode throughput.
- **Files:** `engine/segment.go`, `collection.go`, `index.go`,
  `secondary_index.go`, new `engine/fsync.go`.
- **Tests:** `durability_test.go` — extend the reopen matrix; add a test that
  asserts dir-fsync is invoked per sync mode (inject a hook/counter).
- **Acceptance:** under `always`, no acknowledged write or its containing
  segment file can be lost across a simulated crash (kill + reopen).

### D2 — atomic, off-hot-path `meta.json` writes — **S**
- **Problem:** `persistMeta` uses in-place `os.WriteFile` (`meta.go:38`) — a
  torn write corrupts it — and it runs on **every insert** (`collection.go:314`),
  adding a file write to the hot path even in `SyncModeNone`.
- **Approach:** (a) switch `persistMeta` to tmp+rename like the indexes; (b) stop
  writing it per-insert — persist on rotation, on `Close`, and on a coarse timer
  (or every N writes). The id counter is already recoverable by scan, so this is
  pure optimization + safety, not a correctness regression.
- **Files:** `engine/meta.go`, `collection.go`.
- **Tests:** `collection_test.go` — reopen after N inserts with no clean close
  still recovers the correct id counter (falls back to scan); meta written
  atomically (no partial file observable).
- **Acceptance:** insert throughput in `none` mode improves measurably
  (`make bench`); id counter never regresses across crash/reopen.

### D3 — per-record checksums in segments — **M**
- **Problem:** the primary index has a SHA-256 checksum, but segment lines do
  not. Silent bit-rot in a sealed segment is undetectable — `store.Decode`
  returns wrong data if the corrupted bytes still parse as JSON.
- **Approach:** add an optional `crc` field to the NDJSON entry (CRC32C of the
  canonical `data`), written by `store.Encode` and verified by `store.Decode`
  when present. Keep it backward-compatible: absent `crc` = legacy line, no
  verification. Verification failures surface as a typed `ErrCorruptEntry`.
- **Proto/format:** no proto change; on-disk NDJSON gains an optional key.
  Document in `docs/architecture.md`.
- **Files:** `store/ndjson.go`, `engine/segment.go`
  (`ScanAll`/`ReadAt` error wrapping), `docs/architecture.md`.
- **Tests:** `ndjson_test.go` — round-trip with/without crc; flip a byte →
  `ErrCorruptEntry`. `segment_recovery_fuzz_test.go` — extend fuzz corpus.
- **Acceptance:** a single-bit flip in a sealed segment is detected on read.

### D4 — Watch overflow signal (no silent event loss) — **S/M**
- **Problem:** `emit` drops events when a subscriber's 64-buffer is full
  (`collection.go:524`, `default:` case) with no notification — consumers
  silently miss writes.
- **Approach:** track per-watcher drops; when a drop occurs, set an "overflowed"
  flag and deliver a sentinel `WatchEvent{Op: OpOverflow}` (new op) once the
  channel drains, so the client knows to resync. Make buffer size configurable.
- **Proto/API:** add `OVERFLOW` to the `WatchEvent` op enum in `proto/filedb.proto`;
  regenerate stubs; map in `server/grpc.go` + `server/watch_rest.go`.
- **Files:** `proto/filedb.proto`, `engine/collection.go`,
  `server/grpc.go`, `server/watch_rest.go`, web UI feed handling.
- **Tests:** `collection_test.go` — slow subscriber receives an overflow sentinel
  rather than silently missing events.
- **Acceptance:** a deliberately stalled watcher observes exactly one overflow
  signal and can recover a consistent view by re-reading.

### D5 — stop swallowing rotation errors — **S**
- **Problem:** `Update`, `Delete`, and `CommitTx` discard the rotation result
  (`_ = c.rotateSegment()`, e.g. `collection.go:347, 655`); only `Insert`
  checks it. A failed rotation (e.g. disk full) is silently ignored on three of
  four write paths.
- **Approach:** propagate the error (matching `Insert`'s pattern at
  `collection.go:309`). The write itself already succeeded, so wrap as a
  non-fatal warning return or a logged error — decide one consistent policy and
  apply it on all four paths.
- **Files:** `engine/collection.go`.
- **Tests:** `collection_test.go` — inject a rotation failure; assert it is
  surfaced (not swallowed) on every write path.
- **Acceptance:** no write path silently ignores a rotation error.

### D6 — transaction GC / idle expiry — **S/M**
- **Problem:** `TxManager` (`txmanager.go`) never evicts transactions. A client
  that calls `BeginTx` and disconnects leaks the `*Tx`, its staged ops, and its
  reserved id forever.
- **Approach:** stamp each `Tx` with `createdAt`/`lastUsed`; run a sweeper
  goroutine that rolls back + removes transactions idle beyond a configurable
  TTL (default 5m). Expose `--tx-timeout` (Config + YAML + flag, per the
  "Adding a new server flag" checklist in CLAUDE.md).
- **Files:** `engine/txmanager.go`, `server/config.go`,
  `cmd/filedb/main.go`.
- **Tests:** new `txmanager_test.go` — idle tx is reaped after TTL; active tx is
  not; reaped tx commit fails cleanly.
- **Acceptance:** abandoned transactions are bounded in number and memory.

---

## v0.3.0 — Query at scale

The flagship arc. Today every non-indexed query materializes the entire
collection in RAM (`Scan` slow path, `collection.go:425`) and `limit`/`offset`/
`order_by` are applied *after* the fact in the gRPC handler (`grpc.go:151-178`),
so `Find ... limit 10` still reads the whole collection.

### Q1 — streaming, push-down Find (limit/offset honored before materialization) — **L**
- **Problem:** see above — no I/O or memory savings from `limit`; the streaming
  RPC streams results that were already fully buffered.
- **Approach:** redesign the read path to stream:
  1. Build the live-id set incrementally instead of a full `map[uint64]Entry`.
  2. Push `limit`/`offset` into the engine; stop scanning once `offset+limit`
     matches are produced (when no `order_by`, or when ordering by an indexed
     field).
  3. Stream `ScanResult`s through a channel to `Find` so the server emits as it
     reads rather than buffering. Respect `ctx` cancellation (see Q4).
  4. When `order_by` is present and unindexed, fall back to bounded buffering
     (top-K heap of size `offset+limit`) rather than full sort of all rows.
- **Proto/API:** no breaking change (`limit`/`offset`/`order_by` already exist);
  semantics tightened. Document ordering guarantees.
- **Files:** `engine/collection.go` (new `ScanStream`), `server/grpc.go`
  (consume the stream), `docs/architecture.md`.
- **Tests:** `collection_test.go` + `grpc_integration_test.go` — large-collection
  bench shows `limit 10` reads/holds O(limit), not O(n); top-K ordering correct.
- **Acceptance:** memory + rows-read for a limited query are bounded by
  `offset+limit`, not collection size (verified by a bench/metric).

### Q2 — typed, directional `order_by` — **S/M**
- **Problem:** ordering uses `fmt.Sprintf("%v")` string compare with only a
  float64 fast path (`grpc.go:153`); ints-as-`json.Number` and mixed types sort
  lexically, and there's no descending option.
- **Approach:** reuse the existing typed comparison logic in `query/filter.go`
  (it already orders float/int/string/json.Number for `gt`/`lt`). Add a sort
  direction (`order_dir` / `-field` convention). Centralize comparison so filter
  and sort share one code path.
- **Proto/API:** add `string order_dir = 6;` (or reuse a `-`-prefix) to
  `FindRequest`; regenerate.
- **Files:** `proto/filedb.proto`, `query/filter.go` (export a
  `Compare`), `server/grpc.go`, CLI `find` flag, clients/docs.
- **Tests:** `filter_test.go` + integration — numeric vs string ordering, desc.
- **Acceptance:** `order_by age` sorts 2 < 10; `desc` reverses correctly.

### Q3 — range-capable secondary indexes — **L**
- **Problem:** secondary indexes only accelerate `OpEq` (`collection.go:411`);
  `gt/gte/lt/lte` always full-scan despite being supported filters.
- **Approach:** add an ordered index structure (sorted slice of keys with binary
  search, or a B-tree) alongside the current hash index. `Scan` picks the index
  for range predicates and AND-combines candidate id sets. Persist with the same
  tmp+rename+checksum pattern; rebuild after compaction.
- **Proto/API:** optionally extend `EnsureIndex` with an index-type hint
  (`hash` | `ordered`); default `ordered` so it serves both eq and range.
- **Files:** `engine/secondary_index.go` (or new `ordered_index.go`),
  `collection.go` (planner), `proto/filedb.proto`, `server/grpc.go`, CLI, docs.
- **Tests:** `secondary_index_test.go` — range lookups match full-scan results;
  survives update/delete/compaction; persistence round-trips.
- **Acceptance:** a `gt`/`lt` query on an indexed field reads O(matches), not
  O(collection), and returns identical results to the scan path.

### Q4 — context cancellation into the engine — **S/M**
- **Problem:** handlers don't thread `ctx` into the engine, so a client
  cancelling a long `Find`/`Scan` doesn't stop server-side work.
- **Approach:** add `ctx` params to `ScanStream`/long reads; check
  `ctx.Err()` between segments/batches and abort. Pairs naturally with Q1.
- **Files:** `engine/collection.go`, `server/grpc.go`.
- **Tests:** integration — cancel a streaming Find mid-flight; server stops
  reading promptly.
- **Acceptance:** cancelled queries release engine work within one batch.

---

## v0.4.0 — Feature breadth

### F1 — TTL / expiring records — **M** ✅ (engine + config; wire-protocol surfacing deferred)
- **Why:** natural fit for the cache/IoT/session use cases the README targets.
- **Approach:** optional per-record deadline plus a collection-level default TTL.
  A reaper (on the compactor cadence) tombstones expired ids; compaction drops
  expired records; reads filter them out defensively so an expired record is
  invisible the instant its deadline passes, before reclamation.
- **Delivered:** `store.Entry.ExpiresAt` (Unix-nano, folded into the CRC,
  omitted when unset — backward compatible) and a mirrored `IndexEntry.ExpiresAt`
  so reads drop expired records without a disk hit. Engine API:
  `InsertWithExpiry` / `UpdateWithExpiry` (explicit deadline) and
  `CollectionConfig.DefaultTTL` (now+TTL on inserts lacking one). Updates keep a
  record's deadline (sticky) unless `UpdateWithExpiry` overrides it. Server
  `--default-ttl` flag (Config + YAML + flag). Reaper in `compactLoop`;
  `resolveEntries` drops expired entries during compaction.
- **Deferred (follow-up):** per-record `expires_at` / `ttl_seconds` on the
  Insert/Update RPCs and per-collection default TTL in `CreateCollection`, plus
  the 7 language clients. Kept out of this PR to keep it engine-first and
  reviewable, matching the KEY-1 precedent; TTL is fully usable via the embedded
  engine and the server-wide `--default-ttl`.
- **Files:** `store/ndjson.go`, `engine/index.go`, `engine/collection.go`,
  `engine/ttl.go`, `engine/compactor.go`, `engine/keys.go`, `engine/scan.go`,
  `server/config.go`, `cmd/filedb/main.go`, docs.
- **Tests:** `engine/ttl_test.go` — hidden after deadline, visible before,
  default-TTL applied, sticky across update, override, reaped, reclaimed by
  compaction, survives reopen, excluded from secondary-index lookups;
  `store` round-trip + omitempty.
- **Acceptance:** ✅ expired records are invisible to reads and reclaimed by
  compaction.

### F2 — backup / snapshot — **S/M** ✅
- **Why:** high perceived value, cheap given append-only files.
- **Approach:** `filedb-cli backup <dest>` + a streaming `Snapshot` RPC that
  produces a consistent gzip tarball of the data dir. Restore is just untar into
  `--data`.
- **Delivered:** `DB.SnapshotTo(io.Writer)` writes a gzip-compressed tar of every
  collection's files. Consistency: the DB registry is held read-locked for the
  whole archive (no create/drop/reopen mid-snapshot) and each collection's files
  are copied under its own read lock (no write, rotation, or compaction during
  the copy). The primary `index.json` is **excluded** — it stores absolute
  segment paths and a self-only checksum, so the restored collection rebuilds it
  from segments on open; secondary indexes (path-independent value→id maps) are
  refreshed and included. Streaming `Snapshot` RPC (gRPC-only; binary streaming
  does not map cleanly onto REST) sends 64 KiB gzip chunks. `filedb-cli backup
  <dest>` writes the stream to a `.tar.gz` and prints the restore command; REPL
  `backup` verb added.
- **Proto/API:** new streaming `Snapshot` RPC.
- **Files:** `engine/snapshot.go`, `engine/db.go` (via `SnapshotTo`),
  `proto/filedb.proto`, `server/grpc.go`, `cmd/filedb-cli/commands.go`,
  `cmd/filedb-cli/main.go`, `cmd/filedb-cli/repl.go`, docs.
- **Tests:** `engine/snapshot_test.go` — round-trip (multi-segment, update +
  delete + secondary index) restores to identical query results, empty-DB
  archive; `server/grpc_integration_test.go` `TestIntegration_Snapshot` — stream
  the RPC, extract, reopen, verify record count.
- **Acceptance:** ✅ a backup taken under concurrent writes restores to a
  consistent state (per-collection point-in-time via the read lock; segments are
  append-only so the captured active tail always ends on a valid entry).

### F3 — on-demand compaction (RPC + CLI) — **S** ✅
- **Why:** today compaction is only automatic (dirty-ratio/timer); operators
  want to force it (e.g. before backup).
- **Approach:** add a `Compact(collection)` RPC that runs a synchronous,
  forced compaction pass and returns only after it completes;
  `filedb-cli compact <collection>`.
- **Delivered:** `Collection.CompactNow()` runs a forced pass (bypassing the
  dirty-ratio gate) and `DB.Compact(name)` wraps it. Background and on-demand
  passes serialize via a new `compactMu` so they never race the sealed-segment
  swap; `compact(force bool)` skips the dirty gate when forced. New `Compact`
  RPC (`POST /v1/{collection}/compact`), `server/grpc.go` handler, and
  `filedb-cli compact <collection>` (plus a REPL `compact` verb). A closed
  collection refuses to compact.
- **Proto/API:** new `Compact` RPC.
- **Files:** `proto/filedb.proto`, `engine/compactor.go` (synchronous forced
  entry + serialization), `engine/collection.go`, `engine/db.go`,
  `server/grpc.go`, `cmd/filedb-cli/commands.go`, `cmd/filedb-cli/main.go`,
  `cmd/filedb-cli/repl.go`, docs.
- **Tests:** `engine/compact_ondemand_test.go` — forces a merge below the dirty
  threshold and reduces segment count, refuses on a closed collection;
  `server/grpc_integration_test.go` `TestIntegration_Compact` — RPC round-trip,
  records survive, unknown collection is NotFound.
- **Acceptance:** ✅ `compact` returns after a full compaction pass completes.

---

## v0.5.0 — Auth & multi-tenancy

### A1 — multiple scoped, rotatable API keys — **M/L** — ✅ Delivered
- **Problem:** one shared static key, no per-client identity, rotation, or
  read/write scoping (`internal/auth/apikey.go`).
- **Approach:** load a key set (key → {name, scope}) from config/file; interceptor
  resolves the presented key to a principal and enforces read vs read-write
  scope per RPC. Support hot-reload for rotation. Keep single-key + no-auth modes
  for backward compatibility.
- **Proto/API:** no proto change; config schema gains a `keys:` list.
- **Files:** `internal/auth/`, `server/config.go`, `cmd/filedb/main.go`, docs.
- **Tests:** `auth` unit tests — scope enforcement, unknown key rejected,
  reload picks up new keys; constant-time compare preserved.
- **Acceptance:** a read-scoped key is rejected on writes; keys rotate without
  restart.

---

## Sequencing & delegation

```
v0.2.0  PR-A: D1 + D2 + D5         (durability hardening — do first)
        PR-B: D6 (tx GC)           + new --tx-timeout flag
        PR-C: D4 (watch overflow)  + proto enum
        PR-D: D3 (record checksums)
v0.3.0  Agent-1: Q1 + Q4           (streaming read path — flagship)
        PR-E:    Q2                (typed order_by)
        Agent-2: Q3                (range indexes)
v0.4.0  Agent-3: F1 (TTL)
        PR-F:    F2 (backup) , PR-G: F3 (compact)
v0.5.0  Agent-4: A1 (scoped keys)
```

Each large item (Q1, Q3, F1, A1) is a candidate warden development agent in its
own worktree; the small items ship as direct PRs. Every merged item ticks its
box above and updates `ROADMAP.md`, `docs/`, and `README.md` per CLAUDE.md
conventions.

---

## Checklist

**v0.2.0 — Hardening**
- [x] D1 — directory fsync on create/rotate/rename
- [x] D2 — atomic, off-hot-path `meta.json`
- [x] D3 — per-record segment checksums
- [x] D4 — Watch overflow signal
- [x] D5 — propagate rotation errors on all write paths
- [x] D6 — transaction GC / `--tx-timeout`

**v0.3.0 — Query at scale**
- [x] Q1 — streaming, push-down Find
- [x] Q2 — typed, directional `order_by`
- [x] Q3 — range-capable secondary indexes
- [x] Q4 — context cancellation into the engine

**v0.4.0 — Features**
- [x] F1 — TTL / expiring records (engine + config; RPC/client surfacing deferred)
- [x] F2 — backup / snapshot (streaming `Snapshot` RPC + `filedb-cli backup`)
- [x] F3 — on-demand compaction (RPC + CLI)

**v0.5.0 — Auth**
- [x] A1 — multiple scoped, rotatable API keys (config `keys:` list + `SIGHUP` reload)
