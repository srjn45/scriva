# FileDBv2 — Embedding Roadmap (warden integration)

> Scope note: this is a **use-case roadmap**, separate from the release roadmap
> in [`ROADMAP.md`](ROADMAP.md) / [`docs/roadmap-v0.2.md`](docs/roadmap-v0.2.md).
> It tracks only the features FileDBv2 needs so that **warden** can adopt it as
> an **embedded, in-process store** — no sidecar server, no gRPC, no API key.
> Items here are sized **S** (≤½ day), **M** (1–2 days), **L** (3+ days) and,
> where they overlap the release roadmap, cross-reference it instead of
> duplicating.

---

## The integration model

warden is a **single-user, single-machine, single static binary** daemon. Its
current store is **embedded in the daemon process** (an `RWMutex`-guarded
per-file JSON store; the daemon is the only holder). The only architecture that
preserves warden's single-binary install and zero-sidecar footprint is to
**compile FileDBv2's engine into warden** and call it in-process:

```
warden daemon
  └── import github.com/srjn45/filedbv2/engine   ← in-process, no network
        └── engine.DB  ──►  data/warden/<collection>/seg_*.ndjson
```

So the server, REST gateway, gRPC, and API-key layers are **out of scope** for
this use case — warden never talks to a socket. Everything below is about making
the **engine** a first-class embeddable library.

The **non-negotiable blocker** is that the engine currently lives in
`internal/engine`, which Go forbids other modules from importing. Until EMB-1
lands, warden can only run FileDBv2 as a separate server — which we've ruled out.

---

## Target collection layout (the "separate files" point)

Today warden co-locates or single-files several things that should be their own
collections. FileDBv2's native model (**one directory per collection, one record
per line, append-only**) gives us that split for free — modeling each concern as
a collection is what removes the whole-file-rewrite cost.

| warden store (today) | Today's shape | Problem | FileDB collection (target) | Key |
|---|---|---|---|---|
| `internal/store` sessions | one JSON file / session, **events embedded as a growing slice** | every status tick rewrites the whole growing file (O(n) write-amp) | `sessions` | session id (string) |
| — (events) | embedded `Events []Event` in the session file | see above | **`events`** (append-only) | session id (secondary index) + ts |
| `internal/mailbox` | one JSON file / recipient, **full rewrite on every append** | write-amp per message; inbox grows unbounded | **`messages`** (append-only) | recipient id (secondary index) |
| `internal/ctxstore` | single `context.json`, **whole map rewritten** on every Set/Append, CAS | single-file, doesn't scale; rewrite on each write | **`context`** | context key (string), version for CAS |
| `internal/spend` | ledger | ok-ish | `spend` | — |
| `internal/savings` | NDJSON ledger (already append-only ✅) | already good — the pattern we're generalizing | `savings` | — |
| `internal/pipeline` | store | — | `pipelines` | pipeline id |
| `internal/schedule` | single `schedules.json` (few, low-freq) | fine as-is; migrate for uniformity | `schedules` | schedule id |
| `internal/snapshot` | store | — | `snapshots` | snapshot id |

**Net effect for warden:** `sessions` becomes a thin, bounded record; the
unbounded, append-heavy data (`events`, `messages`) moves to append-only
collections where a write is a single line, not a full-file rewrite; and the
single-file `context.json` becomes a keyed collection with real CAS. This is the
concrete answer to "put session data, messages, and context into separate
files."

---

## Milestones

| Milestone | Theme | Items | Why it's needed for warden |
|---|---|---|---|
| **EMB** | Embeddable engine | EMB-1 … EMB-4 | Make `engine` importable, ergonomic, and dependency-light |
| **KEY** | Keys, uniqueness & CAS | KEY-1 … KEY-4 | warden uses **string** ids, **unique names**, and **compare-and-swap** writes |
| **QRY** | Query fit | QRY-1 … QRY-3 | `List` newest-first, lookups, counts — mostly cross-refs release roadmap |
| **OPS** | Embedded operations | OPS-1 … OPS-5 | durability default, in-process Watch, migration, backup |

---

## EMB — Embeddable engine

### EMB-1 — promote the engine to a public, importable package — **M** — 🔴 blocker
- **Problem:** `internal/engine` cannot be imported from the warden module
  (`internal/` is module-private). This blocks the entire integration.
- **Approach:** move `internal/engine` → a public path (`engine/`, or `pkg/engine`),
  and repoint the server (`server/grpc.go`) and CLI at the public package so there
  is exactly one engine. Keep `internal/store`, `internal/query` either public too
  (they're on the public API surface: `store.Entry`, `query.Filter`) or re-export
  the needed types from `engine`. Nothing about behavior changes — this is a
  visibility + package-move refactor guarded by the existing test suite.
- **API to expose (stable):** `engine.Open`, `engine.DB`, `engine.Collection`
  (`Insert/Update/Delete/FindByID/Scan/Watch`), `engine.CollectionConfig`,
  `engine.SyncMode`, `query.Filter` constructors, `store.Entry`.
- **Files:** `internal/engine/*` → `engine/*`; update imports in `server/`,
  `cmd/`; `internal/query`, `internal/store` visibility.
- **Tests:** existing engine + server suites must pass unchanged after the move.
- **Acceptance:** an external module can `import "github.com/srjn45/filedbv2/engine"`
  and open a DB, with no `internal/` reference.

### EMB-2 — embedded façade / convenience constructor — **S**
- **Problem:** `engine.Open(dataDir, CollectionConfig)` opens with a single
  config; warden wants a DB that hosts several collections (`sessions`, `events`,
  `messages`, `context`, …) with per-collection config, and a dead-simple
  "open a store rooted at this dir" call.
- **Approach:** add a thin embedded entrypoint (e.g. `filedb.Open(dir, opts...)`
  returning a handle with `Collection(name, cfg)` / `MustCollection`), defaulting
  sync mode, segment size, and compaction cadence sensibly for a local daemon.
  This is sugar over the existing `DB` — no engine changes.
- **Files:** new top-level `filedb` package (or `engine.OpenDB` helper), docs.
- **Tests:** open → create N collections → CRUD → reopen recovers all.
- **Acceptance:** warden can stand up its full collection set in <10 lines.

### EMB-3 — dependency hygiene: embed without the server stack — **M**
- **Problem:** if importing `engine` transitively drags in gRPC, grpc-gateway,
  protobuf, cobra, and Prometheus, warden's binary and `go.mod` bloat for code it
  never runs. The embed path must pull **only** the engine's real deps
  (std lib + minimal).
- **Approach:** ensure `engine`/`query`/`store` do not import `server/`, `pb/`,
  `auth/`, `metrics/`, or `cobra`. Metrics stays behind the existing
  `OnCompaction` hook (an interface warden can ignore). Verify with
  `go mod graph` / `go list -deps` that importing `engine` brings in no
  grpc/protobuf/prometheus. Consider a `go.mod` `tool`/build-tag split only if a
  stray dep can't be untangled.
- **Files:** engine package boundary; a CI check (`make deps-check`) asserting the
  embed surface's dependency set.
- **Tests:** a tiny `embeddemo` module that imports only `engine` and builds; CI
  fails if the dep set grows.
- **Acceptance:** `go list -deps ./engine` contains no grpc/protobuf/prometheus/cobra.

### EMB-4 — semver the embedded API + module contract — **S**
- **Problem:** warden pins `github.com/srjn45/filedbv2` as a library dependency;
  it needs a stable, versioned API surface, not "whatever main is."
- **Approach:** document the embedded API as public/stable, tag `v0.x` releases
  that warden can `go get`, and note any breaking change policy. (Distribution for
  this use case is `go get`, **not** brew/apt/GHCR — those stay for the standalone
  server.)
- **Files:** `README.md` (embedding section), `docs/embedding.md` (new), CHANGELOG.
- **Acceptance:** `go get github.com/srjn45/filedbv2/engine@vX.Y.Z` works and the
  documented API matches.

---

## KEY — Keys, uniqueness & CAS

### KEY-1 — caller-supplied string primary keys — **M** — 🔴 core mismatch
- **Problem:** the engine assigns **`uint64`** auto-ids
  (`Insert(data) (uint64, ...)`, `map[uint64]IndexEntry`). warden's identifiers
  are **strings** (session ids, agent names, context keys, recipient ids). Storing
  the warden id only as a data field forces an index lookup on every access and
  loses primary-key uniqueness.
- **Approach:** support a caller-supplied string key. Options (pick one, design in
  an issue): (a) generalize the primary index to `map[string]IndexEntry` with the
  numeric path as a special case; (b) add `InsertWithKey(key string, data)` +
  `FindByKey(key)` backed by a mandatory unique index on a reserved `_key` field.
  Must round-trip through segments, compaction, and index rebuild.
- **Files:** `engine/index.go`, `engine/collection.go`, `store/ndjson.go` (entry
  carries the key), docs.
- **Tests:** insert/get/update/delete by string key; survives compaction + reopen;
  duplicate key rejected (see KEY-2).
- **Acceptance:** `FindByKey("sess-abc123")` is O(1) and stable across compaction.

### KEY-2 — unique index / uniqueness constraint — **S/M**
- **Problem:** warden enforces **name uniqueness** by scanning all sessions under
  a lock (`FileStore.Insert`). A DB-enforced unique index removes the O(n) scan and
  the race window.
- **Approach:** extend `EnsureIndex(field, unique=true)`; a write violating
  uniqueness returns `ErrDuplicateKey`. Builds on the existing secondary-index
  machinery (`sidx_*.json`).
- **Files:** `engine/secondary_index.go`, `engine/collection.go`, docs.
- **Tests:** duplicate value on a unique field is rejected on insert and on update;
  non-unique index unaffected; survives rebuild.
- **Acceptance:** a second `sessions` record with an existing `name` fails with a
  typed duplicate error.

### KEY-3 — compare-and-swap / conditional write — **M** — 🔴 core mismatch
- **Problem:** warden relies on optimistic writes: `UpdateStatusIf` /
  `FinalizeExit` (CAS on expected status) and `ctxstore.CompareAndSet`. The engine
  has optimistic **transactions** but no ergonomic single-record CAS.
- **Approach:** add a per-record version (monotonic `rev`, already implicit in the
  latest-entry-wins model) and a conditional update:
  `UpdateIf(key, expectedRev, data)` and/or predicate form
  `UpdateIf(key, func(cur) bool, data)`. Return `(applied bool, err)` so a
  no-match is a clean no-op, matching warden's CAS semantics. Can be built over the
  existing tx path or as a direct locked read-check-write.
- **Files:** `engine/collection.go`, `store/ndjson.go` (expose `rev`/ts), docs.
- **Tests:** concurrent CAS — exactly one of two racing swaps applies; stale rev
  no-ops; CAS on missing key returns `(false, nil)`.
- **Acceptance:** warden's `UpdateStatusIf` and `context` CAS map 1:1 onto this
  primitive with no application-side locking.

### KEY-4 — upsert — **S**
- **Problem:** several warden call sites are "create-or-replace" (e.g. archive
  overwrites `closed/<id>.json`).
- **Approach:** `Upsert(key, data)` — insert if absent, replace if present, one
  locked op.
- **Files:** `engine/collection.go`.
- **Tests:** upsert absent → insert; upsert present → replace, single live entry.
- **Acceptance:** archive/move flows need no get-then-branch.

---

## QRY — Query fit

Mostly satisfied by the release roadmap; called out here so the warden port has a
tracked dependency.

### QRY-1 — ordered scan by field, newest-first — **cross-ref v0.3.0 Q2**
- warden's `List` returns sessions sorted by `UpdatedAt` **descending**. At
  warden's scale (dozens–hundreds active) an in-memory sort after an eq/prefix
  scan is fine, but a directional `order_by` (release-roadmap **Q2**) makes it
  first-class. **No new item — depends on Q2.**

### QRY-2 — range-capable index for time windows — **cross-ref v0.3.0 Q3**
- `events`, `messages`, `spend`, `savings` are queried by **time window** and by
  **owner id**. Owner id is served by the existing eq secondary index; time-window
  (`ts >= X`) wants the range index from release-roadmap **Q3**. **No new item —
  depends on Q3.**

### QRY-3 — count / exists helpers — **S**
- **Problem:** warden does existence checks (id taken?) and counts (active agents,
  ledger size) that currently mean a full scan.
- **Approach:** `Count(filter)` and `Exists(key)` that use indexes when available
  and short-circuit.
- **Files:** `engine/collection.go`.
- **Tests:** count matches scan length; exists is O(1) on indexed key.
- **Acceptance:** dashboard/list counts don't materialize the collection.

---

## OPS — Embedded operations

### OPS-1 — embedded durability default — **S**
- **Problem:** warden's current store is intentionally **not fsync'd** (localhost;
  torn reads are prevented by temp+rename, power-loss durability is not required).
  The embedded default should match that trade-off, not the server default.
- **Approach:** document + default the embedded façade (EMB-2) to `SyncMode`
  `none` or `interval` (recommend `interval`, ~1s) so warden gets crash-torn-write
  safety without per-write fsync cost; let warden opt into `always` per collection
  (e.g. `spend`) if desired.
- **Files:** `filedb`/`engine` defaults, `docs/embedding.md`.
- **Acceptance:** default embedded write path has no per-write fsync; documented.

### OPS-2 — in-process Watch for the poller/TUI — **S** (mostly exists)
- **Problem/opportunity:** warden's daemon polls the store to drive the TUI and
  guards. The engine already emits `Watch` events in-process — warden can subscribe
  directly instead of re-scanning.
- **Approach:** confirm `Collection.Watch` is usable purely in-process (no gRPC),
  document the embedded subscription API, and honor the **overflow signal**
  (release-roadmap **D4**, already shipped) so a slow warden consumer resyncs.
- **Files:** `docs/embedding.md`, example.
- **Acceptance:** an embedded subscriber receives insert/update/delete + overflow
  sentinel without any server running.

### OPS-3 — migration/import from warden's existing JSON — **M**
- **Problem:** existing warden installs have `sessions/*.json`, `closed/*.json`,
  `mailbox/*.json`, `context.json`, ledgers on disk. Adoption must not lose them.
- **Approach:** this is primarily **warden-side** (a one-shot importer that reads
  the old layout and inserts into the new collections, splitting embedded
  `Events` into the `events` collection). FileDBv2's contribution: a documented
  bulk-load path (`InsertMany`, already present) and guidance in
  `docs/embedding.md`. Optionally a small `LoadJSONL(collection, reader)` helper.
- **Files:** `docs/embedding.md`; optional `engine` bulk-load helper.
- **Tests:** (warden-side) old data round-trips to identical query results.
- **Acceptance:** a one-command migration produces byte-equivalent logical state.

### OPS-4 — backup / snapshot of the data dir — **cross-ref v0.4.0 F2**
- warden users expect their data to be inspectable and backup-able (it's a warden
  ethos — human-readable, git-friendly JSON). The append-only NDJSON files already
  satisfy "inspectable"; **F2** (backup/snapshot) from the release roadmap covers
  consistent backups. **No new item — depends on F2.**

### OPS-5 — on-demand + bounded compaction for a long-lived daemon — **cross-ref v0.4.0 F3 + rebalancer**
- warden's daemon runs for weeks; `events`/`messages` grow steadily. The existing
  background compactor + rebalancer handle this, and on-demand compaction (**F3**)
  lets warden force a pass (e.g. before snapshot). Verify compaction cadence
  defaults are sane for many small collections. **Mostly existing — depends on F3.**

---

## What FileDBv2 already gives us (don't rebuild)

- **One dir per collection, append-only NDJSON** → the "separate files for
  sessions / events / messages / context" model is native.
- **Append-only writes** → kills the whole-file-rewrite write-amplification that
  hurts warden's embedded `Events` slice and `mailbox` inboxes today.
- **In-memory primary index + eq secondary indexes** → O(1) id and equality
  lookups (owner-id fan-out for `events`/`messages`).
- **Background compaction + rebalancer** → bounds disk for long-running daemons.
- **Per-collection `RWMutex`** → matches warden's single-writer daemon; no extra
  locking needed on warden's side.
- **Watch change feed (+ overflow signal)** → can drive warden's poller/TUI.
- **Configurable durability + crash recovery (torn-line truncation, atomic
  rename, dir fsync)** → matches/exceeds the current store's guarantees.
- **Human-readable files** → preserves warden's inspectable-data ethos.

---

## Distribution

- **This use case:** FileDBv2 ships to warden as a **Go module dependency**
  (`go get github.com/srjn45/filedbv2/engine`). Not brew/apt/GHCR — those remain
  for the standalone server/CLI.
- **Requirement:** a public, semver-tagged `engine` package (EMB-1, EMB-4) whose
  import graph is minimal (EMB-3).

---

## Sequencing

```
Phase 1 (unblock):   EMB-1  (public engine)  ← nothing else can start without it
                     EMB-3  (dep hygiene)    ← do alongside; cheap to keep clean
Phase 2 (fit):       KEY-1  (string keys)    ┐ the two real impedance mismatches
                     KEY-3  (CAS)            ┘
                     EMB-2  (embedded façade)
Phase 3 (polish):    KEY-2 (unique idx), KEY-4 (upsert), QRY-3 (count/exists),
                     OPS-1 (durability default), OPS-2 (in-proc Watch),
                     EMB-4 (semver + docs)
Phase 4 (migrate):   OPS-3 (import)  → then warden swaps FileStore → FileDBStore
                                        behind the existing store.Store interface
Depends on release roadmap: QRY-1→Q2, QRY-2→Q3, OPS-4→F2, OPS-5→F3
```

The warden side is a drop-in: `internal/store.Store` is already a clean
interface, so a `FileDBStore` implementation lands behind it with no churn in
callers — the port is gated only on Phase 1–2 here.

---

## Checklist

**EMB — Embeddable engine**
- [ ] EMB-1 — promote `internal/engine` → public `engine` package 🔴
- [ ] EMB-2 — embedded façade / multi-collection `Open`
- [ ] EMB-3 — dependency hygiene (no grpc/protobuf/prometheus/cobra on embed path)
- [ ] EMB-4 — semver the embedded API + `docs/embedding.md`

**KEY — Keys, uniqueness & CAS**
- [x] KEY-1 — caller-supplied string primary keys 🔴
- [x] KEY-2 — unique index / uniqueness constraint
- [x] KEY-3 — compare-and-swap / conditional write 🔴
- [x] KEY-4 — upsert

**QRY — Query fit**
- [ ] QRY-1 — directional order_by (→ release Q2)
- [ ] QRY-2 — range index for time windows (→ release Q3)
- [ ] QRY-3 — count / exists helpers

**OPS — Embedded operations**
- [ ] OPS-1 — embedded durability default
- [ ] OPS-2 — in-process Watch for poller/TUI
- [ ] OPS-3 — migration/import from warden's JSON layout
- [ ] OPS-4 — backup / snapshot (→ release F2)
- [ ] OPS-5 — on-demand + bounded compaction (→ release F3)
