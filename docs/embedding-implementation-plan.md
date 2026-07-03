# Embedding Roadmap — Implementation Plan

> Execution plan for [`roadmap.md`](../roadmap.md) (the warden-embedding roadmap).
> Each task below is sized for **one agent, one isolated worktree, one PR**.
> Tasks are self-contained: an agent should be able to implement its task from
> this document + the roadmap + the codebase, without prior conversation
> context. Design decisions are **already made** and recorded here — do not
> re-open them inside a task; if a task uncovers a reason a decision is wrong,
> stop and surface it instead.

---

## Locked design decisions

These were decided up front after reading the engine source. Every task assumes
them.

1. **EMB-1 moves three packages, not one.** `internal/engine`, `internal/store`,
   and `internal/query` all become public (`engine/`, `store/`, `query/`).
   `store.Entry` and `query.Filter` are on the engine's public API surface;
   re-exporting them via aliases creates type-identity friction for importers.
   Move them.
2. **KEY-1 uses the reserved-`_key` approach (roadmap option b), not a
   generalized primary index (option a).** The uint64 internal id stays
   untouched everywhere (`Index`, `IndexEntry`, `WatchEvent`, `CommitTx`,
   compaction). String keys are a reserved `_key` field in `data`, enforced
   unique by a **mandatory unique secondary index** and resolved to the uint64
   id via `IndexLookup`. Rationale: option (a) touches every offset/tombstone
   path in the engine; option (b) reuses the secondary-index machinery that
   already survives compaction and rebuild (`sidx_*.json` reload in
   `Collection.load`).
   - **Consequence: KEY-2 (unique index) is a hard prerequisite of KEY-1**,
     unlike the roadmap's original phase ordering. This plan reflects that.
3. **KEY-3 adds an explicit per-record `Rev uint64`.** Do **not** derive the
   revision from `Entry.Ts` (not collision-proof). `Rev` is added to
   `store.Entry` (`json:"rev,omitempty"` — old segments decode as rev 0) and
   tracked in `IndexEntry` so the current rev is readable without a segment
   read.
4. **EMB-2 requires one small engine addition:** `DB` today opens every
   collection with a single `defaultCfg`. Add
   `DB.CollectionWithConfig(name string, cfg CollectionConfig)` (lazy
   create-or-open with an explicit config) so the façade can give each
   collection its own durability/compaction settings.
5. **OPS-1 is folded into the EMB-2 task.** The embedded durability default is
   a property of the façade: `SyncModeInterval` (~1s) by default, opt-in
   `SyncModeAlways` per collection. The engine default (`SyncModeNone`) does
   not change.
6. **Cross-referenced items (QRY-1, QRY-2, OPS-4, OPS-5) get no tasks here.**
   They are dependencies on the release roadmap (Q2, Q3, F2, F3) and are
   tracked there.

---

## Task graph

```
T1 EMB-1 (package move)  ── must land ALONE first; every branch conflicts with it
 ├── T2  EMB-3  deps-check CI            (parallel-safe after T1)
 ├── T9  OPS-2  Watch docs + example     (parallel-safe after T1)
 └── T3  KEY-2  unique index
          └── T4  KEY-1  string keys
                   ├── T5  KEY-3  CAS / rev        ┐ all touch collection.go —
                   ├── T6  KEY-4  upsert           │ run SEQUENTIALLY in this
                   └── T7  QRY-3  count/exists     ┘ order, not in parallel
T5 ──► T8  EMB-2 + OPS-1  façade + embedded defaults
T4 ──► T10 OPS-3  bulk-load helper + migration docs   (parallel with T8)
T8, and all of T2–T10 ──► T11 EMB-4  semver + docs/embedding.md + tag
```

**Parallelization rules**

- **T1 runs alone.** It renames directories repo-wide; merge it before
  spawning anything else.
- After T1: **T2, T3, T9 can run in parallel** (disjoint files).
- **T3 → T4 → T5 → T6 → T7 are sequential** — they all edit
  `engine/collection.go` (and several also `engine/secondary_index.go`);
  parallel agents would conflict on every hunk.
- **T8 and T10 can run in parallel** with each other (new package / docs+small
  helper), once their deps are merged.
- **T11 is last** — it documents and tags whatever API actually shipped.

Every task follows CLAUDE.md conventions: race detector on
(`make test`), lint (`make lint`), docs updated in the same PR, conventional
commit style, one PR per task. Commits must use the
`29410402+srjn45@users.noreply.github.com` author email or pushes are rejected.

---

## Tasks

### T1 — EMB-1: promote engine to public packages 🔴 blocker

- **Branch:** `feat/emb-1-public-engine`
- **Depends on:** nothing. **Blocks:** everything.
- **Scope:**
  1. `git mv internal/engine engine`, `git mv internal/store store`,
     `git mv internal/query query`.
  2. Rewrite imports (`github.com/srjn45/filedbv2/internal/{engine,store,query}`
     → `github.com/srjn45/filedbv2/{engine,store,query}`) across `server/`,
     `cmd/`, and all `_test.go` files. Mechanical — sed/gofmt, no logic edits.
  3. Zero behavior change. Do not refactor anything else in the same PR.
- **Notes for the agent:**
  - `engine/index.go` checksums only the serialized entries map — no source
    paths — so existing on-disk `index.json` files stay valid. Nothing on disk
    changes format.
  - Update CLAUDE.md's project-layout section and any docs that reference
    `internal/engine`.
- **Acceptance:**
  - `make test` and `make lint` pass unchanged.
  - A scratch module outside the repo can
    `import "github.com/srjn45/filedbv2/engine"`, call `engine.Open`, and
    build (verify manually with a temp module; the permanent CI check is T2).

### T2 — EMB-3: dependency-hygiene CI gate

- **Branch:** `ci/emb-3-deps-check`
- **Depends on:** T1. **Parallel-safe with:** T3, T9.
- **Scope:**
  1. `make deps-check`: fail if
     `go list -deps ./engine ./store ./query` contains
     `grpc|protobuf|prometheus|cobra|grpc-gateway`.
  2. Add a tiny `embeddemo/` module (own `go.mod`, replaced to the repo root)
     that imports only `engine` and builds in CI.
  3. Wire both into the existing GitHub Actions workflow (alongside the
     proto-stub-diff and golangci-lint jobs).
- **Notes:** the engine is expected to *already* be clean — metrics enter only
  via the `OnCompaction` hook in `CollectionConfig`. This task locks that in;
  if the check fails, that's a finding to fix, not to suppress.
- **Acceptance:** `make deps-check` passes locally and in CI; deliberately
  adding a grpc import to `engine` makes it fail.

### T3 — KEY-2: unique secondary index

- **Branch:** `feat/key-2-unique-index`
- **Depends on:** T1. **Blocks:** T4.
- **Scope:**
  1. Extend `EnsureIndex(field string)` to accept uniqueness — keep the old
     signature working (e.g. add `EnsureUniqueIndex(field)` or variadic
     options; pick whichever fits existing call sites in `server/grpc.go` and
     the CLI with least churn) and persist the unique flag in `sidx_<field>.json`.
  2. Enforcement point: the write path already calls
     `sidxIndexEntry`/`sidxUpdateEntry` under `c.mu.Lock`, so a
     check-then-set there is atomic. A unique index mapping the value to a
     **different** live id ⇒ reject the write with a typed
     `var ErrDuplicateKey = errors.New("engine: duplicate key")`
     (wrap with field/value context). Reject on **insert and update**; the
     rejected write must not append to the segment or mutate any index.
  3. `CommitTx` must enforce it too (pre-validate staged ops before applying).
  4. Rebuild path (`sidx.rebuild`) must tolerate historical duplicates
     gracefully (last-write-wins already resolved them) — uniqueness is
     enforced on new writes.
- **Tests:** duplicate on insert rejected; duplicate on update rejected;
  non-unique index unaffected; unique flag survives persist/reopen/rebuild;
  concurrent inserts of the same value — exactly one wins (race detector).
- **Acceptance:** a second record with an existing value on a unique-indexed
  field fails with `errors.Is(err, ErrDuplicateKey)`.

### T4 — KEY-1: caller-supplied string primary keys 🔴

- **Branch:** `feat/key-1-string-keys`
- **Depends on:** T3. **Blocks:** T5, T6, T7, T10.
- **Scope (locked design — reserved `_key` field, decision #2):**
  1. Reserve the field name `_key`. `Insert`/`Update` reject user data that
     sets `_key` directly (typed error) — it is only settable via the keyed
     API.
  2. `Collection.InsertWithKey(key string, data map[string]any) (uint64, time.Time, error)`
     — stamps `data["_key"] = key`, ensures the collection's unique `_key`
     index exists (create lazily on first keyed write, or at
     collection-open via config), inserts; duplicate key ⇒ `ErrDuplicateKey`
     (from T3).
  3. `FindByKey(key)`, `UpdateByKey(key, data)`, `DeleteByKey(key)` — resolve
     via `IndexLookup("_key", key)` (O(1)) then the uint64 path. Missing key ⇒
     typed `ErrKeyNotFound`.
  4. Round-trip: keyed records must survive compaction, segment rotation, index
     rebuild, and reopen (the `_key` field lives in `data`, so this mostly
     falls out — test it anyway).
- **Out of scope:** changing `Insert`'s uint64 return, the primary index type,
  or `WatchEvent` (its `Data` already carries `_key`).
- **Tests:** CRUD by string key; duplicate rejected; key visible in Watch
  events; survives compaction + reopen; `_key` injection via plain
  `Insert` rejected.
- **Acceptance:** `FindByKey("sess-abc123")` is O(1) and stable across
  compaction and restart.

### T5 — KEY-3: per-record rev + compare-and-swap 🔴

- **Branch:** `feat/key-3-cas`
- **Depends on:** T4. **Blocks:** T8. **Do not run parallel with T6/T7.**
- **Scope (locked design — explicit rev, decision #3):**
  1. `store.Entry` gains `Rev uint64` (`json:"rev,omitempty"`); old segment
     lines decode as rev 0 — fully backward compatible.
  2. `IndexEntry` gains `Rev uint64` so current rev is readable without a
     segment read. Insert ⇒ rev 1; each update ⇒ rev+1. Rebuild recomputes
     revs by replay order (count writes per id). Compaction preserves the
     latest entry's rev.
  3. Expose rev on reads: add `Rev` to `ScanResult` and return it from
     `FindByID`/`FindByKey` (extend return types or add a `Get`-style method
     returning a small record struct — prefer a `Record{ID, Key, Rev, Ts, Data}`
     struct to stop the return-tuple growth).
  4. CAS primitives, both under one `c.mu.Lock` critical section:
     - `UpdateIfRev(key string, expectedRev uint64, data map[string]any) (applied bool, err error)`
     - `UpdateIfMatch(key string, pred func(cur map[string]any) bool, data map[string]any) (applied bool, err error)`
       (warden's `UpdateStatusIf`/`FinalizeExit`/ctxstore CAS check current
       *values*, so the predicate form is the one warden mostly uses).
     - Stale rev / false predicate / missing key ⇒ `(false, nil)` — a clean
       no-op, never an error.
- **Tests:** two goroutines racing the same CAS — exactly one applies (race
  detector); stale rev no-ops; missing key `(false, nil)`; rev increments
  across update/compaction/reopen; old rev-less segments load fine.
- **Acceptance:** warden's `UpdateStatusIf` and context CAS map 1:1 with no
  app-side locking.

### T6 — KEY-4: upsert

- **Branch:** `feat/key-4-upsert`
- **Depends on:** T4 (run after T5 to avoid `collection.go` conflicts).
- **Scope:** `Upsert(key string, data map[string]any)` — one `c.mu.Lock`
  critical section: key present ⇒ append update (rev+1); absent ⇒ append
  insert (rev 1). Emits the corresponding Watch event.
- **Tests:** upsert-absent inserts; upsert-present replaces (single live entry
  after compaction); concurrent upserts on one key serialize cleanly.
- **Acceptance:** archive/move flows need no get-then-branch.

### T7 — QRY-3: count / exists helpers

- **Branch:** `feat/qry-3-count-exists`
- **Depends on:** T4 (run after T6 — same files).
- **Scope:**
  1. `Count(f query.Filter) (uint64, error)`:
     nil/`MatchAll` ⇒ `index.Len()` (O(1)); single eq-filter on an indexed
     field ⇒ `len(IndexLookup(...))`; otherwise fall back to a scan that
     **counts without materializing** results (do not reuse `Scan` and take
     `len` — the slow path currently builds a full `latest` map of all data;
     counting needs ids/ops only, not `Data`).
  2. `Exists(key string) (bool, error)` — `IndexLookup("_key", key)`, O(1).
- **Tests:** count matches `len(Scan(...))` for indexed, non-indexed, and
  match-all filters; exists true/false; exists O(1) (no segment reads —
  assert via a counter hook or by construction).
- **Acceptance:** dashboard/list counts don't materialize the collection.

### T8 — EMB-2 + OPS-1: embedded façade with embedded defaults

- **Branch:** `feat/emb-2-facade`
- **Depends on:** T5 (façade should expose keys + CAS). **Parallel-safe
  with:** T10.
- **Scope:**
  1. Engine addition (decision #4):
     `DB.CollectionWithConfig(name string, cfg CollectionConfig) (*Collection, error)`
     — open-or-create with an explicit per-collection config; plain
     `Collection`/`CreateCollection` behavior unchanged.
  2. New top-level package `filedb/` (public):
     - `filedb.Open(dir string, opts ...Option) (*DB, error)`
     - `(*DB).Collection(name string, opts ...CollectionOption) (*engine.Collection, error)`
       and `MustCollection`.
     - Options for sync mode/interval, segment size, compaction cadence,
       watch buffer, unique indexes to ensure at open.
  3. **Embedded defaults (OPS-1):** `SyncModeInterval` @ 1s, engine defaults
     otherwise. Per-collection override to `SyncModeAlways` (e.g. a spend
     ledger) must be one option. Document the rationale (crash-torn-write
     safety without per-write fsync; matches warden's temp+rename trade-off).
  4. Docs: seed `docs/embedding.md` with the open-collections-CRUD-close
     example (T11 completes this doc).
- **Tests:** open → N collections with different configs → CRUD → close →
  reopen recovers all; default sync mode is interval; per-collection override
  honored.
- **Acceptance:** a warden-shaped store (`sessions`, `events`, `messages`,
  `context`, `spend`) stands up in <10 lines.

### T9 — OPS-2: in-process Watch documentation + example

- **Branch:** `docs/ops-2-embedded-watch`
- **Depends on:** T1 only. **Parallel-safe with:** T2, T3.
- **Scope:** no engine changes — `Collection.Subscribe` already returns a
  buffered channel with the `OpOverflow` resync sentinel, fully in-process.
  1. Document the embedded subscription API and the overflow contract
     (consumer must resync on `OpOverflow`) in `docs/embedding.md`
     (create the file if T8 hasn't; merge cleanly if it has).
  2. Add a runnable example (`examples/watch/` or an `Example*` test) showing
     subscribe → writes → events → overflow handling, no server.
- **Acceptance:** an embedded subscriber receives insert/update/delete +
  overflow sentinel with no server running, per the documented example.

### T10 — OPS-3: bulk-load helper + migration guidance

- **Branch:** `feat/ops-3-bulk-load`
- **Depends on:** T4. **Parallel-safe with:** T8.
- **Scope:**
  1. `Collection.LoadJSONL(r io.Reader, keyField string) (n int, err error)` —
     stream NDJSON records, insert each (via `InsertWithKey` when `keyField`
     is non-empty, else `Insert`), batching under the write lock for
     throughput; document that it uses the normal write path (indexes, watch,
     durability all apply).
  2. `docs/embedding.md`: a "migrating an existing JSON store" section
     covering the warden layout — per-file sessions (split embedded `Events`
     into an `events` collection), mailbox files → `messages`,
     `context.json` → keyed `context` records. The importer itself is
     warden-side; this documents the FileDBv2 half of the contract.
- **Tests:** load N lines → N records queryable; malformed line ⇒ error with
  line number, no partial index corruption; keyed load rejects duplicates.
- **Acceptance:** a documented one-command bulk path exists for the migration.

### T11 — EMB-4: semver contract, embedding docs, tag

- **Branch:** `docs/emb-4-semver`
- **Depends on:** all of T2–T10 merged. **Runs last.**
- **Scope:**
  1. Finish `docs/embedding.md`: full API reference for the stable surface
     (`filedb.Open`, `engine.DB/Collection` incl. keyed/CAS/upsert/count,
     `query.Filter`, `store.Entry`, Watch contract, durability modes,
     migration section) — reconcile the seeds from T8/T9/T10 into one doc.
  2. `README.md`: add an "Embedding" section; note distribution is `go get`
     for this use case (brew/apt/GHCR remain for the standalone server).
  3. CHANGELOG entry; state the v0.x breaking-change policy (minor bumps may
     break until v1).
  4. Tag `v0.x.0` (per CLAUDE.md: push the tag; CI releases — coordinate with
     the release-roadmap versioning before choosing the number).
- **Acceptance:** `go get github.com/srjn45/filedbv2/engine@v0.x.0` works from
  a clean module and the documented API matches what ships.

---

## Status

| Task | Item | Depends on | Status | PR |
|---|---|---|---|---|
| T1 | EMB-1 public engine 🔴 | — | ☐ todo | |
| T2 | EMB-3 deps-check CI | T1 | ☐ todo | |
| T3 | KEY-2 unique index | T1 | ☑ done | [#24](https://github.com/srjn45/FileDBv2/pull/24) |
| T4 | KEY-1 string keys 🔴 | T3 | ☐ todo | |
| T5 | KEY-3 CAS / rev 🔴 | T4 | ☐ todo | |
| T6 | KEY-4 upsert | T4 (after T5) | ☐ todo | |
| T7 | QRY-3 count/exists | T4 (after T6) | ☐ todo | |
| T8 | EMB-2 + OPS-1 façade | T5 | ☐ todo | |
| T9 | OPS-2 Watch docs | T1 | ☐ todo | |
| T10 | OPS-3 bulk load | T4 | ☐ todo | |
| T11 | EMB-4 semver + docs | T2–T10 | ☐ todo | |

Cross-referenced release-roadmap dependencies (no tasks here): QRY-1 → Q2,
QRY-2 → Q3, OPS-4 → F2, OPS-5 → F3.

---

## Delegating a task to an agent

Spawn each task in an **isolated worktree** off up-to-date `main`, one PR per
task. A sufficient agent brief is:

> Read `docs/embedding-implementation-plan.md` and `roadmap.md` in the repo
> root, then implement **task \<Tn\>** exactly as scoped, honoring the locked
> design decisions. Do not start if a dependency task's PR is unmerged. Follow
> CLAUDE.md for build/test/lint/docs/commit conventions. Open a PR titled with
> the task's conventional-commit subject.

Checklist for every task PR:

- [ ] `make test` (race detector) and `make lint` pass
- [ ] New behavior has engine unit tests; API-visible changes also covered in
      `server/grpc_integration_test.go` where applicable
- [ ] Docs updated in the same PR (per CLAUDE.md)
- [ ] Status table above updated (check the box, link the PR)
- [ ] Commit author email `29410402+srjn45@users.noreply.github.com`
