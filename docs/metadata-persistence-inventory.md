# Metadata persistence inventory (hardening §3.4, SR-D4 M4.1)

Evidence step for `metadata-serialization`. No production behaviour changes in this
step; the only production edits are nil-checked test seams (`CollectionConfig.postMetaSnapshotHook`
in `setDefaultTTL` / `applyEncryptionChange`, and `afterAtomicTempCreate` in `writeFileAtomic`).

## Writers of `meta.json`

`metaSnapshot()` (`engine/ttl.go`) reads `idSeq` (atomic), `createdAt` (plain), `explicitDefaultTTLSecs`
(plain `int64`) and `encMeta` (atomic pointer). `persistMeta` → `writeFileAtomic` always uses the single
fixed temp name `meta.json.tmp` opened `O_TRUNC`.

| # | Caller (file:func) | Locks held at snapshot / persist | Error handling | In-memory state vs persist |
|---|---|---|---|---|
| 1 | `ttl.go: setDefaultTTL` (from `db.go` CreateCollection) | `c.mu` for mutate+snapshot; **none** while persisting | returned | `explicitDefaultTTLSecs`, `cfg.DefaultTTL` mutated **before** persist; not rolled back on error |
| 2 | `encryption_admin.go: applyEncryptionChange` (SetEncryptionPolicy, RotateKey) | `encAdminMu` → `c.mu` held across snapshot **and** persist | returned (wrapped) | `encMeta` stored before persist, rolled back on error; `enc` swapped after success |
| 3 | `encryption.go: initEncryption` (first-enable at open) | none (open path, collection not yet shared) | returned (open fails) | `encMeta` and `enc` stored **before** persist; no rollback (open aborts) |
| 4 | `collection.go: load()` slow path (meta missing/unparseable) | none (open) | **swallowed** (`_ =`) | regenerates `idSeq`/`createdAt` and rewrites meta **before** `initEncryption`, so a lost block reads as "plaintext" (the fail-closed gap) |
| 5 | `collection.go: rotateSegment` | `c.mu` (write) held for the whole call; callers hold no other collection lock | returned *after* signalling compactor/persister (`metaErr`) | counter in memory is authoritative; segment already rotated |
| 6 | `collection.go: CommitTx` (only if `maxInsertID > 0`) | **none** (after `c.mu.Unlock`) | **swallowed** (`_ =`) | snapshot read without `c.mu` → unsynchronised read of `createdAt`/`explicitDefaultTTLSecs` |
| 7 | `xtx_commit.go: post()` (S6) | none (participant locks released) | **swallowed** | same unsynchronised snapshot |
| 8 | `replication_apply.go` (bumped collections) | none (after `release()`) | **swallowed** | same unsynchronised snapshot |
| 9 | `collection.go: Close` (both fenced and clean paths) | `compactMu` → `persistMu` → `c.mu` | returned | last writer; fenced path persists counter only |
| 10 | `repair.go` rebuild (~L1017–1031) | offline; no `Collection` exists (`loadMeta` → mutate → `persistMeta` on the dir) | returned | **bypasses `metaSnapshot`**; read-modify-write of the file, keeps encryption block via the loaded copy |

Read-only users (no persist): `load()` fast path, `initEncryption` (`loadMeta`), `verify_checks.go: loadPersisted`,
`snapshot.go` (copies `meta.json`). Other users of `writeFileAtomic` (index, sidx, compact manifest, xtx format,
repl state) use their own distinct paths, so only `meta.json.tmp` is shared between concurrent writers.

> **Status: conflicts in rows 1, 2, 3, 6, 7, 8 and 10 and the shared `meta.json.tmp` are RESOLVED** by
> `metadata-serialization` (`Collection.writeMeta` under `metaMu`, unique temp via `writeFileAtomicUnique`, memory updated only
> after persist, persist errors surfaced: `PostCommitError` for rotation/`CommitTx`/xtx `post()`, logged for replication apply).
> Row 4 (swallowed regeneration write, fail-closed classification) is deferred to the next task. The repro tests now assert.

## Findings relevant to the ratified design

* **Rows 6–8 persist with no lock and swallow errors.** M4.1 makes `CommitTx` return `PostCommitError`; the xtx/replication
  post-lock sites must also stop discarding the error (decide: surface through `post()` / log). They are *callers with no
  collection lock*, which matches "call `writeMeta` with no collection lock" — no conflict, but the snapshot must be taken
  inside `metaMu` and `createdAt` / `explicitDefaultTTLSecs` must become safe to read without `c.mu` (atomic or immutable) —
  today rows 6–8 race with row 1 (`-race` would flag it if a test combined them).
* **Row 1 conflicts with "memory only after persist".** It mutates memory before persisting and never rolls back; the design
  requires update-after-success. `cfg.DefaultTTL` is also read by `resolveInsertExpiry` without a lock — the fix must keep
  that read cheap (atomic) while moving the write after `writeMeta`.
* **Row 2 holds `c.mu` across the fsync** — consistent with the design ("callers already holding `c.mu` keep fsyncing under it"),
  but the candidate block must be installed *inside* `writeMeta`'s mutator, not stored to `encMeta` first (today it is stored
  first and rolled back; a concurrent snapshot from rows 5/9 under the same `c.mu` is excluded, rows 6–8 are not, so they can
  persist the uncommitted candidate block today).
* **Row 3** stores memory before persist during open; harmless (single-threaded, open fails) but should use `writeMeta`
  for the unique temp name.
* **Row 4 conflicts with fail-closed**: regeneration happens inside `load()` before `initEncryption`; classification of
  `meta.json` (E1–E3) must move ahead of it and the swallowed write must not rewrite a damaged file of an encrypted collection.
* **Row 10 (repair)** is offline and has no `Collection`, so it cannot use `c.writeMeta`; give it the unique-temp helper
  (the lower layer of `writeMeta`) so `repair` and a live process never share `meta.json.tmp`. Repair preserves the
  encryption block only because it reloads the file; it must refuse to proceed (not regenerate) when evidence of encryption
  exists and the file is unparseable (already guarded by `AllowMetaReset`).
* No caller takes `metaMu`-incompatible locks: nobody holds `layoutMu`, `persistMu` (except Close, which is *outside* `metaMu`
  in the order) or `DB.mu` while persisting meta, other than Close's `compactMu → persistMu → c.mu`.

## Lock-order constraints for the next task

```
encAdminMu → c.mu → metaMu (leaf)          writeMeta acquires nothing else while holding metaMu
compactMu → persistMu → c.mu               unchanged; Close's persist runs inside this chain, then c.mu → metaMu
```

`writeMeta` snapshot inputs must be atomics/immutables only (no `c.mu`, no `encAdminMu`). `setDefaultTTL` and `CommitTx`
(and rows 7–8) call it with no collection lock. Never call `writeMeta` while holding `layoutMu`, `journal.mu`, `sidxMu`,
or `DB.mu`.

## Reproductions (`engine/meta_race_test.go`)

Barrier-driven (channels, no sleeps) using the test seams above. Observed failures **on today's code** (each failing
assertion is routed through `reportMetaRace`, which only logs while `knownMetaRaceBug == true`; the fix task flips the
const to `false`, turning them into hard failures):

* `TestMetaStaleSnapshotOverwrite/paused_ttl` — `setDefaultTTL` snapshots, `SetEncryptionPolicy` completes and seals a
  record, then the stale snapshot lands: `meta.json lost encryption block`; reopen returns the record as ciphertext:
  `pw: scrivaenc:v1:k1:…` instead of `secret`. (`Close` would re-persist from memory and mask this, so the test restores the
  on-disk bytes to model a crash before Close.)
* `TestMetaStaleSnapshotOverwrite/paused_enc` — enc paused holding `c.mu`; `setDefaultTTL` serializes behind it and the
  result has both. Passes today; kept as a regression guard for the fixed ordering with `metaMu`.
* `TestMetaSharedTmpCorruption` — A opens `meta.json.tmp`, B (larger) persists fully, A resumes:
  `atomic write: rename …meta.json.tmp: no such file or directory` for A, and `meta.json` ends as
  `invalid character '"' after top-level value` (A's short write overlaid B's inode).
