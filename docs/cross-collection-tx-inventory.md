# Cross-collection transactions — code-level inventory

**Status:** design input (no behavior change). **Baseline:** `main` @ `7660844`.
**Scope:** single node, single database root (one `--data` dir, one `LOCK`).
Distributed / XA / multi-root transactions are explicitly **out of scope**.

This document inventories every engine path a durable, crash-recoverable
cross-collection transaction (XTx) must extend or stay compatible with, and
lists the incompatibilities the protocol and review gates must honor. It does
not define the on-disk format; that is the next design step. Line references are
to the baseline commit.

---

## 1. Today's transaction model (what exists)

| Piece | Where | Facts |
|---|---|---|
| Staging | `engine/txmanager.go` (`Tx`, `TxManager`) | In-memory only, one collection per `Tx` (`Tx.Collection`). Idle sweeper reaps (`--tx-timeout`). Owned by `server.GRPCServer.txMgr`, **not** by `engine.DB` — embedded (`scriva.go`) has no tx API. |
| Id reservation | `Collection.ReserveID` (`collection.go:1434`) | `idSeq.Add(1)` at stage time; ids are never persisted until some entry carrying them is written. A rolled-back/reaped tx burns ids (gaps are legal). |
| Commit | `Collection.CommitTx` (`collection.go:1441`) | Under `c.mu` (write): snapshot `before` index entries → validate update/delete targets → `txCheckUnique` → quota pre-check → seal encrypted fields → append each op as an **independent NDJSON entry** → `syncActiveLocked` → `publishCommit` per entry → unlock → rotate if needed → emit watch events → persist meta. |
| Failure | `rollbackBatchLocked` (`collection.go:1004`) | `active.rollback(startSize)` truncates the file, restores `before` index entries, **rebuilds every secondary index** from segments. A failed truncate poisons the segment (`ErrSegmentPoisoned`). |
| Other callers | `loadjsonl.go:68` | Reuses `CommitTx` for bulk load batches. |
| Server | `server/grpc.go:949` (`CommitTx`), `server/readonly.go:43`, `internal/auth/scope.go:60` | `Remove(txID)` happens **before** `col.CommitTx`; a failed commit is not retryable. Followers reject it via the read-only guard. |

**Atomicity today is "all-or-nothing in this process", not "all-or-nothing on
disk".** Batch entries are ordinary entries with no batch identity. See §3.

Other writers that append to the active segment and are therefore additional
XTx interaction points (all hold `c.mu`): `insert` (`:870`), `InsertMany`
(`:957`, own rollback), `update` (`:1086`), `Delete` (`:1120`), CAS
(`:2007`), key writes (`keys.go:164`), TTL reaper (`ttl.go:104`, writes
tombstones), follower `applyEntry` (`replication.go:548/560`).

---

## 2. On-disk layout and open-time classification

Per database root (`engine.Open`, `db.go:51`):

- `LOCK` — exclusive `flock` + in-process registry (`dirlock*.go`).
- `replication.json` — DB-level LSN watermarks (`replication.go:25`).
- `REPAIR_JOURNAL.json` — offline repair journal (`repair.go:44`).
- **Every sub-directory is treated as a collection** (`db.go:77–95`,
  `verify.go:349–355`, `repair` likewise): `OpenCollection(e.Name())`
  `MkdirAll`s and starts compactor/persist goroutines for it.

Per collection dir: `seg_NNNNNN.ndjson` (last = active), `index.json`,
`sidx_<field>.json`, `meta.json`, `compact.manifest`, `.compact_*` temps,
`quarantine/<run>/`.

**Consequences for an XTx log.** Any new root-level *directory* (e.g.
`_tx/`) would be opened as a collection by old and new binaries alike, show up
in `ListCollections`, and be snapshotted/verified/repaired as one. A root-level
*file* is safe from this, is ignored by old binaries' `Open`, but is **not
captured by `SnapshotTo`** (§7) and is not understood by `Verify`/`Repair`
(unknown files are not even flagged: `verify_checks.go` only classifies files
inside collection dirs). The location of any XTx durable state must therefore
be an explicit design decision with the reserved-name rule written down:
either a reserved, non-collection name filtered in `Open`/`Verify`/`Repair`/
`Snapshot` *in a release before the format ships*, or state embedded in the
collection segments themselves.

`collectionMeta` (`meta.go:19`) is a free-form JSON struct without a format
version; unknown fields are ignored by older readers, and `persistMeta`
**rewrites the file from the old binary's struct, dropping unknown fields**
(`metaSnapshot`). Anything stored there by a new binary is erased by an old
binary's next rotation/close.

---

## 3. Segment entry format and replay (`store/ndjson.go`, `engine/segment.go`)

Entry: `{id, op, ts, rev?, data?, epoch?, expires_at?, crc?}`; `op ∈
{insert,update,delete}`.

| Property | Behavior | Impact |
|---|---|---|
| CRC (CRC32C) | Covers id, op, rev, expiry, epoch, data. **Not** `ts`, and any future field is uncovered unless folded in. Fields are folded in only when non-zero. | New field folded into CRC ⇒ old binaries compute a different sum ⇒ `ErrCorruptEntry` on every tx entry (fail-closed, good as a version fence, bad as silent downgrade). New field *not* in CRC ⇒ old binaries ignore it (JSON decode ignores unknown keys) but it is unprotected. |
| Op validation | `Decode` does **not** validate `op`. `validSegmentOp` is only used by tolerant scan/salvage (`salvage.go:176`). | An unknown op written by a new binary is *accepted* by strict replay in old binaries; `applyOne` (`index.go:474`) has no `default:` so the index ignores it; but `Verify`/salvage classify it `wrong-op` bad region and `resolveEntries` (`compactor.go:537`) treats it as a live latest entry for its id (see §6). |
| Entry granularity | One line = one op on one id. No batch/begin/commit/tx id. | A crash mid-`CommitTx` leaves a **prefix** of complete lines which replay applies as ordinary committed writes. Torn final line is truncated by `recoverPartialLine` (`segment.go:126`) at open; complete lines are never dropped. |
| Order / offsets | Offset = byte offset in the segment file; index entries point at `(segment, offset)`. | Markers consume offsets; any commit-marker design must keep data entries at stable offsets or never be indexed. |
| Rev | `applyOne`: `rev = max(prev+1, e.Rev)`. | Replay of a prefix gives revs consistent with a full tx; no extra constraint. |
| Max line | `maxScanTokenSize` rejects oversized records at append. | A prepare record carrying a whole batch payload is bounded by this; large XTx need chunking or per-op entries + a small commit record. |
| Segment scope | `CommitTx` never rotates mid-batch; rotation happens after unlock (`:1626`). | Within one collection a tx's entries are in one segment today. Not guaranteed across a crash+reopen (active may be a fresh segment) — do not rely on it. |
| ID counter | `meta.json` may trail; `tailMaxID` reconciles from the newest non-empty segment (`collection.go:787`). | Reserved-but-unwritten ids are not recovered (harmless). A prepare record that carries ids must feed this reconciliation too. |

**Crash outcomes today (single collection, exact):**

1. Crash before first append: no effect.
2. Crash after k of n entries reached the file (page cache flushed by the OS,
   or fsync'd): reopen replays k entries as committed; **partial transaction is
   visible**. No marker exists to say otherwise.
3. Crash after all n appended, before fsync (`SyncModeNone`/`Interval`): any
   prefix, including 0..n, may survive (and, without ordered writeback, a
   non-prefix subset on filesystems that reorder — torn tail handling assumes a
   prefix).
4. Crash after fsync (`SyncModeAlways`): all n durable.
5. Ack-after-crash: the client sees no ack for 1–3; outcome is indeterminate
   to the client and there is no tx id to query.

An XTx protocol must replace 2/3/5 with a defined all-or-nothing outcome per
`SyncMode`, and must say what `SyncModeNone` collections promise (see §10).

---

## 4. Index recovery (`engine/index_recovery.go`, `index.go`)

- `recoverIndex`: `Load(index.json)` → `planReplay` (v2 coverage: per-segment
  prefix SHA-256) → replay tail via `applyEntries` → `segmentsValid` +
  `spotCheck` → else full `Rebuild` gated by `integrityGate`.
- Secondary: `recoverSecondary` (`:344`) same coverage scheme, per field.
- Index state is a pure fold over entries **per collection, in file order**
  (`applyOne`). It has no notion of another collection, of "uncommitted"
  entries, or of a pending set.

**Extension points**
- `applyOne` / `applyEntries` / `Index.Rebuild` / `rebuildTolerant`
  (`integrity_policy.go:188`) / secondary `rebuild`: the single place a
  "skip entries of an unresolved/aborted tx" rule would plug in. It would have
  to be applied identically in all five — a missed one is a silent desync
  (exactly the #107 class of bug).
- Coverage hashes (`SegmentCoverage`) cover raw bytes; a decision "tx T is
  aborted" that is *not* a byte in a covered segment makes persisted
  `index.json`/`sidx` stale without any coverage mismatch. **Resolution state
  must be derivable from covered bytes, or index persistence must record the
  resolution epoch it was computed under.**

**Incompatibilities**
- Tail-replay is per-collection and incremental; a rule needing "was tx T
  committed in collection B" at replay of A requires cross-collection
  resolution *before* any collection's index is trusted. Today
  `DB.Open` opens collections sequentially with no pre-pass (`db.go:84`).
- `Open` order is `os.ReadDir` order; any resolution must be order-independent.

---

## 5. Open path ordering (where recovery must hook)

`DB.Open` → `lockDir` → `initReplication` → for each dir `OpenCollection` →
`load()`:

1. `recoverCompaction` (roll swap forward or discard temps)
2. glob/sort segments, `openActiveSegmentWith` (torn-tail truncate)
3. `recoverIndex`, sidx recovery, `seedSealedCoverage`
4. id counter restore
5. `initEncryption`; start compactor/persist/sync goroutines

There is **no DB-level recovery phase**. The only DB-level step is
`initReplication`. A cross-collection recovery pass must run between
"segments are physically consistent" (after step 2) and "indexes are trusted"
(step 3) for *every* collection, i.e. it needs a two-phase open (scan all
collections' tx evidence → resolve → then build indexes). This is the largest
structural change and must be designed explicitly; `OpenCollection` is also
called standalone (`CollectionWithConfig` reopen, `CreateCollection`,
embedded paths) where the DB-level resolver would not run.

---

## 6. Compaction (`engine/compactor.go`, `compact_manifest.go`)

- Per collection; operates on **sealed** segments only; the active segment is
  never rewritten.
- `resolveEntries` keeps only the latest entry per id, drops deletes and
  expired records, and **emits survivors sorted by id** into new segments
  (`.compact_*` → rename under `c.mu`, intent in `compact.manifest`).
  Therefore after compaction: file order ≠ commit order, offsets all change,
  superseded versions and **tombstones are gone**.
- Swap crash-safety: manifest written before renames, rolled forward at open
  (`recoverCompaction`), then indexes force-rebuilt.
- Lock order: `compactMu → layoutMu(try) → … → c.mu → sidxMu`, persist under
  `persistMu` (`collection.go` struct comments: `compactMu → persistMu → mu`).

**Incompatibilities / constraints**
1. Any tx evidence stored as segment entries (prepare/commit markers, tx id
   on entries) is **destroyed or reordered** by compaction unless
   `resolveEntries` is taught to retain it. Conversely, retaining it forever
   defeats compaction. The protocol needs an explicit *retirement rule*
   ("tx T fully resolved and no collection still needs its evidence")
   evaluated across collections, which compaction of A cannot decide alone.
2. Unknown-op entries in an old binary: `latest[e.ID] = e` then
   `Op != delete` ⇒ kept and written out, and **a marker reusing a real id
   would shadow that record's latest version** (data loss on downgrade).
   Markers must never share the id space of data entries (or must use a form
   old binaries reject at open, §10).
3. Compaction drops a tombstone whose insert lived in an older sealed segment
   only when both are in the compacted set; with active-segment entries
   (where an uncommitted XTx prefix would live) the older sealed versions are
   still compacted away. A rule "revert an aborted tx's update to the previous
   version" **cannot rely on the previous version still existing** — undo
   must be by not-applying (redo-style, atomic commit record) rather than
   by restoring prior versions.
4. `migrationPendingIn` / `reencryptForMigration` rewrite entries and stamp
   epochs: any tx metadata on an entry must survive re-encryption verbatim.
5. Compaction swap `layoutMu`/scan-lease deferral applies per collection; an
   XTx commit must not hold two collections' `c.mu` while waiting on either
   compactor (compactor takes `c.mu` after `compactMu`; commit never takes
   `compactMu`, so ordering by name is sufficient — verify in review).

---

## 7. Snapshot / backup (`engine/snapshot.go`)

- `DB.SnapshotTo` holds `db.mu.RLock` for the whole archive but takes **each
  collection's `c.mu.RLock` only while copying that collection**
  (`writeSnapshot`, `:61`). Collections are therefore captured at *different
  instants*; a commit to A then B may appear as "B without A".
  **Not a cross-collection-consistent snapshot today.**
- Skips: sub-directories, `index.json`, `compact.manifest`, dotfiles,
  `*.merge`. **Root-level files (`replication.json`, `LOCK`, any XTx log) are
  not archived.** Secondary indexes are re-persisted into the archive.
- Restore = extract and open; indexes rebuilt from segments.
- `Bootstrap` (`server/replication.go:48`) reads `ReplicationStatus` LSN
  *before* `Snapshot`, relying on idempotent per-entry apply to absorb the
  overlap.

**Constraints**
- To be XTx-consistent, snapshot needs a DB-wide commit barrier (a DB-level
  RW lock or an XTx epoch that commit takes shared and snapshot takes
  exclusive — **never** held across a network write) or must archive enough
  evidence that restore-time recovery resolves each tx the same way
  regardless of per-collection capture instants.
- The archive format has no manifest/version; adding root-level evidence
  needs a reserved archive path that old extractors (`extractSnapshot` accepts
  any `TypeReg` under dataDir) would write as a *directory* at root and thus
  later open as a collection (§2).
- Restored snapshots go through normal open ⇒ recovery must be the sole
  authority (no "restore must roll forward" shortcut like the compact
  manifest, which snapshots deliberately exclude because rolling it forward
  on a copy would delete live segments — XTx evidence has the same hazard if
  it embeds absolute paths or collection-local offsets).

---

## 8. Verify / repair / salvage (`verify*.go`, `repair.go`, `salvage.go`, `integrity_policy.go`)

- `Verify` (online per collection under its lock; offline `VerifyDir`) is a
  per-collection check set: segment bytes, manifest, meta, primary/secondary
  index coverage and content, id conflicts (`conflict-*`). Findings carry
  `Severity` and `FindingCode`; codes are an exported API.
- `Repair` (offline, journaled `REPAIR_JOURNAL.json`, verified backup first):
  plans per collection; quarantines unreadable segments to
  `quarantine/<run>/`, writes a **salvage segment** of every decodable entry
  in segment order (`writeSalvageSegment`), rebuilds indexes/meta.
- Open-time `integrityGate` refuses (fail-closed `ErrIntegrity`) unless the
  policy opts into tolerant open.

**Extension points / incompatibilities**
- New `FindingCode`s: unresolved tx evidence, tx-evidence-for-missing-
  collection, commit record without all participants, participant segment
  quarantined. Severity mapping must be decided (an unresolved tx at open is
  *not* corruption if recovery resolves it; a tx whose participant evidence
  was quarantined **is** a data-integrity finding).
- Salvage re-serializes decodable entries from quarantined segments into a
  new segment: it would **detach entries from the commit evidence** (or turn a
  prepare-only prefix into committed-looking data). Repair needs an XTx-aware
  policy: salvaged entries of a tx whose commit evidence is lost are treated
  as uncommitted ("abort") by default and reported.
- Repair is per collection and `collectionNeedsRepair`/plan are
  collection-scoped; a DB-level step (resolve XTx, write/trim evidence) must
  be journaled alongside (`repairJournal` is DB-level already — extendable).
- `Verify` cannot today say "collection A is missing a commit that B has";
  the report is `[]CollectionReport` with no cross-collection finding
  location (`Location` has collection+segment+offset; needs a tx id).
- `verifyOnline` takes only that collection's lock; online cross-collection
  verify must not take multiple collection locks out of order (§9).

---

## 9. Lock hierarchy (current, plus XTx requirements)

Observed order (outer → inner):

```
DB.mu (RW)                       registry: create/drop/reopen/snapshot
  └ Collection.compactMu         serializes compaction + Close
      └ Collection.layoutMu      scan-lease vs swap (TryLock in compactor)
      └ Collection.persistMu     index/sidx file writers
          └ Collection.mu (RW)   data, active segment, index mutations
              └ sidxMu (RW)      secondary indexes
              └ Index.mu         primary index map
              └ Segment.mu       file handle
              └ replicationBroker.mu   publish under c.mu
              └ watchMu          emit happens after c.mu release
DB.roleMu / replMu               role flip / applied LSN (independent)
TxManager.mu → Tx.mu             staging only
```

Facts relevant to XTx:
- No code path today holds two collections' `c.mu` at once. `DB.mu` is held
  (read) across all collections only by `SnapshotTo`/`Verify`.
- `CollectionWithConfig` (`db.go:156`) **closes and reopens** a live
  collection under `DB.mu.Lock`; `DropCollection` removes the directory. A tx
  holding `*Collection` pointers across either operates on a closed/removed
  collection. XTx commit must hold `DB.mu.RLock` for its duration (or
  re-resolve under it).
- `publishCommit` runs under `c.mu`; the broker assigns LSNs in lock-hold
  order. With N collections locked, entries of one XTx can be published
  contiguously only if all locks are held while publishing.

**Required rule (to be ratified in protocol review):** XTx acquires
`DB.mu.RLock`, then each participant's `c.mu` in **ascending collection-name
order**, never `compactMu`/`persistMu`/`layoutMu` while holding any `c.mu`
(already the existing order). Single-collection writers are unchanged and
cannot deadlock against it because they take exactly one `c.mu`. Rotation
(`rotateSegment` takes `c.mu`) must be deferred until all XTx locks are
released, as `CommitTx` does today. Unique-index checks span only one
collection, so there is no cross-collection constraint to order.

---

## 10. Replication (`engine/replication.go`, `server/replication.go`, `server/grpc.go`)

- Leader: `publishCommit(e)` → `broker.publish(collection, entry)`: one
  global LSN per **entry**, per-collection order preserved by `c.mu`,
  cross-collection order = lock-acquisition order. Ring (default 8192) + per-
  follower buffered channel; overflow kills the sub ⇒ follower re-bootstraps.
- Wire: `ReplicationRecord{lsn, collection, op ∈ {INSERT,UPDATE,DELETE}, id,
  ts, rev, expires_at, data}`; `replOpToProto` maps any other op to
  `UNSPECIFIED`, and `replOpFromProto` maps it to `""`, which
  `applyEntry` rejects (`unknown op`) — a follower on an old binary **errors
  its apply loop** on a marker entry rather than skipping it.
- Follower: `ApplyReplication` → `applyEntry` (`replication.go:527`): each
  entry applied independently under that collection's lock, idempotent by
  `rev`, then `SetAppliedLSN(lsn)` per entry. A follower crash between
  entries leaves a **partial tx applied and visible**, and followers serve
  reads (and Watch) during it.
- Bootstrap: LSN read, then `Snapshot` (per-collection instants, §7), then
  tail from LSN with idempotent apply.
- Promotion (`Promote`) checks lag then flips role; `replication.json`
  persists LSNs only.

**Incompatibilities / constraints**
1. LSN-per-entry has no tx boundary; the follower cannot tell a prefix from a
   whole. The feed needs either a tx-group envelope (begin/end LSN or a
   `tx_id` + count on records) or atomic multi-entry LSN assignment, and the
   follower needs an apply-group path that is atomic w.r.t. its readers.
2. `ReplicationOp` is a proto enum and proto is the API source of truth
   (`proto/scriva.proto`, regenerate with `make proto`): additions are a wire
   change gated by the same compatibility policy as the disk format.
3. Idempotency today is by per-id `rev`. A tx group replay after partial
   apply must be idempotent *per group*, not just per entry (already true if
   members are individually idempotent and the group has no extra side
   effects; marker/evidence application must also be idempotent).
4. Follower's disk is a replay of the leader's *entries*, so if tx evidence
   lives in segments it replicates implicitly; if it lives outside segments
   (root-level log) the follower must reconstruct/own its own evidence or
   it will not recover a tx the same way after a crash.
5. Bootstrap watermark argument ("snapshot contains everything ≤ LSN")
   assumes per-entry atomic visibility; with XTx the snapshot may contain
   half a group while the LSN says it is fully before the watermark — the
   follower would then skip the remainder as "already applied". Snapshot
   must be group-consistent (§7) or the watermark rule must be group-aware.
6. Ring/`oldest()` accounting is per entry; a group larger than the ring
   cannot be served to a lagging follower — define a max group size or make
   the group a single ring slot.
7. Followers must refuse to apply XTx evidence they do not understand
   (fail-closed) rather than silently dropping a marker.

---

## 11. Durability configuration

`SyncMode`: `none` (default for `engine.CollectionConfig` zero value),
`interval` (background `syncLoop`), `always` (fsync per write op, in
`syncActiveLocked`). Embedded façade sets per-collection modes
(`CollectionWithConfig`). Rotation fsyncs the dir unless `none`.

- XTx spanning collections with **mixed** sync modes cannot be stronger than
  the weakest participant unless the commit record itself is forced durable.
  Decision needed: XTx always fsyncs participants + its decision record
  regardless of `SyncMode` (recommended), or XTx is refused unless all
  participants are `always`.
- Directory fsync after creating any new evidence file is required
  (`fsyncDir`; Windows variant exists, `fsync_windows.go`).
- `Close()` writes `index.json`/`sidx` under the coverage scheme; an XTx
  decision record must be durable *before* `Close` marks indexes as covering
  entries of that tx.

---

## 12. Encryption, TTL, quota interactions

- Encryption: `e.Epoch` and sealed field blobs are per entry; key-oblivious
  segments mean tx evidence must carry no plaintext (tx ids/collection names
  are acceptable; record payloads in a *separate* log would need sealing).
  `MigrateNow` rewrites entries (epoch stamps) — evidence must not embed
  offsets or epochs it assumes immutable.
- TTL: `ExpiresAt` per entry; reaper tombstones are independent single-op
  writes (`ttl.go:104`) and may race an XTx on the same id under that
  collection's lock only — lock ordering already serializes them.
- Quota: `checkQuotaLocked` is per collection (`CommitTx` pre-checks before
  any append). XTx must run every participant's pre-check *before* the first
  append anywhere (same "validate everything, then write" shape).
- Watch: events emitted after unlocking; XTx should emit only after the
  decision is durable and in group order.

---

## 13. Compatibility policy (proposed; to be ratified)

1. **Fence before format.** A release that *understands but never writes*
   XTx evidence ships first: it must (a) fail closed at open on unknown
   tx evidence (distinct error, not `wrong-op` corruption), (b) filter the
   reserved root name/dirs in `Open`, `Verify`, `Repair`, `SnapshotTo`,
   (c) reject unknown replication ops explicitly. Only a later release may
   write XTx bytes. Downgrade below the fence is unsupported and must be
   detectable (format marker in a file old binaries *refuse* via a mechanism
   they already have: e.g. a per-collection segment filename or CRC change
   that old open fails on, not an ignorable JSON field).
2. **No new ignorable fields as the safety mechanism.** Ignorable JSON keys
   are silently dropped by old `compact`/`salvage`/`persistMeta`.
3. **Single-collection ops unchanged.** A database that never runs an XTx
   writes byte-identical segments, `meta.json`, index and sidx files as today.
4. Wire changes follow the same fence (proto enum additions only after
   followers understand them; leader must not ship XTx groups to a follower
   that has not advertised support — needs a capability handshake in
   `ReplicateRequest`).
5. Integrity fixtures (`engine/testdata/integrity`) regenerate via
   `go generate ./engine`; new fixture classes are required for every new
   `FindingCode` and must never be hand-edited.

---

## 14. Summary of required extension points (checklist for protocol/review gates)

| # | Area | Extension point | Risk if missed |
|---|---|---|---|
| 1 | Write | `CommitTx` / new `DB`-level commit: validate-all → lock set → write → durable decision → publish | partial visible tx |
| 2 | Entry | `store.Entry` + `checksum` + `validSegmentOp` + `applyOne` + salvage | silent mis-replay on old binaries |
| 3 | Open | New DB-level resolution between torn-tail trim and index trust; `OpenCollection` standalone callers | index trusted over unresolved tx |
| 4 | Index | `applyOne`, `Rebuild`, `rebuildTolerant`, sidx `rebuild`, coverage | primary/secondary desync (#107 class) |
| 5 | Compaction | `resolveEntries` evidence retention + retirement rule; marker id-space | lost evidence / shadowed record |
| 6 | Snapshot | DB-wide barrier, root evidence capture, `extractSnapshot` | restore with half a tx |
| 7 | Verify/Repair | new `FindingCode`s, DB-level journaled step, salvage policy | repair "commits" an aborted tx |
| 8 | Replication | group envelope, atomic follower apply, handshake, ring sizing, bootstrap watermark | follower diverges / sees partial tx |
| 9 | Locks | name-ordered `c.mu` acquisition under `DB.mu.RLock`; reopen/drop safety | deadlock / write to closed collection |
| 10 | Durability | forced fsync of participants + decision record; dir fsync | ack of non-durable tx |
| 11 | Server/API | `TxManager` is server-only; embedded façade has no tx; proto change via `proto/scriva.proto` | API drift between embedded/server |
| 12 | Metrics | long-running op ⇒ counter + histogram via hooks, not direct `metrics.*` in engine | convention violation |

## 15. Open questions for the protocol document

- Evidence location: in-segment markers vs a root-level decision log (and the
  reserved-name rule that goes with it).
- Redo vs undo: this inventory argues for redo-only (apply on commit record)
  because compaction removes before-images (§6.3).
- Behavior of `SyncModeNone` participants (§11).
- Max participants/ops/bytes per XTx and its relation to `maxScanTokenSize`
  and the replication ring (§3, §10.6).
- Whether `engine.DB` gains the tx API (and `TxManager` moves into it) or the
  server keeps owning staging (§1).
- Idle-tx expiry and `ReserveID` semantics for multi-collection staging.
