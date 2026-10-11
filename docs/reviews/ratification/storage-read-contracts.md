# Ratified decisions — storage and read contracts

**Status:** ratification draft for consolidation into
[`design-database-integrity-hardening.md`](../../design-database-integrity-hardening.md). Documentation only: nothing here is implemented, and no runtime behaviour changes with this file.
**Run:** autopilot `ap-790b4f397cb1`, task `ratify-storage-and-read-contracts`.
**Grounded at:** `main` `f1c32a0`. File:line references are anchors into that revision.
**Companion evidence:** a sibling worker is rechecking review claims (`evidence.md`). Where a decision depends on a schedule the review only *reasoned* about (H7 metadata interleaving, H8 scan omission), it is marked **holds either way**: the decision is justified by the code shape, not by a reproduction.

## 0. How to read this

- Every decision has: **Decision**, **Rationale**, **Rejected**, **Compatibility**, **Lock order** (where relevant), **Acceptance** (deterministic, turn-into-test), and **Deferral** (only if one exists).
- "Today" statements are verified by reading the cited code. "Required" statements are the ratified target and are **not implemented**.
- Lock names are the real field names: `DB.mu`, `Collection.compactMu`, `layoutMu`, `persistMu`, `Collection.mu` (`c.mu`), `sidxMu`, `Index.mu`, `Segment.mu`, `replicationBroker.mu`, `xtxJournal.mu` (`journal.mu`), `encAdminMu`, `watchMu`. The existing hierarchy (design-cross-collection-transactions §6.1, `collection.go:376-383`) is:

```
DB.mu ─ compactMu ─ { layoutMu , persistMu } ─ c.mu ─ { journal.mu | sidxMu ─ Index.mu ─ Segment.mu ─ broker.mu }
encAdminMu ─ c.mu                      (applyEncryptionChange, encryption_admin.go:158)
```

- Test seams already available: `faultFS` (`engine/faultfs.go`: nth write/sync/rename failure, short write prefix, `crashAt`, step `trace`), `CollectionConfig.scanHook`/`postLocateHook`, child-process `TestKill9_*`. "Power-loss" below means "only explicitly synced bytes survive" (see spec §4.1); the harness must model it, a SIGKILL test alone does not.

## 1. Single-record sync-error semantics (spec §2.3, §3.2; H5, H9)

### Today (verified)
- `insert`/`update`/`Delete`/`Upsert`/CAS/`reapExpired` append, **mutate the primary and secondary index**, then call `syncActiveLocked`; on sync error they unlock and return the error with the index entry left in place and `publishCommit` skipped (`collection.go:923-934`, `1140-1150`, `1174-1183`; `keys.go` Upsert; `ttl.go:104-116`). The local process serves the record; followers never see it.
- `InsertMany`/`CommitTx` do roll back after a sync error (`rollbackBatchLocked`: `Truncate` then index/sidx rebuild) but the truncate is **not fsynced** and runs after a failed `fsync`, so "not applied" is not proven.
- `syncLoop` (`SyncModeInterval`) discards the `Sync` error (`collection.go:544`).
- `Segment.Append` already rolls a failed `write` back with `Truncate` and poisons the segment if truncate fails (`segment.go:Append/rollbackLocked`); `Seal` sets `sealed=true` before `Sync`, so a failed seal sync leaves a permanently sealed active segment (`segment.go:Seal`, `collection.go:rotateSegment`).

### Decision
**Outcome taxonomy** (engine type `WriteOutcome`, carried by typed errors; the exported surface is the error identity, not an enum):

| Outcome | Entered when | Engine state afterwards | Caller sees | Retry |
|---|---|---|---|---|
| **Durable** | return `nil`: append done and, under `SyncModeAlways`, `fsync` succeeded. Under `interval`/`none` the contract is only "appended"; the power-loss window is documented | normal | success | n/a |
| **NotApplied** | rejected before any byte (validation, quota, unique, reserved field), or `write` failed **and** the truncate-to-previous-size succeeded (`Append` rollback), or a batch aborted before its decision (D2) | unchanged, **not** fenced | the existing error (`ErrDuplicateKey`, `ErrQuotaExceeded`, …) | safe |
| **Unknown** | `fsync` of the active segment failed (foreground or `syncLoop`); `Seal` sync failed; truncate failed (segment poisoned); batch decision fsync failed (D2, existing `ErrXTxOutcomeUnknown`) | collection **fenced** (below) | `ErrWriteOutcomeUnknown` wrapping the cause | **not safe** unless the operation is idempotent by construction (see below) |
| **Durable + ancillary failure** | commit point passed; then rotation, meta persist, or event delivery failed | normal, except a failed `Seal` sync fences the *next* write | success plus a typed `*PostCommitError` (`errors.Is(err, ErrPostCommit)`); gRPC returns OK with trailer `scriva-post-commit-warning` | n/a (already applied) |

**Fail-stop, no retry, no rollback after a sync failure.**
1. After any `Sync` error the engine **never retries `fsync` and never reports NotApplied**: after a failed `fsync` the kernel may have dropped dirty pages while marking them clean, so a later successful `fsync` or successful `ftruncate` proves nothing about the earlier bytes.
2. It **fences the collection**: an atomic `fenced` pointer (first cause wins) checked at the top of every write **and read** entry point. Fenced ops return `ErrCollectionFenced`; `Close`, `Verify` (on-disk read) and `Stats` remain allowed; Snapshot of a fenced collection is refused. Reads are fenced because the in-memory index may contain the unknown write while followers/watchers do not.
3. The unknown record is **not** published (`publishCommit` and watch emit are skipped, as today) and the index is **not** reverted: the segment bytes are preserved untouched as evidence. In-memory state is untrusted until reopen.
4. Recovery is reopen of the collection (process restart; a `DB`-level reopen API is a follow-up). Normal open recovery decides from bytes: a complete valid line recovers as **committed even though the caller saw an error** (this is the "unacknowledged but durable" case the spec already allows); a torn tail is truncated. Both are valid post-states.
5. `InsertMany`/`CommitTx` with ≥2 entries leave this path entirely (D2). A batch of exactly one entry is a single-record write and follows this section.
6. Rotation: `Seal` must not set `sealed=true` before `Sync` succeeds; a sync failure there is Unknown + fence (no un-seal, no retry). A rotation failure *after* a successful commit never turns the return into a failure and never skips `emit` (move `emit` before the rotation attempt or into a `defer`).
7. XTx participants: `commitXTx` already refuses a poisoned participant (`xtx_commit.go:345`); the same check must test the `fenced` pointer (pre-decision ⇒ abort, retry safe).

**Resolving Unknown.** There is no per-write operation id and none is added now: single-record writes are *not* exactly-once. Documented client recipes: keyed insert → `FindByKey`/`Upsert` (idempotent by key); update → read `Rev` and use `UpdateIfRev`; insert-by-id-less → treat as at-least-once. A durable operation-status API is a deferral (below).

### Rationale
Fail-stop is the only policy that is correct under the Linux page-cache model and it collapses a large state space (partial rollback × partial publication × partial index) into one: *the process stops pretending*. It reuses the existing poison/`ErrXTxOutcomeUnknown` vocabulary instead of inventing a second one. Fencing reads costs availability of one collection until restart; a failed `fsync` is not transient, so that is the intended trade.

### Rejected
- **Keep serving, roll back index and truncate** (the batch path today): claims NotApplied without proof; also leaves sidx rebuild failing mid-way with no defined state.
- **Retry `fsync` N times**: kernels may report success on retry for dropped pages (the "fsyncgate" class). Rejected outright.
- **Revert the index in memory, keep serving reads, fence writes only**: the revert is unsound when `Truncate` fails (index would point at bytes that may or may not exist); needs a second state machine for little availability gain.
- **Panic the process** (PostgreSQL style): one collection's disk error should not take down the DB; fence achieves the same safety per collection.

### Compatibility
On-disk: none. API: new errors `ErrWriteOutcomeUnknown`, `ErrCollectionFenced`, `ErrPostCommit`; gRPC `codes.Unknown`/reason `WRITE_OUTCOME_UNKNOWN` (same shape as the XTx mapping at `server/xtx.go:311`), `FAILED_PRECONDITION`/`COLLECTION_FENCED`; REST via the existing gateway code map. Replication: unchanged (unknown writes never published). Behaviour change: callers that today see a plain error and keep working now see a fenced collection after a sync failure — intended and release-noted.

### Lock order
`c.mu` (write) held for append+sync as today; `fenced` is an atomic pointer set under `c.mu`, read without a lock. No new lock. `syncLoop` takes `c.mu.RLock` only to read `c.active`, then syncs outside it; on error it sets `fenced` (atomic) — no lock inversion.

### Acceptance
- **S1.1** `faultFS.syncN` fails on the Insert's sync: returns `ErrWriteOutcomeUnknown`; then `Get`/`Scan`/`Count`/`Insert` on that collection return `ErrCollectionFenced`; no watch event; replication LSN unchanged; `OnSegmentPoisoned`/metric `write_outcome_unknown_total` incremented once. Repeat for Update, Delete, Upsert, `UpdateIfRev`, `UpdateIfMatch`, `InsertWithKey`, `reapExpired`.
- **S1.2** Same, then reopen on (a) the page-cache-survives FS: the record is present exactly once, reopen twice yields identical state; (b) a power-loss FS dropping unsynced bytes: the record is absent. The test asserts "present or absent == model of what the FS retained", never a prefix/mixture.
- **S1.3** `writeN` fails with a short write, truncate succeeds: `NotApplied`, collection **not** fenced, next Insert succeeds and gets the next id (ids are not reused for bytes that were durable; id gaps allowed).
- **S1.4** `writeN` fails and truncate fails: `Unknown`, fenced, segment poisoned, reopen recovers the torn tail.
- **S1.5** `Seal` sync fails after a committed Insert: Insert returns success + `PostCommitError`; its watch event is delivered; the next write returns `ErrCollectionFenced`; reopen contains the record; segment is not left `sealed` with `active` pointing at it.
- **S1.6** `SyncModeInterval`, injected `syncLoop` failure: next foreground op returns `ErrCollectionFenced`; counter incremented.
- **S1.7** XTx with a fenced participant aborts pre-decision with `ErrXTxDurability`; no journal COMMIT.
- **S1.8** gRPC/REST/CLI status mapping golden test for each outcome.

### Deferral
Operation-status / idempotency keys for single-record writes. **Owner:** API owner. **Prereqs:** proto field + a durable status store (could reuse the XTx journal's key table via D2). **Interim fail-safe:** at-least-once documented; Unknown is never retried by the engine or the server.

## 2. Single-collection batch protocol (spec §3.1; H4)

### Today (verified)
`InsertMany` and `CommitTx` write N ordinary v1 lines under one `c.mu` hold, fsync once (only if `SyncModeAlways`), and on a crash after k<N complete lines recovery replays the k-line prefix as committed data (`collection.go:1009-1033`, `1594-1674`). No size cap beyond the per-line scan limit. `Collection` has `decisionsFn` (reads the XTx decision table) but no handle to append decisions; `engine.DB` does (`db.CommitXTx`/`BeginXTx` accept a single participant — `xtx_handle.go:204` only rejects *zero*).

### Decision
1. **Reuse the XTx durable decision machinery, with exactly one participant.** A batch of ≥2 entries (`InsertMany`, `CommitTx`) is executed as a one-participant XTx via the `db.commitXTx` path: stamped entries (`TxStamp{t,i,n}` + v2 checksum, `xtx_wire.go`), digest in `COMMIT.parts`, decision in `xtx.journal`, publication as one `TxEnvelope`. **No new record kind, no new entry shape, no new format file.**
2. **Forced durability, independent of `SyncMode`.** Data fsync, then journal COMMIT fsync (the XTx S2–S4 sequence). Rationale: with a relaxed mode the decision could reach disk before the data on power loss, producing journal row 5/6 of the truth table (committed tx with missing run ⇒ `ErrXTxIncomplete`, **Open refuses**). Ordering without `fsync` is impossible, so there is no opt-out in this release.
3. **Same error model as XTx.** Pre-decision failure ⇒ abort, `NotApplied`, retry safe (`ErrXTxDurability`); decision append/fsync failure ⇒ `ErrXTxOutcomeUnknown` and the journal fenced (existing). The engine-generated txid is returned in the error so `XTxStatus(txid)` resolves the outcome; an optional client idempotency key is carried when the proto adds it (deferral).
4. **Recovery rule** is the existing truth table (§7.2) unchanged: no COMMIT ⇒ presumed abort and tail truncation; COMMIT with complete digest-matching run ⇒ visible; a *durable but unacknowledged* COMMIT recovers as committed.
5. **Limits.** Batches are bounded by `defaultXTxLimits()` (`MaxOps` 1000, `MaxBytes` 64 MiB). A larger batch is rejected up front with `ErrXTxTooLarge` (maps to RESOURCE_EXHAUSTED); it is never silently split, because splitting would break atomicity. This is a behaviour change for oversized `InsertMany` callers and is release-noted.
6. **Staged ids** (`ReserveID`, `txmanager`) remain provisional until commit; unchanged.
7. **Unmanaged collections.** A `Collection` opened without a `DB` (`engine.OpenCollection` directly; the façade `scriva.Open` always has one) has no coordinator. For ≥2 entries it returns `ErrBatchNotAtomic` unless `CollectionConfig.AllowNonAtomicBatch` is set (documented as legacy prefix-visible behaviour, intended for tests/tools).
8. **Rollout gate (format is one-way).** Today any XTx write makes the root non-downgradable and there is no drain tool (`release-readiness-xtx.md` residual risk 1). So atomic batches are **default-on for new roots and for roots that already hold `xtx.format`**; an existing *legacy* root keeps the old non-atomic batch path, logs a one-time startup warning, and is converted only by an explicit operator opt-in (`serve --enable-atomic-batches`, which runs `ensureXTxFormat` and records the conversion). The architecture doc must say "atomic" only for roots in atomic mode.

### Rationale
One commit mechanism, one visibility predicate (`entryVisible`), one recovery proof, one replication envelope; the crash-boundary work is already done and tested. It also fixes a latent replication hazard: followers currently receive batch entries one by one and can expose a prefix, whereas a single-participant envelope is applied atomically (`design-xtx-replication.md` §3).

### Rejected
- **B. Collection-local batch marker/log:** second protocol, second recovery predicate, must be re-taught to compaction, snapshot, verify/repair, encryption migration and replication. Rejected unless D2.1 is proven infeasible by the ownership refactor.
- **One physical line holding the whole batch** (torn-tail logic gives atomicity for free): breaks the one-entry-per-line invariant that index offsets, compaction, replication and `maxScanTokenSize` assume; old readers would see an unknown op rather than fail on a checksum; caps batch size at the line limit. Rejected.
- **C. Weaken the contract:** acceptable only by explicit product decision; not chosen because the docs and clients treat these as atomic.
- **Write batch to a side file then rename:** a second durable-publish path with the same recovery obligations as B and extra directory fsyncs.
- **Honor `SyncMode` for the decision:** see item 2.

### Compatibility
- **On-disk:** none new. Stamped v2 lines + `xtx.format`/`xtx.journal` already exist and are fenced.
- **Old readers:** a pre-fence binary fails closed on stamped lines (v2 CRC ≠ v1) and on `xtx.journal` without understanding it (`TestXTxDowngrade_*`). It never silently exposes batch entries.
- **New readers on legacy roots:** unchanged bytes, unchanged behaviour until opt-in (item 8).
- **Downgrade:** unsupported after conversion until a drain tool exists (owner: storage/ops; the design's `scriva-cli xtx drain` is **not** implemented). Backup before opt-in is the documented mitigation.
- **Replication:** v2 envelope; a v1-only follower gets `REPLICATION_PROTOCOL_UNSUPPORTED` (existing, non-silent). Upgrade followers first.
- **Latency/throughput:** ≥2 fsyncs per batch regardless of mode; release-note with measured numbers before default-on.

### Lock order
Exactly the XTx order: `DB.mu.RLock` → participant `c.mu` → `journal.mu` (leaf) at the decision step; single-collection ordinary writers still never take `journal.mu`. Rotation, meta persist and watch emission after all locks are released (XTx rule 7). `Collection.InsertMany`/`CommitTx` must not call into `DB` while holding `c.mu`: the wrapper takes the DB path *before* locking the collection. The `Collection` therefore needs a back-reference `db *DB` (or a `batchCommitter` interface), set by `DB.Open`.

### Acceptance
- **B2.1** Child-process crash matrix (spec §4 minimum list) for a one-participant batch at every `faultFS.trace` step: after first complete entry; after each later entry; before data sync; during sync; after data sync before COMMIT; partial COMMIT; complete COMMIT before sync; failed COMMIT sync; after COMMIT sync before index apply; during sidx apply; before/after event publication. After reopen (twice): all N entries visible with identical revs/ts/data, or none; compare the full ordered record set and `id` counter, not counts. `SCRIVA_KILL_SEED` replay applies.
- **B2.2** `SyncModeNone`/`interval`/`always` all produce identical crash-matrix results (batch durability is mode-independent); a power-loss FS never yields row 5/6.
- **B2.3** 1001 ops or >64 MiB: `ErrXTxTooLarge`, nothing written, no txid in the journal.
- **B2.4** Legacy root: `InsertMany` still works, root bytes gain no `xtx.*`; opt-in flag converts; second open honours it; pre-fence binary refuses the converted root without mutating it.
- **B2.5** Unmanaged `OpenCollection`: 2-entry batch ⇒ `ErrBatchNotAtomic`; with `AllowNonAtomicBatch` ⇒ legacy behaviour.
- **B2.6** Leader batch → follower: observed as one envelope; fault-injected follower crash mid-apply shows all-or-none after restart; v1 follower gets the typed refusal.
- **B2.7** Compaction, snapshot/restore, verify/repair and encryption migration preserve stamps and digests across a retained batch (existing XTx matrix, re-run with one participant).

## 3. XTx participant drop / retirement (spec §2.4.4, §3.3; H6)

### Today (verified)
`DropCollection` takes `DB.mu.Lock`, calls `col.Close()`, then `os.RemoveAll(dir)` with **no journal inspection** (`db.go` DropCollection). Recovery rejects a non-retired `COMMIT` naming a missing participant (truth table row 6, `xtx_recovery.go:306-312`), so the DB then fails to open. `xtxJournal.retire` and `checkpoint` exist (`xtx_journal.go:963,1152`) but have **no production caller** (only the header comment); recovery explicitly leaves evidence live (`xtx_recovery.go:417-420`). Therefore today *no* decision is ever retired at runtime, and compaction keeps every stamped entry (`compactor.go:~580`, `hasEvidence`). Recovery already treats `RETIRE(commit)` as final with no participant check (row 9, `xtx_recovery.go:296`). No drop operation exists in `engine/replication*.go` (grep); drop is not replicated.

### Decision
**Proof obligation.** A collection `C` may be dropped iff, evaluated while holding the drop barrier, for **every** journal decision `T` whose `parts` names `C`:
1. `T` is `RETIRE(commit)` or `RETIRE(abort)`, *or* `T` is `ABORT`; **and**
2. for every `T` that is a non-retired `COMMIT` naming `C`, drop first **retires it** by the verified path: all participants of `T` are re-derived under their `c.mu.RLock` (name order), `rev.complete && rev.n == p.N && rev.digest == p.D` for each part (the §4.4 step-1 check, same code as recovery row 3/5), then `RETIRE{T, commit}` is appended and **fsynced**. A verification mismatch, missing sibling participant, or failed fsync ⇒ drop is **refused** with `ErrParticipantRetained{Collection, TxIDs, Reason}` and **nothing** is modified (no Close, no removal);
3. there is no live in-flight XTx (state STAGED..COMMITTING) that includes `C`: guaranteed by taking `DB.mu.Lock` (XTx holds `DB.mu.RLock` for its whole duration); a staged handle that later commits fails `ErrCollectionNotFound` before any write (`xtx_handle.go` participant re-resolution);
4. undecided stamped runs for `C` (no decision) are covered by presumed abort and are removed with the directory — they need no journal record.

**Coordinator evidence required** is therefore: a durable `RETIRE` for each committed tx that names `C`, in the same journal epoch, and a verified digest pass for its other participants *at retirement time*. A `RETIRE` is never written from `Open`/recovery success alone. A bare "DROP tombstone" is not accepted as evidence (see Rejected).

**Atomic drop sequence** (crash-safe): (a) retire as above; (b) `col.Close()`; (c) `rename(<root>/C, <root>/.drop-C-<txid-less nonce>)` + `fsyncDir(root)`; (d) `RemoveAll` the renamed directory; (e) startup deletes any `.drop-*` directory. A crash before (c) leaves `C` intact (with extra RETIRE records, harmless); after (c) the collection is gone and recovery sees only retired decisions. `RemoveAll` of an in-place name is no longer used (a crash mid-removal would otherwise re-open a half-deleted collection).

**Backups/snapshots.** Retirement changes the journal, so a snapshot taken before retirement restores a self-consistent (C present, decisions live) database, and one taken after restores (C absent, decisions retired). The snapshot journal capture already uses `DB.mu.RLock` → `c.mu` → `journal.mu` (§9.1 rule 4); drop's exclusive `DB.mu.Lock` serialises with it. Restoring *journal from one point and collection dirs from another* is out of contract and is caught by row 5/6.

**Replication.** Drop remains local-only (not replicated) in this release; a follower of a leader that dropped `C` keeps `C` and will diverge by design. Documented; making drop a replicated operation is a deferral (below).

### Rationale
This keeps the decision-evidence invariant of the XTx design (never commit a subset, never abort a decided tx) and spends only machinery that already exists and is recovery-tested (`RETIRE`, row 9). The check-and-drop is serialised by the same lock that excludes XTx.

### Rejected
- **Block drop forever while any evidence exists** (the review's minimum): with retirement unimplemented today this would make drop impossible after the first XTx; kept only as the interim fail-safe.
- **Write a `DROP{collection}` journal record** and let recovery treat that participant as absent: new record kind ⇒ new fence level, and it permanently weakens row 5/6 for that tx without verifying sibling participants first.
- **`--force` drop:** can produce an unopenable root; refused, not offered.
- **Background-only checkpointer** (design §4.4 as written): still needed for stamp reclamation but cannot serve drop latency; on-demand retirement at drop is the minimal path.

### Compatibility
On-disk: no new record kinds; the journal gains `RETIRE` records (already specified/decoded). Old readers: a pre-fence binary already refuses an XTx root. API: new `ErrParticipantRetained` (gRPC `FAILED_PRECONDITION`, reason `PARTICIPANT_RETAINED`, txids in details). Replication: none. Embedded `DropCollection` signature unchanged.

### Lock order
`DB.mu.Lock` (exclusive; excludes XTx, `CollectionWithConfig`, snapshot capture) → participants' `c.mu.RLock` in canonical name order for verification → `journal.mu` for `retire` (leaf) → released → `col.Close()` (takes `compactMu` → `persistMu` → `c.mu`) → rename/remove with no collection lock. `journal.mu` is never held while acquiring any `c.mu`. Do **not** take `compactMu` of a *sibling* participant during verification (compactor holds `compactMu`→`c.mu`; verification takes only `c.mu`, as the checkpointer rule 4 states).

### Acceptance
- **P3.1** XTx commits across {a,b}; `Drop(a)` returns after retiring, reopen succeeds, `b`'s entries from that tx remain visible and de-stampable; `Drop(a)` before retirement implementation ⇒ `ErrParticipantRetained`, directory bytes byte-identical (hash before/after).
- **P3.2** Corrupt `b`'s run (digest mismatch) then `Drop(a)`: refused, no RETIRE written, no files changed; `Verify` reports the same finding.
- **P3.3** Drop racing `commitXTx` forced at each boundary with barriers (not sleeps): staged-before-lock, prepared, after COMMIT before index apply, after apply. Outcome is always either tx committed then drop retires, or drop first and tx fails `ErrCollectionNotFound` pre-write; never an unopenable root.
- **P3.4** Crash points in the drop sequence ({after RETIRE fsync, after Close, after rename, mid-RemoveAll}) via child-process kill: reopen succeeds; `C` fully present or fully absent; `.drop-*` leftovers removed at open.
- **P3.5** Failed RETIRE fsync ⇒ `ErrXTxOutcomeUnknown`-class result from `retire`, drop refused, journal fenced per existing rules.
- **P3.6** Snapshot taken before and after a retire+drop restores and opens.

### Deferral
(a) Background checkpointer wiring `retire` + `checkpoint` (stamp de-stamping/journal GC) and (b) replicated drop. **Owner:** storage/ops owner (a), replication owner (b). **Prereqs:** this decision (D3) and the XTx replication design. **Interim fail-safe:** drop retires on demand; any verification failure refuses the drop; no force path.

## 4. Metadata serialization and encryption fail-closed (spec §3.4; H7)

### Today (verified)
- `persistMeta` → `writeFileAtomic` uses the **fixed** temp name `meta.json.tmp` opened `O_TRUNC` (`fsync.go`), so two overlapping persists share one temp file. File and parent-directory fsync **are** already performed (the review's "fsync the directory" item is already satisfied).
- Callers and their lock context: `rotateSegment` (under `c.mu`, result ignored `collection.go:1442`); `CommitTx` (after unlock, ignored, `:1692`); `Close` (under `c.mu`); `setDefaultTTL` (snapshot under `c.mu`, **persist after unlock**, `ttl.go:35-47`); `applyEncryptionChange` (under `c.mu`, `encryption_admin.go:158-181`); `load` slow path and `initEncryption` first-enable.
- `explicitDefaultTTLSecs` is a plain field read by `metaSnapshot` from callers that do not hold `c.mu` (`CommitTx` post-unlock).
- `load()` rewrites `meta.json` from `metaSnapshot()` when it is missing **or unparseable** (`collection.go:826-832`) *before* `initEncryption` runs, and `initEncryption` treats missing/corrupt meta as "no encryption" (`encryption.go:483-489`). So a corrupt meta of an encrypted collection is replaced by a plaintext meta and then opened as plaintext. **Holds either way** (this follows from code order and needs no schedule). The H7 stale-snapshot overwrite is plausible from the same code but unreproduced.

### Decision — serialization
1. Add a leaf mutex `Collection.metaMu`. The only persist path is `c.writeMeta(mut func(*collectionMeta))`: acquire `metaMu`; **take the snapshot inside it** (so a persist always writes current state; an older snapshot can never land after a newer one); apply `mut` (used by encryption change to install the *candidate* encryption block into the snapshot); write via `os.CreateTemp(dir, "meta.json.*")` → fsync → rename → `fsyncDir`; on failure remove the temp and return the error. Memory (`encMeta`, `enc`, `explicitDefaultTTLSecs`) is updated **only after** `writeMeta` succeeds (reverse of today's store-then-rollback order, which let a concurrent post-unlock persist publish an unpersisted policy).
2. Make `explicitDefaultTTLSecs` an `atomic.Int64`; `metaSnapshot` then needs no collection lock.
3. Persist errors are no longer swallowed on rotation/`CommitTx`/`Close`: rotation and `CommitTx` return them as `PostCommitError` (D1) because the id counter is reconstructible from segments; policy/TTL changes return the error to the caller with memory unchanged.

### Decision — encryption fail-closed
Open classifies `meta.json` **before any rewrite**:
- **Evidence of encryption** = (E1) any primary-index entry with `Epoch > 0` (the index mirrors epochs; O(1) via a max-epoch counter alongside `countAtEpoch`), or (E2) the sentinel file `encryption.sentinel` in the collection directory, written (file + dir fsync) *before* the first encrypted write ever takes effect and by `initEncryption` self-heal when a valid block is found, or (E3) on a missing/unparseable meta only: a marker-prefix scan (`\x00scrivaenc:`) of the segments.
- If evidence exists and meta is missing/unparseable/lacks `encryption`, or `meta.Encryption.Epoch < max entry epoch` (stale), open fails with `ErrEncryptionMetaIntegrity` (distinct from `ErrWrongKey`/`ErrKeyUnavailable`). It **never** infers plaintext and **never** rewrites the file; an unparseable file is copied to `meta.json.corrupt-<unixnano>` and left in place.
- This error is **not tolerable** under any integrity policy (`integrity_policy.go` tolerant modes must not downgrade it). Logs carry collection name, evidence kind and file name only — no keys, no plaintext.
- A wrong key (`VerifyKeyCheck` mismatch) already fails closed and is unchanged.

### Rationale
A single leaf mutex with snapshot-inside-lock is the smallest change that makes lost updates impossible; no versioned CAS is needed because all writers are in one process under the directory lock. The sentinel is the only evidence independent of `meta.json` that costs O(1) at open.

### Rejected
- **Extend `c.mu` to cover snapshot+persist everywhere:** `CommitTx`/`setDefaultTTL` persist outside `c.mu` on purpose (fsync under the write lock would stall appenders); moving them inside creates the lock-order problems the spec flags.
- **Immutable value + generation CAS with retry:** more machinery than a single-process leaf lock requires.
- **Always scan for ciphertext markers at open:** O(data) at every open of plaintext collections.
- **Derive "encrypted" only from the index epochs:** misses epoch-0 legacy ciphertext (encrypted before epochs existed, `meta` comment at `meta.go`), hence the sentinel/E3.

### Compatibility
On-disk: one new optional sentinel file; `meta.json` shape unchanged. An old binary ignoring the sentinel is the same exposure as today. *Unverified:* that no existing tool rejects unknown files in a collection directory — confirm in verify/repair/snapshot before adding the file. API: new error only. Snapshots/backups must include the sentinel (extraction allowlist, spec §3.8).

### Lock order
`encAdminMu` → `c.mu` (existing) → `metaMu` (new leaf). `writeMeta` acquires nothing else while holding `metaMu` (all snapshot inputs are atomics/immutables). Callers that already hold `c.mu` (`rotateSegment`, `Close`, `applyEncryptionChange`) keep doing fsync under it exactly as today; `setDefaultTTL` and `CommitTx` call it with no collection lock. `persistMu` (index files) is unrelated and stays ordered `compactMu → persistMu → c.mu`.

### Acceptance
- **M4.1** Barrier after `metaSnapshot` equivalent / before write: concurrently `setDefaultTTL` and `SetEncryptionPolicy`, release in both orders: final `meta.json` contains both the TTL and the encryption block; reopen decrypts existing records. (Under D4 the barrier sits inside `writeMeta`, so the test asserts the ordering property directly.)
- **M4.2** `faultFS` rename failure in `writeMeta` during `SetEncryptionPolicy`: error returned, `EncryptionStatus()` unchanged, a concurrent `CommitTx` persist writes the **old** policy, reopen consistent.
- **M4.3** Corrupt `meta.json` of an encrypted collection (E1 and E3 variants): open ⇒ `ErrEncryptionMetaIntegrity`, file bytes unchanged, `.corrupt-*` copy created, `Verify` offline reports it; plaintext collection with the same corruption ⇒ opens, meta regenerated.
- **M4.4** Meta with block removed + sentinel present ⇒ fails; meta epoch lower than entry epoch ⇒ fails; correct key but missing meta of legacy epoch-0 ciphertext ⇒ fails via E3.
- **M4.5** Two goroutines hammer `writeMeta`: no `ENOENT` rename error, no truncated JSON, `-race` clean.
- **M4.6** Rotation with persist failure returns `PostCommitError`; record visible after reopen.

## 5. Scan view semantics (spec §2.5, §3.5; H8)

### Today (verified)
`streamLive` holds `layoutMu.RLock` (excludes compaction swap), copies `sealed+active` under `c.mu.RLock`, releases `c.mu`, then for each entry consults the **live** primary index (`scan.go:371-431`). Consequences from the code shape (**holds either way; not reproduced**): (i) *omission*: record R updated into a segment created after the copy (rotation) is skipped as a "genuine stale version" and never emitted; (ii) *duplicate*: R emitted from its old location, then updated into a later segment still in the copy, emitted again; (iii) an updated record's active-segment read extends to the file's current end, so writes after scan start are visible in some segments and not others. Index-driven paths (`visitCandidates`) re-read each id live (`getStored`), so they see a per-id latest version at different instants. `Count` shares `forEachMatch`.

### Decision
**Per-collection repeatable view at scan start ("snapshot at start"), bounded in time and memory.**
1. **Capture** under `layoutMu.RLock` then `c.mu.RLock` (one critical section): the segment list with each segment's committed `Size()` (end offset), a **copy of the primary index** (`id → IndexEntry`), the secondary-index candidate set for the filter if an index serves it, `layoutGen`, and `now` (D6). Release `c.mu`.
2. **Iterate** captured segments only up to their captured end; an entry is emitted iff the captured index maps its id to exactly `(segment path, offset)` — emits each record **exactly once at the version live at capture**. Records deleted, updated, or expired after capture are still returned at their captured version; records inserted after capture are excluded. Candidate paths read the captured location, never the live index.
3. **Pin** (layout lease): `layoutMu.RLock` is held for the scan so compaction/rotation unlink cannot remove captured bytes; rotation appends new segments without disturbing captured ones.
4. **Bounds:** at most `MaxScanViews` concurrent captured views per collection (default 8; extra scans wait on the context); a view older than `ScanViewMaxAge` (default 10 min) ends with retryable `ErrScanViewExpired` rather than pinning compaction forever. Cancellation/backpressure must release the lease and the copy.
5. **No silent shortening:** if a captured segment cannot be read (`layoutGen` changed unexpectedly, file missing), the scan fails with `IntegrityError`/`ErrScanViewExpired`; it never skips.
6. `Count` and every aggregate input use the same captured view (Count(MatchAll) remains O(1) *only* when no expiring records exist — see D6).
7. **Scope:** per collection only. A cross-collection scan pair is two independent views; XTx isolation remains as the XTx design states and nothing here implies cross-collection snapshots (spec §2.5.3).

### Rationale
Append-only segments keep every old version on disk until compaction, so a start-time view needs only (index copy + end offsets + a compaction lease): no MVCC, no new on-disk state. The copy costs O(live records) memory for the scan's lifetime (≈ 64 B/record order of magnitude — measure) and a short `c.mu.RLock`.

### Rejected
- **Status quo lazy live-index lookup:** produces (i)/(ii) above.
- **Hold `c.mu.RLock` for the whole scan:** a slow streaming client would stall all writers (the reason `layoutMu` exists).
- **Generation check with restart/retry** (detect `layoutGen`/rotation, restart the scan): fixes omission but not duplicates from in-place updates, and can starve under steady writes.
- **Weak "read committed per record, no duplicates/omissions of continuously-present records":** needs an emitted-id set (also O(n)) *and* still has moving results; the copy gives a stronger, simpler contract for the same order of cost.

### Compatibility
On-disk: none. API: ordering unchanged (insertion order / id order for candidates); results can now include a record deleted *after* the scan began (previously "maybe"). New `ErrScanViewExpired` (gRPC `ABORTED`/reason `SCAN_VIEW_EXPIRED`, retryable); config `max_scan_views`, `scan_view_max_age`. Replication: none (followers apply the same capture on their own state).

### Lock order
`layoutMu.RLock` → `c.mu.RLock` (capture only) → `Index.mu.RLock` (copy) → `sidxMu.RLock` (candidates, after `c.mu` as on the write path). No lock is held while invoking the visitor. `compactMu` is not taken by scans (compactor takes `compactMu` → `layoutMu.Lock` + deferral already exists).

### Acceptance (barriers via `scanHook`; run under `-race`)
- **V5.1** Start scan; at `scanAtSegment(0)` update R into a new segment after forced rotation: R emitted exactly once at its start version, no duplicate.
- **V5.2** Start scan; delete R (not yet visited): R still emitted once; insert new record N: not emitted.
- **V5.3** Update R twice, once in the captured active tail and once beyond the captured end: emitted once at captured version.
- **V5.4** Compaction requested mid-scan is deferred (`compactRetryPending`) and runs after release; scan output identical to a no-compaction control.
- **V5.5** Cancel mid-scan / slow consumer: `layoutMu` and the copy are released (goroutine/lock leak check); a second compaction proceeds.
- **V5.6** `ScanViewMaxAge` exceeded ⇒ `ErrScanViewExpired`, never fewer rows without error; `MaxScanViews` honoured.
- **V5.7** `Count(f)` equals `len(Scan(f))` taken from the same view under concurrent writes; both equal a model snapshot taken at the capture barrier.

## 6. TTL visibility clock (spec §3.5; H11)

### Today (verified)
Expiry is an absolute `ExpiresAt` (Unix ns) stamped at write time from `time.Now()` + TTL (`ttl.go:resolveInsertExpiry`). Visibility compares `ExpiresAt` with a *fresh* `time.Now()` **per call and per row**: `readLocated` (`collection.go:1251`), `streamLive` row loop (`scan.go:443`, so a long scan has a moving cutoff). `Count(MatchAll)` returns `index.Len()` (`count.go:29`) and the eq fast path returns a secondary-index bucket size, neither consulting expiry; `IndexLookup` returns bucket ids unfiltered (`IndexLookup`), and `sidx.conflict` ignores expiry, so an expired-unreaped holder of a unique value (including `_key`) blocks a new insert. `reapExpired` and compaction each take their own `time.Now()` (`ttl.go:80`, `compactor.go:580`); the reaper's tombstones are **not** `publishCommit`ted (`ttl.go:79-120`), so followers rely on their own wall clock and their own compaction. XTx commit checks target expiry at its own instant (`xtx_commit.go:408`).

### Decision
1. **One predicate:** `visible(entry, now) := !tombstone && !(ExpiresAt != 0 && now >= ExpiresAt)` (existing `expired()`), evaluated against **one `now` captured once per operation**: per `Get`, per scan/`Count`/aggregate view (D5 captures it with the view), per `Find`/index lookup, per XTx commit (captured under the participant locks), per reaper pass, per compaction pass.
2. **One clock source:** the node's wall clock via a collection clock seam `now()`; test hook `CollectionConfig.nowFn`. The clock is **floored monotonically**: `now() = max(wall, floor)`, where `floor` is an in-process high-water mark and, at open, the newest `Ts` seen in the active-segment tail scan that `load` already performs (`tailMaxID`). This makes a backwards wall-clock step unable to un-expire a hidden-but-unreaped record within a process lifetime or across a restart following recent writes, with no on-disk change.
3. **Every consumer applies the same predicate:** `Count(MatchAll)` is `index.Len()` only when the index's `nExpiring` counter (entries with `ExpiresAt != 0`) is 0, otherwise an in-memory walk of the index (no segment I/O) excluding expired ids; the eq fast path and `IndexLookup`/range lookups filter ids through the index entry with the same `now`; unique checks (`sidxCheckUnique`, `txCheckUnique`) treat an expired holder as vacant.
4. **Reaper and compaction never define visibility**; they only reclaim records the predicate already hides. Physical reclamation uses `now − ReclaimGrace` (default 60 s) so that clock skew or a small step never makes a record physically disappear *earlier* than a peer's visibility says. The reaper's delete entries **are published** (`publishCommit`) so followers converge physically without relying on their own clocks.
5. **Replication:** entries carry absolute `ExpiresAt`; each node evaluates visibility with its own clock. The documented bound is `|leader clock − follower clock|` (operators must run NTP); followers do not reap/compact-away records inside the grace window. A follower read can therefore differ from the leader by at most the skew; this is stated, not hidden.
6. **Write-time stamping** uses the same `now()` and one `now` per batch (today `CommitTx` re-evaluates `resolveInsertExpiry` per op).

### Rationale
The inconsistency is entirely "who reads the clock when". A captured `now` per operation plus a single predicate makes Count/Scan/Find/index/XTx agree by construction, and a floor + reclaim grace converts clock steps from a visibility bug into a bounded, documented skew.

### Rejected
- **Logical clock / per-collection revision as TTL basis:** TTLs are user-facing wall durations; would change record format and semantics.
- **Treat expiry as physical-cleanup-only (count expired as live):** contradicts `Get`/`Scan` and the documented count behaviour; makes unique keys unusable until the reaper runs.
- **Persist the high-water mark in `meta.json`:** unnecessary given the tail-scan floor and adds another field written by many paths (and dropped by old binaries).
- **Per-row clock reads (status quo):** non-repeatable within one operation.

### Compatibility
On-disk: none. API: `Count` can decrease for collections with TTL records that are expired but unreaped (aligns with `Scan`); `InsertWithKey` over an expired key now succeeds (previously `ErrDuplicateKey`); `Exists(key)` for an expired key becomes false. Replication: reaper deletes now flow as ordinary delete entries (additive; v1 followers handle them). Release-note all three.

### Lock order
No new lock. `now` is captured before taking `c.mu` for reads and *under* `c.mu` for writers/XTx commit (so a write's expiry check and its append share one instant). The `nExpiring` counter lives in `Index` under `Index.mu`.

### Acceptance (inject `nowFn`; no sleeps)
- **T6.1** For each of {before expiry, at `ExpiresAt` exactly, after expiry before reaper, after reaper, after compaction, after reopen, after index rebuild, after sidx rebuild}: `Get`, `Scan`, `Count(MatchAll)`, `Count(eq on indexed field)`, `Find` via index, `Exists(key)` agree (record visible iff `now < ExpiresAt`).
- **T6.2** Clock stepped backwards by 1 h after expiry (before reaper): record stays hidden; across reopen (tail `Ts` floor) too.
- **T6.3** A scan whose clock crosses `ExpiresAt` mid-scan returns the record (cutoff captured once).
- **T6.4** Unique `_key` held by an expired record: `InsertWithKey` succeeds; reopen + rebuild keep exactly one live holder.
- **T6.5** Reaper pass emits one replicated delete per reaped id; follower state converges without its own reap; follower inside `ReclaimGrace` retains bytes.
- **T6.6** XTx update of an expired target ⇒ `ErrXTxConflict` using the commit-captured `now`.
- **T6.7** `nExpiring == 0` ⇒ `Count(MatchAll)` performs no index walk (benchmark guard).

## 7. `_key` / reserved-field compatibility (spec §3.7; H10)

### Today (verified)
Reserved set: `_key` (`keys.go:KeyField`), `secure_data` when record-level encryption is active (`encryption.go:29,194`), string values beginning with the encryption marker `\x00scrivaenc` (`ErrReservedPrefix`), and collection names with prefix `xtx.` (`xtx_format.go:isReservedXTxName`). Every public id-less write path (`Insert*`, `InsertMany`, `Update*`, `UpdateByKey`, `UpdateIfRev/IfMatch`, `Upsert`) rejects `_key` in caller data with `ErrReservedField`. **Gaps:** `Update(id, data)` / `UpdateWithExpiry(id, …)` on a keyed record writes `data` without `_key` (`collection.go:1110-1163`, no preservation), so the unique-index entry is removed and the key becomes reusable; the XTx update branch (`xtx_commit.go`) has no preservation and a grep finds no `ErrReservedField`/`KeyField` rejection in `xtx_commit.go` or `xtx_handle.go` (the latter only *reads* `_key`, line 546), so a staged op may carry or drop `_key`; `replication_apply.go:114` only ensures the key index. Replication apply and legacy data are not re-validated.

### Decision
1. **`_key` is reserved and immutable once assigned.** The key is set only by `InsertWithKey`/`Upsert`/keyed XTx insert and never changes. There is no rename operation (delete + insert).
2. **Updates preserve it.** All id-addressed write paths — `Update`, `UpdateWithExpiry`, XTx staged/committed update — stamp the record's current `_key` into the new payload when the collection has a `_key` index and the current record carries a string `_key`, via one helper `preserveReservedLocked(c, id, data)` called under `c.mu` (current key obtained from the key index's reverse mapping or one segment read; **skipped entirely** when no `_key` index exists). Supplying `_key` in caller data stays `ErrReservedField` (gRPC `INVALID_ARGUMENT`) on every path, **including** XTx stage ops, which must reject at stage time, not commit time. Preservation is silent (not an error): it is a compatible fix because today's behaviour silently loses the key.
3. **Existing data.** Open never rewrites records. Records already containing a `_key` field are read/scanned exactly as stored. When the `_key` index is built (`ensureKeyIndex`/open), only **string** values participate; non-string `_key` values are ordinary legacy data, ignored by the index and left intact. Duplicate string `_key` values among *live* records make the build fail with `ErrKeyIndexConflict{Collection, Key, IDs[≤16]}`: the keyed API (`InsertWithKey`, `FindByKey`, `Upsert`, `Exists`) returns that error for the collection; all id-based operations keep working; `Verify` reports `key-duplicate` findings; the engine **never selects an arbitrary winner**. Resolution is operator action (delete/rewrite the duplicate by id); an automated repair is a deferral.
4. **Replication:** followers apply leader entries verbatim (leader normalises); on a follower a `_key`-bearing apply that violates uniqueness is an integrity error that stops the stream (typed), not silently skipped. `LoadJSONL` keeps its explicit key-field mapping and rejects stray `_key`.
5. **Other reserved names** are unchanged in behaviour but listed in the API docs as one set: `_key`, `secure_data` (record encryption only), marker-prefixed string values, `xtx.*` collection names.

### Rationale
Immutability + silent preservation closes the data-integrity hole without breaking any caller that was working; failing the keyed API (not the whole collection) on ambiguous legacy data keeps blast radius small and honest.

### Rejected
- **Reject id-addressed updates that omit `_key`:** breaks every existing caller of `Update(id,…)` on keyed collections for a case they cannot sensibly handle.
- **Allow `_key` rename via update:** needs a two-index atomic move and new uniqueness proofs; no demand evidenced.
- **Migrate legacy `_key` fields at open:** rewrites user data implicitly, unrecoverable by downgrade.
- **Pick lowest/highest id on duplicates:** arbitrary winner = silent data selection.

### Compatibility
On-disk: none (records written under preservation are ordinary v1/v2 lines identical in shape to keyed updates via `UpdateByKey`). API: `Update(id)` on a keyed record now keeps `_key` (behaviour fix, release-noted); new `ErrKeyIndexConflict`; gRPC `FAILED_PRECONDITION`/`KEY_INDEX_CONFLICT`. Replication: unchanged wire format.

### Lock order
`c.mu` (write) → `sidxMu` (read) as on every write path; the reverse key lookup must not take `c.mu` itself (callers hold it — mirror `IndexLookup` use in `Upsert`). XTx: preservation runs inside the commit step under participant `c.mu`, never at stage time.

### Acceptance
- **K7.1** `InsertWithKey("k", d)`; `Update(id, {x:2})`: `FindByKey("k")` still resolves; `Insert`ing key `"k"` again ⇒ `ErrDuplicateKey`; survives compaction and reopen.
- **K7.2** Same through XTx update and `UpdateWithExpiry`; XTx stage op containing `_key` ⇒ `ErrReservedField` at stage time.
- **K7.3** Collection without a `_key` index: `Update` allocates no extra read (benchmark/hook assertion).
- **K7.4** Legacy fixture with duplicate string `_key` values: open succeeds, `FindByKey` ⇒ `ErrKeyIndexConflict` listing both ids, `Get`/`Scan`/`Update(id)` work, `Verify` reports `key-duplicate`; fixture with non-string `_key` is ignored by the index and returned unchanged.
- **K7.5** Follower receiving a duplicate-key entry stops with the typed error and applies nothing from that record.

## Cross-decision interactions

- **D1 ↔ D2:** single-record fence (D1) applies to ≥2-entry batches only via the XTx error model; both use `ErrXTxOutcomeUnknown`-shaped status (`codes.Unknown`).
- **D3 ↔ D2:** once batches are one-participant XTx, *every* collection that ever took a batch has retained decisions; D3's on-demand retirement is therefore a **prerequisite for default-on atomic batches** (otherwise drop would be permanently refused). Ship D3 retirement-at-drop before or with D2's default flip.
- **D4 ↔ D1/D5:** `writeMeta` failure is a post-commit ancillary failure, never an Unknown outcome; D5/D6 capture is read-only and takes no meta lock.
- **D5 ↔ D6:** the view captures `now` once; Count's expiry predicate is the same captured value.

## Spec sections to update

| Decision | Spec section(s) to amend at consolidation | §6 row it resolves |
|---|---|---|
| D1 single-record sync error, taxonomy, fence | §2.3 (items 2, last two paragraphs), §3.2 (state machine + failure table: add "Fenced" row and unfenced `NotApplied` condition), §3.6 (Seal ordering), §4 "I/O outcome" row, §1.2 H5/H9 | "Single-record sync error" |
| D2 batch protocol | §3.1 (promote Option A to *approved*, record items 1–8), §3.1 "Required batch protocol decisions" (answers 1–6), §4.2 (rollout gate), §5 stage 3, §7 architecture wording | "Batch commit format", "Batch durability" |
| D3 participant drop/retirement | §2.4 item 4, §3.3 (proof obligation, drop sequence, lock order), §4 "Participant lifecycle", §5 stage 4 | "Participant retirement/drop" |
| D4 metadata + encryption fail-closed | §3.4 (decision `metaMu`, evidence rules, error identity), §2.4 item 3, §4 "Metadata concurrency" | "Metadata lock strategy" |
| D5 scan view | §2.5 items 1–2 (state "snapshot at start", bounds), §3.5 first half, §4 "Scan stability" | "Scan semantics" |
| D6 TTL clock | §2.5 item 1, §3.5 second half, §4 "Expiration/count" | "TTL clock semantics" |
| D7 `_key` | §3.7 (replace "define API compatibility" with decisions 1–5), §4 "Key invariants" | not in §6 — add a new §6 row "Reserved `_key` compatibility" marked ratified |
