# Cross-collection transactions (XTx) — protocol and on-disk design

**Status:** design, ratification pending. **No code, format or API changes ship
with this document.** **Baseline:** integration branch
`autopilot/01-cross-collection-transactions-design-and-format` (on `main` @
`7660844` + the inventory).
**Input:** [`cross-collection-tx-inventory.md`](cross-collection-tx-inventory.md)
(section references `inv §N` below).

## 0. Scope, non-goals, summary

**In scope.** An atomic, durable, crash-recoverable write transaction spanning
2..N collections of **one `engine.DB` — one data directory, one `LOCK`, one
process**. After a crash, every acknowledged XTx is fully present and every
unacknowledged XTx is fully absent, in every participant, whatever the
`SyncMode` of the participants.

**Explicit non-goals.**

- Distributed transactions, XA, two-phase commit across nodes or across data
  directories, multi-root transactions, and any consensus protocol. Replication
  stays leader→follower, single-writer; a follower never decides.
- Cross-collection read snapshots / snapshot isolation (§9).
- Changing single-collection semantics. A database that never runs an XTx
  writes byte-identical segments, `meta.json`, `index.json` and `sidx` files
  (inv §13.3).
- Foreign keys or cross-collection unique constraints.

**The design in six sentences.**

1. Redo-only. Participants append their ops as *stamped, invisible* entries;
   nothing becomes visible until a decision exists.
2. The decision lives in one place: a **root-level coordinator journal**
   (`xtx.journal`, a regular file). A durable `COMMIT` record there is the sole
   commit point, and it is authoritative.
3. No decision record ⇒ **presumed abort**. A client ack is only ever sent
   after the `COMMIT` fsync returns, so presumed abort is always safe.
4. Stamped entries carry a **domain-separated checksum** that every older binary
   computes differently, so older binaries fail closed instead of misreading
   them; a root `xtx.format` file gives newer binaries a precise error.
5. Recovery is a DB-level phase that runs *between* "segments are physically
   consistent" and "indexes are trusted" (inv §5), and treats index snapshots as
   derived caches that must agree with the journal.
6. Participants are always locked in ascending collection-name order under
   `DB.mu.RLock`, and every XTx forces its own durability (n+1 fsyncs)
   regardless of `SyncMode`.

---

## 1. Terminology and invariants

| Term | Meaning |
|---|---|
| **XTx** | One cross-collection transaction, identified by `txid`. |
| **Participant** | A collection with ≥1 op in the XTx. |
| **Run** | The contiguous, stamped, appended entries one participant writes for one XTx. |
| **Stamped entry** | A segment entry with a `tx` object and a v2 checksum (§3). |
| **Journal** | `xtx.journal`: the append-only root-level coordinator log (§4). |
| **Decision** | `COMMIT` or `ABORT` for a txid, durable in the journal (or retired, §4.4). |
| **Presumed abort** | Stamped entries with no durable decision are not part of the database. |
| **Prepared** | Derived state (not a record): every participant's complete run is fsynced. |

**Invariants (each is enforced by a named test in §14).**

- **I1 Atomic visibility on disk.** For one txid, either every run is applied to
  the primary and secondary indexes of every participant, or none is.
- **I2 Single commit point.** A txid is committed iff a `COMMIT` record for it is
  fully durable in the journal (or a `RETIRE(committed)` record, §4.4). Nothing
  else — not an index file, not a segment byte, not a replica — can commit it.
- **I3 Ack ⇒ durable.** A client sees success only after steps S2 and S4 of §6.
- **I4 No unfenced reader.** No released binary can open a database containing
  stamped entries and silently treat them as ordinary entries (§11).
- **I5 Order independence.** Recovery's result does not depend on `os.ReadDir`
  order, on goroutine scheduling, or on how many times recovery is re-run
  (idempotent).
- **I6 No fabricated commits.** No tool (Repair, salvage, snapshot restore,
  replication bootstrap) may turn an undecided or aborted run into visible
  data.

---

## 2. Evidence location (answers inv §15, first question)

Two kinds of durable evidence, with a strict division of labour:

| Evidence | Where | Decides? |
|---|---|---|
| Payload (the ops) | Stamped entries **in each participant's own segments** | No — cannot commit anything |
| Decision | Root-level **file** `xtx.journal` | Yes — the only thing that does |

Why payload in segments (not in the journal): the data then flows through the
existing machinery unchanged — encryption sealing (`initEncryption`, per-entry
epoch), quota, compaction, index offsets, replication, snapshot copy, salvage.
A journal-carried payload would need its own sealing, compaction, and
replication story (inv §12, §6).

Why the decision outside the segments: a decision that must be consistent
across N collections cannot live in any one of them without making that
collection special, and a decision spread over N segments is N independent
facts that can disagree after a partial restore. One record = one fact.

### 2.1 Reserved names

All XTx root-level state is **regular files** whose names start with `xtx.`:

| Name | Purpose |
|---|---|
| `xtx.format` | version gate (§11.2) |
| `xtx.journal` | the coordinator journal |
| `xtx.journal.tmp` | journal GC rewrite (§4.5), deleted at open |

Rules, all shipped in the *fence* release (§11.3) before any XTx byte is
written:

1. Root *files* are ignored by `Open`'s collection scan today (only
   directories are opened — inv §2), so no collection is created from them. A
   root *directory* with a reserved name is refused at open with
   `ErrReservedName` (it would be a collection that shadows the journal).
2. `CreateCollection` rejects any name with prefix `xtx.`.
3. `Verify`, `Repair`, `SnapshotTo` learn the reserved names so that they are
   neither flagged as unknown nor silently dropped (§10, §12).
4. Nothing XTx-related is stored in `meta.json` (`persistMeta` rewrites it from
   the *old* binary's struct and would erase unknown fields — inv §2).

---

## 3. Transaction-aware entry format

### 3.1 Wire shape

A stamped entry is an ordinary `store.Entry` JSON line plus one object:

```json
{"id":42,"op":"update","ts":"2026-10-10T12:00:00Z","rev":3,"data":{"…":"…"},
 "epoch":2,"expires_at":0,
 "tx":{"t":"4f9c…e1a7-0000000000000012","i":0,"n":2},
 "crc":2870312345}
```

| Field | Meaning |
|---|---|
| `tx.t` | txid (§5.1), ASCII, ≤ 64 bytes |
| `tx.i` | 0-based index of this entry within **this participant's** run |
| `tx.n` | total ops in this participant's run (so `i == n-1` is the last) |
| `op`, `id`, `rev`, `data`, `epoch`, `expires_at` | exactly as today; `op ∈ {insert, update, delete}` |

Notes:

- **`op` is deliberately not extended.** Old strict replay accepts unknown ops
  and `compactor.resolveEntries` keeps them as "latest", shadowing real records
  (inv §3, §6.2). Stamped entries use real ops and real ids, so they are
  *correct data* if (and only if) the decision says commit.
- The participant list and the other participants' counts are **not** repeated
  per entry; they live in the `COMMIT` record (§4.2). Entries stay small.
- A stamped entry in a collection that is *not* in the decision's participant
  list is evidence of corruption (`xtx-foreign-run`, §10).

### 3.2 Checksum v2 (the old-binary fence)

`store.Decode` verifies `crc` **only when present** and recomputes with
`checksum()` (CRC32C over id, op, rev, expiry, epoch, canonical data; inv §3).
So the fence must keep `crc` present and make it *deterministically differ*
under the v1 algorithm.

```
crc_v2 = CRC32C( "XTX1\x00" ‖ len(tx.t) ‖ tx.t ‖ u32le(tx.i) ‖ u32le(tx.n)
                 ‖ <exactly the v1 input bytes: id, op, rev?, expires_at?, epoch?, data?> )
```

- A new reader that sees `tx` verifies **v2 only**. A stamped entry whose `crc`
  verifies under v1 is rejected (`ErrCorruptEntry`): it means a tx stamp was
  spliced onto a v1 line.
- Stripping `tx` from a stamped line makes it fail v1 verification (probability
  of accidental pass is 2⁻³²), so a tool that drops unknown JSON keys
  (old `compact`, `salvage`, `persistMeta`-style rewrites, `jq`) produces a
  *detectably* corrupt line, not a silently committed one.
- `ts` stays uncovered, exactly as in v1 (changing that is out of scope).
- The tx fields are covered, closing the "new field is unprotected" gap
  (inv §3 table row 1).

### 3.3 What a stamped entry means

An entry's *effective status* is a pure function of `(txid, journal)`:

| Journal says | Effective status |
|---|---|
| `COMMIT`, or `RETIRE(committed)` | normal committed entry, ordered by file position |
| `ABORT`, `RETIRE(aborted)`, or nothing | **dead**: skipped by every fold (index, sidx, compaction, scan, Verify id-conflict checks) |

"Every fold" is the five places of inv §4/§14 row 4: `applyOne`/`applyEntries`,
`Index.Rebuild`, `rebuildTolerant`, secondary `rebuild`, and the
`Verify`/`Repair` scanners. They must all consult **one** shared predicate
`entryVisible(e, decisions)`; the design forbids an inlined variant (a missed
site is the #107 desync class).

### 3.4 Position invariant

While a participant holds its `c.mu` from the first run append until after the
decision is applied (§6), no other writer can append to that collection.
Therefore **a run is always the physical tail of that collection's active
segment at any crash point**, and rotation never splits a run (rotation is
deferred until all XTx locks are released, as `CommitTx` does today — inv §3).
Recovery relies on this: undecided evidence is searched only in the tail of the
active segment (§7), bounded by `n`, not by database size.

---

## 4. The coordinator journal

### 4.1 File and record framing

`xtx.journal` is NDJSON, one record per line, each ending in `\n`:

```
line 1:  {"magic":"SCRIVA-XTX","v":1,"epoch":"<16 hex>","created":"<ts>","crc":N}
line k:  {"k":"<kind>", …fields…, "crc":N}
```

- `crc` is CRC32C(Castagnoli) over the record's canonical JSON with `crc`
  omitted — the same discipline as `store.Entry`.
- `epoch` is 8 random bytes chosen at journal creation. Together with the
  monotonic `seq` it makes a txid unique across a restored/forked data
  directory (§5.1).
- Records are written with a single `write(2)` (O_APPEND) of one whole line.
- The file is only ever appended, except for GC (§4.5), which is
  write-new-then-rename.

### 4.2 Record kinds

```json
{"k":"commit","tx":"<txid>","key":"<idempotency key|''>","ts":"<rfc3339nano>",
 "parts":[{"c":"orders","n":2,"d":"<sha256 hex>"},{"c":"stock","n":1,"d":"…"}],
 "pd":"<sha256 hex>","crc":N}

{"k":"abort","tx":"<txid>","key":"…","why":"conflict|quota|io|recovery|client","ts":"…","crc":N}

{"k":"retire","tx":"<txid>","o":"commit|abort","key":"…","ts":"…","crc":N}
```

- `parts` is sorted by collection name **bytewise**, no duplicates (canonical
  participant order, §6.1). `n` is that participant's run length.
- **`d` — participant digest:** `SHA-256` over, for `i = 0..n-1`:
  `u32le(i) ‖ u64le(id) ‖ op ‖ 0x00 ‖ u64le(rev)`. Deliberately **excludes
  `data`, `epoch`, ciphertext and file offsets**: those legitimately change
  under `MigrateNow` re-encryption and compaction (inv §6.4, §12), and data
  integrity is already covered by the v2 entry CRC. `d` therefore pins *which
  ops exist*, which is exactly what "was the commit's evidence lost?" needs.
- **`pd` — participant-set digest:** `SHA-256` over the canonical
  concatenation `name ‖ 0x00 ‖ u32le(n) ‖ d` for each part. It makes a single
  field comparison sufficient in tools and tests, and lets `Verify` detect a
  hand-edited `parts` list.
- `key` is the client idempotency key (§8.2). It is stored, so status survives
  restart.
- There is **no `begin`/`prepare` record**. "Prepared" is derived from the
  complete fsynced runs; writing it would cost a second journal fsync per tx
  for no recovery value (a prepared tx with no `COMMIT` aborts either way).
  This is the main performance decision of the protocol.

### 4.3 Journal-level recovery (torn tail vs corruption)

At open the journal is scanned front to back:

| Observation | Action |
|---|---|
| Final line lacks `\n`, or is all `0x00`, **and** no CRC-valid record follows it | torn tail: truncate to the last good `\n`, `fsync` |
| A *complete* (newline-terminated) record fails CRC, or fails to parse, **anywhere** | **fail closed:** `ErrXTxJournalCorrupt`. Open refuses. Never "presume abort". |
| A good record follows a bad one | same: fail closed |
| First line missing/invalid magic | fail closed |
| Duplicate decision for one txid with the *same* kind | idempotent, ignore |
| `COMMIT` and `ABORT` for the same txid | fail closed (`xtx-decision-conflict`) |

Rationale for "fail closed" rather than presumed-abort on a corrupt middle
record: a lost `COMMIT` would silently convert *acknowledged* data into
"aborted" and delete it (§7 truncates aborted runs). Presumed abort is only
valid for the *absence* of a record at the tail of an append-only log — the
only place a crash can remove one.

### 4.4 Retirement

Decisions cannot live in the journal forever, and stamped entries cannot stay
stamped forever (compaction would have to preserve them, inv §6.1). `RETIRE`
is the handshake between the two:

1. **Verification.** A background *checkpointer* (also run at `Close` and at
   the end of recovery) takes, for a decided tx, each participant's `c.mu`
   read lock in name order, re-derives each `d`, and compares with the
   `COMMIT`'s `parts`. Mismatch ⇒ `xtx-participant-missing` /
   `xtx-digest-mismatch`, tx is **not** retired, Open refuses unless the
   integrity policy tolerates it (§10).
2. **Record.** `RETIRE{tx, o}` is appended and fsynced. From this point the
   outcome is final and no longer needs any participant evidence.
3. **De-stamping.** Compaction may now rewrite entries of a retired tx:
   committed entries become **plain v1 entries** (no `tx`, v1 CRC), aborted
   ones are dropped. Until retired, `resolveEntries` must copy stamped entries
   of that tx verbatim (it may still reorder/merge *other* entries).
4. **Journal GC** (§4.5) may drop a retired tx's records once (a) no stamped
   entry for it remains in any collection (tracked by a per-collection counter
   persisted in the *index/sidx coverage record*, rebuilt by a tail+sealed
   scan during open when absent), and (b) its status-retention window (§8.2)
   has elapsed.

Compaction therefore *shrinks the stamped footprint over time* and a fully
drained database can return to the pre-XTx byte format (§11.5).

### 4.5 Journal GC

When retired-and-expired records exceed 50 % of the file (or at `Close`):
write `xtx.journal.tmp` with a fresh header (same `epoch`, bumped
`generation`) containing every live record; `fsync` file; `rename` over
`xtx.journal`; `fsync` directory. A crash leaves either the old or the new
file whole; `xtx.journal.tmp` is deleted at open. GC takes `journal.mu` only
(§9.1).

---

## 5. Identity, ids and limits

### 5.1 txid

`txid = <journal epoch, 16 hex>-<seq, 16 hex>`; `seq` is a per-journal
monotonic counter allocated under `journal.mu` at commit time (not at stage
time). The counter is restored from `max(seq in journal, seq in stamped
entries found at recovery) + 1`. A restored backup or a replication bootstrap
that copies the journal keeps the same `epoch`; a freshly created journal on a
different directory picks a new one, so two lineages cannot alias.

### 5.2 Record ids (answers the `ReserveID` open question)

- `Collection.ReserveID` is unchanged: ids are reserved in memory at stage time
  and burned if the tx aborts. Gaps are legal today (inv §1).
- Ids are **provisional until the ack.** After a crash an id that was only ever
  reserved (or whose stamped entry was aborted and truncated) may be handed out
  again. A client must not treat an id learned from a *staged* op as durable
  before the commit result or a `COMMITTED` status.
- `tailMaxID` (inv §3) must count **every** stamped entry still physically
  present, committed or not, so a surviving dead entry never causes reuse.

### 5.3 Limits (config, validated at startup)

| Limit | Default | Why |
|---|---|---|
| ops per XTx | 1 000 | bounds recovery tail scan and replication group |
| participants | 16 | bounds lock set and journal record |
| bytes per XTx | 64 MiB | each op is its own line ≤ `maxScanTokenSize` (16 MiB) |
| replication ring | must be ≥ 2 × ops-per-XTx | a group must fit one ring window (inv §10.6) |

Exceeding a limit is `ErrXTxTooLarge`, reported **before** any write.

---

## 6. Commit protocol

### 6.1 Canonical participant ordering and lock hierarchy

Participants are ordered by **bytewise-ascending collection name** (the same
order for lock acquisition, run writing, fsync, `parts` in the record, and
recovery iteration). Duplicate names are merged before ordering; the
ordering is a pure function of the participant set, never of staging order.

Lock hierarchy (outer → inner), extending inv §9:

```
DB.mu (RW)                      XTx holds RLock for its whole duration
  └ Collection.compactMu        (never held with a c.mu by XTx)
      └ layoutMu / persistMu
          └ Collection.mu       XTx: all participants, ascending name
              └ journal.mu      leaf-most coordinator lock (seq alloc, append, fsync)
              └ sidxMu → Index.mu → Segment.mu → replicationBroker.mu
```

Rules:

1. XTx takes `DB.mu.RLock`, then each participant's `c.mu.Lock` in canonical
   order, then — only at the decision step — `journal.mu`. `journal.mu` is
   never held while acquiring any `c.mu`, `compactMu`, `persistMu` or
   `DB.mu`.
2. The XTx never acquires `compactMu`/`persistMu`/`layoutMu` while holding a
   `c.mu`. The compactor takes `compactMu` first and `c.mu` after, so
   ordering by name is sufficient and no cycle exists (inv §6.5).
3. Single-collection writers take exactly one `c.mu` and never `journal.mu`;
   they cannot cycle with an XTx.
4. The checkpointer, `Verify(cross-collection)` and the snapshot journal
   capture use the same order: `DB.mu.RLock` → `c.mu` (name order) →
   `journal.mu`.
5. Compaction reads decisions from an immutable, atomically swapped
   *decision table* (copy-on-write); it never takes `journal.mu`.
6. `DB.mu.RLock` held across the XTx makes `CollectionWithConfig`
   close/reopen and `DropCollection` (both `DB.mu.Lock`) wait for it, so a
   participant pointer is never used after close (inv §9). Participants are
   (re)resolved *under* `DB.mu.RLock`; a missing one fails the XTx with
   `ErrCollectionNotFound` before any write.
7. Rotation, meta persistence, watch emission and `TxManager` bookkeeping all
   happen *after* every XTx lock is released (inv §3, §12).

### 6.2 Steps

State machine of a live XTx: `STAGED → VALIDATED → PREPARING → PREPARED →
COMMITTED → APPLIED → ACKED`, with exits `ABORTED` from any state before
`COMMITTED`. Only `COMMITTED` (journal fsync returned) is a decision; the
others are in-memory.

| Step | Action | Durable effect |
|---|---|---|
| S0 | Acquire `DB.mu.RLock`, participants' `c.mu` (canonical order). Validate **everything** under the locks: op targets exist, `expected_rev` (§9.3), unique constraints (`txCheckUnique`), quota for **every** participant (`checkQuotaLocked`), limits (§5.3), poisoned segments. Seal encrypted fields. Compute rev/ids. | none |
| S1 | For each participant in order: append its run (stamped, v2 CRC). Remember `startSize` per participant. **No index mutation.** | bytes in page cache |
| S2 | For each participant in order: `fsync` the active segment — **unconditionally**, regardless of `SyncMode` (§6.3). Directory `fsync` if the active segment was created in this XTx. | runs durable ⇒ **PREPARED** |
| S3 | Take `journal.mu`; allocate `seq`; append `COMMIT{txid, key, parts, pd}`. | bytes in page cache |
| S4 | `fsync` `xtx.journal` (and the root directory if the journal file was just created). **Commit point.** Group commit across concurrent XTx is permitted here (one fsync, many records), because the records are independent and each XTx waits for the fsync covering its own record. | tx **COMMITTED** |
| S5 | Under the still-held `c.mu`s: apply every run to each participant's primary index + secondary indexes; `publishCommit` the whole group contiguously (§12.1); update decision table. | in-memory |
| S6 | Release locks in reverse order; `DB.mu.RUnlock`; then rotate if needed, emit watch events in group order, persist meta. | — |
| S7 | Ack the client. | — |

### 6.3 Durability requirements (strict)

1. **Ordering is the protocol.** `fsync(all runs)` happens-before
   `append(COMMIT)`, and `fsync(journal)` happens-before S5/S7. An
   implementation may not weaken S2 for `SyncModeNone`/`Interval`
   participants; it may not reorder S3 before the last S2 completes; and it may
   not ack before S4 returns.
2. **Mixed `SyncMode`.** XTx forces its own durability, so participants with
   `none`/`interval` are acceptable (inv §11 recommended option). Side effect,
   documented: the S2 `fsync` also flushes every earlier un-synced entry in
   that segment. XTx durability never *relaxes* a participant's own mode.
3. **fsync failure is fatal for the XTx and poisons what it touched.** If any
   S2/S4 `fsync` returns an error the kernel may have dropped the dirty pages;
   retrying `fsync` is not proof of durability. The XTx fails (§8.1,
   `ErrXTxDurability`) and the affected segment/journal is marked poisoned
   (the existing `ErrSegmentPoisoned` mechanism; the journal gets the same).
   Process-level recovery (restart) is the only way back.
4. **New files need a directory fsync:** a new `xtx.journal`, `xtx.format`, a
   new segment from rotation, and the rename in journal GC.
5. **`xtx.format` is durable before the first stamped byte** (§11.2): S1 of the
   first-ever XTx is preceded by "write file, fsync file, fsync dir".
6. **Close ordering (inv §11):** `Close` writes `index.json`/`sidx` only after
   the journal has been fsynced past every XTx whose entries those indexes
   cover; indexes never cover in-flight (undecided) runs (S5 is the only place
   they are applied).
7. Platform: `fsync` semantics are those of the existing `fsyncDir`/Windows
   variants. Storage that acknowledges `fsync` without durability is outside
   the guarantee, as it already is for single-collection writes.

### 6.4 In-process failure (process survives)

| Failure at | Handling | Outcome |
|---|---|---|
| S0 validation | nothing written; unlock | `ErrXTxConflict`/`ErrQuotaExceeded`/`ErrXTxTooLarge`/…, no state |
| S1 append error on participant *k* | `rollback(startSize)` (truncate) on participants 1..k under their held locks; indexes untouched; best-effort `ABORT` | aborted, retryable |
| S1 rollback truncate fails | that segment poisoned (`ErrSegmentPoisoned`); best-effort `ABORT`; presumed abort covers the rest | aborted, collection read-only until restart |
| S2 fsync error | §6.3 rule 3: poison, truncate best effort, best-effort `ABORT` | `ErrXTxDurability`, **aborted** |
| S3 append error | same as S2 (journal poisoned) | `ErrXTxDurability`, aborted-or-unknown (see below) |
| S4 fsync error | **outcome is unknown to this process** (the record may be durable). Journal poisoned, DB goes read-only for XTx; client gets `ErrXTxOutcomeUnknown`; truth is whatever recovery finds | resolved by restart + status (§8.2) |
| S5 apply error | impossible by construction (validated in S0); if an invariant breaks, the process **must crash** (`panic`) — continuing would leave the in-memory index disagreeing with a durable `COMMIT`; recovery redoes it | — |

`ABORT` is advisory: losing it never changes the outcome (presumed abort), it
only lets retirement and status skip a recovery pass.

---

## 7. Recovery

### 7.1 Where it runs (inv §3, §5)

`DB.Open` becomes two-phase. `OpenCollection` called **standalone** (inv §5,
`CreateCollection`, embedded paths) never trusts indexes in a database whose
`xtx.format` says XTx is enabled until the DB-level phase has produced a
decision table; it receives the table by argument. A standalone open with no
table in an XTx-enabled root fails with `ErrXTxRecoveryRequired`.

```
Phase 0  lockDir; read xtx.format (gate, §11.2); refuse if min_reader > this binary
Phase 1  journal: open, §4.3 torn-tail/corruption handling, build decision table
         {txid → commit | abort | retired(commit|abort)}, restore seq
Phase 2  for each collection (any order — I5): recoverCompaction, glob/sort
         segments, openActiveSegment (torn-line truncate)      [existing steps 1–2]
Phase 3  for each collection: scan the active-segment TAIL for stamped entries
         → set of (txid, run) candidates; also read the persisted
         xtx_applied set from index/sidx coverage (§7.3)
Phase 4  resolve (table below), cross-validate COMMITs against evidence (§4.4 step 1)
Phase 5  apply resolution: truncate dead tail runs, write missing ABORTs,
         invalidate distrusted index snapshots
Phase 6  for each collection: recoverIndex / sidx recovery / seedSealedCoverage
         using entryVisible(); id counter; encryption; goroutines  [existing 3–5]
Phase 7  checkpointer pass (§4.4); open for traffic
```

Phases 2–6 for different collections may run concurrently; Phase 4 is the
only cross-collection step and is single-threaded over an immutable view.

### 7.2 Truth table

`Journal` = what Phase 1 learned for the txid. `Evidence` = what Phases 2–3
found across the **participants named by the decision** (for `COMMIT`), or in
any collection (when there is no decision).

| # | Journal | Evidence (runs) | Outcome | Action |
|---|---|---|---|---|
| 1 | no record | none anywhere | nothing happened | — |
| 2 | no record | any subset of runs, complete or torn | **ABORT** (presumed) | truncate tail run(s); append `ABORT{why:recovery}`; fsync |
| 3 | `COMMIT` | every participant's run complete, digests match | **COMMITTED** | entries visible; indexes rebuilt/tail-replayed with them; nothing to write |
| 4 | `COMMIT` | every run complete; some entries already in sealed segments / de-stamped | COMMITTED | as 3, digest checked over what is stamped |
| 5 | `COMMIT` | a participant's run missing, short, or digest mismatch | **INTEGRITY FAILURE** | `ErrXTxIncomplete`; Open refuses; **never** commit a subset, never abort a decided tx; Verify/Repair finding (§10) |
| 6 | `COMMIT` | participant collection directory absent | **INTEGRITY FAILURE** | `xtx-missing-collection`, as 5 |
| 7 | `COMMIT` | stamped entry in a collection not in `parts` | INTEGRITY FAILURE | `xtx-foreign-run` |
| 8 | `ABORT` | any runs | **ABORTED** | skip entries; truncate if still at tail; no ack was ever sent |
| 9 | `RETIRE(commit)` | stamped or de-stamped | COMMITTED | stamped entries visible; no digest check |
| 10 | `RETIRE(abort)` | any stamped | ABORTED | skip; drop at next compaction |
| 11 | `COMMIT` and `ABORT` both | — | INTEGRITY FAILURE | `xtx-decision-conflict` (§4.3) |
| 12 | journal file absent/empty-with-header, `xtx.format` says enabled, **stamped entries exist** | stamped runs | **ErrXTxJournalMissing** | refuse; do **not** presume abort (the journal may have been lost, not the tx); operator uses `repair --xtx-presume-abort` (§10) |
| 13 | journal torn at tail (§4.3) | complete runs, `COMMIT` not durable | ABORTED (row 2) | — |

### 7.3 Index snapshots are checked against the journal

`index.json` and `sidx_*.json` today certify only "these bytes hash to X"
(`SegmentCoverage`, inv §4). That is not enough: coverage proves a prefix of
*bytes*, not the *decision* those bytes were folded under. New coverage records
carry, **when and only when stamped entries are inside the covered range**, a
field `xtx_applied` — the sorted list of non-retired txids whose entries the
snapshot folded as visible (plus `xtx_journal_epoch`). Absent field ⇒ the
snapshot contains no stamped data (every pre-XTx snapshot).

At Phase 4:

- every txid in `xtx_applied` must be `COMMIT` or `RETIRE(commit)` in the
  decision table, same journal epoch; otherwise that collection's index and
  sidx are **discarded and fully rebuilt** from segments under
  `entryVisible()`;
- a snapshot with no `xtx_applied` but covering stamped bytes that are
  committed (e.g. written by a pre-fence binary — impossible by I4, but cheap
  to check) is likewise rebuilt;
- a decided-commit tx whose entries the snapshot does *not* cover is simply
  tail-replayed (the existing incremental path).

The snapshot is never *promoted* to evidence of a commit; see §13.

---

## 8. Status, errors and idempotency contract

### 8.1 Commit outcomes (what a caller can observe)

| Result | Meaning | State on disk | Retry safe? |
|---|---|---|---|
| `OK{txid, ops[{collection,id,rev}]}` | committed, durable | COMMIT durable | n/a |
| `ErrXTxConflict` (expected_rev / unique) | rejected at S0 | none | yes, after re-reading |
| `ErrQuotaExceeded` | rejected at S0 | none | after freeing space |
| `ErrXTxTooLarge`, `ErrCollectionNotFound`, `ErrReadOnly` (follower) | rejected at S0 | none | no / elsewhere |
| `ErrXTxDurability` | aborted; a participant or the journal is poisoned | none visible | after restart |
| `ErrXTxOutcomeUnknown` | S4 failed or connection lost after S3 | **either** | **only via status/idempotency key** |
| `ErrXTxInProgress` | same key currently executing | — | wait |
| `ErrFormatTooNew` / `ErrXTxUnsupported` | binary/config cannot do XTx | none | no |

`ErrXTxOutcomeUnknown` is also what the *network* layer reports when the
connection drops after commit was submitted; it is the only ambiguous result.

### 8.2 Idempotency and status

- `Commit(..., key)`: `key` is a client-chosen opaque string (≤ 128 bytes).
  Servers are not required to accept keyless XTx for retried clients, but
  keyless is allowed (no dedupe).
- The `key` is stored in `COMMIT`/`ABORT`/`RETIRE` records, so it survives
  restart and failover (a follower journals it too, §12.2).
- `TxStatus(txid | key)` returns one of:

  | Status | Definition |
  |---|---|
  | `COMMITTED` | decision `COMMIT`/`RETIRE(commit)`; returns the original ops/revs |
  | `ABORTED` | decision `ABORT`/`RETIRE(abort)`, or recovery presumed abort |
  | `PENDING` | in progress in this process (staged-and-executing) |
  | `UNKNOWN` | no record **and** within the retention window — means "never reached S3 durably" **only if** the server has been restarted since the call was issued (otherwise `PENDING`); safe to retry |
  | `EXPIRED` | record garbage-collected (§4.5); idempotency can no longer be guaranteed |

- Retrying `Commit` with a key whose status is `COMMITTED` returns the original
  result without re-executing; `ABORTED`/`UNKNOWN` re-executes as a fresh XTx
  (new txid) — correct because aborted XTx has no visible effect.
- Retention window: `xtx_status_retention`, default 24 h, a hard floor above
  the maximum client retry horizon the API documents. Status for
  `EXPIRED` keys is the **only** place the contract degrades, and it degrades
  loudly (`EXPIRED`, not `UNKNOWN`).
- Staged-but-never-committed transactions have no durable existence: the idle
  sweeper (`--tx-timeout`) just burns reserved ids (inv §1).

---

## 9. Isolation and conflict semantics

### 9.1 Guarantees

- **Atomic and durable** (I1–I3).
- **Serializable write sets.** All participants are write-locked for the
  validate→apply span; XTx and single-collection writers to the same
  collection are totally ordered by that collection's `c.mu`; two XTx with
  overlapping participants are ordered by lock acquisition (deadlock-free by
  canonical order).
- **Per-collection read committed.** A reader of one collection sees a tx
  entirely or not at all (S5 happens under that collection's write lock).
- **Not guaranteed:** an atomic *multi-collection read*. A reader that reads A
  then B can see A-before and B-after. Offering that needs a `DB`-level read
  gate (RW lock taken shared by multi-collection readers, exclusive by S5); it
  is intentionally **not** in this protocol and, if added later, is
  in-memory-only and cannot affect the on-disk format.

### 9.2 Conflict detection

Validation happens at S0 under the locks, so it is exact (no TOCTOU between
check and write):

1. Update/delete targets exist and are live (as `CommitTx` today).
2. Optional per-op `expected_rev`: mismatch ⇒ `ErrXTxConflict` naming
   `{collection, id, expected, actual}`. This gives optimistic concurrency
   across collections.
3. Unique-index constraints — per collection, as today; there is no
   cross-collection constraint.
4. The same `(collection, id)` appearing twice in one XTx is rejected at stage
   time (`ErrXTxDuplicateOp`), keeping runs trivially order-independent.
5. TTL: an entry whose target has expired is "not found". The TTL reaper takes
   one `c.mu`, so it serializes against the XTx per collection as it does
   against `CommitTx`.

### 9.3 Interaction with other writers and watchers

- Watch events are emitted after the decision is durable and the locks are
  released, in participant order then op order (inv §12). A watcher never sees
  an event for a tx that later aborts.
- Quota is checked per participant in S0 *before the first append anywhere*.

---

## 10. Verify, Repair, salvage

New `Verify` finding codes (exported API ⇒ added to the code list; fixtures
regenerated via `go generate ./engine`, never hand-edited — inv §13.5):

| Code | Severity | Meaning |
|---|---|---|
| `xtx-undecided-run` | info (resolved at open) / warning (found by offline `VerifyDir`) | stamped run with no decision; will be presumed aborted |
| `xtx-participant-missing` | **corruption** | `COMMIT` whose participant run is short/missing (truth table row 5) |
| `xtx-digest-mismatch` | **corruption** | run present, ops differ from `COMMIT.parts` |
| `xtx-missing-collection` | **corruption** | row 6 |
| `xtx-foreign-run` | **corruption** | row 7 |
| `xtx-decision-conflict` | **corruption** | row 11 |
| `xtx-journal-corrupt` / `xtx-journal-missing` | **corruption** | §4.3 / row 12 |
| `xtx-index-mismatch` | warning | snapshot disagrees with journal; rebuilt (§7.3) |

`Finding.Location` gains an optional `Tx string`. Cross-collection verify takes
`DB.mu.RLock` then the participants' `c.mu` in canonical order (§6.1 rule 4),
never an arbitrary order.

Repair policy:

1. **Never fabricate a commit** (I6). Salvaged entries from a quarantined
   segment are re-serialized as **plain entries only when their tx decision is
   `COMMIT`/`RETIRE(commit)` and that decision is intact**; salvaged stamped
   entries of an aborted/undecided tx are dropped and reported.
2. A `COMMIT` whose participant evidence was quarantined stays *committed*;
   Repair reports the lost ops (`xtx-participant-missing`) and, with explicit
   operator consent, writes a `RETIRE(commit)` so the DB opens — losing that
   participant's ops is a **reported data loss**, not a silent one.
3. Journal-level damage (`xtx-journal-corrupt`/`-missing`) has exactly one
   automatic path: `repair --xtx-presume-abort`, which records the decision in
   `REPAIR_JOURNAL.json` (the existing DB-level journal is extended with an
   `xtx` step), takes the verified backup first, and for every undecided stamped
   run writes `ABORT`. It cannot be the default and prints the list of txids
   that will vanish.
4. `Repair` renames/rewrites segments (`writeSalvageSegment`, inv §8): since
   stamps contain no offsets or segment names, runs survive renumbering; a
   run split across the salvage boundary is by definition incomplete and is
   handled by rows 2 / 5.

---

## 11. Version gate and compatibility policy

### 11.1 Threat model

Old binaries read stamped segments through *four* paths that all fail open
today (inv §3, §6): strict replay accepts unknown ops; `Decode` ignores
unknown JSON keys; compaction keeps unknown-op entries and can shadow records;
`persistMeta` drops unknown fields. The fence must work even though the old
code cannot be changed.

### 11.2 Three independent layers

| Layer | Mechanism | Catches |
|---|---|---|
| **L1 — entry fence** | v2 checksum (§3.2): every old binary computes a different CRC for every stamped line → `ErrCorruptEntry` at open/scan/compaction/salvage | pre-fence binaries, any tool that re-reads segments |
| **L2 — format file** | `xtx.format`: `{"format":"scriva-xtx","min_reader":1,"features":["journal","stamped-v2"],"created_by":"v1.x.y","crc":N}` | fence-and-later binaries get a precise `ErrFormatTooNew` *before* touching segments; humans get an explanation |
| **L3 — wire fence** | replication handshake capability (§12.2) | follower on an older binary being fed XTx groups |

L1 is the only layer that works on binaries already in the wild, and it is
deliberately an *accident-proof* mechanism (a deterministic checksum mismatch
on a complete line), not a "format version" field that can be ignored.

**Known residual (documented, accepted):** a pre-fence binary opening an
XTx-enabled root fails closed with `ErrIntegrity` (corrupt entry). If an
operator then runs that old binary's `Repair`, it will quarantine the stamped
segments. Mitigations: `Repair` takes a verified backup first; the fence
release makes `Repair` check `xtx.format` and refuse; release notes mark
"downgrade below the fence is unsupported once XTx is enabled".

### 11.3 Release sequence

| Release | Behaviour | Writes XTx bytes? |
|---|---|---|
| **R0 — fence** | Understands v2 entries and `xtx.*` names; `Open`/`Verify`/`Repair`/`SnapshotTo` handle reserved names; `CreateCollection` rejects `xtx.`; refuses `xtx.format` with `min_reader` > its own (`ErrFormatTooNew`); refuses stamped entries / journal it cannot interpret (`ErrXTxUnsupported`, distinct from `wrong-op` corruption); replication refuses unknown ops explicitly (§12.2). Full decision/recovery code exists but is unreachable (no writer). | **No** |
| **R1 — writer, off** | R0 + XTx engine API + server RPCs behind `--enable-xtx` (default **off**). First XTx writes `xtx.format` then journal then stamped runs. | Yes, opt-in |
| **R2 — default on** | Flag default on only after R1 has soaked; no format change. | Yes |

R1 must not ship until R0 has been out for at least one minor release
(operators downgrade between adjacent releases; the fence must exist on the
version they land on). The order within R1's first XTx is fixed:
`xtx.format` (file+dir fsync) → `xtx.journal` (file+dir fsync) → stamped runs.

### 11.4 Compatibility matrix

| Reader ↓ / data → | no XTx ever | XTx-enabled, drained | XTx-enabled, live stamped |
|---|---|---|---|
| pre-fence | ✔ | ✔ (no `xtx.*` seen by it; byte-identical segments if §11.5 drained) | ✘ fail closed (L1) |
| R0 | ✔ | ✔ | ✘ `ErrXTxUnsupported` (understands, never writes or applies) |
| R1 / R2 | ✔ | ✔ | ✔ |

### 11.5 Disabling / downgrade path

`scriva-cli xtx drain` (offline): runs recovery, writes `RETIRE` for all,
forces compaction to de-stamp every retained entry, verifies no stamped entry
remains in any collection, deletes `xtx.journal`, then `xtx.format` last (each
with directory fsync). After it, every collection is byte-format-identical to a
pre-XTx database. Not drainable while any run is undecided; `drain` runs
recovery first so that cannot persist.

### 11.6 Compaction, TTL, encryption

1. `resolveEntries` keeps stamped entries of non-retired txs verbatim (not
   merged away, not dropped even if superseded) — otherwise `d` could never be
   recomputed.
2. After `RETIRE`, committed entries are de-stamped (§4.4) with a **v1**
   checksum over the re-sealed data; aborted entries are dropped.
3. `MigrateNow` re-encryption rewrites `data` and `epoch` only; the stamp and
   its v2 CRC are *recomputed* over the new data (the CRC covers data), and `d`
   is unaffected by construction (§4.2).
4. TTL expiry applies to entries after the decision, independent of the tx.

---

## 12. Replication, snapshot, backup

### 12.1 Leader

- A leader publishes a tx to the broker **only after S4**, as one contiguous
  group: its stamped entries (in participant order), then one `TX_COMMIT`
  record carrying `{txid, key, parts}`. S5 holds all `c.mu`s while
  publishing, so no other entry interleaves and LSNs are contiguous.
- Aborted XTx never replicate (nothing was published before S4), so followers
  need no `ABORT` stream.
- Per-entry LSN assignment and the ring are unchanged; the ring-size limit
  of §5.3 guarantees a whole group is serveable.

### 12.2 Follower

- Capability handshake: `ReplicateRequest` gains `supports_xtx` (proto change,
  gated by the same fence; R0 adds the field and the leader-side check, R1
  starts sending groups). A leader that has XTx in its tail range and a
  non-supporting follower fails the subscription with `FAILED_PRECONDITION`
  rather than flattening or dropping markers.
- A follower is **its own coordinator** for its own durability: on receiving a
  group it appends the stamped entries (same bytes, same `txid`), fsyncs,
  writes its own `COMMIT` to its own journal (same `key`), fsyncs, then applies
  to indexes atomically under the participants' locks and **only then**
  advances `SetAppliedLSN` — to the group's last LSN, **not** per entry
  (inv §10: today it advances per entry).
- A follower crash mid-group leaves undecided runs ⇒ presumed abort at its
  recovery ⇒ the group is re-requested from the unchanged applied LSN and
  re-applied. Re-apply is idempotent by `txid` (journal lookup) before
  `rev`.
- Followers serve reads and Watch only from applied (decided) state, so a
  partial group is never visible.
- Promotion: a follower's journal is complete for everything it applied; no
  extra step. The new leader's `seq` continues from its own journal.

### 12.3 Bootstrap / snapshot (`SnapshotTo`, inv §7)

Snapshot is **not** made globally point-in-time; it is made XTx-*atomic*, using
capture order instead of a global barrier:

1. read replication LSN (`Bootstrap` already does);
2. take `DB.mu.RLock`; **capture `xtx.journal` first** (copy under
   `journal.mu`, brief);
3. then copy collections one at a time under their own `c.mu.RLock`, as today.

Why this is consistent: a participant's run is always fsynced **before** its
`COMMIT` is appended (S2 < S3). A `COMMIT` captured at time *t_j* therefore has
all participant data durable before *t_j* ≤ every later collection capture, so
the copied segments contain the whole run. A tx committed *after* *t_j* has no
decision in the copied journal ⇒ presumed abort in **every** participant ⇒
all-or-nothing is preserved even though participants were copied at different
instants. The restore needs no roll-forward: normal open + recovery is the sole
authority (the hazard noted for `compact.manifest` in inv §7 does not arise,
because stamps/journal contain no offsets or paths).

Consequences (documented as guarantees *and* non-guarantees):

- ✔ A restored snapshot never contains half a tx, and never invents one.
- ✘ It is not a cross-collection point-in-time cut of **non-tx** writes
  (unchanged from today). A write made after *t_j* that depended on a tx the
  snapshot dropped can appear without it. A "strict" snapshot that holds a
  DB-wide write barrier for the whole copy is a possible future option; it
  would cost write availability and is not part of this protocol.
- Bootstrap watermark rule becomes safe: LSN read ≤ journal capture ≤
  collection copies, so every group whose `TX_COMMIT` LSN ≤ watermark has its
  `COMMIT` in the captured journal; groups beyond it are streamed (their
  stamped entries re-delivered, deduplicated by `txid`). The inv §10.5 hazard
  ("snapshot has half a group but the LSN says applied") cannot occur because
  an LSN-covered group always has its decision captured.
- `extractSnapshot` accepts root-level regular files `xtx.journal` and
  `xtx.format` (it already writes any `TypeReg` under dataDir). The reserved
  path rule (§2.1) ensures they are never written as directories.
- Filesystem-level backups (rsync, LVM) are covered by the same argument
  provided the journal is copied **before** the collections; a backup that
  copies it last can contain a `COMMIT` without participant data **only if**
  the collection copy predates the prepare — surfaced as truth-table row 5,
  never silently. Runbooks must state the order.

---

## 13. Why the root `COMMIT` is authoritative, and index snapshots are not

**Authoritative `COMMIT`.**

1. *One atomic point.* A decision spread over N participant files is N facts
   that crash, copy, corrupt and repair independently; an agreement protocol
   between them is exactly the two-phase commit we scoped out. One record, one
   `write` + one `fsync`, is the only place atomicity across files can come
   from on a single node.
2. *Presumed abort needs an asymmetric record.* Because "no record" means
   "abort", only the *existence* of the commit record matters; all failure
   modes of a missing record are safe (never acked). The converse failure — a
   *lost durable* record — is detectable (§4.3 corruption vs tear, §7.2 row 12)
   and refused rather than guessed.
3. *Ack ordering gives the authority real meaning.* The client's knowledge
   (ack) and the disk (`COMMIT`) are tied by S4 < S7; any client-visible fact
   is implied by the record.
4. *Self-describing.* `parts` + `d` let recovery prove the evidence is whole
   before committing it (rows 3 vs 5).

**Index snapshots are derived, non-authoritative state.**

1. An index snapshot is a pure fold of segment bytes under *some* decision
   table at *some* time (inv §4). It is a performance cache whose only claim is
   "the covered bytes hash to X" — it says nothing about whether a stamped
   entry in those bytes was ever committed.
2. It can be **ahead** of the journal after anything that restores or replaces
   the journal independently (older backup of `xtx.journal`, repair
   `--xtx-presume-abort`, a journal truncated by fault): the index says
   "applied", the journal says "no commit". If the index won, an unacked or
   operator-aborted run would resurrect. Therefore the index is checked
   against the journal (§7.3) and discarded on any disagreement.
3. It can be **behind**: crash after S4, before the next persist. Tail replay
   under `entryVisible()` reaches the same state, so no information in the
   snapshot is needed to commit.
4. It is rebuildable at any time from segments + journal; the opposite is not
   true, so it must never be the tie-breaker. (Same stance the repo already
   takes with `index.json`/`sidx`: coverage mismatch ⇒ rebuild.)
5. It is per-collection and so *cannot even express* a cross-collection
   decision.

---

## 14. Test and review gates (required before any format byte ships)

All hermetic (`t.TempDir()`), `-race`, CI-default; soak variants behind env
vars like `SCRIVA_MODEL_*`.

1. **Crash matrix** (extends `engine/crash_matrix_test.go`, fault-injecting
   FS): crash at every boundary S0…S7 plus *inside* S1/S2/S3/S4 (short and
   torn writes); assert I1–I3 and the truth table row for each point
   (`TestXTxCrashMatrix_*`).
2. **Kill-9** (`TestKill9_XTx*`): re-exec'd child killed mid-XTx; recovered
   state == set of acked txids; replay with `SCRIVA_KILL_SEED`.
3. **Model harness**: add `xtx` ops (commit, commit-with-injected-failure,
   crash-reopen mid-XTx, snapshot-restore, repair no-op) to `TestModel`'s
   in-memory model.
4. **Truth-table unit tests**, one per row of §7.2, with synthetic
   directories in `engine/testdata/integrity/` generated by `go generate`.
5. **Fence tests**: build the *pre-fence* tag's `store.Decode` and
   `engine.Open` against a stamped fixture ⇒ must return
   `ErrCorruptEntry`/`ErrIntegrity`, never data; an R0 binary against a
   `min_reader:99` `xtx.format` ⇒ `ErrFormatTooNew`.
6. **Single-path predicate test:** a static test (analyzer or grep gate) that
   `entryVisible` is the only caller deciding stamped visibility in
   `index.go`, `integrity_policy.go`, sidx rebuild, `verify`, `repair`.
7. **Lock-order test:** XTx with participants in staging orders {a,b},{b,a} and
   a concurrent single-collection writer, `CollectionWithConfig`, drop and
   compaction under `-race` — no deadlock, no use-after-close.
8. **Replication:** follower killed mid-group re-applies idempotently;
   bootstrap with concurrent XTx never yields a partial group; group > ring is
   refused at config time.
9. **Snapshot:** concurrent XTx during `SnapshotTo`; restore is
   all-or-nothing in every participant.
10. **Indexes:** corrupt/ahead/behind `index.json`+`sidx` vs journal ⇒ rebuilt
    (`xtx-index-mismatch`).
11. **No-XTx byte-identity:** a workload with `--enable-xtx` off (and one that
    never calls XTx with it on) produces byte-identical files to `main`.
12. **Metrics:** counter + histogram for XTx commit, recovery, retirement via
    `engine.CollectionConfig`/DB hooks, never direct `metrics.*` in the engine
    (CLAUDE.md).

---

## 15. Open questions resolved here, and what remains

| inv §15 question | Decision |
|---|---|
| Evidence location | Payload in participant segments (stamped); decision in root file `xtx.journal`; reserved `xtx.` file names, fenced in R0 (§2) |
| Redo vs undo | **Redo-only.** Compaction drops before-images (inv §6.3); dead entries are skipped, never "reverted" (§3.3) |
| `SyncModeNone` participants | XTx forces S2/S4 fsync regardless of mode (§6.3) |
| Max participants/ops/bytes | 16 / 1 000 / 64 MiB, ring ≥ 2 × ops (§5.3) |
| Where tx API lives | `engine.DB` owns the XTx API and journal; the server's `TxManager` stays a staging layer that calls it, and the embedded façade can expose the same API (§0, to be wired in the API stage) |
| Idle expiry / `ReserveID` | Unchanged; ids provisional until ack (§5.2) |

**Remaining for the implementation plan (not decided here):** exact proto
shapes and RPC names for XTx begin/stage/commit/status; whether embedded mode
exposes XTx in R1 or R2; the `xtx.journal` group-commit batching window;
checkpointer cadence and `xtx_status_retention` defaults; whether to add the
optional multi-collection read gate (§9.1).

## 16. Review checklist (inventory §14 → this document)

| Inventory row | Resolved in |
|---|---|
| 1 Write path | §6 |
| 2 Entry / checksum / ops | §3, §11.2 |
| 3 Open / recovery hook | §7 |
| 4 Index folds + coverage | §3.3, §7.3, §13 |
| 5 Compaction retention / marker id space | §3.1, §4.4, §11.6 |
| 6 Snapshot | §12.3 |
| 7 Verify / repair / salvage | §10 |
| 8 Replication | §12.1–12.2 |
| 9 Locks | §6.1 |
| 10 Durability | §6.3 |
| 11 Server/API | §15 (shapes deferred) |
| 12 Metrics | §14.12 |
