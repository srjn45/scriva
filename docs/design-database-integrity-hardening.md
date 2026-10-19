# Database integrity and security hardening — technical specification

**Status:** ratified design. Every decision in §6 is resolved; **no implementation is claimed by this document** — each contract below describes the target, and a "Today" statement describes verified current behavior at `main` = `f1c32a0`. Implementation completion is tracked separately (§7).
**Input review:** [Intensive database review, 2026-10-11](reviews/2026-10-11-intensive-database-review.md).
**Ratification record (detail, rationale and full acceptance lists):** [evidence recheck](reviews/ratification/evidence.md), [security contracts](reviews/ratification/security-contracts.md) (decisions D1–D5, cited here as *SC-D1…SC-D5*), [storage and read contracts](reviews/ratification/storage-read-contracts.md) (decisions D1–D7, cited here as *SR-D1…SR-D7*).
**Related design:** [Cross-collection transaction protocol](design-cross-collection-transactions.md), [XTx fault boundaries](design-xtx-fault-boundaries.md), [architecture](architecture.md).

## 1. Purpose and outcome

This specification turns the review findings into implementable contracts. The goal is that malformed names, concurrency, process crashes, and storage errors cannot silently cause path escape, partial batches, stale indexes, lost authorization boundaries, or misleading success/failure results.

The implementation is staged. Until the relevant stage lands, existing behavior must not be described as satisfying the stronger contract. Passing unit/race tests is not a substitute for crash-boundary tests, and a returned I/O error is not proof that an operation was absent.

Where more than one viable design existed, §2–§3 record the decision, the alternatives rejected, the compatibility impact, the lock order where it matters, and testable acceptance criteria. Format changes, compatibility behavior and externally observable error semantics are release-level decisions; they are settled here, not to be re-opened while implementing a local fix. A change to any decision must update §6 and the affected acceptance tests first.

### 1.1 Threat and failure model

The design covers malformed/untrusted API input; an authenticated client exceeding its granted resource scope; concurrent goroutines in one process; abrupt process termination at any storage operation; torn writes; ordinary filesystem errors including short write, failed sync, failed truncate, failed rename, and failed directory sync; stale or corrupt derived indexes; and backup/restore or replication cuts that omit or duplicate data.

It assumes a single ScrivaDB process owns one data root under the existing directory lock and relies on the documented local-filesystem contract for atomic rename and honest `fsync`. It does not promise safety on a filesystem that lies about completed syncs, prevent a privileged local user from rewriting files, provide multi-process shared-directory access, or provide distributed consensus. If authenticated encryption is enabled, missing keys remain an operator/configuration failure; the engine must not silently interpret ciphertext as plaintext.

### 1.2 Review finding identifiers, evidence labels and code anchors

These identifiers are stable references for design discussions and implementation plans. Paths are anchors, not a claim that line numbers remain fixed. **Evidence labels** come from the [evidence recheck](reviews/ratification/evidence.md) at `f1c32a0`: **CONFIRMED** = reproduced or traced in code as stated; **UNCONFIRMED RISK** = plausible from code shape, not reproduced. "Confirmed" does not mean every deployment is exposed; H2 and H12 are confirmed *code facts* whose severity is deployment-dependent.

| ID | Finding | Evidence label | Primary code/document anchors |
|---|---|---|---|
| H1 | Collection-name path traversal / unsafe recursive drop | CONFIRMED (reproduced) | `server/grpc.go` collection RPCs; `engine/collection.go:OpenCollection`; `engine/db.go:DropCollection` |
| H2 | All-interface listeners and no-auth empty-key mode by default | CONFIRMED (code trace; documented behavior) | `server/config.go:DefaultConfig`; `internal/auth/apikey.go`; `cmd/scriva/main.go` |
| H3 | Scoped credentials do not cover every resource shape | CONFIRMED (reproduced at the interceptor; end-to-end snapshot stream not run) | `internal/auth/apikey.go:checkCollectionAccess`; `internal/auth/scope.go`; collection create/drop/list and Snapshot RPCs |
| H4 | `InsertMany` / `CommitTx` crash-prefix visibility | CONFIRMED (reproduced via crash injection) | `engine/collection.go` batch commit paths; `engine/segment.go` partial-line recovery; `docs/architecture.md` |
| H5 | Failed `SyncModeAlways` sync leaves a single-record or reaper mutation visible locally but unpublished | CONFIRMED for `Insert` (reproduced); `Update`/`Delete`/CAS/`Upsert`/`reapExpired` by code trace only until tests exist. `InsertMany`/`CommitTx` already roll back on sync failure (but stay crash-prefix-visible, H4) | `insert`/`update`/`Delete`/CAS in `engine/collection.go`, `engine/keys.go`, `engine/ttl.go` |
| H6 | Dropping an XTx participant makes the database fail to reopen | CONFIRMED (reproduced); interim fail-safe shipped (refuse drop), retirement-at-drop deferred | `engine/db.go:DropCollection`; `engine/xtx_recovery.go`; XTx §7.2 |
| H7 | Stale metadata persist can undo encryption policy, and a shared `meta.json.tmp` corrupts `meta.json` under concurrent persists; missing/corrupt metadata then fails open | CONFIRMED (reproduced both the ordered schedule and natural concurrency: 2 lost blocks and 19 corrupt files in 300 runs on one machine — not a production rate) | `engine/ttl.go:setDefaultTTL`; `engine/encryption_admin.go`; `engine/meta.go`; `engine/fsync.go`; `engine/encryption.go:initEncryption` |
| H8 | Live scan can omit a record that was updated across a rotation | CONFIRMED (reproduced deterministically with `scanHook`). Duplicate emission and "writes after start visible in some segments" are UNCONFIRMED RISK (code shape only) | `engine/scan.go:streamLive`; compaction/layout lease code |
| H9 | Seal/rotation error leaves the active append target unusable and reports failure for a committed write | CONFIRMED (reproduced) | `engine/collection.go:rotateSegment`; `engine/segment.go:Seal` |
| H10 | `Update` drops reserved `_key`, making the key reusable | CONFIRMED (reproduced for `Update`); staged XTx updates by code trace only | `engine/keys.go`; `engine/collection.go:Update`; `engine/xtx_handle.go` |
| H11 | `Count` and `Scan` disagree on expired-but-unreaped records | CONFIRMED for `Count(nil)` (reproduced). Secondary-index equality and range `Count` fast paths: **UNCONFIRMED RISK** until covered by a test | `engine/count.go`; `engine/scan.go`; TTL visibility/reaper code |
| H12 | Replication TLS, archive extraction limits, file/socket permissions, metrics, CORS, HTTP timeouts | CONFIRMED per sub-item by code trace (no runtime exercise); severity topology-dependent | `cmd/scriva/main.go`; `server/replication.go:extractSnapshot`; `server/config.go`; `server/rest.go` |
| H13 | Makefile bench/fuzz paths point to a missing package | CONFIRMED (reproduced). Whether the referenced fuzz/benchmark functions exist under `./engine` is not yet checked | `Makefile` bench/fuzz targets; `go list ./internal/engine` |

Composition: H1 and H3 compose (a scoped key can create `../x` and drop it); H6 is reachable by any key allowed to call `DropCollection`, which today is every key. No severity was downgraded by the recheck. The H7 and H8 interleavings are no longer static-schedule risks: their reproductions are the **first regression tests** (§4), and the minimal-fix choice they inform is made in §3.4 and §3.5.

## 2. Normative contracts

### 2.1 Names and filesystem containment

Decision source: SC-D1, SC-D2.

1. **One validator.** `engine.ValidateCollectionName(name) error` (typed `ErrInvalidName`, wrapping `ErrReservedName` for reserved cases) is the only name check. It is called by `OpenCollection`, `CreateCollection*`, `CollectionWithConfig`, `DropCollection`, `Collection()`, XTx begin/stage participant resolution and follower `ApplyReplication`. gRPC maps it to `InvalidArgument`; REST and CLI inherit that. No layer keeps a private check.
2. **Grammar for new names:** `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` — ASCII only, 1–64 bytes, alphanumeric first character, no dots, whitespace or non-ASCII.
3. **Case:** names are case-preserving and compared exactly, but **uniqueness is case-insensitive (ASCII fold)**: creating `Users` while `users` exists returns `AlreadyExists`.
4. **Unicode:** non-ASCII, invalid UTF-8, NUL, controls and bidi are **rejected, never normalized**; two distinct inputs must never alias one on-disk collection.
5. **Reserved (case-insensitive):** prefix `xtx.`; exact names `xtx`, `collections`, `replication`; Windows device names (`con prn aux nul com1–9 lpt1–9`); the engine's root file names (`LOCK`, `xtx.journal`, `xtx.journal.tmp`, `xtx.format`). Collision of `collections`/`replication` with REST route segments is a precaution, *not verified*.
6. **Legacy names (compatibility option (b) chosen):** a directory already on disk whose name fails 2–5, but is a single path component, not a symlink and not `xtx.`-prefixed, is *legacy-named*. It **opens, reads, writes, indexes, watches and participates in XTx** normally (one `WARN` per name, gauge `scriva_legacy_collection_names`); `CreateCollection` of that name is `InvalidArgument` (policy checked before existence); `DropCollection` is allowed for database-level admin through the containment helper; no rename RPC exists (migration is an explicit offline operator command, never automatic). Two legacy names equal under case-fold both load with a warning; a third fold-equal create is refused. Entries that are symlinks or not single components are skipped at discovery (`ERROR` log, `scriva_skipped_root_entries`) and never opened, created over or dropped.
7. **Config-supplied names** (quotas, default TTL, indexes, key allow-lists) use the strict grammar for new entries but accept names matching an existing legacy directory with a warning. A literal `"*"` allow-list entry warns in release N and is rejected from N+1 (today it silently matches nothing).
8. **Containment.** The root is made absolute and `EvalSymlinks`-resolved once in `Open`; the resolved path is the only join base (a symlinked root is allowed, everything below it must be real). Collection directories must be real directories: `Lstat` at discovery, create, open and drop; a symlink or non-directory refuses with `ErrUnsafePath`; creation uses `Mkdir`, not `MkdirAll`. The guarantee is that no component below the root is resolved through a symlink/reparse point, including between validation and use (`os.Root`/`openat`-style resolution is the suggested mechanism, `go 1.24`, but the mechanism is the implementer's). **Drop** = validate name → `Lstat` target is a non-symlink directory whose parent is the resolved root → remove; embedded and repair callers use the same helper; there is no force flag. A lexical `Join`/`Clean` check alone is not sufficient.
9. **Export:** only `Lstat`-checked regular files from an allow-list of engine file kinds (segments, secondary-index files, `meta`, XTx coordinator files, and the encryption sentinel of §3.4); symlinks and unknown files are skipped with a counted `WARN` and never followed.
10. **Import / restore / follower bootstrap:** extract into a new `0700` temp directory beside the target (same filesystem), never into a live root; accept only regular files at depth ≤ 2 whose names pass the allow-list and D1 (legacy minimum) rules, no duplicates; reject symlink, hardlink, device, FIFO, `..`, absolute and unknown-root entries **with an error** (not `continue`). Defaults: ≤ 1,048,576 entries, per-file ≤ 8 GiB and exactly the header size, total declared bytes ≤ free space minus 5%. Remove the temp directory on any error; promote by `rename` then parent `fsync`; stale temp directories are removed at next start. Newly created files `0600`, directories `0700`; pre-existing files are never chmod'ed.

**Rejected:** lowercase-only names; dots/Unicode with NFC normalization; auto-rename or auto-migrate at open; refusing to open DBs containing legacy names; server-only validation (the embedded façade and engine are public API); lexical-only containment; `EvalSymlinks`-per-use as the guarantee (check-then-use race); following symlinked collection dirs under an allow policy; in-place extraction with rollback.

**Compatibility.** New creates with spaces, dots, non-ASCII or > 64 bytes start failing with `InvalidArgument` (some of those forms escaped the root). Existing data is unaffected. Clients creating `collections`/`xtx` named collections break (intended). Deployments that symlink an individual collection directory into the root: opening it by name now refuses with `ErrUnsafePath`. Exported backups stay restorable by older binaries; restores of archives with extra top-level files now fail loudly.

**Rollout:** all paths in release N, no flag — the escape is in Open/Drop, which a flag cannot gate.

**Acceptance** (full lists: SC-D1 N-1…N-5, SC-D2 C-1…C-6): hostile-name corpus (`""`, `.`, `..`, `a/b`, `a\b`, `../x`, `/abs`, `C:\x`, `\\h\s`, NUL, `a b`, `é`, NFC/NFD pair, 65 bytes, `xtx`, `xtx.foo`, `CON`, `nul`, `collections`, a case-fold pair) rejected at engine, gRPC, REST, CLI and embedded entry points with the directory tree inside **and outside** the root byte-identical; legacy fixtures (`legacy name`, `Ünï`, `a.b`) open and operate, create is refused, drop removes only that directory; a symlink swapped in between `Open` and `Drop` returns `ErrUnsafePath`; hostile-archive table leaves the live root byte-identical and no temp directory; kill mid-extraction leaves only a stale temp directory.

### 2.2 Authorization and network exposure

Decision source: SC-D3, SC-D4, SC-D5.

**Capability model.** A principal keeps today's *scope* (`read` | `read-write`) and optional *collection allow-list*; no new config key or scope value is added in N. From these:

| Capability | Resource | Held by |
|---|---|---|
| `read(C)` | collection `C` | any principal; with an allow-list, only for `C` in it |
| `write(C)` | collection `C` | scope `read-write`, and `C` in the allow-list if present |
| `db:read` | whole database | scope `read`/`read-write` **without** an allow-list |
| `db:admin` | whole database | scope `read-write` **without** an allow-list (so every existing unrestricted key and mTLS principal keeps full access) |

Having an allow-list removes all `db:*` capabilities — that is the whole change.

**Interceptor rules.** (a) Every full method name appears in one explicit classification table generated from the service descriptor; a CI test fails when a method has no entry, and an unclassified method is denied to everyone except `db:admin`. (b) The resource extractor reads **all** collection-bearing fields (`collection`, `collections`, `name`). (c) A multi-collection request is all-or-nothing and never reveals whether a *different* collection exists. (d) A scoped principal asking for an out-of-list collection always gets `PermissionDenied`, regardless of existence (no existence oracle). (e) REST routes pass through the same gRPC interceptors; a route that bypasses them is a bug.

**RPC classification (all RPCs in `proto/scriva.proto`; the generated table in the repository is authoritative):**

| RPC | Required capability | Note |
|---|---|---|
| `CreateCollection` | `db:admin`, **or** `write(name)` when `name` is literally in the allow-list | gap today (H3) |
| `DropCollection` | `db:admin` only | destructive; XTx-evidence interaction (§3.3) |
| `ListCollections` | `db:read` → all; scoped → **filtered** to allow-list ∩ existing, sorted; `xtx.*` never listed | no existence oracle |
| `Insert`, `InsertMany`, `Update`, `Delete`, `Upsert`, `UpdateByKey`, `DeleteByKey`, `UpdateIfRev`, `EnsureIndex`, `DropIndex`, `Compact`, `BeginTx` | `write(C)` | `Upsert`/`*ByKey`/`UpdateIfRev` classified explicitly (today unknown→write) |
| `FindById`, `Find`, `FindByKey`, `Aggregate`, `Watch`, `CollectionStats`, `ListIndexes` | `read(C)` | `FindByKey`/`Aggregate` become explicit reads |
| `CommitTx`, `RollbackTx`, `CommitXTx`, `RollbackXTx` | `write` + caller must own the handle | handle-owner binding is new |
| `BeginXTx` | `write(C)` for **every** C | already enforced |
| `StageXTx`, `GetXTx` | `write(C)` + owns handle + `C` is a participant | `GetXTx` stays write-scoped |
| `XTxStatus` | any authenticated principal | response carries only status + `tx_id` |
| `Snapshot`, `Replicate`, `ReplicationStatus` | `db:read` | gap today; scoped → `PermissionDenied` |
| `Promote` | `db:admin` | gap today |
| gRPC health | probe rule (§3.8) | classification of the health service under the interceptors is *not verified* — see §6 pre-implementation checks |

Non-RPC surfaces: `/healthz`/`/readyz` unauthenticated and carry no collection names or error detail; `/metrics` loopback by default, and if bound non-loopback requires `db:read` credentials; the Unix socket uses the same interceptor chain with filesystem permissions as the only authentication when keys are disabled; a SIGHUP key reload that would take the key set from non-empty to empty while any non-loopback listener is bound is **rejected** and the old set kept.

**`ListCollections` / `Snapshot`.** Scoped principals receive the filtered list. `Snapshot`, `Replicate` and `ReplicationStatus` are `db:read`-only: **in release N a scoped principal gets `PermissionDenied`, with no bytes sent.** Scoped partial export is designed but *deliberately deferred* (§6, row "Scoped partial `Snapshot`"): `SnapshotRequest` gains wire-compatible `repeated string collections = 1`, the archive carries a manifest (`kind: partial`), omits XTx coordinator files, is not a valid follower bootstrap, and is rejected by import unless an explicit partial-restore flag is passed.

**Rejected:** prefix/namespace create grants; a third `admin` scope value now; scoped keys dropping their own collection; denying `ListCollections` to scoped keys; returning the unfiltered list; scoped Snapshot that includes the journal; a transition flag restoring the holes (`legacy_scoped_admin` — it would preserve the vulnerability exactly for the keys meant to be confined); proto-annotation-driven policy (deferred, not rejected: the generated table plus CI completeness test achieves the same fail-closed property).

**Compatibility.** Breaking by design for scoped keys that relied on the holes (create/drop of non-listed names, `Snapshot`, `Replicate`, `Promote`, `ReplicationStatus`, unfiltered list). Unrestricted keys, the legacy `--api-key` and mTLS principals are unchanged; `read` keys gain `FindByKey`/`Aggregate`; callers committing a handle created by a different principal start failing. Handle-ownership binding ships in N regardless of handle-id entropy.

**Acceptance** (full lists: SC-D3 A-1…A-9, SC-D4 L-1…L-4): descriptor completeness test (a synthetic extra method is denied to all but `db:admin`); matrix of principals {unrestricted admin, `read`, `read-write`, `read`+list, `read-write`+list, anonymous with auth on, bad key, mTLS} × every RPC asserting **status and absence of out-of-scope data/names**; `read-write`+`[a]`: create `a` OK, create `b` denied with no directory, drop/Snapshot/Replicate/Promote/ReplicationStatus denied; `BeginXTx{[a,b]}` for `[a]` denied with no handle and no journal write; cross-principal handle use denied while the owner can still use it; `PermissionDenied` (not `NotFound`) for out-of-list names whether or not they exist; reload-to-zero-keys rejected; REST matrix equivalent; `/metrics` 401/200/403 by credential.

### 2.3 Durable write outcomes

Decision source: SR-D1 (single record), SR-D2 (batches).

1. Every operation that reports success is recoverable according to its configured durability mode. Under `SyncModeAlways`, success means the required data and decision records completed the defined file and directory sync sequence. For `none`/`interval`, a successful operation may be acknowledged before stable-storage sync by contract; the power-loss window is documented.
2. **Outcome taxonomy.**

| Outcome | Entered when | Engine state | Caller sees | Retry |
|---|---|---|---|---|
| **Durable** | `nil`: append done and, under `always`, `fsync` succeeded | normal | success | n/a |
| **NotApplied** | rejected before any byte; or `write` failed **and** truncate-to-previous-size succeeded; or a batch aborted before its decision | unchanged, **not** fenced | the existing error (`ErrDuplicateKey`, `ErrQuotaExceeded`, …) | safe |
| **Unknown** | `fsync` of the active segment failed (foreground or `syncLoop`); `Seal` sync failed; truncate failed (segment poisoned); batch decision fsync failed (`ErrXTxOutcomeUnknown`) | collection **fenced** | `ErrWriteOutcomeUnknown` wrapping the cause | **not safe** unless idempotent by construction |
| **Durable + ancillary failure** | commit point passed; then rotation, meta persist or event delivery failed | normal, except a failed `Seal` sync fences the *next* write | success plus typed `*PostCommitError` (`errors.Is(err, ErrPostCommit)`); gRPC OK with trailer `scriva-post-commit-warning` | n/a |

3. **Fail-stop.** After any `Sync` error the engine never retries `fsync` and never reports NotApplied (a later successful `fsync` or `ftruncate` proves nothing about earlier bytes). It **fences the collection** with an atomic first-cause-wins `fenced` pointer checked at the top of every write **and read**; `Close`, `Verify` and `Stats` remain allowed, Snapshot of a fenced collection is refused. The unknown record is not published (no `publishCommit`, no watch event) and the in-memory index is not reverted; segment bytes are preserved as evidence and in-memory state is untrusted until reopen. Recovery is reopen of the collection (process restart; a `DB`-level reopen API is a deferral): a complete valid line recovers as committed even though the caller saw an error; a torn tail is truncated; both are valid post-states.
4. Indexes, secondary indexes, watch events, replication publication and returned revisions derive from the same commit decision and may not advance independently of it.
5. A batch API with atomic semantics is all-or-nothing after reopen (§3.1).
6. **Resolving Unknown.** No per-write operation id is added now: single-record writes are not exactly-once. Documented client recipes: keyed insert → `FindByKey`/`Upsert`; update → read `Rev` and use `UpdateIfRev`; id-less insert → at-least-once. Batches return the engine txid so `XTxStatus(txid)` resolves the outcome. A durable operation-status/idempotency key for single-record writes is a deferral (§6).
7. **Errors and metrics.** gRPC `codes.Unknown`/reason `WRITE_OUTCOME_UNKNOWN`; `FAILED_PRECONDITION`/`COLLECTION_FENCED`; REST/CLI/Go wrappers must let callers distinguish retry-safe validation/conflict, committed-with-ancillary-failure and outcome-unknown without parsing text. Unknown outcomes are counted separately (`write_outcome_unknown_total`) from proven aborts. Logs carry operation/transaction id and collection, never payloads, keys or plaintext.

**Rejected:** keep serving and roll back index + truncate (claims NotApplied without proof; the batch path today); retry `fsync`; revert the index in memory and keep serving reads (unsound when truncate fails); panic the process (fencing gives the same safety per collection).

**Compatibility.** No on-disk change. New errors `ErrWriteOutcomeUnknown`, `ErrCollectionFenced`, `ErrPostCommit`. Callers that today see a plain error and keep working now see a fenced collection after a sync failure — intended and release-noted. Replication unchanged (unknown writes are never published).

**Lock order.** `c.mu` (write) held across append+sync as today; `fenced` is set under `c.mu` and read lock-free. `syncLoop` takes `c.mu.RLock` only to read `c.active`, syncs outside it, and sets `fenced` atomically. No new lock.

**Acceptance** (full list: SR-D1 S1.1…S1.8): injected sync failure on each of Insert, Update, Delete, Upsert, `UpdateIfRev`, `UpdateIfMatch`, `InsertWithKey`, `reapExpired` → `ErrWriteOutcomeUnknown`, then reads/writes return `ErrCollectionFenced`, no watch event, replication LSN unchanged, counter once; reopen on a page-cache-survives FS shows the record exactly once (idempotent across two reopens), on a power-loss FS absent — never a prefix or mixture; short write with successful truncate → NotApplied, not fenced, ids not reused; write+truncate failure → Unknown, poisoned, torn tail recovered; `Seal` sync failure after a committed insert → success + `PostCommitError`, event delivered, next write fenced, segment not left sealed-and-active; `SyncModeInterval` loop failure fences; XTx with a fenced participant aborts pre-decision; status-mapping golden test.

### 2.4 Recovery and derived state

1. Segment bytes plus durable transaction/batch decisions are the source of truth. Primary and secondary indexes are rebuildable caches.
2. Every replay, rebuild, compaction, scan, verification, repair, snapshot and replication path uses the same visibility predicate for transaction-tagged entries.
3. If decision evidence, participant data or encryption metadata is incomplete or contradictory, recovery fails closed with a diagnostic and preserves evidence; it must not fabricate a commit or downgrade encryption (concrete rule: §3.4).
4. A collection referenced by retained committed transaction evidence cannot be dropped until that evidence is retired by the verified path of §3.3.

### 2.5 Concurrent read semantics

Decision source: SR-D5 (scan view), SR-D6 (TTL clock).

1. `Scan`, `Count` and every aggregate input use one documented visibility predicate for tombstones and TTL and **one stable per-collection view captured at operation start** (repeatable read; §3.5).
2. If a scan cannot honor its captured view it fails with a retryable `ErrScanViewExpired` or an integrity error; it never silently shortens or skips.
3. Cross-collection isolation remains as the XTx design states. The scan fix implies no cross-collection snapshot; `XTx.Scan` is rejected by the handle and remains so. The confirmed impact of H8 is on plain `Scan`/`Find`/`Aggregate`/`Count` non-index paths (`forEachMatch` → `streamLive`), not on XTx read sets; not every `streamLive` caller was audited.

## 3. Design decisions and implementation requirements

### 3.1 Single-collection batches use the durable decision machinery

Decision source: SR-D2. **Option A (reuse the DB coordinator with one participant) is approved.**

`InsertMany` and single-collection `CommitTx` currently write N ordinary v1 lines under one `c.mu` hold and sync once; a crash after k < N complete lines replays the k-line prefix as committed data (H4, reproduced). The ratified protocol:

1. **Mechanism.** A batch of ≥ 2 entries is executed as a **one-participant XTx** through the `db.commitXTx` path: stamped entries (`TxStamp{t,i,n}` + v2 checksum), digest in `COMMIT.parts`, decision in `xtx.journal`, publication as one `TxEnvelope`. **No new record kind, entry shape or format file.** A batch of exactly one entry is a single-record write and follows §2.3.
2. **Forced durability, independent of `SyncMode`.** Data fsync, then journal COMMIT fsync (the XTx S2–S4 sequence). With a relaxed mode the decision could reach disk before the data on power loss, producing a committed transaction with a missing run (`ErrXTxIncomplete`; Open refuses). Ordering without `fsync` is impossible, so there is no opt-out.
3. **Same error model as XTx.** Pre-decision failure → abort, NotApplied, retry safe (`ErrXTxDurability`); decision append/fsync failure → `ErrXTxOutcomeUnknown` with the journal fenced. The txid is returned so `XTxStatus(txid)` resolves it.
4. **Recovery** is the existing truth table unchanged: no COMMIT → presumed abort and tail truncation; COMMIT with a complete digest-matching run → visible; a durable but unacknowledged COMMIT recovers as committed.
5. **Limits.** Bounded by `defaultXTxLimits()` (`MaxOps` 1000, `MaxBytes` 64 MiB). Larger batches are rejected up front with `ErrXTxTooLarge` (`RESOURCE_EXHAUSTED`); never silently split, because splitting breaks atomicity. Behavior change for oversized `InsertMany` callers, release-noted.
6. **Staged ids** remain provisional until commit; unchanged.
7. **Unmanaged collections.** A `Collection` opened without a `DB` (`engine.OpenCollection` directly; the façade `scriva.Open` always has one) has no coordinator: ≥ 2 entries return `ErrBatchNotAtomic` unless `CollectionConfig.AllowNonAtomicBatch` is set (legacy prefix-visible behavior for tests/tools).
8. **Rollout gate (the format is one-way).** Any XTx write makes a root non-downgradable and no drain tool exists. Atomic batches are **default-on for new roots and roots that already hold `xtx.format`**; an existing *legacy* root keeps the old non-atomic path, logs a one-time startup warning, and is converted only by explicit operator opt-in (`serve --enable-atomic-batches`, which runs `ensureXTxFormat` and records the conversion). Documentation may say "atomic" only for roots in atomic mode.

**Rejected:** B (collection-local batch marker — second protocol and recovery predicate to teach to compaction, snapshot, verify/repair, encryption and replication); one physical line holding the whole batch (breaks the one-entry-per-line invariant and the line-size limit; old readers would see an unknown op, not a checksum failure); C (weakening the contract; only by explicit product decision, not chosen because docs and clients treat these as atomic); side file + rename; honoring `SyncMode` for the decision.

**Compatibility.** On-disk: nothing new. Old readers fail closed on stamped lines and on `xtx.journal`. Legacy roots unchanged until opt-in. Downgrade unsupported after conversion until a drain tool exists (deferral); backup before opt-in is the documented mitigation. Replication: a v1-only follower gets `REPLICATION_PROTOCOL_UNSUPPORTED` (existing, non-silent); upgrade followers first; a single-participant envelope is applied atomically, which also removes the follower prefix-exposure hazard. Cost: ≥ 2 fsyncs per batch regardless of mode — release-note with measured numbers before default-on. All §4.2 gates apply.

**Lock order.** Exactly the XTx order: `DB.mu.RLock` → participant `c.mu` → `journal.mu` (leaf) at the decision step. `Collection.InsertMany`/`CommitTx` must take the DB path *before* locking the collection, so `Collection` gains a back-reference (`db *DB` or a `batchCommitter` interface) set by `DB.Open`. Rotation, meta persist and watch emission happen after all locks are released.

**Acceptance** (full list: SR-D2 B2.1…B2.7): the minimum crash matrix of §4 for a one-participant batch at every `faultFS.trace` step, after reopen (twice) all N entries visible with identical revs/ts/data or none, comparing the full ordered record set and id counter; identical results under `none`/`interval`/`always`, and a power-loss FS never yields the incomplete-run state; 1001 ops or > 64 MiB → `ErrXTxTooLarge` with nothing written; legacy root gains no `xtx.*` bytes until opt-in, then a pre-fence binary refuses without mutating; unmanaged 2-entry batch → `ErrBatchNotAtomic`; leader batch observed as one envelope by the follower and all-or-none after follower crash; compaction, snapshot/restore, verify/repair and encryption migration preserve stamps and digests.

### 3.2 I/O failure and commit state machine

Each write is staged as: validate → append → required sync → establish/observe decision → apply/publish indexes → publish events/replication → acknowledge. The durable decision point differs by class: for a transaction (including a batch) it is the coordinator decision; for an ordinary single-entry write it is the entry plus its configured sync requirement. Not every write is forced through a journal.

- Before decision: rollback may be attempted; if truncate is not proven complete the segment is poisoned and the result is Unknown (§2.3).
- After decision: the write is committed even if index application, rotation or event delivery fails; the result must identify a post-commit ancillary failure (`PostCommitError`), never "the data write failed". A rotation failure after a successful commit never skips `emit` (emit before the rotation attempt or in a `defer`).
- A sync error on a decision record is outcome-unknown; the journal and participating writers are fenced until reopen/recovery.
- Watch and replication publication are recovered from committed evidence or use a cursor that cannot silently skip an acknowledged commit.
- A persist failure of `meta.json` is a post-commit ancillary failure, never Unknown (the id counter is reconstructible from segments).

#### Failure-state table

| Boundary | Required immediate state | Recovery result | Client-visible result |
|---|---|---|---|
| Validation before bytes | No data/index/event change | Absent | Ordinary validation/conflict error |
| Append fails; truncate-to-previous-size succeeds | Bytes removed; index unchanged; **not** fenced | Absent | NotApplied; retry safe |
| Append fails and truncate fails | Segment poisoned; collection fenced; bytes preserved | Open trims the torn tail or recovers a complete line | Unknown (`ErrWriteOutcomeUnknown`) |
| Single-record `fsync` fails (foreground or `syncLoop`) | Collection fenced; record unpublished; index not reverted; no retry | Complete valid line → committed; torn tail → absent | Unknown; then `ErrCollectionFenced` |
| Data sync fails before transaction/batch decision | No visibility; writer fenced; no decision appended | Abort if no decision; missing evidence fails closed | NotApplied only if rollback proven (pre-decision abort, `ErrXTxDurability`); otherwise Unknown |
| Decision append/sync fails | Journal fenced; no speculative commit applied | Commit if a complete durable decision validates; otherwise presumed abort; corruption fails closed | `ErrXTxOutcomeUnknown`; status via `XTxStatus(txid)` after reopen |
| Decision durable, index/apply fails | Decision authoritative; stop serving inconsistent writes or force rebuild | Committed; indexes rebuilt | Committed result or explicit committed-with-maintenance error; never plain "not committed" |
| Commit durable, rotation fails (incl. `Seal` sync) | Data committed; event published; next write fenced if seal sync failed | Committed | Success + `PostCommitError` |
| Commit durable, event delivery fails | Data committed; publication retried/reconstructed | Committed | Committed; ancillary failure surfaced separately |
| Meta persist fails | Memory unchanged for policy/TTL changes; id counter reconstructible | Consistent with last persisted generation | Error to caller (policy/TTL) or `PostCommitError` (rotation/`CommitTx`) |

This table is reconciled with `docs/design-xtx-fault-boundaries.md`, which already distinguishes abort from outcome-unknown at journal fsync; XTx error behavior changes only by the participant-fence check (a fenced participant aborts pre-decision).

**Tooling.** Fault injection around `Write`, short write, `Sync`, `Truncate`, rename, directory sync, index application and publication. For each injected point assert the immediate API result, subsequent reads, emitted events, replication cursor and state after reopen (§4).

### 3.3 Collection lifecycle and coordinator evidence

Decision source: SR-D3. **Retire on demand at drop; refuse on any verification failure; no force path.**

**Proof obligation.** While holding the drop barrier, `C` may be dropped iff for **every** journal decision `T` whose parts name `C`:
1. `T` is `RETIRE(commit)`, `RETIRE(abort)` or `ABORT`; **or**
2. `T` is a non-retired `COMMIT` naming `C`, and drop first **retires it by the verified path**: all participants of `T` are re-derived under `c.mu.RLock` in name order, `rev.complete && rev.n == p.N && rev.digest == p.D` for each part (the recovery step-1 check), then `RETIRE{T, commit}` is appended and **fsynced**. A mismatch, missing sibling participant or failed fsync → drop **refused** with `ErrParticipantRetained{Collection, TxIDs, Reason}` and nothing is modified (no Close, no removal);
3. no in-flight XTx includes `C` (guaranteed by `DB.mu.Lock`, which XTx excludes via its `DB.mu.RLock`; a staged handle that later commits fails `ErrCollectionNotFound` before any write);
4. undecided stamped runs for `C` are covered by presumed abort and removed with the directory.

A `RETIRE` is never written from `Open`/recovery success alone. A bare "DROP tombstone" is not accepted as evidence.

**Atomic drop sequence.** (a) retire as above; (b) `col.Close()`; (c) `rename(<root>/C, <root>/.drop-C-<nonce>)` + `fsyncDir(root)`; (d) `RemoveAll` the renamed directory; (e) startup deletes any `.drop-*` directory. A crash before (c) leaves `C` intact (extra RETIRE records are harmless); after (c) the collection is gone and recovery sees only retired decisions. In-place `RemoveAll` is no longer used.

**Backups.** A snapshot before retirement restores a self-consistent database (C present, decisions live); after retirement, (C absent, decisions retired). Drop's exclusive `DB.mu.Lock` serializes with the snapshot barrier. Restoring a journal from one point with collection directories from another is out of contract and is caught by the missing-participant check.

**Replication.** Drop remains local-only in this release; a follower of a leader that dropped `C` keeps `C` and diverges by design (documented). Replicated drop is a deferral.

**Status (H6): interim fail-safe shipped; retirement-at-drop deferred.** `DropCollection` takes the exclusive `DB.mu` and refuses with `ErrParticipantRetained` (`FAILED_PRECONDITION` / `PARTICIPANT_RETAINED`) while any non-retired journal COMMIT names the collection, changing nothing; the corruption of H6 (unreopenable root) can no longer be produced through the API. The atomic drop sequence, `retire` at drop and the checkpointer are **not** implemented: `retire`/`checkpoint` still have no production caller, so no decision is retired at runtime and every collection that took an XTx stays undroppable. Retirement-at-drop remains a prerequisite for the default-on flip of atomic batches (§3.1, §5). Concurrency, snapshot, reopen and crash behaviour of the fail-safe is covered by `engine/drop_concurrency_test.go`; the operator runbook is in `docs/operations.md`.

**Rejected:** block drop forever while evidence exists (impossible after the first XTx — kept only as the interim fail-safe); a `DROP{collection}` journal record (new record kind and fence level; weakens the missing-participant check without verifying siblings); `--force` drop; background-only checkpointer as the drop mechanism (still needed for stamp reclamation, cannot serve drop latency).

**Compatibility.** No new record kinds (`RETIRE` is already specified and decoded; recovery treats `RETIRE(commit)` as final). New `ErrParticipantRetained` (`FAILED_PRECONDITION`, reason `PARTICIPANT_RETAINED`, txids in details). Embedded `DropCollection` signature unchanged.

**Lock order.** `DB.mu.Lock` (exclusive) → participants' `c.mu.RLock` in canonical name order → `journal.mu` for `retire` (leaf) → all released → `col.Close()` (`compactMu` → `persistMu` → `c.mu`) → rename/remove with no collection lock. `journal.mu` is never held while acquiring a `c.mu`. Verification must **not** take a sibling's `compactMu`. The full hierarchy is:

```
DB.mu ─ compactMu ─ { layoutMu , persistMu } ─ c.mu ─ { journal.mu | sidxMu ─ Index.mu ─ Segment.mu ─ broker.mu }
encAdminMu ─ c.mu ─ metaMu                                   (§3.4)
```

**Acceptance** (full list: SR-D3 P3.1…P3.6): commit across {a,b}, `Drop(a)` retires, reopen succeeds, `b`'s entries remain visible; before retirement exists, drop returns `ErrParticipantRetained` with the directory byte-identical (hash before/after); corrupted sibling run → refused, no RETIRE written, `Verify` reports the same finding; drop racing `commitXTx` forced by barriers (not sleeps) at staged-before-lock, prepared, after COMMIT before apply, after apply → always tx-committed-then-retired or drop-first-then-`ErrCollectionNotFound`, never an unopenable root; child-process kill at {after RETIRE fsync, after Close, after rename, mid-RemoveAll} reopens with `C` fully present or fully absent and `.drop-*` leftovers removed; failed RETIRE fsync refuses the drop; snapshot taken before and after restores and opens.

### 3.4 Metadata serialization and encryption fail-closed behavior

Decision source: SR-D4. Both H7 defects are confirmed: a **stale snapshot overwrite** and a **shared `meta.json.tmp` opened `O_TRUNC`** that corrupts `meta.json` under concurrent persists. Unique temp names are therefore required by evidence, not hygiene. A separate required change is fail-closed open: in the reproduction a lost or corrupt `meta.json` made reopen return ciphertext envelopes as data and accept plaintext writes.

**Serialization decision.** A leaf mutex `Collection.metaMu`; the only persist path is `c.writeMeta(mut func(*collectionMeta))`: acquire `metaMu`; **take the snapshot inside it** (so an older snapshot can never land after a newer one); apply `mut` (the encryption change installs its *candidate* block here); write via `os.CreateTemp(dir, "meta.json.*")` → fsync → rename → `fsyncDir`; on failure remove the temp and return the error. In-memory state (`encMeta`, `enc`, `explicitDefaultTTLSecs`) is updated **only after** `writeMeta` succeeds. `explicitDefaultTTLSecs` becomes an `atomic.Int64` so `metaSnapshot` needs no collection lock. Persist errors are no longer swallowed: rotation and `CommitTx` return `PostCommitError`; policy/TTL changes return the error with memory unchanged.

**Fail-closed decision.** Open classifies `meta.json` **before any rewrite** (today `load()` regenerates a missing/unparseable file *before* `initEncryption`, which then reads "no encryption"). Evidence of encryption is: (E1) any primary-index entry with `Epoch > 0` (O(1) via a max-epoch counter); (E2) the sentinel file `encryption.sentinel` in the collection directory, written (file + dir fsync) *before* the first encrypted write takes effect and self-healed by `initEncryption` when a valid block is found; (E3) on a missing/unparseable meta only, a marker-prefix scan (`\x00scrivaenc:`) of the segments. If evidence exists and meta is missing/unparseable/lacks `encryption`, or `meta.Encryption.Epoch` < max entry epoch, open fails with **`ErrEncryptionMetaIntegrity`** (distinct from `ErrWrongKey`/`ErrKeyUnavailable`). It never infers plaintext and never rewrites the file; an unparseable file is copied to `meta.json.corrupt-<unixnano>` and left in place. The error is **not tolerable** under any integrity policy. Logs carry collection name, evidence kind and file name only. A wrong key still fails closed as today.

**Status: implemented** (M4.3–M4.6): `ErrEncryptionMetaIntegrity`, E1/E2/E3 classification before any rewrite, `encryption.sentinel` (snapshot allow-list, self-heal, `Verify` `encryption-meta-lost`, `Repair` refusal), `DATA_LOSS` mapping; E1 is a single pass over the index at open rather than a maintained counter. Runbook: [`operations.md`](operations.md#runbook-errencryptionmetaintegrity).

**Rejected:** extending `c.mu` over snapshot+persist everywhere (`CommitTx`/`setDefaultTTL` persist outside `c.mu` on purpose; moving them in stalls appenders and creates lock-order problems); immutable value + generation CAS (more machinery than a single-process leaf lock needs); scanning for ciphertext markers on every open of plaintext collections; deriving "encrypted" only from index epochs (misses epoch-0 legacy ciphertext, hence sentinel/E3).

**Compatibility.** One new optional sentinel file; `meta.json` shape unchanged; an old binary ignoring the sentinel is the same exposure as today. The sentinel must be in the snapshot/extraction allow-list (§2.1). Verifying that verify/repair/snapshot tooling does not reject unknown files in a collection directory is a pre-implementation check (§6). API: new error only.

**Lock order.** `encAdminMu` → `c.mu` → `metaMu` (new leaf). `writeMeta` acquires nothing else while holding `metaMu` (all snapshot inputs are atomics/immutables). Callers already holding `c.mu` (`rotateSegment`, `Close`, `applyEncryptionChange`) keep fsyncing under it as today; `setDefaultTTL` and `CommitTx` call it with no collection lock. `persistMu` stays ordered `compactMu → persistMu → c.mu`.

**Inventory and reproductions.** Every `meta.json` writer, its lock context and error handling, and the deterministic barrier tests that reproduce both H7 defects are recorded in [`metadata-persistence-inventory.md`](metadata-persistence-inventory.md).

**Acceptance** (full list: SR-D4 M4.1…M4.6): the evidence-recheck reproductions are the first tests — the ordered `setDefaultTTL`/`SetEncryptionPolicy` schedule released in both orders leaves a `meta.json` with both the TTL and the encryption block and reopen decrypts existing records; a 300-iteration free-concurrency hammer produces no corrupt JSON and no `ENOENT` rename error under `-race`; rename failure in `writeMeta` returns an error with `EncryptionStatus()` unchanged and a concurrent persist writing the old policy; corrupt/missing meta of an encrypted collection (E1, E2, E3 variants, including legacy epoch-0 ciphertext and a lower meta epoch) → `ErrEncryptionMetaIntegrity` with bytes unchanged and a `.corrupt-*` copy, while a plaintext collection with the same corruption opens and regenerates; rotation persist failure returns `PostCommitError` with the record visible after reopen.

### 3.5 Stable scan view and shared expiration semantics

Decision source: SR-D5, SR-D6. H8's omission is reproduced; the duplicate-emission and moving-tail schedules are UNCONFIRMED RISK and are covered by the same design because it removes the shared cause (lazy lookup into the live index).

**Scan view: repeatable "snapshot at start", bounded in time and memory.**
1. **Capture** under `layoutMu.RLock` then `c.mu.RLock` in one critical section: segment list with each segment's committed end offset, a copy of the primary index (`id → IndexEntry`), the secondary-index candidate set when an index serves the filter, `layoutGen`, and `now`.
2. **Iterate** only captured segments up to their captured end; emit an entry iff the captured index maps its id to exactly `(segment path, offset)`. Each record is emitted exactly once at the version live at capture; records deleted/updated/expired after capture are still returned at that version; records inserted after capture are excluded. Candidate paths read the captured location, never the live index.
3. **Pin:** `layoutMu.RLock` is held for the scan so compaction cannot unlink captured bytes; rotation appends new segments without disturbing them.
4. **Bounds:** at most `MaxScanViews` concurrent views per collection (default 8; extra scans wait on the context); a view older than `ScanViewMaxAge` (default 10 min) ends with retryable `ErrScanViewExpired`. Cancellation/backpressure releases the lease and the copy.
5. **No silent shortening:** an unreadable captured segment fails the scan with an integrity error or `ErrScanViewExpired`.
6. `Count` and aggregates use the same captured view; `Count(MatchAll)` stays O(1) only when no expiring records exist.
7. **Scope:** per collection only.

**TTL visibility clock.**
1. **One predicate:** `visible(entry, now) := !tombstone && !(ExpiresAt != 0 && now >= ExpiresAt)`, evaluated against **one `now` captured once per operation** (per `Get`, per scan/`Count`/aggregate view, per `Find`/index lookup, per XTx commit under the participant locks, per reaper pass, per compaction pass). Write-time stamping uses one `now` per batch.
2. **One clock source with a monotonic floor:** `now() = max(wall, floor)`, where `floor` is an in-process high-water mark seeded at open from the newest `Ts` in the active-segment tail scan (`tailMaxID`). A backwards wall-clock step cannot un-expire a hidden-but-unreaped record within a process lifetime or across a restart after recent writes; no on-disk change. Test hook `CollectionConfig.nowFn`.
3. **Every consumer applies the predicate:** `Count(MatchAll)` is `index.Len()` only when the index's `nExpiring` counter is 0, otherwise an in-memory index walk excluding expired ids; the eq fast path and `IndexLookup`/range lookups filter ids through the index entry with the same `now` (closing the UNCONFIRMED RISK in H11); unique checks (`sidxCheckUnique`, `txCheckUnique`) treat an expired holder as vacant.
4. **Reaper and compaction never define visibility;** they reclaim records the predicate already hides, using `now − ReclaimGrace` (default 60 s) so skew or a small step never removes a record earlier than a peer's visibility says. Reaper delete entries **are published** (`publishCommit`) so followers converge without their own clocks.
5. **Replication:** entries carry absolute `ExpiresAt`; each node evaluates with its own clock; a follower read can differ from the leader by at most the skew (operators must run NTP), stated, not hidden.

**Rejected:** status-quo lazy live-index lookup; holding `c.mu.RLock` for the whole scan (a slow client would stall writers); generation check with restart (does not fix in-place duplicate emission and can starve); weak "per-record read committed" (still needs an O(n) emitted-id set and gives moving results); logical clock as TTL basis; treating expiry as physical-cleanup-only (unique keys unusable until the reaper runs); persisting the high-water mark in `meta.json`; per-row clock reads.

**Compatibility.** No on-disk change. A scan may now include a record deleted *after* it began (previously "maybe"). `Count` can decrease for collections with expired-unreaped records (aligns with `Scan`); `InsertWithKey` over an expired key now succeeds (previously `ErrDuplicateKey`); `Exists(key)` of an expired key becomes false; reaper deletes now flow as ordinary delete entries (additive). New `ErrScanViewExpired` (`ABORTED`/`SCAN_VIEW_EXPIRED`, retryable) and config `max_scan_views`, `scan_view_max_age`. Release-note all of these. Memory cost is O(live records) per view (order of 64 B/record — to be measured before default limits are finalized).

**Lock order.** Scans: `layoutMu.RLock` → `c.mu.RLock` (capture only) → `Index.mu.RLock` (copy) → `sidxMu.RLock` (after `c.mu`, as on the write path); no lock is held while invoking the visitor; scans do not take `compactMu`. TTL: no new lock; `now` is captured before `c.mu` for reads and *under* `c.mu` for writers/XTx commit so a write's expiry check and append share one instant.

**Acceptance** (full lists: SR-D5 V5.1…V5.7, SR-D6 T6.1…T6.7; barriers via `scanHook`/`nowFn`, no sleeps, `-race`): the H8 reproduction (update across a forced rotation at `scanAtSegment(0)`) emits the record exactly once at its start version; a record deleted mid-scan is still emitted once and a newly inserted one is not; compaction requested mid-scan is deferred and the output equals a no-compaction control; cancellation and slow consumers release the lease and copy; `ScanViewMaxAge` yields `ErrScanViewExpired`, never fewer rows without error; `Count(f)` equals `len(Scan(f))` from the same view under concurrent writes; across {before expiry, at `ExpiresAt` exactly, after expiry before reaper, after reaper, after compaction, after reopen, after index and sidx rebuild} `Get`, `Scan`, `Count(MatchAll)`, `Count(eq indexed)`, `Find` via index and `Exists(key)` agree; a backward 1 h clock step keeps the record hidden, also across reopen; a scan crossing `ExpiresAt` mid-scan still returns the record; expired unique `_key` holder lets `InsertWithKey` succeed and rebuild keeps one live holder; one replicated delete per reaped id and a follower inside the grace window retains bytes; XTx update of an expired target → `ErrXTxConflict` at the commit-captured `now`; `nExpiring == 0` ⇒ no index walk.

### 3.6 Rotation/sealing failures

Decision source: SR-D1 item 6. `Seal` must **not** set `sealed=true` before `Sync` succeeds. A failed seal sync is **Unknown + fence**: no un-seal, no retry, because kernel durability is uncertain; the active segment is never left sealed-and-active. A rotation failure *after* a successful commit never turns the return into a failure and never skips `emit` (§3.2). An explicit `ACTIVE`/`SEALING`/`SEALED`/`ROTATING`/`POISONED` state set is not mandated; the fence plus the existing poison state encode the unsafe states, and the tests below are the proof obligation. **Acceptance:** SR-D1 S1.5 plus fault injection at seal/sync/create/rename/directory-sync boundaries asserting no permanently sealed active segment, no false failure for a committed write, and no lost event.

### 3.7 Immutable keyed-record metadata (`_key`)

Decision source: SR-D7.

1. **`_key` is reserved and immutable once assigned.** It is set only by `InsertWithKey`/`Upsert`/keyed XTx insert and never changes; there is no rename operation (delete + insert).
2. **Updates preserve it.** All id-addressed write paths — `Update`, `UpdateWithExpiry`, XTx staged/committed update — stamp the record's current `_key` into the new payload when the collection has a `_key` index and the current record carries a string `_key`, via one helper `preserveReservedLocked(c, id, data)` called under `c.mu` (skipped entirely when no `_key` index exists, so the common path pays nothing). Supplying `_key` in caller data stays `ErrReservedField` (`INVALID_ARGUMENT`) on every path, **including XTx stage ops, which reject at stage time**. Preservation is silent: it is a compatible fix because today's behavior silently loses the key (H10).
3. **Existing data.** Open never rewrites records. Only **string** `_key` values participate in the index; non-string values are legacy data, ignored by the index and left intact. Duplicate string `_key` among *live* records make the index build fail with `ErrKeyIndexConflict{Collection, Key, IDs[≤16]}`: the keyed API (`InsertWithKey`, `FindByKey`, `Upsert`, `Exists`) returns it for that collection, id-based operations keep working, `Verify` reports `key-duplicate`, and the engine **never selects an arbitrary winner**. Resolution is operator action (delete/rewrite the duplicate by id); automated repair is a deferral.
4. **Replication:** followers apply leader entries verbatim; a `_key`-bearing apply that violates uniqueness is a typed integrity error that stops the stream, not silently skipped. `LoadJSONL` keeps its explicit key-field mapping and rejects stray `_key`.
5. **Reserved names as one documented set:** `_key`, `secure_data` (record encryption only), marker-prefixed string values (`ErrReservedPrefix`), `xtx.*` collection names.

**Rejected:** rejecting id-addressed updates that omit `_key` (breaks working callers); `_key` rename via update (two-index atomic move, no demand); migrating legacy `_key` at open (implicit rewrite, not downgradable); lowest/highest-id winner on duplicates.

**Compatibility.** No on-disk change. `Update(id)` on a keyed record now keeps `_key` (release-noted); new `ErrKeyIndexConflict` (`FAILED_PRECONDITION`/`KEY_INDEX_CONFLICT`); replication wire unchanged. The XTx stage-time rejection of `_key` is *by code trace*: no `ErrReservedField` check was found in `xtx_commit.go`/`xtx_handle.go`.

**Lock order.** `c.mu` (write) → `sidxMu` (read) as on every write path; the reverse key lookup must not take `c.mu` itself. XTx preservation runs in the commit step under participant `c.mu`, never at stage time.

**Acceptance** (full list: SR-D7 K7.1…K7.5): after `InsertWithKey("k", d)` and `Update(id, {x:2})`, `FindByKey("k")` resolves and re-inserting `"k"` → `ErrDuplicateKey`, surviving compaction and reopen; same through XTx update and `UpdateWithExpiry`; XTx stage op containing `_key` → `ErrReservedField` at stage time; no `_key` index ⇒ no extra read; duplicate-key legacy fixture opens, `FindByKey` → `ErrKeyIndexConflict` listing both ids, `Verify` reports `key-duplicate`; a follower receiving a duplicate-key entry stops with the typed error and applies nothing from it.

### 3.8 Secure deployment and archive handling

Decision source: SC-D5 (plus SC-D2 for archives and permissions, §2.1).

**Target ("strict") policy.**
1. **Default binds:** `127.0.0.1:5433`, `127.0.0.1:8080`, `127.0.0.1:9090`.
2. **Exposure rule:** a listener is *exposed* if its host is empty, `0.0.0.0`, `::`, any non-loopback IP, or any hostname other than `localhost` (no DNS lookups; unparseable = exposed). An exposed gRPC/REST/metrics listener requires **auth configured and TLS configured**, else startup is refused with an error naming the listener and the exact override `allow_insecure_remote: true` (explicit, logged `WARN` every start, never implied by another setting). Exposure and auth are validated together; "empty API key" alone is never treated as a security decision.
3. **Probe rule:** `/healthz`/`/readyz` stay unauthenticated on exposed listeners and are exempt from the auth part.
4. **Unix socket:** parent directory owned by the server uid with mode ≤ `0700`; socket `0600`; a pre-existing path is removed only if `Lstat` shows a socket owned by the server uid; default moves from `/tmp/scriva.sock` to `/tmp/scriva-<uid>/scriva.sock` and the CLI default follows.
5. **Replication:** an exposed leader listener follows rule 2; follower gains `replicate_tls_ca`/`replicate_tls_cert`/`replicate_tls_key`; dialing a non-loopback leader without TLS is refused unless `replicate_insecure: true`.
6. **HTTP hardening:** metrics and REST servers set `ReadHeaderTimeout` 10 s, `ReadTimeout` 30 s (watch/stream routes exempt via per-route deadlines), `IdleTimeout` 120 s, `MaxHeaderBytes` 1 MiB, body ≤ 16 MiB (configurable). No CORS headers unless `cors_allowed_origins` lists origins; only listed origins are echoed, never `*`. REST serves TLS with the same certificate when configured, unless `rest_plaintext: true`.
7. **Existing credentials:** `api_key` stays one unrestricted `read-write` principal named `default`; `keys:` entries keep scope and allow-list; mTLS unchanged. No key-format migration.
8. **Archives and permissions:** §2.1 items 9–10. Never chmod pre-existing user files; tighten only files the server creates.

**Staged rollout and fail-safe.**

| Stage | Behavior | Fail-safe |
|---|---|---|
| **N — warn + opt-in** | The security *fixes* (SC-D1…D4, archive and permission fixes) ship with no flag. New `secure_defaults: true` / `--secure-defaults` applies the strict policy now. Without it defaults are unchanged, but startup logs one `WARN` per exposed listener lacking auth/TLS (listener, reason, override name; no secrets) and always logs effective binds at `INFO`. New-socket mode/ownership tightening applies. | An invalid value in any new key is a startup error, not a silent default. Warnings never include key material. |
| **N+1 — secure defaults** | Loopback default binds; exposed + **no auth** refused unless `allow_insecure_remote`; HTTP timeouts and CORS restrictive by default; socket default path moves; follower TLS keys available and plaintext non-loopback replication warns. `secure_defaults` becomes a no-op accepted flag. | Configs with *explicit* addresses keep their binds; configs relying on the old all-interface default fail loudly by connection-refused, and the `INFO` bind line names the effective address. |
| **N+2 — complete** | Exposed + auth-without-TLS also refused; non-loopback plaintext replication refused; REST TLS on by default when a cert is configured. Overrides remain as permanent explicit flags. | The refusal text enumerates each failing listener and the single flag that would permit it. |

At every stage a **reload cannot weaken** the effective policy, and `DB.Open` never depends on listener policy: refusing to start a listener never alters data. Version numbers for N, N+1, N+2 are the release owner's call; the stage contents are fixed.

**Rejected:** flipping all defaults in N (an outage for every default-config deployment with no warning release); never flipping (fails H2); inferring safety from an empty API key; refusing to start whenever auth is disabled even on loopback (a legitimate non-exposed configuration); a CLI-only override flag; extraction in place; a flag gating symlink refusal.

**Compatibility.** Operators with intentional all-interface plaintext deployments must add TLS+auth or `allow_insecure_remote: true` (N+1/N+2); browser clients on another origin need `cors_allowed_origins`; clients with a hard-coded `/tmp/scriva.sock` need the new path or an explicit `unix_socket`. Legacy `api_key` and scoped `keys:` files load unchanged.

**Acceptance** (full list: SC-D5 S-1…S-7): a `(host, auth, TLS, override, stage)` table over `:5433`, `0.0.0.0`, `::`, `10.0.0.5`, `db.example`, `localhost`, `127.0.0.1`, `[::1]` and an unparseable host → start/refuse/warn; `DefaultConfig()` in N+1 has loopback hosts while an explicit `":5433"` is kept (with a warning when no auth); captured startup logs contain listener name and override key but no key, certificate or token; socket created `0600` in a `0700` parent, symlink or foreign-owned pre-existing path refuses; timeout struct inspection plus a slow-header client disconnected within the bound; `Origin: https://evil` receives no `Access-Control-Allow-Origin`; follower to a non-loopback leader without TLS refused (N+2) / warned (N+1) and connects over TLS with `replicate_tls_ca`; golden-config test for previous-release config files.

## 4. Acceptance test matrix

Per-decision acceptance criteria are stated in §2–§3 and enumerated in full in the ratification files; this matrix is the cross-cutting index and the minimum evidence for closing each finding.

| Area | Required test evidence |
|---|---|
| Path containment | Hostile-name corpus across engine, gRPC, REST, CLI, embedded API, Windows-style separators/drive paths, `xtx`, `collections`, case-fold pair; prove no outside file is created or deleted; symlink swap between `Open` and `Drop`; legacy-name fixtures (§2.1). |
| Authorization | Descriptor-completeness test plus principal × RPC matrix asserting status **and** no out-of-scope data/name disclosure; handle ownership; reload-to-empty; REST equivalence; `/metrics` credentials (§2.2). |
| Batch atomicity | Child-process crash at every append/decision/fsync boundary; after reopen (twice) the batch is wholly absent or wholly present with identical ordered records, revisions and id counter; identical across `SyncMode`s (§3.1). |
| I/O outcome | Inject append/sync/truncate/rename/directory-sync errors; correlate returned outcome with immediate reads, indexes, watches, replication and reopened state; golden status-mapping test (§2.3, §3.2). |
| Participant lifecycle | Drop during/after XTx via barriers; refusal leaves bytes unchanged; valid retirement then drop/reopen succeeds; corrupted sibling refuses; crash points in the drop sequence (§3.3). |
| Metadata concurrency | The H7 reproductions as regression tests; both writers' changes persist; no corrupt JSON under hammer; encrypted data reopens; missing/corrupt/stale metadata fails closed with `ErrEncryptionMetaIntegrity` (§3.4). |
| Scan stability | The H8 reproduction plus barrier schedule: no missing/duplicate IDs relative to the start view, stable revisions/data, leases released on cancel, `-race` (§3.5). |
| Expiration/count | Same results across `Get`/`Scan`/`Count`/index/`Exists` before expiry, at expiry, before/after reaper, after compaction, restart and rebuilds; clock-step tests (§3.5). Includes the currently UNCONFIRMED eq-index and range `Count` fast paths. |
| Rotation | Faults at seal/sync/create/rename/dir-sync boundaries; no permanently sealed active segment, false failure for a committed write, or lost event (§3.6). |
| Key invariants | Update/transaction/import cannot remove/change reserved key; uniqueness survives reopen and compaction; legacy duplicate fixture (§3.7). |
| Security hardening | Defaults/exposure table, socket permissions, metrics isolation, CORS/timeouts; archive traversal, symlink, hardlink and decompression-bomb rejection with no temp residue (§3.8, §2.1). |
| Tooling | Every Makefile test/bench/fuzz target resolves **and** the referenced fuzz/benchmark functions exist under `./engine`; CI runs vet, race suite, focused fuzz smoke tests, and scheduled longer crash/soak suites. |

All tests must assert logical state, not only absence of data races. Crash tests use subprocess termination or a fault-injected filesystem, with deterministic seeds and a retained failure artifact.

The minimum batch crash matrix is: after first complete entry; after each later entry; after all entries before sync; during each participant/segment sync; after all data syncs before decision append; partial decision write; complete decision write before sync; failed decision sync; successful decision sync before index apply; during primary/secondary index apply; before/after event publication; and before client acknowledgment. For one-participant batches this tests the same semantic boundary as a multi-participant XTx.

The I/O matrix compares four observations for every operation: returned result, same-process point read and secondary-index lookup, emitted watch/replication sequence, and state after close/reopen. For batch recovery also compare the entire ordered record set and revision sequence, not only counts.

### 4.1 Test harness requirements

The harness separates three failure classes: process crash (kernel page cache survives), machine/power-loss model (only explicitly synced state survives), and returned I/O errors (the syscall returns an error but partial effects may remain). A fake filesystem must model short writes, successful write with failed sync, failed truncate, rename failure, directory-sync failure and reordering where relevant (`engine/faultfs.go` already offers nth-write/sync/rename failure, short-write prefix, `crashAt` and step `trace`; a power-loss mode that drops unsynced bytes must be added). A subprocess kill test alone is not a power-loss simulation.

For each crash-matrix row, record exact durable files/bytes before recovery and assert one of: complete old state, complete new state, or documented fail-closed error. Never accept a mixed batch/transaction state. Reopen twice to assert recovery idempotence. Run each seed repeatedly and retain failing roots. Mark assumptions that cannot be simulated.

### 4.2 Compatibility gates for any on-disk change

Before a new format is enabled by default, require: old binary rejects safely (does not mutate/quarantine); new binary opens legacy roots unchanged; verify/repair understand the format or refuse safely; snapshots and restored snapshots preserve decision evidence; replication cannot flatten/partially apply new records; compaction and encryption migration preserve stamps/decision digests; downgrade behavior is documented and tested; interrupted upgrade/first-write initialization is recoverable. Roll out a reader/fence release before a writer release if an older binary might otherwise silently accept the new bytes, following the XTx staged-release precedent. Atomic batches (§3.1) reuse the existing XTx format and are gated by item 8 of that section; the only new durable artifact in this specification is the encryption sentinel (§3.4).

### 4.3 CI tiers

Per-PR: unit/integration, `go test -race ./...`, `go vet ./...`, lint, short deterministic crash matrix, fuzz smoke (bounded duration), Make target validation. Nightly or scheduled: longer fuzz campaigns, randomized crash/reopen model tests, concurrent scan/write/compaction stress, soak/resource-leak checks. Release candidate: full crash matrix on supported platforms and the restore/replication compatibility matrix. A green per-PR suite is not a substitute for scheduled suites.

## 5. Delivery sequence

1. **Containment and authorization:** `ValidateCollectionName`, containment helper, path-safe drop, descriptor-generated RPC table with the capability model, filtered `ListCollections`, `PermissionDenied` for scoped `Snapshot`/`Replicate`/`Promote`, XTx handle ownership, archive import/export hardening and new-file permissions (§2.1, §2.2, SC-D1…D4). Release N, no flag.
2. **Outcome truthfulness:** single-record fail-stop and fencing, `Seal` ordering, `PostCommitError`, error mapping, `emit` before rotation (§2.3, §3.2, §3.6).
3. **Atomic batches:** one-participant XTx routing, `Collection` back-reference, limits, `ErrBatchNotAtomic`, opt-in conversion for legacy roots, replication envelope, compaction/snapshot/verify/repair coverage (§3.1). **Default-on for new roots does not ship until stage 4's retirement-at-drop is available** (§3.3 "Today").
4. **Lifecycle and metadata:** retirement-at-drop with the rename-then-remove sequence and `ErrParticipantRetained`; `metaMu` + unique temp names; encryption fail-closed with the sentinel (§3.3, §3.4).
5. **Read consistency and business semantics:** scan view, TTL predicate/clock/reaper publication, `_key` preservation (§3.5, §3.7).
6. **Deployment and test infrastructure:** staged secure defaults N → N+1 → N+2 (the *fixes* of stage 1 are not gated on this), HTTP hardening, socket path, follower TLS; fix Make targets and broaden fuzz/soak CI (§3.8, §4.3).

Stages 1 and 2 are release blockers for remotely reachable deployments. Stage 3 is a correctness blocker for claiming durable atomic batch transactions. Stage 4 is a blocker for safe lifecycle management of databases that have used XTx, and — with stage 3 — for default-on atomic batches. Any stage may be split into smaller implementation plans, but its acceptance criteria remain mandatory.

Each stage still requires a short implementation record (alternatives are already settled here and need not be re-litigated): API/error mapping, performance measurements named in this document (scan-view memory, batch latency), and the test artifacts. Security-critical containment and authorization can proceed as separately reviewed, narrowly scoped fixes; they do not depend on the batch format decision.

## 6. Decisions and deliberate deferrals

### 6.1 Resolved decisions

| Decision | Resolution | Section | Source |
|---|---|---|---|
| Collection naming / legacy compatibility | Strict grammar for new names; legacy names stay operable but not creatable; fold-uniqueness; no rename RPC; validation on every path in N, no flag | §2.1 | SC-D1 |
| Path and symlink containment, archive handling | Resolved root, real directories only, `Lstat`/no-follow, temp-dir extraction with limits and error-on-hostile-entry | §2.1 | SC-D2 |
| Authorization scopes | `read(C)`/`write(C)`/`db:read`/`db:admin` derived from existing scope + allow-list; generated, fail-closed RPC table; handle ownership | §2.2 | SC-D3 |
| Scoped `ListCollections` and `Snapshot` | List filtered to allow-list; scoped `Snapshot`/`Replicate` denied in N | §2.2 | SC-D4 |
| Secure-default rollout | Warn + opt-in in N, secure defaults N+1, complete N+2; explicit permanent overrides | §3.8 | SC-D5 |
| Write outcome semantics (single record) | Fail-stop: Durable/NotApplied/Unknown/ancillary; collection fenced on Unknown; no retry, no rollback; at-least-once for single-record without an operation id | §2.3 | SR-D1 |
| Single-collection batch protocol | Option A approved: one-participant XTx, forced durability, XTx limits, legacy roots converted by opt-in | §3.1 | SR-D2 |
| Batch durability | Data + decision fsync independent of `SyncMode` | §3.1 | SR-D2 |
| Participant retirement/drop | Verified retire-on-demand at drop, rename-then-remove, refuse on any failure, no force | §3.3 | SR-D3 |
| Metadata serialization and encryption fail-closed | `metaMu` leaf lock with snapshot inside, unique temp names, memory updated after persist; evidence E1/E2/E3 → `ErrEncryptionMetaIntegrity` | §3.4 | SR-D4 |
| Scan consistency | Repeatable snapshot-at-start with lease, `MaxScanViews`, `ScanViewMaxAge` | §3.5 | SR-D5 |
| TTL visibility | One predicate, one `now` per operation, monotonic floor, reclaim grace, published reaper deletes | §3.5 | SR-D6 |
| Reserved-key behavior | `_key` immutable and preserved on update; legacy duplicates fail the keyed API, never pick a winner | §3.7 | SR-D7 |

### 6.2 Deliberate deferrals

None of these blocks implementation of the stages above; each has an interim fail-safe that is itself part of the contract.

| Deferral | Owner | Prerequisites | Interim fail-safe |
|---|---|---|---|
| Scoped partial `Snapshot` | storage/operations + security/API | participant-retirement checkpoint (§3.3); partial-manifest format reviewed under §4.2 | scoped `Snapshot` = `PermissionDenied`; export with an unrestricted key |
| Per-certificate scope/allow-list for mTLS principals | security/API | config schema mapping cert subject → scope/allow-list; matrix extension | a CA-signed client cert is unrestricted `read-write` (CA is the boundary, as documented) |
| Prefix/namespace create grants | security/API | measured tenant need; interaction with fold-uniqueness and legacy names | create limited to literal allow-list names or `db:admin` |
| Offline legacy-name migration command | engine/API + release | SC-D1 table; retirement when XTx evidence references the name | legacy names stay operable; the command, when built, must refuse with "participant retained" |
| Operation status / idempotency keys for single-record writes | API owner | proto field + durable status store (may reuse the XTx journal key table) | at-least-once documented; Unknown is never retried by engine or server |
| `DB`-level reopen API for a fenced collection | engine owner | §2.3 fencing landed | process restart is the recovery |
| Background checkpointer wiring `retire` + `checkpoint` (stamp reclamation, journal GC) | storage/ops owner | §3.3 | retire on demand at drop; verification failure refuses the drop; no force path |
| Replicated drop | replication owner | §3.3 + XTx replication design | drop is local-only and documented as divergent |
| Drain tool / downgrade of a converted root (`scriva-cli xtx drain`, not implemented) | storage/ops owner | §3.1 | backup before opt-in; converted roots are non-downgradable and old binaries refuse them without mutating |
| Automated repair of legacy duplicate `_key` | storage/ops owner | §3.7 | keyed API returns `ErrKeyIndexConflict`; operator resolves by id |
| Proto-annotation-driven RPC policy | security/API | §2.2 table + CI completeness test in place | generated table with fail-closed unclassified-method rule |

### 6.3 Pre-implementation verification items

These are facts the ratification could not establish by reading code. Each is a check the implementing PR must perform; none changes a decision, and each has a stated outcome-either-way action.

| Item | Why unverified | Action either way |
|---|---|---|
| Entropy of XTx handle ids (`server/xtx.go`) | not inspected | ownership binding ships regardless; a weak id is additionally fixed in the same PR |
| `grpc.health.v1` classification under the interceptors | not exercised | classify explicitly as a probe in the generated table; the completeness test covers it |
| REST route collision of reserved names with `/v1/{collection}/…` | not exercised | names stay reserved either way (precaution) |
| Tools that reject unknown files in a collection directory (verify/repair/snapshot) vs. the new `encryption.sentinel` | not checked | add the sentinel to every allow-list in the same PR; no format fence needed (old binaries ignore it) |
| Whether `FuzzSegmentRecovery`/`FuzzCompaction`/benchmarks exist under `./engine` | not checked | repoint Make targets if they do; otherwise remove the targets or add the tests (decide in the tooling PR) |
| Remaining non-`Insert` H5 paths (`Update`, `Delete`, CAS, `Upsert`, `reapExpired`) and staged XTx update key loss (H10) | trace-only | S1.1 and K7.2 reproduce them first; the contracts do not depend on the outcome |

Any change from these decisions must be captured in §6.1 and reflected in the affected acceptance tests before implementation begins.

## 7. Documentation and compatibility requirements

- `architecture.md` must distinguish ordinary single-record durability, batch atomic visibility, and cross-collection transaction guarantees, and says "atomic" for batches only for roots in atomic mode (§3.1 item 8). Do not claim `CommitTx`/`InsertMany` crash atomicity until stage 3 passes the crash matrix.
- Maintain explicit compatibility tests for legacy roots, mixed binaries, replication, backups, verify/repair and downgrade fences whenever durable formats change.
- Add each resolved finding to the review report with the fixing revision and test evidence; do not silently rewrite the review as though the original gap never existed.
- Keep operational recovery instructions for indeterminate outcomes, fenced/poisoned writers, retained participants (`ErrParticipantRetained`) and encryption metadata errors (`ErrEncryptionMetaIntegrity`).
- Release-note the behavior changes listed under each **Compatibility** paragraph (names, scoped-key scope, oversized `InsertMany`, `Count`/expiry, `Update` key preservation, fenced collections, secure defaults).
- This document is a **ratified design**. Implementation completion is tracked separately and must cite its code revision and evidence; do not describe a contract as "implemented" because one stage lands.
