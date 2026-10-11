# Database integrity and security hardening — technical specification

**Status:** design draft for review; no implementation is claimed by this document.
**Input review:** [Intensive database review, 2026-10-11](reviews/2026-10-11-intensive-database-review.md).
**Related design:** [Cross-collection transaction protocol](design-cross-collection-transactions.md), [XTx fault boundaries](design-xtx-fault-boundaries.md), [architecture](architecture.md).

## 1. Purpose and outcome

This specification turns the review findings into a set of implementable contracts. The goal is that malformed names, concurrency, process crashes, and storage errors cannot silently cause path escape, partial batches, stale indexes, lost authorization boundaries, or misleading success/failure results.

The implementation is staged. Until the relevant stage lands, existing behavior must not be described as satisfying the stronger contract. In particular, passing unit/race tests is not a substitute for crash-boundary tests, and a returned I/O error is not proof that an operation was absent.

This document is intentionally more conservative than an implementation checklist. Where more than one viable design exists, it records the alternatives and the decision that must be made before code is written. Format changes, compatibility behavior, and externally observable error semantics are release-level decisions, not details to settle while implementing a local fix.

### 1.1 Threat and failure model

The design covers malformed/untrusted API input; an authenticated client exceeding its granted resource scope; concurrent goroutines in one process; abrupt process termination at any storage operation; torn writes; ordinary filesystem errors including short write, failed sync, failed truncate, failed rename, and failed directory sync; stale or corrupt derived indexes; and backup/restore or replication cuts that omit or duplicate data.

It assumes a single ScrivaDB process owns one data root under the existing directory lock and relies on the documented local-filesystem contract for atomic rename and honest `fsync`. It does not promise safety on a filesystem that lies about completed syncs, prevent a privileged local user from rewriting files, provide multi-process shared-directory access, or provide distributed consensus. If authenticated encryption is enabled, missing keys remain an operator/configuration failure; the engine must not silently interpret ciphertext as plaintext.

### 1.2 Review finding identifiers and code anchors

These identifiers are stable references for design discussions and later implementation plans. Paths are anchors, not a claim that the cited line numbers remain fixed.

| ID | Finding | Primary code/document anchors |
|---|---|---|
| H1 | Collection-name path traversal / unsafe recursive drop | `server/grpc.go` collection RPCs; `engine/collection.go:OpenCollection`; `engine/db.go:DropCollection` |
| H2 | All-interface listeners and no-auth empty-key mode by default | `server/config.go:DefaultConfig`; `internal/auth/apikey.go`; `cmd/scriva/main.go` |
| H3 | Scoped credentials do not cover every resource shape | `internal/auth/apikey.go:checkCollectionAccess`; collection create/drop/list and Snapshot RPCs |
| H4 | `InsertMany` / `CommitTx` crash-prefix visibility | `engine/collection.go` batch commit paths; `engine/segment.go` partial-line recovery; `docs/architecture.md` |
| H5 | Failed `SyncModeAlways` sync may disagree with live visibility/publication | single-record mutation paths and `syncActiveLocked` in `engine/collection.go`, `engine/keys.go`, `engine/ttl.go` |
| H6 | Dropping an XTx participant may make committed recovery impossible | `engine/db.go:DropCollection`; `engine/xtx_recovery.go`; XTx §7.2 |
| H7 | Metadata snapshots/persistence can race; encryption metadata must not be lost | `engine/ttl.go:setDefaultTTL`; `engine/encryption_admin.go`; `engine/meta.go:persistMeta`; `engine/fsync.go` |
| H8 | Scan can combine a segment snapshot with changing index/layout | `engine/scan.go:streamLive`; compaction/layout lease code |
| H9 | Seal/rotation error may leave active append target unusable | `engine/collection.go:rotateSegment`; `engine/segment.go:Seal` |
| H10 | Update may remove reserved `_key` | `engine/keys.go`; `engine/collection.go:Update`; `engine/xtx_handle.go` |
| H11 | Count and scan may disagree on expired records | `engine/count.go`; `engine/scan.go`; TTL visibility/reaper code |
| H12 | Network, socket, metrics, CORS, permissions, archive resource controls | `cmd/scriva/main.go`; `server/replication.go:extractSnapshot`; `server/config.go` |
| H13 | Makefile bench/fuzz paths point to missing package | `Makefile` bench/fuzz targets; `go list ./internal/engine` |

Review-time evidence and limits are recorded in the [review report](reviews/2026-10-11-intensive-database-review.md). H8 and H7's interleaving are plausible code-level schedules, not reproduced runtime failures; their tests must first establish or refute the exact counterexample before deciding the minimal fix.

## 2. Normative contracts

### 2.1 Names and filesystem containment

1. A collection name is validated at every public boundary and again inside the engine before any filesystem operation.
2. Accepted names are a documented, platform-independent allowlist (recommended: ASCII alphanumeric plus `_` and `-`, beginning with an alphanumeric; length bounded). Empty names, `.`/`..`, separators, drive prefixes, NUL, reserved names, and any value whose resolved path is not a direct child of the canonical DB root are rejected.
3. The root is canonicalized once. Operations must not follow a symlink from the root or a collection path to escape the root. Create/open/drop must use the same validation and containment helper.
4. Drop is never allowed to recursively remove a path outside the root, even when called through embedded APIs or repair tooling.

The allowlist in item 2 is a compatibility decision, not frozen yet. Before adoption, inventory real collection names in supported deployments and choose among: (a) strict new-name policy for all versions; (b) strict policy for newly created collections while legacy names remain operable but cannot be renamed/recreated; or (c) a versioned migration. Do not silently normalize names: two distinct inputs must never alias the same on-disk collection.

Validation must cover names arriving through Create/Open/Drop, every RPC request field, XTx participant lists, config-defined quotas/TTL/indexes, snapshot archive paths, repair commands, and embedded APIs. A safe design should resolve collection paths relative to a canonical root using platform-appropriate no-follow semantics, and must account for symlink/reparse-point substitution between validation and use. A lexical `Join`/`Clean` check alone is not sufficient for the filesystem guarantee. For destructive operations, validate the final target immediately before deletion and refuse a symlinked collection directory unless there is an explicit, separately reviewed policy.

### 2.2 Authorization and network exposure

1. Every RPC is assigned an explicit resource scope and required capability. Unknown/unclassified methods fail closed.
2. Collection-scoped credentials may act only on named collections. A database-wide snapshot requires a database-level capability; collection enumeration must either be filtered to the principal's allowlist or require database-level capability. Create/drop are checked against the target name and an explicit admin capability.
3. Defaults must not expose unauthenticated database operations to non-loopback interfaces. If backward compatibility prevents changing defaults immediately, startup must emit an actionable warning and a staged deprecation/secure-default policy must be documented.
4. Replication must authenticate peers and support encrypted transport. Metrics, REST CORS, Unix sockets, and archive extraction follow least privilege and bounded-resource defaults.

The authorization design must distinguish `collection` capability from `database-admin` capability. It must decide whether `ListCollections` returns only allowed names or is denied to collection-scoped principals; whether Snapshot accepts an explicit collection selection or is database-admin-only; and whether create is granted for a namespace/prefix or only to database admins. These choices must be represented in config/API docs and tested, not inferred from the request struct's Go interfaces.

Before implementation, generate and review a method inventory from the service descriptors rather than hand-maintaining only a partial list. For every method record: read/write/admin classification, resource extractor, required capability, whether response can disclose other collection names/data, streaming status, and unknown-resource behavior. Include nested/multi-collection requests and methods with no collection field. Interceptors should reject a newly added method until it is explicitly classified. REST-gateway routes must exercise the same authorization policy as gRPC, not a parallel weaker one.

### 2.3 Durable write outcomes

1. Every operation that reports success is recoverable according to its configured durability mode. Under `SyncModeAlways`, success means the required data and decision records have completed the defined file and directory sync sequence.
2. A failed write/sync has one of two outcomes: (a) rollback is proven complete and the operation is absent, or (b) the API returns an explicit indeterminate/outcome-unknown error and the affected writer is fenced until recovery. It must never return an ordinary failure while continuing to expose the operation as committed without documenting that ambiguity.
3. Indexes, secondary indexes, watch events, replication publication, and returned revisions derive from the same commit decision. They may not advance independently of it.
4. A batch API with atomic semantics is all-or-nothing after reopen, regardless of how many complete entry lines reached storage before a crash.

The durability contract must distinguish atomic visibility from stable-storage durability. An unacknowledged write may be present after recovery if its complete decision reached stable storage immediately before the client/process failed; it may not be partially present. An acknowledged write under `SyncModeAlways` must be present after recovery. The caller-facing outcome for an `fsync` error is `unknown` unless a successful rollback and its durability can be proved; “error” must not be interpreted by clients as “not committed.”

For an ordinary single-record operation, define two related moments: the in-process linearization point at which the new value becomes visible to subsequent readers, and the durability point at which the configured sync policy has completed. Under `SyncModeAlways`, do not publish the new value to readers/watchers/replication before the required sync succeeds. If sync fails after a full append, fence the collection and return `OutcomeUnknown`; do not serve a mixture of pre-error and post-error state while recovery could resolve differently. For `none`/`interval`, a successful operation may be visible and acknowledged before stable-storage sync by contract; document that power-loss window explicitly.

### 2.4 Recovery and derived state

1. Segment bytes plus durable transaction/batch decisions are the source of truth. Primary and secondary indexes are rebuildable caches.
2. Every replay, rebuild, compaction, scan, verification, repair, snapshot, and replication path uses the same visibility predicate for transaction-tagged entries.
3. If decision evidence, participant data, or encryption metadata is incomplete or contradictory, recovery fails closed with a diagnostic and preserves evidence; it must not fabricate a commit or downgrade encryption.
4. A collection referenced by retained committed transaction evidence cannot be dropped until a safe database-level checkpoint has retired that evidence.

### 2.5 Concurrent read semantics

1. `Scan` and `Count` use one documented visibility definition for TTL and one stable per-collection view for the duration of the operation.
2. If a scan cannot maintain its captured view because of compaction, rotation, or concurrent writes, it retries against a new view or returns an explicit retryable error; it never silently shortens results.
3. Cross-collection transaction isolation remains the contract in the XTx design. The scan fix must not imply cross-collection snapshot isolation unless that separate feature is implemented.

## 3. Design decisions and implementation requirements

### 3.1 Single-collection batches use the durable decision machinery

`InsertMany` and single-collection `CommitTx` currently write independent ordinary entries. A complete entry prefix can be replayed after a crash. The accepted target is one durable batch decision and one shared visibility rule: before a commit decision all batch entries are invisible after recovery; after a commit decision all entries are visible. Batch identity and run evidence must be verifiable and unambiguous after restart.

#### Design options to evaluate before implementation

| Option | Description | Advantages | Costs / risks |
|---|---|---|---|
| A. Reuse the DB coordinator | Route batch commits through the existing XTx journal with one participant; preserve its stamped-entry and recovery model. `BeginXTx` already models 1..N participants, but current collection-only APIs do not necessarily own a `DB` reference. | One decision format, one recovery predicate, existing crash-boundary design and status model. | May require ownership/API refactor so `Collection.InsertMany` and `CommitTx` can reach the DB coordinator; format compatibility and publication paths need review; single-collection operations become coupled to DB lifecycle. |
| B. Collection-local batch log/marker | Add a batch identity/decision record scoped to one collection. | Keeps `Collection` standalone and limits cross-collection coordination changes. | Creates a second protocol and recovery predicate; must coordinate with compaction, snapshot, replication, verify/repair and encryption; risks divergent semantics from XTx. |
| C. Weaken API contract | Keep ordinary entries, document that crash can expose a prefix. | No new format or recovery complexity. | Does not satisfy atomic batch expectations or the current architecture claim; likely unacceptable for the requested integrity goal. |

Option A is the leading design direction because it minimizes independent commit mechanisms, but is **not approved** until a prototype/design review answers the ownership, locking, standalone-collection, replication and backward-compatibility questions below. Option C is only acceptable if product/API owners explicitly decide these APIs are non-atomic and all docs/clients are changed accordingly.

#### Required batch protocol decisions

Before implementation, ratify: (1) whether batch data and decision fsync are forced independent of `SyncMode` (XTx currently forces them); (2) whether ordinary `InsertMany` and `CommitTx` share one protocol and error model; (3) what idempotency/status mechanism lets clients resolve `OutcomeUnknown`; (4) whether an unacknowledged but durable commit may recover committed (recommended yes, as with XTx); (5) whether IDs exposed during staging remain provisional; and (6) exact size/participant limits and backpressure behavior.

The implementation must preserve legacy roots and ordinary one-record byte formats. Any new on-disk format needs the same old-reader fence, recovery pre-pass, compaction rules, snapshot/backup handling, verify/repair support, and replication compatibility review used by XTx. `SyncMode` may relax single-record persistence according to its documented contract, but it must not weaken a batch's atomic visibility. The successful-ack guarantee remains governed by the configured durability mode unless the batch API explicitly requires forced durability.

### 3.2 I/O failure and commit state machine

Represent each write as explicit stages: validate → append → required sync → establish/observe decision → apply/publish indexes → publish events/replication → acknowledge. The durable decision point differs by operation class: for a transaction it is the coordinator decision; for an ordinary single-entry write it is the entry itself plus its configured sync requirement. Do not force every write through a heavyweight journal without measuring and approving the performance/format implications.

- Before decision: rollback may be attempted. If truncate/rollback is not proven durable and complete, poison the segment/collection and report an indeterminate result.
- After decision: the write is committed even if index application, rotation, or event delivery fails. Rebuild/replay repairs derived state; return/report must identify a post-commit ancillary failure rather than say the data write failed.
- A sync error on a decision record is outcome-unknown. The journal and participating writers are fenced from additional decisions until reopen/recovery resolves the state.
- Watch and replication publication are retried/recovered from committed evidence or use an outbox/cursor that cannot silently skip an acknowledged commit.

The public API must distinguish at least: `NotCommitted` (validated rollback proven), `Committed` (decision known durable under requested mode), and `OutcomeUnknown` (decision write/sync result uncertain). Internally, the engine may use typed errors/state rather than an exported enum, but server, REST, CLI, embedded API, logs, metrics, and client documentation must preserve the distinction. For `OutcomeUnknown`, define what status reference the caller can query. If no durable operation ID exists for a single-record write, do not promise exactly-once retry; document possible duplicate effects and consider idempotency keys as a separate API change.

Error mapping is part of the storage contract. gRPC status/details, REST status/body, CLI exit code, and Go error wrapping must let a caller distinguish retry-safe validation/conflict, committed-with-ancillary-failure, and outcome-unknown without parsing human text. Metrics must count unknown outcomes separately from proven aborts. Logs should include operation/transaction ID and collection but never record data payloads, API keys, encryption keys, or plaintext.

#### Failure-state table

| Boundary | Required immediate state | Recovery result | Client-visible result |
|---|---|---|---|
| Validation before bytes | No data/index/event change | Absent | Ordinary validation/conflict error |
| Append fails; rollback and rollback durability proven | Bytes removed; index unchanged | Absent | Not committed; retry may be safe |
| Append or rollback fails; tail cannot be proven | Writer fenced; no new appends; preserve bytes | Recovery decides from valid record/decision evidence or fails closed | Outcome unknown / durability error, not ordinary retry-safe failure |
| Data sync fails before transaction decision | No visibility; writer fenced; do not append decision | Transaction abort if no decision; corruption/missing evidence fails closed | Not committed only if rollback is proven; otherwise unknown |
| Decision append/sync fails | Journal fenced; do not apply speculative commit | Commit if complete durable decision validates; otherwise presumed abort; corruption fails closed | Outcome unknown; status after reopen |
| Decision durable, index/apply fails | Decision remains authoritative; process must stop serving inconsistent writes or force rebuild | Committed and indexes rebuilt | Committed result or explicit committed-with-maintenance error; never plain “not committed” |
| Decision durable, rotation/event delivery fails | Data remains committed; publication must be retried/reconstructed | Committed | Committed; ancillary failure surfaced separately |

This table must be reconciled with `docs/design-xtx-fault-boundaries.md` before changing XTx error behavior; that document already distinguishes abort from outcome-unknown at journal fsync.

Add fault injection around `Write`, short write, `Sync`, `Truncate`, rename, directory sync, index application, and publication. For each injected point assert the immediate API result, subsequent reads, emitted events, replication cursor, and state after reopen.

### 3.3 Collection lifecycle and coordinator evidence

`DropCollection` must acquire the DB-level lifecycle barrier and coordinate with transaction commit. Before removing bytes, inspect coordinator state for every retained decision referencing the collection. If evidence remains, return a specific “participant retained” error without modifying anything.

Lock order must be written down before coding. The existing XTx order begins with the XTx snapshot barrier / `DB.mu.RLock`, then collection locks in canonical order, then journal lock at the decision step. Drop and checkpoint need an order compatible with this hierarchy; no code may take `journal.mu` and then wait for a collection lock. Tests must force a transaction and drop to contend at each lifecycle boundary, not rely on sleep-based races.

Safe retirement requires a checkpoint that proves all participant effects are represented in durable materialized segments, all required snapshots/backups contain the coordinator epoch/state, and no recovery or replication path needs the original participant run. Until such proof exists, preserve journal decisions and block drop. A forced drop is not allowed to silently produce an unreopenable DB.

### 3.4 Metadata serialization and encryption fail-closed behavior

The observed race requires a deterministic test before selecting the minimal lock design. Candidate designs are: (a) a dedicated metadata mutex serializing mutation, snapshot and persist; (b) an immutable metadata value updated by generation/CAS with retrying persistence; or (c) extending the existing collection lock to cover every snapshot+persist caller. Option (c) can create lock-order problems because some paths persist while holding `c.mu` and others persist outside it. Do not add a lock until all `metaSnapshot`/`persistMeta` callers and their current lock contexts have been inventoried.

Whichever design is chosen, use unique temporary names (or a serialized fixed temp) and preserve required file and parent-directory sync ordering. A stale snapshot must never overwrite a newer policy. Failed persistence must leave memory and disk in a documented state; encryption policy changes must not switch the in-memory encryptor if durable metadata installation failed.

Encryption metadata is security-critical. If encrypted entries/epochs exist but encryption metadata is missing, malformed, stale, or inconsistent, open returns a dedicated integrity error. It must not infer plaintext mode. Recovery diagnostics must avoid logging keys or plaintext.

Tests must orchestrate TTL and encryption changes to overlap deterministically, prove the final persisted generation contains both changes, and reopen/decrypt existing records.

### 3.5 Stable scan view and shared expiration semantics

First specify scan behavior for inserts, updates, deletes, TTL expiry, compaction, and writes concurrent with the scan. The review observed a segment-list snapshot followed by reads from a live index; the exact omission schedule must be captured by a deterministic test. Candidate implementations include (a) a snapshot of immutable index state plus pinned segment handles, (b) a generation protocol with retry/restart, or (c) a read lease that delays compaction/rotation. Compare memory/latency costs and streaming backpressure before choosing. A long-lived stream must not pin unbounded segments or block writers indefinitely.

Once selected, use a per-collection view generation tied to segment layout and index publication. A scan captures one consistent generation and either honors it or returns/retries explicitly. Compaction must honor the same lease/generation rules. Document whether concurrent writes after scan start are excluded (recommended repeatable view) or included under weaker semantics.

Decide the semantic instant for expiry (`now` captured once per operation vs checked per row). Capture the clock once for scans/counts so a long query does not have a moving visibility cutoff. Create one visibility policy accounting for tombstones and expiration; Count, Scan, Find, secondary-index lookup, and aggregate inputs must agree. The reaper only changes physical cleanup state. Check whether Count intentionally includes expired records in any API mode before changing behavior, and update API docs and compatibility tests.

### 3.6 Rotation/sealing failures

Model active segment lifecycle explicitly (`ACTIVE`, `SEALING`, `SEALED`, `ROTATING`, `POISONED`) or show that existing states encode these without ambiguity. A failed sync during sealing is not equivalent to “unsealed”: kernel durability is uncertain. Determine whether the process may retry sync, must reopen/revalidate, or must poison and stop appending. Do not simply reset `sealed=false` after an error unless the on-disk tail and writer state are proven safe. If a record's commit point preceded rotation, rotation failure is ancillary and cannot suppress its event/replication publication. Add crash and in-process error cases for every transition.

### 3.7 Immutable keyed-record metadata

Before enforcing immutability, define API compatibility: is `_key` reserved and unavailable as user data, can a key be renamed through a dedicated operation, and how are legacy records with missing/duplicate keys handled? Preferred default is immutable once assigned; updates preserve it or reject attempts to change/remove it. Every path (Update, Upsert, XTx stage, import/load, replication, repair) must use the same normalization and uniqueness rule. Key-index rebuild must detect duplicate/corrupt ownership and fail or report an integrity finding rather than select an arbitrary winner.

### 3.8 Secure deployment and archive handling

- Default bind addresses should be loopback. Non-loopback binds require configured authentication and TLS, or an explicit insecure override with a prominent warning.
- Replication peers authenticate and encrypt by default; compatibility with old followers is explicit and observable.
- Archive extraction enforces an allowlist of expected regular files, canonical path containment, per-file and total uncompressed size limits, entry-count limits, and rejects symlinks/hardlinks/device nodes.
- Data and metadata files are owner-only by default where supported; Unix socket creation uses a private parent directory or explicit restrictive mode and refuses unsafe pre-existing paths.
- Metrics listener is private/loopback by default or independently authenticated. REST CORS is restrictive; HTTP read/header/write/idle timeouts and request/body limits are set.

Configuration changes require migration notes, tests for explicit legacy mode, and warnings that do not disclose secrets.

Secure defaults are a potentially breaking change. Define a two-release migration: warn when a non-loopback listener is active without auth/TLS; expose an explicit legacy override for the transition; then select secure defaults in a stated release with a clear operator migration guide. Alternatively, if immediate secure defaults are chosen, document which existing deployments stop starting and provide an explicit opt-in. Do not infer security from “empty API key” alone: validate transport exposure and auth mode together.

For snapshot extraction, strengthen lexical path checks with total decompressed byte, per-file byte, entry-count and path-depth limits; reject unknown roots/files and non-regular entries; ensure a pre-existing symlink in any parent component cannot redirect writes; and define cleanup for a partial extraction. Prefer extraction into a new private temporary directory followed by validated atomic promotion rather than modifying a live data root in place.

For file permissions, distinguish newly created files from legacy files and platform behavior. Define expected modes for data, metadata, journal, backups, and socket directory; do not chmod existing user-owned files without explicit migration. Protect the `/tmp` socket from symlink replacement and cross-user access by using a private directory or a verified ownership/mode check.

## 4. Acceptance test matrix

| Area | Required test evidence |
|---|---|
| Path containment | Table-driven malicious names across engine, gRPC, REST, CLI, embedded API, Windows-style separators/drive paths; prove no outside file is created/deleted. |
| Authorization | Exhaustive RPC-to-scope matrix for admin, collection-scoped read/write, missing key, and unknown method; include Snapshot/Create/Drop/List. |
| Batch atomicity | Child-process crash at every append/decision/fsync boundary; after reopen batch is wholly absent or wholly present, never a prefix. |
| I/O outcome | Inject append/sync/truncate/rename/directory-sync errors; correlate returned error with immediate reads, indexes, watches, replication, and reopened state. |
| Participant lifecycle | Drop during/after XTx; blocked drop leaves bytes unchanged; valid checkpoint then drop/reopen succeeds; missing participant fails closed with actionable diagnosis. |
| Metadata concurrency | Deterministic interleaving of TTL and encryption metadata writes; final metadata retains both; encrypted data reopens correctly; malformed/missing metadata fails closed. |
| Scan stability | Controlled scan/write/rotation/compaction schedule; no missing/duplicate records relative to documented start-time view; run under `-race`. |
| Expiration/count | Same results across Count/Scan/query before expiry, after expiry, before reaper, after reaper, restart, and index rebuild. |
| Rotation | Faults at seal/sync/create/rename/dir-sync boundaries; no permanently sealed active segment, false failure for committed write, or lost event. |
| Key invariants | Update/transaction/import cannot remove/change reserved key; uniqueness survives reopen and compaction. |
| Security hardening | TLS/auth defaults, socket permissions, metrics isolation, CORS/timeouts; archive traversal, symlink and decompression-bomb rejection. |
| Tooling | Every Makefile test/bench/fuzz target resolves; CI runs vet, race suite, focused fuzz smoke tests, and scheduled longer crash/soak suites. |

All tests must assert logical state, not only absence of data races. Crash tests should use subprocess termination or a fault-injected filesystem, with deterministic seeds and a retained failure artifact.

The minimum batch crash matrix is: after first complete entry; after each later entry; after all entries before sync; during each participant/segment sync; after all data syncs before decision append; partial decision write; complete decision write before sync; failed decision sync; successful decision sync before index apply; during primary/secondary index apply; before/after event publication; and before client acknowledgment. For one-participant batches, this still tests the same semantic boundary as a multi-participant XTx.

The minimum authorization matrix includes unrestricted admin, collection-scoped read, collection-scoped read-write, unknown/missing key, and anonymous access; each against Find/Insert, BeginXTx with allowed and mixed participants, Create/Drop of allowed and disallowed names, ListCollections, Snapshot, metrics, replication, and an unknown/future method. Assert both status and data disclosure. The path test corpus includes empty, dot, dot-dot, repeated separators, absolute Unix paths, drive/UNC paths, NUL, Unicode normalization variants, reserved `xtx.*`, long names, symlink/reparse-point targets, and names colliding under case-insensitive filesystems.

The I/O matrix must compare four observations for every operation: returned result, same-process point read and secondary-index lookup, emitted watch/replication sequence, and state after close/reopen. For batch recovery also compare the entire ordered record set and revision sequence, not only counts. This catches cases where primary and secondary indexes agree with each other but both disagree with durable segments.

The scan test should use barriers at: captured segment set, captured index/version state, each segment boundary, each row lookup, compaction swap, rotation, and visitor callback. Assert no duplicate IDs, no omitted IDs relative to the chosen view, stable revisions/data, and that cancellation/backpressure releases all leases. Include updates and deletes to an ID that moves to a segment absent from the initial segment set.

For metadata race tests, inject a barrier after `metaSnapshot` and before `persistMeta`: concurrently change encryption policy, release both writers in both orders, then inspect exact JSON and reopen. Repeat with TTL and ID-counter updates. Verify a failed `persistMeta` leaves memory, encryptor pointer, and on-disk generation consistent according to the chosen failure contract.

### 4.1 Test harness requirements

The test harness must separate three failure classes: process crash (kernel page cache survives), machine/power-loss model (only explicitly synced state survives), and returned I/O errors (the syscall returns an error but partial effects may remain). A fake filesystem must model short writes, successful write with failed sync, failed truncate, rename failure, directory-sync failure, and reordering where relevant. A subprocess kill test alone is not a power-loss simulation.

For each crash matrix row, record exact durable files/bytes before recovery and assert one of: complete old state, complete new state, or documented fail-closed error. Never accept a mixed batch/transaction state. Reopen twice to assert recovery idempotence. Run each seed repeatedly and retain failing roots/artifacts. Test on supported OS/filesystem CI where practical; explicitly mark assumptions that cannot be simulated.

### 4.2 Compatibility gates for any on-disk change

Before a new format is enabled by default, require: old binary rejects safely (does not mutate/quarantine); new binary opens legacy roots unchanged; verify/repair understand the format or refuse safely; snapshots and restored snapshots preserve decision evidence; replication cannot flatten/partially apply new records; compaction and encryption migration preserve stamps/decision digests; downgrade behavior is documented and tested; and interrupted upgrade/first-write initialization is recoverable. Roll out a reader/fence release before a writer release if an older binary might otherwise silently accept the new bytes, following the existing XTx staged-release precedent.

### 4.3 CI tiers

Per-PR: unit/integration, `go test -race ./...`, `go vet ./...`, lint, short deterministic crash matrix, fuzz smoke (bounded duration), Make target validation. Nightly or scheduled: longer fuzz campaigns, randomized crash/reopen model tests, concurrent scan/write/compaction stress, and soak/resource-leak checks. Release candidate: full crash matrix on supported platforms and restore/replication compatibility matrix. A green per-PR suite is not a substitute for scheduled suites.

## 5. Delivery sequence

1. **Containment and authorization:** collection name validation, path-safe drop, full RPC scope matrix; block the highest-risk remote/destructive paths.
2. **Outcome truthfulness:** define and implement write/sync/rotation failure states; add fault-injected storage tests.
3. **Atomic batches:** durable single-collection batch protocol, old-reader fence, recovery, indexes, compaction, snapshot/verify/repair/replication support.
4. **Lifecycle and metadata:** participant retirement/drop barrier, serialized metadata persistence, encryption fail-closed checks.
5. **Read consistency and business semantics:** stable scans, common TTL visibility, immutable key preservation.
6. **Deployment and test infrastructure:** secure network defaults and archive/permissions hardening; fix Make targets and broaden fuzz/soak CI.

Stages 1 and 2 are release blockers for remotely reachable deployments. Stage 3 is a correctness blocker for claiming durable atomic batch transactions. Stage 4 is a blocker for safe lifecycle management of databases that have used XTx. Any stage may be split into smaller implementation plans, but its acceptance criteria remain mandatory.

No stage should be started directly from this draft without first producing a short design decision record for its unresolved choices. That record should include alternatives rejected, compatibility impact, performance expectations, recovery behavior, API/error semantics, lock ordering, and test artifacts. Security-critical path containment and authorization can proceed as separately reviewed, narrowly scoped fixes once their API compatibility choices are ratified; they do not depend on the batch format decision.

## 6. Unresolved decisions before implementation

| Decision | Proposed default | Must be resolved by |
|---|---|---|
| Collection-name policy for legacy names | Strict for new names; preserve operability of existing names pending migration policy | Engine/API owner + release owner |
| Scoped `ListCollections` | Filter to allowlist; deny database-wide snapshot without admin capability | Security/API owner |
| Batch commit format | Prototype reuse of existing XTx coordinator for one-participant batches; reject duplicate local protocol unless a constraint requires it | Storage/engine design review |
| Batch durability | Force data and decision sync like XTx, independent of `SyncMode`, unless product explicitly accepts weaker acknowledged durability | Storage/API owner |
| Single-record sync error | Outcome unknown and writer fenced until recovery; define operation-status/idempotency capability | Storage/API owner |
| Metadata lock strategy | Inventory all callers/lock contexts first; avoid nested lock order expansion | Engine owner |
| Scan semantics | Repeatable per-collection view at scan start, with bounded resource lifetime | Query/API owner |
| TTL clock semantics | One captured `now` per operation | Query/API owner |
| Secure defaults rollout | Warn + explicit compatibility override, then secure defaults on a stated release boundary | Release/operator owner |
| Participant retirement/drop | Block until checkpoint proof and snapshot/replication implications are explicit | Storage/operations owner |

Any change from these defaults must be captured in this table and reflected in the affected acceptance tests before implementation begins.

## 7. Documentation and compatibility requirements

- Update `architecture.md` to distinguish ordinary single-record durability, batch atomic visibility, and cross-collection transaction guarantees. Do not claim `CommitTx` crash atomicity until stage 3 passes the crash matrix.
- Maintain explicit compatibility tests for legacy roots, mixed binaries, replication, backups, verify/repair, and downgrade fences whenever durable formats change.
- Add each resolved finding to the review report with the fixing revision and test evidence; do not silently rewrite the review as though the original gap never existed.
- Keep operational recovery instructions for indeterminate outcomes, poisoned writers, retained participants, and encryption metadata errors.
- Keep this document marked “design draft” until all decisions in §6 are ratified. Implementation completion is tracked separately and must cite its code revision and evidence; do not change the status to “implemented” merely because one stage lands.
