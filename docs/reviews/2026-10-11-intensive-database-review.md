# Intensive database review — 2026-10-11

**Reviewed revision:** `614ba5af410da2bba0489fb0f6d2168022ffbb0d` (`main` at review time)
**Scope:** integrity and recovery, concurrency, security, API/business logic, edge cases, tests and maintainability.
**Method:** read-only code and documentation review, four focused Warden review tracks, and local Go checks. This is a point-in-time review, not a proof that all schedules or crash states are safe.
**Evidence recheck:** every finding was rechecked at `main` = `f1c32a0` in [ratification/evidence.md](ratification/evidence.md); the labels below follow it. The [hardening specification](../design-database-integrity-hardening.md) records the ratified fixes. **Nothing in this report is fixed yet** — it describes behavior at the reviewed/rechecked revisions.

## Executive summary

The repository has meaningful integrity protections: checksummed append-only records, index validation/rebuild, segment poisoning on some I/O failures, a durable cross-collection transaction coordinator, and concurrency/race tests. The review nevertheless found several high-impact gaps that should be addressed before treating ScrivaDB as safe against hostile input, process crashes, or all I/O failures.

Confidence labels: **CONFIRMED** means the recheck reproduced the behavior or traced it in code as stated; **UNCONFIRMED RISK** means plausible from code shape but not reproduced. A confirmed finding does not mean every deployment is exposed. The two risks the original review marked as not reproduced (stale metadata persistence, live-scan omission) were both reproduced by the recheck and are now confirmed.

The highest priorities are collection-name path traversal (including deletion outside the DB root), fail-closed authorization for every collection-scoped RPC, honest crash-atomicity guarantees for single-collection batches, and consistent handling of failed `fsync`. Additional risks affect transaction participant lifecycle, metadata races, scans during concurrent writes, and API semantics. Findings are detailed below and mapped to the companion [database integrity and security hardening specification](../design-database-integrity-hardening.md).

## Verification performed

- `go test ./... -count=1 -cover` — passed (review agent; selected coverage figures follow).
- `go test -race ./...` — passed locally.
- `go vet ./...` — passed locally.
- `go list ./internal/engine` — failed because that package path does not exist; the Makefile benchmark/fuzz targets currently reference it.
- Worktree was clean at review start. Review work made no source-code changes.

Selected coverage figures from the full test run: `engine` 87.3%, `crypto` 93.5%, `internal/xtxspec` 89.2%, `auth` 87.4%, `query` 86.5%, `store` 75.6%, `server` 74.6%, and `internal/metrics` 49.3%. Coverage is a line-execution indicator, not a measure of crash-boundary or concurrency-schedule completeness.

Passing tests and the race detector do not establish crash consistency or rule out logical races. The specific concurrency schedules and injected storage failures below still need deterministic tests.

## Findings

Severity is based on plausible impact. “Confirmed” means the code path or behavior was reproduced, or (where stated as a *code fact*) traced in code, not that every deployment is exposed. Headings carry the label from the recheck; sub-items not covered by a reproduction are called out as **unconfirmed risk**. The `H*` identifiers are those of the specification §1.2.

### Critical — collection path traversal can escape the data directory (H1; confirmed, reproduced)

`server/grpc.go` validates that collection names are non-empty, while `engine/collection.go` joins the supplied name to the DB root and `engine/db.go` removes that joined directory during drop. Names such as `../outside` can therefore target paths outside the DB root. A temporary-directory proof of concept reproduced create/delete traversal, and the recheck reproduced it again at the engine boundary (create `../outside` wrote collection files into a sibling directory; drop then deleted it including a pre-existing file). The gRPC/REST wrappers were not exercised end to end. If the unauthenticated default server is reachable, or a key that may call Create/Drop is held (see H3), the issue can be remote; H1 and H3 compose.

**Required fix:** validate collection names once at the engine boundary using a strict allowlist and path containment check; reject separators, dot components, reserved names, and platform-specific path forms. Validate every RPC and embedded API, and ensure drop never removes a path outside the canonical root. Add traversal tests for create/open/drop, snapshots, repair, and platform path variants.

### High — unsafe network and authorization defaults (H2; confirmed as code facts, severity deployment-dependent)

The default listeners bind to all interfaces (`:5433`, `:8080`, `:9090`). With an empty API key, auth accepts requests without credentials. TLS is not enabled by default. This is documented behavior but creates an unsafe out-of-box posture for a database server. The recheck confirmed it by code trace only (no runtime listener test) and found no startup warning for the no-auth case; whether it is exploitable depends on network topology.

**Required fix:** define an explicit secure-by-default listener/auth policy, with loopback-only defaults or mandatory auth/TLS before non-loopback exposure. Make insecure remote mode an explicit operator choice with startup warning. Do not silently break existing deployments; specify migration behavior and test it.

### High — collection-scoped API key authorization has gaps (H3; confirmed, reproduced at the interceptor)

The collection access check only covers request types exposing `GetCollection`. Create/drop requests identify a collection by `name`; snapshot requests do not identify one collection. Consequently, an allowlisted key can perform operations outside its collection scope. The recheck drove real request types through the interceptor with a key scoped to `["a"]`: `CreateCollection`, `DropCollection`, `ListCollections` and `Snapshot` reached the handler and returned OK, while `Insert` into another collection was correctly denied. A full `Snapshot` stream to a scoped key was **not** run end to end (the gate decision was proven); the same applies to the other collection-less RPCs (`Replicate`, `ReplicationStatus`, `Promote`).

**Required fix:** classify each RPC by its complete resource scope, including create/drop/list/snapshot and future methods. Deny ambiguous database-wide actions to scoped keys unless an explicit capability grants them. Add table-driven tests proving every RPC is either authorized against its resource or rejected.

### High — single-collection batch crash atomicity is overstated (H4; confirmed, reproduced)

`CommitTx` and `InsertMany` append ordinary independent entries and sync after the batch. A crash after a complete prefix can replay that prefix as visible data. The architecture documentation currently says `CommitTx` is all-or-nothing, which is stronger than the entry format/recovery path establishes. This is a pre-existing gap, not a newly introduced regression. The recheck reproduced it with crash injection: after two of three entries landed, reopen showed 2 of 3 records visible for both `CommitTx` and `InsertMany` with nothing acknowledged. The architecture documentation was qualified in `f1c32a0`; a remaining passage describing `InsertMany` as “a genuinely atomic engine operation” has been qualified in the consolidation (atomic against validation/quota and in-process failures, not crash-atomic).

**Required fix:** provide a durable batch decision/evidence protocol for these APIs or explicitly weaken their documented contract. To maintain ACID-style guarantees, reuse/generalize the transaction visibility mechanism so a batch is either fully visible or fully absent after recovery. Add process-kill tests at every append/sync boundary.

### High — failed `fsync` can leave visible data without normal publication (H5; confirmed for `Insert`, other paths by code trace)

In `SyncModeAlways`, single-record insert/update/delete/CAS/Upsert and TTL reap paths can (reproduced for `Insert`; the other four paths are confirmed by identical code trace only, not by an injected-failure test) mutate index/secondary-index state after append, then return on sync failure without rolling back or consistently publishing events/replication state. The caller receives an error while this process may still read the changed record, and observers can miss the change.

**Required fix:** define an explicit “outcome unknown” state for sync errors; poison the affected writer/segment until recovery or safely rollback and verify truncation. Do not leave an operation visible locally while reporting failure unless the API explicitly returns an indeterminate outcome. Keep indexes, watches, replication, and acknowledgments on the same commit decision. Inject sync failures at each path in tests. Scope note from the recheck: `InsertMany`/`CommitTx` already roll back on sync failure, so this finding is specific to single-record and reaper paths (their crash-prefix problem is H4). The reproduction: `Insert` returned `id=0` and an I/O error while `Get` and `Count` still showed the record locally and no commit was published.

### High — dropping a transaction participant can make the database fail to reopen (H6; confirmed, reproduced)

Dropping a collection removes its directory. Recovery rejects a durable committed transaction when one of its participants is missing. Since there is no runtime-safe retirement/checkpoint before drop, a normal collection drop can leave the DB permanently unable to open. Reproduced: after committing a transaction across `a` and `b`, `DropCollection("b")` returned nil and the next `Open` failed with a missing-participant error. Whether `scriva repair` can recover that state was not tested. The drop is reachable by any key permitted to call `DropCollection`, which is every key until H3 is fixed.

**Required fix:** block drop while journal evidence references the collection, unless a verified database-level checkpoint safely retires all relevant decisions and preserves backup/restore semantics. The check and drop must be serialized with transaction commit and snapshot/repair operations. Test reopen after attempted drop and after a permitted retirement.

### High — stale metadata persistence can undo encryption policy and corrupt `meta.json` (H7; confirmed, reproduced)

`setDefaultTTL` snapshots metadata under the collection lock and persists after unlocking, while encryption policy updates persist independently. Shared fixed temporary filenames also permit overlapping atomic-write attempts. A stale metadata snapshot can overwrite newer encryption metadata; on reopen, missing metadata is interpreted as no encryption. **Reproduced by the recheck**, not merely code-supported: (a) the ordered schedule (TTL snapshot taken, encryption policy applied, TTL persist resumes) left `meta.json` without the encryption block while memory still had it; after an unclean exit and reopen with the same key provider, the ciphertext envelope was returned as ordinary data and new writes were stored as plaintext in the same collection. (b) Under plain concurrency (`setDefaultTTL` and `SetEncryptionPolicy` in two goroutines, 300 iterations on one machine) 2 runs lost the encryption block and 19 produced a corrupt `meta.json`. The corrupt-file case is a second, independently confirmed defect — the shared fixed `meta.json.tmp` opened with truncation — not just a hardening note, and it falls into the same fail-open branch on reopen. These rates are for that test shape and are not a production estimate. The recheck also notes the directory `fsync` the original fix asked for is already performed.

**Required fix:** serialize metadata read-modify-write and persistence through one documented lock/sequence; use unique temp files and fsync the containing directory after rename where durability requires it. Missing/corrupt encryption metadata must fail closed when encrypted data/epochs exist. Add a barrier-controlled interleaving test and reopen validation.

### Medium — live scans can race writes/rotation (H8; confirmed, reproduced)

`streamLive` snapshots segments and consults changing index state while scanning without holding a consistent collection view. A concurrent append/rotation can make a record’s latest location fall outside the segment snapshot, omitting a row. **Reproduced by the recheck** with the existing `scanHook` barrier: a record that existed throughout, and is returned by `Get`, was silently omitted from a scan that returned `err=nil` after an update landed in a segment created by a rotation after the scan captured its segment list. Narrowing of the original wording: `XTx.Scan` is rejected by the handle, so the original claim about cross-collection transaction reads does not hold; the confirmed impact is on plain `Scan`/`Find`/`Aggregate`/`Count` non-index paths. Not every `streamLive` caller was audited. Duplicate emission of a record updated within the captured segment set, and writes after scan start being visible in some segments only, are **unconfirmed risks** from code shape.

**Required fix:** define scan isolation semantics and implement a stable read view (for example, a scan lease plus captured index generation/segment set, or a suitable lock). On generation mismatch, retry or return a documented retryable error rather than silently omitting records. Test writes and rotation interleaved at controlled scan barriers.

### Medium — rotation failure can leave the active segment sealed (H9; confirmed, reproduced)

`Segment.Seal` sets the sealed flag before syncing. If sync fails, `rotateSegment` can return while the active segment remains sealed, causing future appends to fail. A rotation error may also be reported after the record itself committed, with event publication skipped. Reproduced: with an injected sync failure at seal, the first insert returned a rotation error although the record was committed and readable, and every later append failed with an append-to-sealed-segment error until reopen. That reopen recovers was not tested (no reason to expect otherwise).

**Required fix:** make sealing/rotation state transitions recoverable and distinguish “write committed, rotation failed” from “write failed.” Poison and surface a stable error if safe continuation cannot be guaranteed; do not claim the write failed if it is durable. Add injected seal/sync/rename failure tests.

### Medium — keyed update can remove the key and permit duplicate reuse (H10; confirmed for `Update`, reproduced)

`Update(id, data)` replaces the stored payload without preserving the reserved `_key` field. A keyed record may lose its lookup key, making that key appear available for reuse. Reproduced for `Update`: after `Update`, `FindByKey` returned not-found and `InsertWithKey` with the same key succeeded. Staged XTx updates share this path by intent and code trace but were **not** separately reproduced (unconfirmed).

**Required fix:** preserve immutable reserved key metadata on updates, or reject updates that omit/change it. Apply the same rule to staged XTx writes and all update/upsert/import paths. Test key lookup, uniqueness, update, delete, and reopen behavior.

### Medium — `Count` and `Scan` disagree on expired records (H11; confirmed for match-all `Count`)

Count paths can include expired-but-not-yet-reaped records while scans filter them. Reproduced for `Count(nil)` (2 vs a scan of 1 after one record expired before the reaper ran). The single-equality secondary-index fast path and the range-index paths likely share the problem but are an **unconfirmed risk** until a table test covers them. This conflicts with the documented count behavior and can produce inconsistent query/business results.

**Required fix:** define whether expiration is logical visibility or asynchronous physical cleanup, then use the same visibility predicate for Count, scans, indexes, and queries. Test immediately before/after expiry and before/after the reaper.

### Medium — auxiliary security hardening (H12; confirmed per item as code facts, severity topology-dependent)

Review identified additional conditional exposure areas: replication transport without TLS, permissive extraction filenames/sizes, broad file permissions, a predictable Unix socket path in `/tmp`, unauthenticated metrics, wildcard CORS, and HTTP handlers without explicit timeouts. Their severity depends on deployment topology and operator configuration. The recheck confirmed each item by code trace (follower dials with insecure credentials; extraction copies the header size with no cap and no entry limit and writes `0o644`/`0o755`; `meta.json` is `0o644`; the socket is removed then listened on with the process umask; `/metrics` bypasses the auth interceptor; CORS is `*`; HTTP servers set no timeouts). None was exercised at runtime.

**Required fix:** document the threat model and apply defense-in-depth: authenticated/encrypted replication, bounded archive entries and safe extraction, restrictive file/socket permissions, private metrics by default, constrained CORS, and read/write/idle timeouts. Include negative tests for malformed archives and remote unauthenticated access.

### Low — benchmark/fuzz Makefile targets point at a missing package (H13; confirmed, reproduced)

`Makefile` references `./internal/engine` (lines 61, 65, 66), which does not exist; `go list` confirmed the path fails. Whether the referenced fuzz and benchmark functions exist under `./engine` was not checked.

**Required fix:** point targets at the actual package(s), or remove/rename targets. Add a lightweight CI check that each documented Make target resolves and runs.

## Status and resolution

All findings above are **open**; ratified fixes and their acceptance criteria are in the [hardening specification](../design-database-integrity-hardening.md) (decisions in §6.1, deliberate deferrals in §6.2). When a fix lands, add the fixing revision and test evidence here; do not rewrite the finding as though the gap never existed.

## Prioritized test additions

1. Path traversal and RPC authorization matrix (critical security gate).
2. Crash matrix for `InsertMany`, `CommitTx`, and all single-record durability paths.
3. Injected `fsync`, truncate, seal, rename, and directory-sync failures; verify reported outcome equals local reads, reopen state, watch events, and replication state.
4. Cross-collection participant drop/retirement, missing participant, snapshot and repair lifecycle tests.
5. Deterministic metadata race test across TTL/encryption persistence; reopen encrypted data afterward.
6. Scan/count/expiry tests with concurrent writes and segment rotation; include race detector runs but assert logical completeness separately.
7. Fuzz database-name/path, archive extraction, entry codecs, and recovery/journal state machines; schedule crash/soak suites in CI.
8. Validate Makefile bench/fuzz target package paths.

## Scope limitations

This review did not run a long-duration soak, filesystem/power-loss test on multiple filesystems, or exhaustive fuzz campaign. `go test -race` detects data races exercised by tests; it does not prove serializability, crash atomicity, or completeness of a concurrent scan. Security findings should be rechecked against the exact deployment configuration before release decisions.
