# Intensive database review — 2026-10-11

**Reviewed revision:** `614ba5af410da2bba0489fb0f6d2168022ffbb0d` (`main` at review time)
**Scope:** integrity and recovery, concurrency, security, API/business logic, edge cases, tests and maintainability.
**Method:** read-only code and documentation review, four focused Warden review tracks, and local Go checks. This is a point-in-time review, not a proof that all schedules or crash states are safe.

## Executive summary

The repository has meaningful integrity protections: checksummed append-only records, index validation/rebuild, segment poisoning on some I/O failures, a durable cross-collection transaction coordinator, and concurrency/race tests. The review nevertheless found several high-impact gaps that should be addressed before treating ScrivaDB as safe against hostile input, process crashes, or all I/O failures.

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

Severity is based on plausible impact; “confirmed” means the code path or behavior was reproduced/verified in review, not that every deployment is exposed.

### Critical — collection path traversal can escape the data directory (confirmed)

`server/grpc.go` validates that collection names are non-empty, while `engine/collection.go` joins the supplied name to the DB root and `engine/db.go` removes that joined directory during drop. Names such as `../outside` can therefore target paths outside the DB root. A temporary-directory proof of concept reproduced create/delete traversal. If the unauthenticated default server is reachable, the issue can be remote.

**Required fix:** validate collection names once at the engine boundary using a strict allowlist and path containment check; reject separators, dot components, reserved names, and platform-specific path forms. Validate every RPC and embedded API, and ensure drop never removes a path outside the canonical root. Add traversal tests for create/open/drop, snapshots, repair, and platform path variants.

### High — unsafe network and authorization defaults

The default listeners bind to all interfaces (`:5433`, `:8080`, `:9090`). With an empty API key, auth accepts requests without credentials. TLS is not enabled by default. This is documented behavior but creates an unsafe out-of-box posture for a database server.

**Required fix:** define an explicit secure-by-default listener/auth policy, with loopback-only defaults or mandatory auth/TLS before non-loopback exposure. Make insecure remote mode an explicit operator choice with startup warning. Do not silently break existing deployments; specify migration behavior and test it.

### High — collection-scoped API key authorization has gaps

The collection access check only covers request types exposing `GetCollection`. Create/drop requests identify a collection by `name`; snapshot requests do not identify one collection. Consequently, an allowlisted key can potentially perform operations outside its collection scope or obtain a database-wide snapshot.

**Required fix:** classify each RPC by its complete resource scope, including create/drop/list/snapshot and future methods. Deny ambiguous database-wide actions to scoped keys unless an explicit capability grants them. Add table-driven tests proving every RPC is either authorized against its resource or rejected.

### High — single-collection batch crash atomicity is overstated

`CommitTx` and `InsertMany` append ordinary independent entries and sync after the batch. A crash after a complete prefix can replay that prefix as visible data. The architecture documentation currently says `CommitTx` is all-or-nothing, which is stronger than the entry format/recovery path establishes. This is a pre-existing gap, not a newly introduced regression.

**Required fix:** provide a durable batch decision/evidence protocol for these APIs or explicitly weaken their documented contract. To maintain ACID-style guarantees, reuse/generalize the transaction visibility mechanism so a batch is either fully visible or fully absent after recovery. Add process-kill tests at every append/sync boundary.

### High — failed `fsync` can leave visible data without normal publication

In `SyncModeAlways`, single-record insert/update/delete/CAS/Upsert and TTL reap paths can mutate index/secondary-index state after append, then return on sync failure without rolling back or consistently publishing events/replication state. The caller receives an error while this process may still read the changed record, and observers can miss the change.

**Required fix:** define an explicit “outcome unknown” state for sync errors; poison the affected writer/segment until recovery or safely rollback and verify truncation. Do not leave an operation visible locally while reporting failure unless the API explicitly returns an indeterminate outcome. Keep indexes, watches, replication, and acknowledgments on the same commit decision. Inject sync failures at each path in tests.

### High — dropping a transaction participant can make the database fail to reopen

Dropping a collection removes its directory. Recovery rejects a durable committed transaction when one of its participants is missing. Since there is no runtime-safe retirement/checkpoint before drop, a normal collection drop can leave the DB permanently unable to open.

**Required fix:** block drop while journal evidence references the collection, unless a verified database-level checkpoint safely retires all relevant decisions and preserves backup/restore semantics. The check and drop must be serialized with transaction commit and snapshot/repair operations. Test reopen after attempted drop and after a permitted retirement.

### High — stale metadata persistence can undo encryption policy (concurrency risk)

`setDefaultTTL` snapshots metadata under the collection lock and persists after unlocking, while encryption policy updates persist independently. Shared fixed temporary filenames also permit overlapping atomic-write attempts. A stale metadata snapshot can overwrite newer encryption metadata; on reopen, missing metadata may be interpreted as no encryption. This interleaving is supported by the code path but was not reproduced under a deterministic runtime schedule.

**Required fix:** serialize metadata read-modify-write and persistence through one documented lock/sequence; use unique temp files and fsync the containing directory after rename where durability requires it. Missing/corrupt encryption metadata must fail closed when encrypted data/epochs exist. Add a barrier-controlled interleaving test and reopen validation.

### Medium — live scans can race writes/rotation

`streamLive` snapshots segments and consults changing index state while scanning without holding a consistent collection view. A concurrent append/rotation can make a record’s latest location fall outside the segment snapshot, potentially omitting a row and undermining the per-collection consistency relied on by cross-collection transaction reads. This is a static schedule assessment; no runtime reproducer was completed.

**Required fix:** define scan isolation semantics and implement a stable read view (for example, a scan lease plus captured index generation/segment set, or a suitable lock). On generation mismatch, retry or return a documented retryable error rather than silently omitting records. Test writes and rotation interleaved at controlled scan barriers.

### Medium — rotation failure can leave the active segment sealed

`Segment.Seal` sets the sealed flag before syncing. If sync fails, `rotateSegment` can return while the active segment remains sealed, causing future appends to fail. A rotation error may also be reported after the record itself committed, with event publication skipped.

**Required fix:** make sealing/rotation state transitions recoverable and distinguish “write committed, rotation failed” from “write failed.” Poison and surface a stable error if safe continuation cannot be guaranteed; do not claim the write failed if it is durable. Add injected seal/sync/rename failure tests.

### Medium — keyed update can remove the key and permit duplicate reuse

`Update(id, data)` replaces the stored payload without preserving the reserved `_key` field. A keyed record may lose its lookup key, making that key appear available for reuse.

**Required fix:** preserve immutable reserved key metadata on updates, or reject updates that omit/change it. Apply the same rule to staged XTx writes and all update/upsert/import paths. Test key lookup, uniqueness, update, delete, and reopen behavior.

### Medium — `Count` and `Scan` disagree on expired records

Count paths can include expired-but-not-yet-reaped records while scans filter them. This conflicts with the documented count behavior and can produce inconsistent query/business results.

**Required fix:** define whether expiration is logical visibility or asynchronous physical cleanup, then use the same visibility predicate for Count, scans, indexes, and queries. Test immediately before/after expiry and before/after the reaper.

### Medium — auxiliary security hardening

Review identified additional conditional exposure areas: replication transport without TLS, permissive extraction filenames/sizes, broad file permissions, a predictable Unix socket path in `/tmp`, unauthenticated metrics, wildcard CORS, and HTTP handlers without explicit timeouts. Their severity depends on deployment topology and operator configuration.

**Required fix:** document the threat model and apply defense-in-depth: authenticated/encrypted replication, bounded archive entries and safe extraction, restrictive file/socket permissions, private metrics by default, constrained CORS, and read/write/idle timeouts. Include negative tests for malformed archives and remote unauthenticated access.

### Low — benchmark/fuzz Makefile targets point at a missing package

`Makefile` references `./internal/engine`, which does not exist; `go list` confirmed the path fails.

**Required fix:** point targets at the actual package(s), or remove/rename targets. Add a lightweight CI check that each documented Make target resolves and runs.

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
