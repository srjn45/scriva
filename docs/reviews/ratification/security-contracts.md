# Ratified security contracts — names, containment, authorization, secure defaults

**Status:** ratified design decisions for the [database integrity and security hardening spec](../../design-database-integrity-hardening.md) (§2.1, §2.2, §3.8, §6). **Documentation only — nothing here is implemented**, unless a "Today" line says so. Companion inputs: the [review report](../2026-10-11-intensive-database-review.md) (findings H1, H2, H3, H12).
**Run:** autopilot `ap-790b4f397cb1`, task `ratify-security-contracts`. Sibling files `evidence.md` and `storage-read-contracts.md` cover evidence and storage/read contracts; a later consolidation folds all three into the spec.

**Conventions.** "Release N" is the first release that contains a decision; N+1, N+2 are the following releases (version numbers are the release owner's call). *Security fix* = ships in N with no opt-out, because it closes a hole rather than changing a default. Review risks that were not reproduced are marked *(unverified)*. Code references were read at `main` = `f1c32a0`.

---

## D1. Collection-name policy (resolves spec §2.1 items 1–2 and the §6 "legacy names" row)

### Today (verified in code)
- `server/grpc.go:CreateCollection` only rejects `""`; `DropCollection`/`ListCollections` do no validation. `engine.OpenCollection` (`engine/collection.go`) rejects only the `xtx.` prefix (`isReservedXTxName`) and then runs `filepath.Join(dataDir, name)` + `os.MkdirAll`. `DB.DropCollection` runs `os.RemoveAll(filepath.Join(dataDir, name))`. `CollectionWithConfig` and XTx participant names reach the same join.
- `DB.Open` → `recoverCoordinator` (`engine/xtx_recovery.go`) loads **every** directory in the root as a collection (`e.IsDir()`), so any directory name that exists on disk is a collection name. A directory with an `xtx.` prefix aborts open with `ErrReservedName`.

### Decision
1. **One function** `engine.ValidateCollectionName(name) error` (typed `ErrInvalidName`, wrapping `ErrReservedName` for reserved cases) is the only name check. It is called by `OpenCollection`, `CreateCollection*`, `CollectionWithConfig`, `DropCollection`, `Collection()`, XTx begin/stage participant resolution, and follower `ApplyReplication`. The gRPC layer maps it to `InvalidArgument`; REST/CLI inherit that. No layer keeps a private check.
2. **Grammar for new names:** `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` — ASCII only, 1–64 bytes, starts alphanumeric, **no dots**, no whitespace, no non-ASCII. Dots are excluded to remove `.`/`..`/hidden/extension and Windows trailing-dot ambiguity in one rule; the existing `xtx.` prefix is therefore unreachable by new names but is still checked for legacy input.
3. **Case:** names are case-preserving and compared exactly everywhere (lookup, allow-lists), but **uniqueness is case-insensitive (ASCII fold)**: creating `Users` while `users` exists returns `AlreadyExists`. Rationale: a case-insensitive filesystem (macOS/Windows default) would otherwise alias two "distinct" names onto one directory.
4. **Unicode:** non-ASCII (including invalid UTF-8, NUL, controls, bidi, full-width forms) is **rejected, never normalized**. Normalization would let two inputs alias one directory (spec §2.1 forbids this).
5. **Reserved (case-insensitive):** prefix `xtx.` (existing); exact names `xtx`, `collections`, `replication` (REST route segments `/v1/xtx/…`, `/v1/collections`, `/v1/replication/…` could collide with `/v1/{collection}/…` — a precaution, collision behavior *(unverified)*); Windows device names `con prn aux nul com1–9 lpt1–9`; and the engine's own root file names (`LOCK`, `xtx.journal`, `xtx.journal.tmp`, `xtx.format`).
6. **Legacy names** (a directory already on disk whose name fails 2–5 but is a single path component, not a symlink, not `xtx.`-prefixed) are **"legacy-named"**:

| Operation | Legacy-named collection | Rationale |
|---|---|---|
| Open at startup | Loads normally; one `WARN` per name; gauge `scriva_legacy_collection_names` | Never make an existing DB unopenable for a naming rule |
| Read / write / index / watch / XTx participant | Allowed (name is looked up exactly; it must already exist) | Preserves data availability; XTx recovery already references names |
| `CreateCollection` of that name | `InvalidArgument` (policy checked **before** existence) | Policy is about *creating* names |
| `DropCollection` | Allowed for database-level admin, via the containment helper (D2) | Operators must be able to get rid of them |
| Rename | No rename RPC is added. Migration is an explicit **offline** operator command that copies to a valid name; never automatic at open | Journal evidence references names (§3.3) |
| Scoped allow-list entry naming it | Accepted (exact match); `WARN` at config load | Allow-lists may legitimately name legacy data |
| Two legacy names equal under case-fold | Both load (case-sensitive FS only); `WARN` naming the pair; create of a third fold-equal name refused | Never choose a winner |

7. **Not legacy-nameable (hard refusal):** a root entry that is a symlink, or whose name cannot be a single component, is skipped at discovery with an `ERROR` log and `scriva_skipped_root_entries`; it is never opened, created over, or dropped (see D2).
8. **Config-supplied names** (quotas, default TTL, indexes, key allow-lists) use the strict grammar for *new* entries, but entries that match an existing legacy directory are accepted with a warning. A literal glob such as `"*"` in an allow-list is rejected at load starting N+1 (warn in N): today it silently matches nothing.

### Rationale
Closes H1 at the single choke point the review identified, with the smallest grammar that is safe on every supported filesystem. Keeping legacy names *operable but not creatable* is the only option that neither strands data nor re-admits unsafe names.

### Rejected alternatives
- **Lowercase-only:** rejected — breaks common mixed-case names for no safety gain once fold-uniqueness exists.
- **Allow dots/Unicode with NFC normalization:** rejected — normalization aliasing and Windows/macOS filename rules cost more than they give.
- **Auto-rename or auto-migrate legacy names at open:** rejected — changes on-disk identity under XTx journal evidence and replication peers.
- **Refuse to open DBs containing legacy names:** rejected — turns a validation rule into an outage.
- **Server-only validation (gRPC):** rejected — the embedded façade and engine are public API (`scriva.go`, `engine/`).

### Compatibility impact
New creates with spaces, dots, non-ASCII or >64 bytes start failing with `InvalidArgument` (previously worked, and some forms escaped the root). Existing data is unaffected. Clients that create `collections`/`xtx` named collections break (intended).

### Rollout alternatives considered
(a) validation-on-create only in N, validation on every path in N+1 — rejected: the escape is in Open/Drop, which must be fixed in N. (b) opt-in `strict_names` flag — rejected: a flag cannot gate a traversal fix. **Chosen: all paths in N, no flag.**

### Acceptance criteria
- **N-1** Table-driven corpus (`""`, `.`, `..`, `a/b`, `a\b`, `../x`, `/abs`, `C:\x`, `\\h\s`, NUL, `a b`, `é`, NFC/NFD pair, 65-byte name, `xtx`, `xtx.foo`, `CON`, `nul`) → `ErrInvalidName`/`ErrReservedName` from engine `OpenCollection`, `CreateCollection`, `DropCollection`, `Collection`, gRPC create/drop/stage/begin (all → `InvalidArgument`), REST and CLI. After every case the directory tree under and **outside** the root is byte-identical.
- **N-2** Create `Users` after `users` → `AlreadyExists`; both lookups are case-exact; no second directory appears.
- **N-3** Fixture root with dirs `legacy name`, `Ünï`, `a.b` (valid single components): `Open` succeeds, all three readable/writable, `CreateCollection("a.b")` → `InvalidArgument`, `DropCollection("a.b")` succeeds and removes only that directory, warnings and gauge emitted.
- **N-4** Fixture root containing a symlink entry `evil -> /elsewhere`: open succeeds without loading it; `CreateCollection("evil")`/`DropCollection("evil")` fail; target untouched.
- **N-5** Open of a root with `xtx.foo` directory still fails with `ErrReservedName` (existing behavior preserved).

---

## D2. Path and symlink containment (resolves spec §2.1 items 3–4, §3.8 archive bullets)

### Today (verified in code)
- No canonicalization of `dataDir`; `Join` without containment or `Lstat`. `OpenCollection` calls `MkdirAll` on a path that may be a symlink and then opens segments through it; `DropCollection` `RemoveAll`s the joined path.
- `Collection.writeSnapshot` (`engine/snapshot.go`) enumerates `os.ReadDir(c.dir)`, skips dotfiles/`index.json`/compaction manifest and `e.IsDir()` entries, and `writeFileToTar` uses `os.Open` + `Stat`, i.e. it **follows a symlink file** and archives its target's bytes under the entry's name.
- `server/replication.go:extractSnapshot` checks `filepath.Clean` lexically, silently `continue`s on non-regular entries, `MkdirAll(0o755)`, `OpenFile(0o644)` into the **live** data dir, with no size/count/depth limit and no handling of pre-existing symlink parents or partial extraction.

### Decision
1. **Root:** `dataDir` is made absolute and `EvalSymlinks`-resolved **once** in `Open`; the resolved path is stored and is the only base for later joins. A symlinked root (common for operators) is allowed; everything below it must be real.
2. **Collection directories** must be real directories: `Lstat` at discovery, create, open and drop; a symlink or non-directory refuses with `ErrUnsafePath`. Creation uses `Mkdir` (not `MkdirAll`) and fails if the entry exists but is not a pre-existing real directory. The implementation mechanism is the implementer's choice (`os.Root`/`openat`-style no-follow resolution is available since `go 1.24.0` in `go.mod`); the **guarantee** is: no path component below the root is resolved through a symlink/reparse point, including between validation and use.
3. **Drop:** validate name (D1; legacy minimum = single component) → `Lstat` target is a non-symlink directory whose parent is the resolved root → `RemoveAll`. (`os.RemoveAll` unlinks symlinks inside rather than following them.) Embedded and repair callers use the same helper; there is no force/bypass flag.
4. **Export** (`SnapshotTo`, stream or file): emit only regular files, `Lstat`-checked, from an allow-list of engine file kinds (segments, secondary-index files, `meta`, XTx coordinator files); symlink and unknown files are skipped with a counted `WARN`, never followed. Archive names are `<validated-collection>/<file>` or a known root file.
5. **Import / restore / follower bootstrap:** extract into a **new `0700` temp directory created next to the target** (same filesystem), never into a live root; accept only: regular files, depth ≤ 2, names matching the same allow-list and D1 (legacy minimum) rules, no duplicate names; reject symlink, hardlink, device, FIFO, `..`, absolute, or unknown-root entries with an error (not `continue`). Limits (configurable, defaults): ≤ 1,048,576 entries; per-file ≤ 8 GiB and exactly equal to header `Size`; total declared bytes ≤ free space on the target filesystem minus 5%. On any error the temp directory is removed. Success promotes by `rename` of the temp directory into place (follower bootstrap requires an empty target, as today), then `fsync` of the parent. File modes: `0600`, directories `0700` (D5).
6. **Never chmod/chown pre-existing user files** (spec §3.8); permission tightening applies to files the server creates.

### Rationale
Lexical checks alone cannot stop a pre-existing symlink parent or a swap between check and use (spec §2.1 already says so). Import into a temp dir makes partial/hostile archives harmless to the live root.

### Rejected alternatives
- **Lexical `Clean`/`HasPrefix` only:** rejected — insufficient (spec §2.1).
- **`EvalSymlinks` on every path before use:** rejected — check-then-use race; fine as a defense in depth, not as the guarantee.
- **Following symlinked collection dirs under an explicit allow policy:** rejected for now — no demonstrated need; would need a separately reviewed policy per spec §2.1.
- **Extract in place and roll back on error:** rejected — a crash leaves a half-restored live root.
- **Silently skipping bad archive entries (today's `continue`):** rejected — hides tampering.

### Compatibility impact
Deployments that symlink an individual collection directory into the root: discovery already skips the entry (`ReadDir` reports the symlink type, so `IsDir()` is false), but opening/creating it by name can still go through the link today; that path now refuses with `ErrUnsafePath`. Backups created by this export remain restorable by older binaries (same layout). Restores with archives containing extra top-level files now fail loudly.

### Rollout alternatives considered
Gate extraction limits behind a flag — rejected: defaults are generous enough for any real archive; the flag exists only to *raise* limits. Gate symlink refusal behind `allow_symlinked_collections` — rejected, see above.

### Acceptance criteria
- **C-1** Collection dir replaced by a symlink to a temp dir outside the root between `Open` and `DropCollection`: drop returns `ErrUnsafePath`; outside tree intact.
- **C-2** Root passed as a symlink to a real dir: all operations succeed and files appear only under the resolved target.
- **C-3** Export of a collection containing a symlink file pointing outside the root: archive has no entry for it (and never the target's bytes); counted warning present.
- **C-4** Table of hostile archives (`../x`, `/abs`, `a/../../b`, symlink, hardlink, char device, depth 3, unknown root file, duplicate name, header size ≠ bytes, > entry limit, > size limit, pre-existing symlink as target parent): every case returns an error, leaves the live root byte-identical and leaves **no** temp directory.
- **C-5** Kill the process mid-extraction (fault-injected FS or child SIGKILL): target dir is absent/empty and only a stale temp dir remains, removed on next start.
- **C-6** Newly created data/segment files are `0600`, collection dirs `0700` on Unix; pre-existing files keep their mode.

---

## D3. Resource-capability model and RPC table (resolves spec §2.2 items 1–2 and the method-inventory requirement)

### Today (verified in code)
`internal/auth/scope.go`: two scopes (`read`, `read-write`); `methodRequiresWrite` consults `writeMethods`/`readMethods` and treats **unknown methods as writes**. `apikey.go:checkCollectionAccess` enforces the allow-list only for request types exposing `GetCollection()` or `GetCollections()` (unary: before handler; streams: on first `RecvMsg`). Consequences by reading the code (to be proven by the D3 tests): `CreateCollection`/`DropCollection` expose `name`, `Snapshot`/`Replicate`/`ListCollections`/`ReplicationStatus`/`Promote` expose nothing, so a scoped key passes them unchecked; `FindByKey` and `Aggregate` are in neither map, so a `read` key is denied them as "unknown". mTLS principals are `read-write` with **no allow-list** (`mtls.go`). `Promote` requires only `read-write`. The XTx handle id is not tied to the principal that created it (no principal reference in `server/xtx.go`); whether ids are unguessable was not verified.

### Decision
**Model.** A principal has a *scope* (`read` | `read-write`, unchanged config spelling) and an optional *collection allow-list* (unchanged). From these, three capability levels on two resource kinds:

| Capability | Resource | Held by |
|---|---|---|
| `read(C)` | collection `C` | any principal; if allow-list present, only for `C` in it |
| `write(C)` | collection `C` | scope `read-write`, and `C` in allow-list if present |
| `db:read` | the database as a whole | scope `read`/`read-write` **without** an allow-list |
| `db:admin` | the database as a whole | scope `read-write` **without** an allow-list (so every existing unrestricted read-write key and every mTLS principal keeps full access) |

No new config key and no new scope value in N. An allow-list therefore *removes* all `db:*` capabilities — that is the whole change. **Interceptor rules:** (a) every full method name must appear in one explicit classification table generated from the service descriptor; a CI test fails if `ServiceDesc.Methods/Streams` contains a method with no entry, and an unclassified method at runtime is denied to everyone except `db:admin` (fail closed, as today). (b) The resource extractor reads **all** collection-bearing fields (`collection`, `collections`, `name`) — not only `GetCollection()`. (c) A multi-collection request is all-or-nothing: one disallowed name denies the whole RPC with `PermissionDenied`, and the error never reveals whether a *different* collection exists. (d) `NotFound` vs `PermissionDenied`: a scoped principal asking for an out-of-list collection always gets `PermissionDenied` regardless of existence (no existence oracle).

**RPC table (all 34 RPCs in `proto/scriva.proto`; REST routes map 1:1 through the same gRPC interceptors):**

| RPC | Class | Resource extractor | Required capability | Note (today → decision) |
|---|---|---|---|---|
| `CreateCollection` | admin | `name` | `db:admin`; **or** `write(name)` when `name` is literally in the principal's allow-list | gap today: unchecked for scoped keys |
| `DropCollection` | admin | `name` | `db:admin` only | scoped keys may not drop (destructive, XTx-evidence interaction, spec §3.3) |
| `ListCollections` | read | none | `db:read` → all names; scoped → **filtered** to allow-list ∩ existing (D4) | |
| `Insert`, `InsertMany`, `Update`, `Delete`, `Upsert`, `UpdateByKey`, `DeleteByKey`, `UpdateIfRev` | write | `collection` | `write(C)` | `Upsert`/`*ByKey`/`UpdateIfRev` rely on unknown→write today; made explicit |
| `FindById`, `Find`, `FindByKey`, `Aggregate` | read | `collection` | `read(C)` | `FindByKey`/`Aggregate` become explicit reads (read keys gain them) |
| `Watch` | read | `collection` (first message) | `read(C)` | already enforced on first `RecvMsg` |
| `CollectionStats`, `ListIndexes` | read | `collection` | `read(C)` | |
| `EnsureIndex`, `DropIndex`, `Compact` | write | `collection` | `write(C)` | |
| `BeginTx` | write | `collection` | `write(C)`; tx handle bound to principal | |
| `CommitTx`, `RollbackTx` | write | `tx_id` | `write` + caller must own the tx | handle owner check is new |
| `BeginXTx` | write | `collections[]` | `write(C)` for **every** C | already enforced via `GetCollections()` |
| `StageXTx`, `GetXTx` | write | `collection` + handle | `write(C)` + caller owns handle + C is a participant | `GetXTx` stays write-scoped (adds to read set) |
| `CommitXTx`, `RollbackXTx` | write | `xtx_id` | `write` + caller owns handle | no collection field: ownership is the control |
| `XTxStatus` | read | `ref` | any authenticated principal | response carries only status + `tx_id` (`XTxStatusResponse`), no collection/data |
| `Snapshot` | export | none (D4: `collections` field) | `db:read` | gap today: scoped key gets the full DB |
| `Replicate` | export | none | `db:read` (replication feed = every collection, incl. XTx envelopes) | gap today: scoped read key streams all collections |
| `ReplicationStatus` | read | none | `db:read` | exposes follower ids/LSNs |
| `Promote` | admin | none | `db:admin` | gap today: any read-write key incl. scoped |
| gRPC health (`grpc.health.v1`) | probe | none | per D5 probe rule | not in either auth map today → treated as write if interceptors apply *(unverified)* |

Non-RPC surfaces:

| Surface | Decision |
|---|---|
| `/healthz`, `/readyz` (REST) | unauthenticated by design; must not include collection names, paths, or error detail beyond ok/not-ready |
| `/metrics` | loopback-bound by default; if `metrics_addr` is non-loopback it requires `db:read` credentials (`x-api-key` or `Authorization: Bearer`); per-collection labels are otherwise a disclosure of collection names |
| REST gateway | no authorization logic of its own; forwards `x-api-key`; any route that bypassed gRPC interceptors is a bug |
| Unix socket | same interceptor chain (as in `cmd/scriva/main.go`); filesystem permissions (D5) are the only authentication when keys are disabled |
| SIGHUP key reload | a reload that would take the key set from non-empty to empty **while any non-loopback listener is bound** is rejected and the old set kept (today `Reload` accepts an empty set, which disables auth — verified in `main.go`/`buildKeys`) |

### Rationale
It reuses the existing config surface, so no operator has to learn a new vocabulary, while closing the H3 holes and giving every future RPC a mandatory classification. Treating "has an allow-list" as "no database-level capability" is the minimal rule that makes the three ambiguous shapes (create/drop, no-collection, multi-collection) decidable.

### Rejected alternatives
- **Prefix/namespace create grants (`tenant_*`):** rejected — pattern semantics collide with legacy names and fold-uniqueness (D1); revisit with a measured need.
- **A third `admin` scope value now:** rejected — existing unrestricted read-write keys already are the admin boundary (comment on `Promote` in `scope.go`); a new value would force every operator to re-key.
- **Allow scoped keys to drop their own collection:** rejected — see the `DropCollection` row.
- **Deny `ListCollections` to scoped keys:** rejected — breaks CLI/SDK discovery for no confidentiality benefit once filtered.
- **Proto-annotation-driven policy (custom options):** deferred, not rejected — the generated table plus CI completeness test achieves the same fail-closed property without proto churn.

### Compatibility impact
Breaking *by design* for scoped keys that relied on the holes: create/drop of non-listed names, `Snapshot`, `Replicate`, `Promote`, `ReplicationStatus`, and unfiltered `ListCollections` now fail or shrink. Unrestricted keys, the legacy `--api-key`, and mTLS principals are unchanged. `read` keys additionally gain `FindByKey`/`Aggregate`. Callers of `CommitTx`/`CommitXTx` with a handle created by a different principal start failing.

### Rollout alternatives considered
A transition flag `legacy_scoped_admin: true` restoring the holes — **rejected**: it preserves the vulnerability exactly for the keys that were meant to be confined. Ownership binding of handles in N+1 instead of N — rejected as acceptable only if handle ids are proven high-entropy; until proven, ship in N.

### Acceptance criteria
- **A-1** Descriptor completeness: iterate `Scriva_ServiceDesc` (+ health); every method name has exactly one classification entry; a synthetic extra method is denied to `read`, `read-write+allow-list`, and allowed only for `db:admin`.
- **A-2** Matrix test (principals: unrestricted admin, `read`, `read-write`, `read`+list, `read-write`+list, anonymous with auth on, bad key, mTLS) × all RPCs in the table above, asserting both status code **and** that no out-of-scope data/name appears in the response.
- **A-3** `read-write`+`[a]`: `CreateCollection{a}` OK; `CreateCollection{b}` → `PermissionDenied` and no directory `b`; `DropCollection{a}` → `PermissionDenied`; `Snapshot`, `Replicate`, `Promote`, `ReplicationStatus` → `PermissionDenied`.
- **A-4** `BeginXTx{[a,b]}` for `[a]` → `PermissionDenied`, no handle created, no journal write.
- **A-5** Principal 2 calling `CommitXTx`/`RollbackXTx`/`StageXTx` on principal 1's handle → `PermissionDenied`; handle still usable by principal 1.
- **A-6** `PermissionDenied` is returned (not `NotFound`) for an out-of-list collection both when it exists and when it does not.
- **A-7** SIGHUP reload to zero keys with non-loopback gRPC bound → reload rejected, request without key still `Unauthenticated`.
- **A-8** REST: the same matrix via HTTP routes yields the equivalent HTTP status mapping.
- **A-9** `/metrics` on a non-loopback address without credentials → 401; with a `db:read` key → 200; with a scoped key → 403.

---

## D4. List and snapshot behavior for scoped principals (resolves the §6 "Scoped ListCollections" row and spec §2.2 item 2)

### Decision
1. **`ListCollections`:** a principal with an allow-list receives `allow-list ∩ existing collections`, sorted. Names in the list that do not exist are omitted (no existence oracle); legacy-named collections appear if listed. Unrestricted principals see everything. Reserved/internal (`xtx.*`) names never appear for anyone.
2. **`Snapshot`/`Replicate`/`ReplicationStatus`:** `db:read` only (D3); a scoped principal gets `PermissionDenied` — **in N this is the whole behavior**.
3. **Scoped partial export (designed, deferred):** `SnapshotRequest` gains `repeated string collections = 1` (wire-compatible: today the message is empty). A scoped principal must name a non-empty subset of its allow-list; an unrestricted principal may omit it (= all, today's behavior). The archive then contains only those collections, carries a `scriva-snapshot.json` manifest (`kind: partial`, collection list, source engine version), omits XTx coordinator files, is **not** a valid follower bootstrap, and is rejected by the import path unless the operator passes an explicit partial-restore flag.
4. **Watch/Find/Aggregate/Stats** already confine to the request's collection (D3); no cross-collection leakage exists in their responses.

### Rationale
Filtering is the §6 proposed default and costs nothing. Partial export is the only way a scoped tenant can get a backup, but stamped entries are only interpretable with the coordinator journal (`snapshot.go`: "Coordinator metadata is authoritative for stamped entries"), so a partial archive is unsafe until evidence retirement is designed.

### Rejected alternatives
- **Return `PermissionDenied` for `ListCollections` to scoped keys:** rejected (D3).
- **Return the unfiltered list (today):** rejected — discloses other tenants' collection names.
- **Scoped Snapshot that silently includes the journal:** rejected — leaks other collections' commit evidence and participant names.
- **Server-side filtering of an archive built for the whole DB:** rejected — still takes the global `xtxSnapshotMu` cut and risks inconsistent evidence.

### Compatibility impact
Scoped keys see fewer names; scoped `Snapshot` callers fail. The proto addition is wire-compatible and unused until the deferral lands.

### Deliberate deferral — scoped partial export
**Owner:** storage/operations owner with security/API owner. **Prerequisites:** (i) participant-retirement checkpoint (spec §3.3) so a collection's stamped entries can be resolved or rewritten without the journal; (ii) manifest + partial-restore format reviewed under the §4.2 compatibility gates. **Interim fail-safe:** scoped `Snapshot` = `PermissionDenied`; operators export with an unrestricted key.

### Acceptance criteria
- **L-1** Scoped `[a,c]` where only `a`,`b` exist → `["a"]`; unrestricted → `["a","b"]` (no `xtx.*`).
- **L-2** Removing a name from a key's allow-list and SIGHUP-reloading changes the next `ListCollections` result with no restart.
- **L-3** Scoped `Snapshot{}` and `Snapshot{collections:[a]}` (N) → `PermissionDenied` with an empty stream; no archive bytes sent.
- **L-4** `Replicate` by scoped key → `PermissionDenied` before any record is sent.

---

## D5. Secure-default migration and rollout (resolves spec §3.8 deployment bullets and the §6 "Secure defaults rollout" row)

### Today (verified in code)
Defaults (`server/config.go:DefaultConfig`): `:5433`, `:8080`, `:9090`, unix socket `/tmp/scriva.sock`; empty `APIKey` and `Keys` = auth disabled; TLS off. `cmd/scriva/main.go`: metrics `http.Server` has no timeouts and no auth; REST `http.Server` has no timeouts and serves plain HTTP even when TLS is configured (the REST→gRPC hop uses TLS with `InsecureSkipVerify`); `rest.go:corsMiddleware` sets `Access-Control-Allow-Origin: *`; the unix socket is `os.Remove`d then listened on with the process umask; the follower dials the leader with `insecure.NewCredentials()` (`main.go`, replicate-from branch).

### Decision — the target policy ("strict")
1. **Default binds:** `127.0.0.1:5433`, `127.0.0.1:8080`, `127.0.0.1:9090`.
2. **Exposure rule:** a listener is *exposed* if its host is empty, `0.0.0.0`, `::`, any non-loopback IP, or any hostname other than `localhost` (no DNS lookups; unparseable = exposed). An exposed gRPC/REST/metrics listener requires **(auth configured) and (TLS configured)**, else startup is refused with an error naming the listener and the exact override. Override: `allow_insecure_remote: true` (explicit, logged `WARN` every start, never implied by any other setting).
3. **Probe rule:** `/healthz`/`/readyz` stay unauthenticated on exposed listeners (D3) and are exempt from the exposure rule's auth part.
4. **Unix socket:** parent directory must be owned by the server uid with mode ≤ `0700`; socket mode `0600`; a pre-existing path is removed only if `Lstat` shows a socket owned by the server uid. Default path moves from `/tmp/scriva.sock` to `/tmp/scriva-<uid>/scriva.sock`; the CLI default follows.
5. **Replication:** exposed leader listener follows rule 2 (so replication is TLS+auth by construction); follower gains `replicate_tls_ca`/`replicate_tls_cert`/`replicate_tls_key`; dialing a non-loopback leader without TLS is refused unless `replicate_insecure: true`.
6. **HTTP hardening:** metrics and REST servers set `ReadHeaderTimeout` 10 s, `ReadTimeout` 30 s (REST watch/stream routes exempt via per-route deadlines), `IdleTimeout` 120 s, `MaxHeaderBytes` 1 MiB, request body ≤ 16 MiB (configurable). CORS: no CORS headers unless `cors_allowed_origins` lists origins; only listed origins are echoed, never `*`. REST serves TLS with the same certificate when `tls_cert`/`tls_key` are set, unless `rest_plaintext: true`.
7. **Existing credentials:** `api_key` still means one unrestricted `read-write` principal named `default`; `keys:` entries keep their scope and allow-list; mTLS principals unchanged. No key format or file migration is needed.

### Staged rollout and fail-safe at each stage

| Stage | Behavior | Fail-safe |
|---|---|---|
| **N — warn + opt-in** | Security fixes of D1–D4 and D2 archive/permission fixes ship (no flag). New config `secure_defaults: true` (and `--secure-defaults`) applies the strict policy now. Without it: defaults unchanged, but startup logs one `WARN` per exposed listener lacking auth/TLS (listener, reason, override name; no secrets); effective binds are always logged at `INFO`. Unix-socket mode/ownership tightening applies (new socket only). | An invalid/unparseable value anywhere in the new keys is a startup error, not a silent default. Warnings never include key material. |
| **N+1 — secure defaults** | Default binds become loopback; exposed + **no auth** is refused unless `allow_insecure_remote: true`; HTTP timeouts and CORS restrictive by default; socket default path moves; follower TLS keys available and plaintext non-loopback replication warns. `secure_defaults` becomes a no-op accepted flag. | Existing configs with *explicit* addresses keep their binds; configs relying on the old all-interface default now fail by connection-refused (loud) rather than by silent exposure, and the startup `INFO` line names the effective bind. |
| **N+2 — complete** | Exposed + auth-without-TLS is also refused (rule 2 fully enforced); non-loopback plaintext replication refused; REST TLS enabled by default when a cert is configured. `allow_insecure_remote`/`replicate_insecure`/`rest_plaintext` remain as permanent explicit overrides. | Same refusal text enumerates each failing listener and the single flag that would permit it. |

At every stage a **reload cannot weaken** the effective policy (D3 reload rule), and `DB.Open` never depends on listener policy: refusing to start a listener never alters data.

### Rationale
Only a staged change satisfies spec §3.8's "warn, override, then default" and the review's "do not silently break existing deployments". Separating *fixes* (N, no flag) from *defaults* (N+1/N+2) keeps the breaking surface to what operators can configure.

### Rejected alternatives
- **Flip all defaults in N:** rejected — outage for every default-config deployment with no warning release (spec §3.8 alternative, not chosen).
- **Never flip, document only:** rejected — fails H2; the review flags the empty-key default as unsafe out of the box.
- **Infer safety from "empty API key" alone:** rejected (spec §3.8) — exposure and auth are validated together.
- **Refuse to start whenever auth is disabled, even on loopback:** rejected — loopback-only and Unix-socket-only no-auth use (embedded/CLI tests, local development) is a legitimate, non-exposed configuration.
- **Make `allow_insecure_remote` a CLI-only flag:** rejected — operators need it in config files for orchestrated deployments.

### Compatibility impact
Operators with intentional all-interface plaintext deployments must add either TLS+auth or `allow_insecure_remote: true` (N+1/N+2); browser clients on another origin need `cors_allowed_origins`; clients with a hard-coded `/tmp/scriva.sock` need the new path or an explicit `unix_socket`.

### Rollout alternatives considered
Two-release variant (warn in N, enforce everything in N+1) — acceptable if the release owner prefers speed, but TLS enforcement for REST requires the REST TLS listener, which is a behavior change of its own; kept separate. Feature-flag-only (never default) — rejected above.

### Acceptance criteria
- **S-1** Table of `(host, auth, TLS, allow_insecure_remote, stage)` → `start` / `refuse` / `warn`: `:5433,no-auth,no-TLS`, `0.0.0.0`, `::`, `10.0.0.5`, `db.example`, `localhost`, `127.0.0.1`, `[::1]`, unparseable. In N+1, `127.0.0.1`+no-auth starts; `0.0.0.0`+no-auth refuses; with the override it starts and warns.
- **S-2** `DefaultConfig()` in N+1 has loopback hosts for all three addresses; a config file with explicit `grpc_addr: ":5433"` keeps `:5433` (and warns when no auth).
- **S-3** Startup warnings contain listener name and override key but never a key, cert path contents or token (grep test over captured log).
- **S-4** Socket: created `0600` in a `0700` parent; a pre-existing symlink or foreign-owned file at the path → startup refuses to replace it; a stale own socket is replaced.
- **S-5** Metrics/REST servers have the timeouts above (struct inspection) and a slow-header client is disconnected within the bound; `Origin: https://evil` receives no `Access-Control-Allow-Origin` by default.
- **S-6** Follower pointed at non-loopback leader without TLS → refused in N+2, warned in N+1; with `replicate_tls_ca` it connects over TLS (integration test with in-process TLS leader).
- **S-7** Legacy `api_key` and scoped `keys:` config files from the previous release load unchanged and authenticate to the same principals (golden-config test).

---

## Deferrals (all named, bounded, with interim fail-safe)

| Deferral | Owner | Prerequisites | Interim fail-safe |
|---|---|---|---|
| Scoped partial `Snapshot` | storage/operations + security/API | participant-retirement checkpoint (§3.3); partial-manifest format review (§4.2) | scoped `Snapshot` = `PermissionDenied` |
| Per-certificate scope/allow-list for mTLS principals | security/API | config schema mapping cert subject → scope/allow-list; test matrix extension | a client CA-signed cert = unrestricted `read-write` (documented CA-is-the-boundary, as `mtls.go` states) |
| Prefix/namespace create grants | security/API | measured tenant need; interaction with fold-uniqueness and legacy names | create restricted to literal allow-list names or `db:admin` |
| Offline legacy-name migration command | engine/API + release | D1 table; retirement checkpoint when XTx evidence references the name | legacy names stay operable; command (when built) must refuse with "participant retained" |
| Handle-ownership binding proof of id entropy | engine owner | confirm XTx handle id generation | ownership check ships in N regardless |

---

## Spec sections to update (consolidation checklist)

| Decision | Spec location it resolves / amends |
|---|---|
| D1 name grammar, case, Unicode, reserved set | §2.1 items 1–2 (replace "recommended" allowlist with the ratified grammar); §6 row "Collection-name policy for legacy names" |
| D1 legacy-name behavior table, config-name rules | §2.1 final paragraph options (a)/(b)/(c) → chosen option (b)+explicit offline migration; §6 same row; §4 path corpus row (add `xtx`, `collections`, case-fold pair) |
| D2 root/collection/drop containment | §2.1 items 3–4 and the "safe design should resolve…" paragraph; §1.2 H1 anchors; §4 "Path containment" row |
| D2 export/import limits and temp-dir promotion, modes | §3.8 archive and permissions paragraphs; §1.2 H12; §4 "Security hardening" row |
| D3 capability model and RPC table | §2.2 items 1–2 and the method-inventory paragraph; §1.2 H3; §4 "Authorization" row and minimum authorization matrix |
| D3 XTx handle ownership, reload-to-empty rule, metrics/REST/socket rows | §2.2 items 3–4; §4 authorization matrix (add handle-owner and reload cases) |
| D4 scoped list/snapshot | §6 row "Scoped `ListCollections`"; §2.2 final paragraph (list/snapshot/create questions); deferral → new §6 row "Scoped partial export" |
| D5 secure defaults, staged rollout, fail-safe | §2.2 item 3; §3.8 first bullets and the secure-defaults paragraph; §6 row "Secure defaults rollout"; §5 stage 6 (move network defaults earlier than "deployment and test infrastructure": D3/D4 fixes are stage 1, defaults N+1/N+2) |
| Deferrals table | §6 (new rows with owner/prerequisite/interim) and §7 documentation requirements |
