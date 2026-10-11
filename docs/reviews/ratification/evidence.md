# Evidence recheck of the 2026-10-11 database review

**Autopilot run:** `ap-790b4f397cb1`, task `confirm-evidence`. Documentation only.
**Inspected commit:** `f1c32a07be535d3b95c014105021f5ddd5f129da` (`main`, "docs: add database hardening review and spec (#168)").
**Review revision:** `614ba5af410da2bba0489fb0f6d2168022ffbb0d`.
**Intervening commits:** exactly one, `f1c32a0`. `git diff --stat 614ba5a..HEAD` touches only `docs/architecture.md`, `docs/design-database-integrity-hardening.md` and the review report. No runtime code changed, so no finding was invalidated or moved by a code change; line numbers below are the review-revision line numbers and are still current.

Method: code-path trace for every finding, plus throwaway reproductions (uncommitted scratch `_test.go` files, deleted before commit) using the repo's own fault-injection FS (`engine/faultfs.go`) and `scanHook`. Go 1.24, `go test` in each package. Identifiers `H1`–`H13` are the spec §1.2 identifiers.

## Summary table

| ID | Finding | Anchor (at `f1c32a0`) | Changed since review? | Label |
|---|---|---|---|---|
| H1 | Collection-name traversal / out-of-root drop | `server/grpc.go:115-117` (only `Name == ""`), `engine/collection.go:408`, `engine/db.go:212` | no | **CONFIRMED** (reproduced) |
| H2 | All-interface listeners, empty-key = no auth | `server/config.go:135-138`, `internal/auth/apikey.go:339-345`, `cmd/scriva/main.go:508,692` | no | **CONFIRMED** (code trace; documented behavior) |
| H3 | Scoped keys not enforced for create/drop/list/snapshot | `internal/auth/apikey.go:71-94`, `internal/auth/scope.go:81-92` | no | **CONFIRMED** (reproduced) |
| H4 | `InsertMany`/`CommitTx` crash-prefix visibility | `engine/collection.go:960` (InsertMany), `:1495` (CommitTx), `engine/segment.go:216-217` | docs wording only (architecture.md already weakened) | **CONFIRMED** (reproduced via crash injection) |
| H5 | Failed `SyncModeAlways` sync leaves mutation visible, unpublished | `engine/collection.go:930,1147,1180,2068`, `engine/keys.go:175`, `engine/ttl.go:112` | no | **CONFIRMED** (reproduced for Insert; trace for rest) |
| H6 | Dropping XTx participant makes DB unopenable | `engine/db.go:201-217`, `engine/xtx_recovery.go:304`, `server/grpc.go:136-140` | no | **CONFIRMED** (reproduced) |
| H7 | Stale metadata persist undoes encryption policy | `engine/ttl.go:37-47`, `engine/encryption_admin.go:170-180`, `engine/fsync.go:14-44`, `engine/encryption.go:477-487` | no | **CONFIRMED** (reproduced under real concurrency) |
| H8 | Live scan omits a record across write + rotation | `engine/scan.go:371-457` | no | **CONFIRMED** (reproduced with `scanHook` barrier) |
| H9 | Seal failure leaves active segment sealed | `engine/segment.go:442-461`, `engine/collection.go:1415-1422,939-941` | no | **CONFIRMED** (reproduced) |
| H10 | `Update` drops `_key`, key reusable | `engine/collection.go:1087-1093,1110-1165`, `engine/keys.go:65` | no | **CONFIRMED** (reproduced) |
| H11 | `Count` vs `Scan` disagree on expired records | `engine/count.go:23-34` vs `engine/scan.go:443` | no | **CONFIRMED** (reproduced for match-all) |
| H12 | Auxiliary hardening (replication TLS, extraction, perms, socket, metrics, CORS, timeouts) | see note | no | **CONFIRMED** per sub-item by code trace; severity remains **topology-dependent** |
| H13 | Makefile targets reference missing package | `Makefile:61,65,66` | no | **CONFIRMED** (reproduced) |

Nothing was INVALIDATED. The two review-flagged unverified schedules (H7, H8) were both reproduced; they are no longer "static schedule" risks.

## Per-finding evidence

### H1 — path traversal (CONFIRMED)

Trace: `server/grpc.go:115-117` rejects only the empty name → `engine.DB.CreateCollection` → `OpenCollection` does `filepath.Join(dataDir, name)` + `os.MkdirAll` (`collection.go:408-409`); `DropCollection` does `os.RemoveAll(filepath.Join(db.dataDir, name))` (`db.go:212-213`). The only engine-side name check is `isReservedXTxName`. Drop requires the name to be in `db.collections`, which `CreateCollection` of the same traversal name satisfies, so create-then-drop deletes outside the root.

Reproduction (scratch `engine/zz_scratch_test.go`):

```go
root := t.TempDir(); dataDir := filepath.Join(root, "data"); victim := filepath.Join(root, "outside")
os.MkdirAll(victim, 0o755); os.WriteFile(filepath.Join(victim, "precious.txt"), []byte("x"), 0o644)
db, _ := Open(dataDir, CollectionConfig{})
_, err = db.CreateCollection("../outside")   // then list victim dir
err = db.DropCollection("../outside")        // then os.Stat(victim)
```

Output:

```
create ../outside err=<nil>
outside/ contains index.json / meta.json / precious.txt / seg_000001.ndjson
drop err=<nil>
outside dir stat after drop: ... no such file or directory
```

The engine wrote collection files into a sibling of the data dir and then deleted that directory, including a pre-existing file. The gRPC layer adds no validation, so the same holds remotely for any caller allowed to call Create/Drop (and see H3). The gRPC and REST wrappers themselves were not exercised end to end; the engine boundary is where the bug is.

### H2 — default exposure (CONFIRMED, by trace)

`DefaultConfig` sets `GRPCAddr ":5433"`, `RESTAddr ":8080"`, `MetricsAddr ":9090"` (`server/config.go:135-138`): all interfaces. `authorize` returns an unrestricted principal when there are no keys and no cert auth (`apikey.go:339-345`). `main.go` builds keys only from `cfg.Keys`/`cfg.APIKey` (`:680-692`) and contains no startup warning for the no-auth case (searched for APIKey/warn/auth). TLS is opt-in (`main.go:373-391`). This is documented behavior; the finding is about the default posture. Not reproduced at runtime (a listener test adds nothing beyond the trace).

### H3 — scope gaps (CONFIRMED)

Trace: `checkCollectionAccess` only inspects `GetCollections()` / `GetCollection()` (`apikey.go:71-94`). `CreateCollectionRequest` and `DropCollectionRequest` expose only `GetName()`; `ListCollectionsRequest` and `SnapshotRequest` are empty (`internal/pb/proto/scriva.pb.go:763,866`). `Snapshot`, `Replicate`, `ReplicationStatus`, `ListCollections` are in `readMethods` and `Promote` is write (`scope.go:81-92`), but none carries a collection, so a scoped read-write key passes.

Reproduction (scratch `internal/auth/zz_scratch_test.go`, key `Collections: ["a"]`, real `pb` request types through `Interceptors()`):

```
CreateCollection         handlerRan=true  code=OK
DropCollection           handlerRan=true  code=OK
ListCollections          handlerRan=true  code=OK
Snapshot(unary-shape)    handlerRan=true  code=OK
Insert other             handlerRan=false code=PermissionDenied
```

`Insert` into another collection is correctly denied; create/drop of `"other"` and the unscoped RPCs are not. Streams use the same `checkCollectionAccess` via `RecvMsg` (`apikey.go:175-187`), so `Snapshot` (stream, empty request) is allowed identically. Not tested: that a scoped key actually receives a snapshot of other collections end to end; the gate decision is what was proven.

### H4 — batch crash prefix (CONFIRMED)

Trace: `CommitTx` and `InsertMany` append ordinary entries one `Write` at a time (`segment.go:216-217`) and `syncActiveLocked` after the batch; the in-process rollback (`rollbackBatchLocked`) cannot run after a crash. Recovery replays every complete line.

Reproduction: `fs := newFaultFS(); openFaulty(...CollectionConfig{}); fs.crashAfter(fs.totalSteps()+2)` (two line writes land, then the FS freezes like `kill -9`), run a 3-op `CommitTx` / 3-record `InsertMany`, `reopenAfterCrash`, `Count(nil)`:

```
CommitTx:   returned err=tx commit: insert id 3: ... simulated crash (rollback: ... poisoned ...)
CommitTx:   after crash+reopen, visible records = 2 of 3 (acknowledged: none)
InsertMany: after crash+reopen, visible records = 2 of 3 (acknowledged: none)
```

A partial batch became permanently visible. This is a pre-existing gap, as the review states. `docs/architecture.md` was already rewritten in `f1c32a0` to stop claiming all-or-nothing after crash, but `architecture.md:100-101` still calls `InsertMany` "a genuinely atomic engine operation" (quota rejection context); see consolidator list.

### H5 — failed sync leaves local state changed (CONFIRMED for Insert; the other paths by identical trace)

Trace: in `insert` the sequence is `Append` → `index.Set` → `sidxIndexEntry` → `syncActiveLocked` → on error `return` (`collection.go:923-933`), skipping `publishCommit` and `emit`, with no rollback. The same shape is at `update` (`:1139-1150`), `Delete` (`:1175-1183`), CAS (`:2061-2070`), `Upsert` (`keys.go:168-178`) and `reapExpired` (`ttl.go:100-115`). Batches (`InsertMany`, `CommitTx`) do roll back on sync failure (`:1667-1673`), so H5 is specific to the single-record and reaper paths.

Reproduction: `SyncModeAlways`, `fs.failSyncAt(fs.count("sync")+1, syscall.EIO)`, then `Insert`:

```
Insert returned id=0 err=collection: insert: segment: sync ...: input/output error
Get(1) after failed Insert: data=map[v:x] err=<nil>
Count after failed Insert = 1
```

The caller got an error and `id=0`, yet the record is readable and indexed locally. Because the return precedes `publishCommit`/`emit`, replication and watch never see it. I did not separately drive `Update`/`Delete`/CAS/Upsert/reap with injected sync errors; those are CONFIRMED by trace only, and the reproduction should be extended when the fix lands.

### H6 — drop of an XTx participant (CONFIRMED)

Reproduction: create `a`, `b`; `BeginXTx` over both, `Insert` into each, `Commit`; `DropCollection("b")`; `Close`; `Open`:

```
DropCollection(b) err=<nil>
reopen err=engine: committed cross-collection transaction is missing participant evidence (tx 83ddcc8112550342-0000000000000001): participant collection "b" missing
```

The database cannot reopen after a normal drop. `server/grpc.go:136-140` forwards to `db.DropCollection` with no guard. Not tested: whether `scriva repair` can recover from this state (that is a mitigation question, not part of the finding).

### H7 — metadata persistence interleaving (CONFIRMED; the review's first unverified schedule)

Trace: `setDefaultTTL` builds `snap := c.metaSnapshot()` under `c.mu`, unlocks, then calls `persistMeta` (`ttl.go:41-46`). `applyEncryptionChange` takes `c.mu`, stores the new `encMeta` and persists while holding it (`encryption_admin.go:170-180`). So the TTL persist can write a snapshot whose `Encryption` field is the pre-change value after the encryption persist. Separately, `writeFileAtomic` uses the fixed temp name `path + ".tmp"` opened `O_TRUNC` (`fsync.go:16-18`), so two concurrent persists share one temp file. `rotateSegment`, `CommitTx` (after unlock), `Close` also call `persistMeta` from other paths.

Reproduction (a), the ordered schedule, using the exact steps of `setDefaultTTL` split at its unlock:

```go
col.mu.Lock(); col.explicitDefaultTTLSecs = 60; snap := col.metaSnapshot(); col.mu.Unlock() // T1 up to unlock
col.SetEncryptionPolicy(ctx, fieldsPolicy("secret"))                                          // T2 completes
persistMeta(metaPath(col.dir), snap)                                                         // T1 resumes
```

```
after T2: meta.json has encryption block = true
after T1 resumes: meta.json has encryption block = false; in-memory encMeta set = true
```

Then, with an encrypted record written, a crash (no `Close`) and reopen with the same key provider and `Encryption: nil`:

```
reopen err=<nil>
Get after reopen: data=map[secret:\u0000scrivaenc:v1:k1:W7Rzn...]   // ciphertext envelope returned as plain data
insert after reopen err=<nil>; policy enc configured=false
segment: line 1 secret = ciphertext (epoch 1), line 2 secret = "new-plain" (no epoch)
```

`initEncryption` treats a missing or corrupt `meta.json` as "no encryption block" (`encryption.go:477-487`), so it fails open: new writes are plaintext in a collection that holds ciphertext.

Reproduction (b), natural concurrency with the real functions (no hooks): per iteration open an unencrypted collection with a key provider, then run `setDefaultTTL(...)` and `SetEncryptionPolicy(...)` in two goroutines, `wg.Wait()`, read `meta.json`. 300 iterations:

```
300 runs: meta.json ok=279, encryption block lost=2, corrupt JSON=19
```

So under plain concurrency about 7% of runs ended with a bad `meta.json`: 2 lost the encryption block (the review's schedule), and 19 produced a corrupt file made of two interleaved writes (shared `.tmp` with `O_TRUNC`; observed shape `{...}"encryption":{...}}`). The corrupt case also falls into the same fail-open branch on reopen. This is stronger than the review's claim: the shared temp filename is a second, independently confirmed defect, not only a hardening note. Rates are for this machine and test shape and are not a production estimate.

### H8 — live scan omission (CONFIRMED; the review's second unverified schedule)

Trace: `streamLive` copies `c.sealed` plus `c.active` under `c.mu.RLock`, releases it, then walks those segments consulting the live index (`scan.go:380-384,401`). `layoutMu` guards compaction only, not writers (`:376`). A record updated after the snapshot into a segment created by a later rotation has an index entry pointing outside the snapshot; its old entry is classified "genuine stale version" (`:438`) and the new one is never visited.

Reproduction using the existing `scanHook` barrier (`scanAtSegment`, arg 0, fires after the snapshot, before the first segment is opened): the hook inserts padding until the captured active segment rotates, then `Update(X)` so the new version of `X` lands in the new active segment `A'`.

```
scan err=<nil> rows=5; Count after = 24
X (id=1, live in index with v=new) present in scan result = false
Get(X) = map[v:new] err=<nil>
```

`X` existed before the scan began, existed throughout, and is returned by `Get`, but the scan silently omits it, with `err=nil`. This is the review's schedule, now reproduced deterministically. Scope note: `XTx.Scan` is rejected by the handle (`engine/xtx_handle.go:573-582`, `rejectScan`), so the review's remark about cross-collection transaction reads relying on scan consistency should be narrowed: the confirmed impact is on plain `Scan`/`Find`/`Aggregate`/`Count` (non-index paths use `forEachMatch` → `streamLive`), not on XTx read sets. I did not audit every `streamLive` caller for that claim.

### H9 — seal failure (CONFIRMED)

Trace: `Seal` sets `s.sealed = true` before `Sync` (`segment.go:449-452`); `rotateSegment` returns the error after `Seal` without creating a new active (`collection.go:1418-1421`), and `insert` returns `id, ts, err` before `c.emit` (`:939-941`).

Reproduction: `SegmentMaxSize: 64`, `SyncModeNone`, `fs.failSyncAt(fs.count("sync")+1, EIO)`, two inserts:

```
Insert#1 id=1 err=collection: rotate after insert: segment: sync ...: input/output error
Insert#1 record IS readable (write committed although error returned)
Insert#2 id=0 err=collection: insert: segment: append to sealed segment ".../seg_000001.ndjson"
```

The first insert's error is reported although the record is committed; every later append to the collection fails until reopen. Not tested: that reopen recovers (no reason to expect otherwise; the data are intact).

### H10 — `Update` drops `_key` (CONFIRMED)

Trace: `Update` rejects a payload that *contains* `_key` and otherwise stores `data` unchanged (`collection.go:1087-1093`); only `UpdateByKey` stamps the key.

Reproduction: `InsertWithKey("k1", {v:1})`, `Update(id, {v:2})`:

```
Update err=<nil>
FindByKey(k1) after Update: err=engine: key not found: "k1"
Exists(k1)=false
InsertWithKey(k1) again: id=2 err=<nil> (orig id=1)
```

The key is lost and then reusable, so two live records can claim `k1` over time and the original record becomes unreachable by key. Staged XTx updates share this path in intent (`xtx_handle.go`) but were not separately reproduced; that part stays a trace-level statement.

### H11 — `Count` vs `Scan` expiry (CONFIRMED for match-all)

Trace: `Count(nil)` returns `index.Len()` (`count.go:29-31`), which includes expired-unreaped entries; `streamLive` filters with `isExpired` (`scan.go:443`).

Reproduction: one record with a 80 ms expiry and one without, sleep 150 ms (reaper interval is far longer in the test config):

```
Count(nil)=2 len(Scan(nil))=1
```

Not tested: the single-equality secondary-index fast path in `Count` (`count.go:36-41`) and range-index paths; they likely share the problem but that is UNCONFIRMED RISK for those sub-cases, to be covered by the fix's table test.

### H12 — auxiliary hardening (CONFIRMED per item by trace; severity depends on deployment)

| Item | Evidence |
|---|---|
| Replication transport without TLS | follower dials the leader with `insecure.NewCredentials()` unconditionally (`cmd/scriva/main.go:268`) |
| Extraction filenames/sizes | `extractSnapshot` rejects `..`/absolute and skips non-regular entries, but copies `hdr.Size` bytes with no cap and no file-count/total limit (`server/replication.go:112-127`); files created `0o644`, dirs `0o755` |
| Permissions | `meta.json` `0o644` (`meta.go:101`), dirs `0o755` (`db.go:73`, `collection.go:409`) |
| Unix socket | default `/tmp/scriva.sock` (`config.go:137`); `os.Remove` then `net.Listen` with no chmod; served with `insecure` creds (`main.go:514-526`) |
| Metrics | `/metrics` served without the auth interceptor on `MetricsAddr` `:9090` (`main.go:356-358`) |
| CORS | `Access-Control-Allow-Origin: *` (`server/rest.go:35`) |
| HTTP timeouts | `http.Server{Addr, Handler}` only, for REST and metrics (`main.go:358,558`) |

None was exercised at runtime. These are unconditional code facts; whether each is a vulnerability is a threat-model question the review already scoped.

### H13 — Makefile (CONFIRMED)

`Makefile:61,65,66` use `./internal/engine`; `go list ./internal/engine` → `stat .../internal/engine: directory not found`. The engine package is `./engine`. Not checked: whether `FuzzSegmentRecovery` / `FuzzCompaction` / benchmarks exist under `./engine` today, which the fix must determine.

## What I could not or did not verify

- H2, H12: no runtime listener or end-to-end gRPC/REST reproduction; trace only.
- H3: the gate decision was proven at the interceptor; a full `Snapshot` stream to a scoped key was not run.
- H5: only `Insert` was driven with an injected sync error; `Update`, `Delete`, CAS, `Upsert` and `reapExpired` are by trace.
- H7: the production interleaving was observed under free concurrency, but the exact stale-snapshot step order was also forced. The reopen fail-open demonstration used an unclean exit simulated by not calling `Close`.
- No claim is made about behavior on other filesystems, Windows path forms, or symlink substitution (spec §2.1); those remain open design points, not review findings.

## Changes the consolidator must make

1. **Review report, H7 section** ("stale metadata persistence", currently "concurrency risk"; "not reproduced under a deterministic runtime schedule"): relabel CONFIRMED, replace the last sentence with the reproduction results, and add the second defect: the fixed `meta.json.tmp` name produces corrupt `meta.json` under concurrent persists (19/300 runs); both the lost block and the corrupt file fall into the fail-open path in `initEncryption`.
2. **Review report, H8 section** ("static schedule assessment; no runtime reproducer was completed"): relabel CONFIRMED and cite the `scanHook` reproduction; narrow the "cross-collection transaction reads" statement because `XTx.Scan` is rejected, while plain `Scan`/`Find`/`Aggregate`/`Count` (non-index paths) are affected.
3. **Spec §1.2 closing paragraph** ("H8 and H7's interleaving are plausible code-level schedules, not reproduced runtime failures; their tests must first establish or refute the exact counterexample"): rewrite to say both were reproduced, give the counterexamples above as the first regression tests, and remove the "establish or refute" precondition. Keep the minimal-fix decision open.
4. **Spec H7 row / §3 (metadata)**: record that unique temp filenames are required by evidence, not just hygiene, and that fail-closed behavior on missing/corrupt `meta.json` for a directory with epoch-stamped entries is a separate required change (reopen returned ciphertext as data and accepted plaintext writes).
5. **Spec §2.3 / H5**: scope the finding to single-record and reaper paths; state that `InsertMany`/`CommitTx` already roll back on sync failure but remain crash-prefix-visible (H4). Keep the Insert reproduction; note the other four paths are trace-only until tests exist.
6. **Review report "confirmed" wording** ("confirmed means reproduced/verified in review"): H2 and H12 are confirmed as code facts only; keep their severity as deployment-dependent and say so.
7. **Review report severity labels**: no downgrade is warranted by this recheck. Note that H1+H3 compose (a scoped key can create `../x` and drop it) and that H6 is reachable by any key allowed to call `DropCollection`, which is every key under H3.
8. **`docs/architecture.md:100-101`**: still describes `InsertMany` as "a genuinely atomic engine operation"; qualify it as atomic against validation/quota rejection and in-process failures, not crash-atomic, consistent with the sentence added in `f1c32a0`.
9. **Spec H11 row**: label the match-all Count mismatch CONFIRMED and the secondary-index/range-index Count fast paths UNCONFIRMED RISK until covered by a test.
10. **Spec H13 row**: also require checking that the referenced fuzz/benchmark functions exist in `./engine`.
