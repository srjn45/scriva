# Crash-atomic single-collection batches — release-readiness report

**Scope:** integration branch `autopilot/05-single-collection-batch-crash-atomicity`
(hardening §3.1, SR-D2 option A) plus the fixes made by the release-gate task.
**No tag or release was made.**

## Verdict

**Ready, conditional on the residual risks below being accepted.** The release
gate found three defects; all are fixed here. The writer gate
(`atomicBatchWriterReady`) was still `false` and new roots were not initialised,
so the feature was dead code in production; every prerequisite
(`atomicBatchPrereqs`) is satisfied, the gate is now open and
`TestAtomicBatchGate_ClosedWhilePrerequisitesMissing` asserts the two stay in
step.

## Gate matrix

Invariant checked after every cut: the recovered collection equals exactly the
state before the in-flight batch or exactly the state after it; everything
acknowledged is present; the secondary index agrees with a scan; four
consecutive reopens are identical; `Verify` (full) is clean. A prefix matches
neither state and fails.

| Boundary | Test | Cuts |
|---|---|---|
| append, data sync, decision append, decision sync, apply (every write/sync/rename step of a 3-`CommitTx` + `InsertMany` workload) | `TestBatchGate_StepBoundaries` (`always`, `interval`, `none`) | every step × 3 sync modes |
| same, with page cache lost (cut to last fsync; relaxed modes depend only on the batch's forced fsyncs) | `TestBatchGate_PowerLoss` | every step × 2 modes |
| named phases: before locks, locked, prepared (data durable, no decision), decided, applied-not-acknowledged | `TestBatchGate_PhaseHooks` | before ⇒ full old, from decision ⇒ full new, `XTxStatus` agrees |
| injected EIO on sync, ENOSPC (partial / zero-byte) on write at ordinals 1–6 of a batch, then crash image | `TestBatchGate_InjectedFaults` | typed error (`ErrXTxDurability` / `ErrXTxOutcomeUnknown` / fenced), live view never a prefix, image full old/new |
| real process SIGKILL mid-`InsertMany` / mid-`CommitTx`, state carried across iterations, 3 reopens each | `TestKill9_AtomicBatches` | seeded; `SCRIVA_KILL_SOAK=1` for 300 |
| concurrent writers (6×12 batches) and readers; final state exactly the acknowledged batches, twice reopened | `TestBatchGate_ConcurrentWritersAndReaders` | race detector on |
| recovery, compaction, coverage tracking, verify/repair, snapshot, replication, downgrade, legacy opt-in | pre-existing: `batch_recovery_test.go`, `batch_tools_test.go`, `batch_replication_test.go`, `xtx_downgrade_test.go` | — |

Mutation check: with the gate closed the same step matrix fails with "neither
full-old nor full-new" (the prefix the feature removes), so the matrix detects
what it claims to.

Whole-project: `make test` (race) all packages ok (engine 88.0%), `make vet`,
`make lint` 0 issues, `make test-soak SOAK_SEEDS=30 SOAK_STEPS=300` ok, kill-9
batch soak: see PR description for the final count.

## Defects found and fixed

1. **Gate closed / new roots not initialised.** The feature never ran outside
   tests. Fixed: gate open; a new, non-follower root gets `xtx.format` and
   `xtx.journal` at `Open`; legacy roots stay legacy until
   `--enable-atomic-batches`; roots with `xtx.format` stay atomic
   (`TestAtomicBatch_RootDefaults`).
2. **Post-commit rotation failure was swallowed.** The XTx apply path did
   `_ = c.rotateSegment()`, so a committed batch whose rotation failed returned
   `nil`, contradicting the stage-3 contract (`*PostCommitError`, collection
   fenced). Fixed in `engine/xtx_commit.go` / `batch_atomic.go`; cross-collection
   XTx behaviour is unchanged. Covered by the existing
   `TestRotation_PostCommit_MultiRecordWrites`, now running on the atomic path.
3. **Test-only:** `TestBatchRecovery_IncompleteRun_FailsClosed` truncated through
   an fd-relative `/proc/self/fd` path that dies with `Close` and flaked under
   load; it now uses the real path. Existing tests updated for the new default
   (standalone collections set `AllowNonAtomicBatch`; legacy-root XTx format
   tests pin the gate closed; replication LSN delta for a batch is 1 envelope).

## Compatibility / mixed versions

* New binary on a legacy root: unchanged, one warning line (`TestAtomicBatch_RootDefaults`, `TestLegacyRoot_*`).
* Old binary on a converted or new root: fails closed on stamped lines / `xtx.journal`, never mutates (`TestXTxDowngrade_*`, `TestDowngrade_ConvertedRootRefusedWithoutMutation`). **Downgrade is unsupported without a backup.**
* Old follower: `REPLICATION_PROTOCOL_UNSUPPORTED`; upgrade followers first.
* Snapshot/restore and `repair --backup` carry decision evidence.
* `created_by` in `xtx.format` for default roots is `default` (a label containing "root" tripped the encryption cleartext test, `TestFacadeRecordEncryption`, which greps the directory).

## Performance

`go test ./engine -run xxx -bench BatchWrite -benchtime=200x` (`engine/batch_bench_test.go`),
`InsertMany`, 8 cores, **tmpfs** (so fsync is nearly free and these numbers are
the CPU/serialisation overhead only; real disks add ≥ 2 fsync latencies per batch
in every sync mode):

| batch size | legacy | atomic | ratio |
|---|---|---|---|
| 2 (`sync=always`) | 34 µs | 105 µs | 3.1× |
| 10 | 158 µs | 264 µs | 1.7× |
| 100 | 1.22 ms | 3.26 ms | 2.7× |
| 2 (`sync=none`) | 28 µs | 110 µs | 4.0× |
| 100 (`sync=none`) | 1.27 ms | 3.21 ms | 2.5× |

Single-record writes are untouched (batches of one stay on the single-write path).

## Residual risks

* **One-way format on every new directory.** New roots cannot be opened by pre-XTx binaries. Release-note it (done in CHANGELOG).
* **Throughput of small batches** drops 2–4× CPU-side, plus two forced fsyncs on real disks; relaxed `sync_mode` no longer speeds batches up. Profile of the extra CPU (digest + journal encoding) not done.
* **Scans are not isolated from in-flight batches** (hardening §3.5, separate stage); a concurrent scan can see part of a batch. Crash/recovery never exposes a prefix.
* **Batches > 1000 ops / 64 MiB now fail** (`ErrXTxTooLarge`); callers with huge `InsertMany` need to chunk, losing atomicity across chunks.
* **No drain/downgrade tool** (deferred by the design).
* `.warden/check.yml` is absent, so `wd check` cannot run; the project checks above were run directly.
