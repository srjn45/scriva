# Cross-collection transactions — release-readiness report

**Scope:** integration branch `autopilot/06-cross-collection-transactions-hardening-release`
(base `1e0a1ee`, includes #164 adversarial matrix and #165 upgrade/operations),
plus the small fixes made by this task. **No tag or release was made.**

## Verdict

**Ready to release as an additive minor (v1.5.0 candidate), conditional on the
final GitHub Build & Test run of this branch being green.** One real defect was
found by the soak run and fixed here (compactor overshoot, below); no known
data-loss, integrity or compatibility issue remains open.

## What was run

| Check | Command | Result |
|---|---|---|
| Lint | `make lint` | 0 issues |
| Vet | `make vet` | clean |
| Full suite, race detector | `make test` (`go test ./... -race -count=1`) | all packages `ok`, total coverage 58.1%, engine 87.2% |
| XTx engine tests, repeated | `go test ./engine -race -count=3 -run XTx` | ok (128s) |
| XTx / replication server tests, repeated | `go test ./server -race -count=3 -run 'XTx\|Replication'` | ok |
| Randomized model soak (default) | `make test-soak` (100 seeds × 500 steps, base 1000) | ok (374s) |
| Randomized model soak (extra range) | `make test-soak SOAK_SEEDS=60 SOAK_STEPS=500 SOAK_BASE=5000` | **initially failed at seed 5003**, passes after the fix |
| Model soak, small | `make test-soak SOAK_SEEDS=20 SOAK_STEPS=300` | ok |
| kill -9 soak | `SCRIVA_KILL_SOAK=1 go test ./engine -race -count=1 -run TestKill9` | **partial** (seeds 1000-1184 of writer mode, no failure); see "Not run to completion" |

The model harness includes `xtx-commit`, injected write failure, crash-image
recovery, verify and repair-noop ops, so the soaks exercise XTx under crash,
torn-write and compaction interleavings.

## Defect found and fixed: compaction aborts on oversized batch segments

`SCRIVA_MODEL_SEED=5003 SCRIVA_MODEL_STEPS=500` failed at step 175 with
`compactor: compacted output (4 segments) exceeds input (3); aborting before swap`.

* **Cause.** A `CommitTx` batch (or XTx run) is appended to one segment whole, so
  a sealed segment can be several times `SegmentMaxSize` (1585 B vs a 1024 B
  limit in the failing run). The compactor re-packs entry by entry, needing more
  output segments than there are input segment names, and the pass aborts.
* **Impact.** Safe — it aborts before any swap, no data is lost — but that
  collection's compaction would fail on every pass until the layout changed.
  Pre-existing for single-collection `CommitTx`; XTx makes large batches easier
  to produce.
* **Fix.** `writeCompactEntries` takes a `maxSegs` cap; the last permitted output
  segment absorbs the overflow instead of rotating. The "exceeds input" guard is
  retained as defence in depth. Regression test:
  `TestCompact_OversizedInputBatchStillCompacts` (fails before, passes after).

## Compatibility and error-contract audit

| Area | Finding | Evidence |
|---|---|---|
| API (gRPC/REST) | Purely additive: new XTx RPCs; no existing RPC or field changed | `proto/scriva.proto`; semver policy in CHANGELOG |
| CLI / embedded | `begin-xtx …`, `db.Transact`; single-collection `begin-tx`/`CommitTx` unchanged | `TestCLI_XTx_*`, getting-started |
| Unknown outcome | Surfaces as `XTX_OUTCOME_UNKNOWN`, directs to `XTxStatus`, metric `outcome="unknown"`; never converted to abort | `TestIntegration_XTx_OutcomeUnknown_EndToEnd`, `TestCommitXTx_S4FsyncFailurePoisonsJournalOutcomeUnknown`, `TestCLI_XTx_OutcomeUnknownGuidance` |
| Replication mixed version | Legacy follower gets typed `REPLICATION_PROTOCOL_UNSUPPORTED`, never per-entry records; interleaved legacy + tx converge | `TestReplicationV2_LegacyFollowerGetsTypedRefusal`, `TestReplicationV2_InterleavedLegacyAndTxConverge` |
| Upgrade from ≤ v1.4 | Legacy root opens unchanged; first XTx upgrades in place | `TestXTxLifecycle_*` (v1.4.0 fixture), `TestXTxLegacyRootUntouched` |
| Downgrade / unsupported | Pre-fence decoder fails closed; too-new `xtx.format`, unknown feature, journal without format, stamped runs without journal all refused without mutation; lost journal never presumes abort | `TestXTxDowngrade_*`, `TestXTxOpenRefusesTooNewFormat` |
| Recovery / repair | `Verify` refuses missing committed participant; `Repair` returns precise typed errors and never reconciles what it cannot prove | `TestVerifyDirXTxGraphRefusesMissingCommittedParticipant`, repair tests |
| Disk format | A database that never runs an XTx is byte-identical to before | `TestXTxLegacyRootUntouched` |

### Documentation gaps found and closed

* `docs/architecture.md` still said XTx was "designed but **not shipped**" and
  that "ScrivaDB has no cross-collection transactions" — replaced with the
  implemented-status pointer.
* `docs/design-xtx-fault-boundaries.md` said "Nothing here is enabled in
  `engine.Open`" — status corrected.
* `CHANGELOG.md` had only hardening bullets under `[Unreleased]`; added the
  feature-level entry (API, CLI, replication v2, metrics, non-downgradable
  warning).

## Residual risks and intentional non-coverage

1. **Not downgradable.** Once a root holds XTx state, a pre-fence binary cannot
   read it and there is no in-place drain command (documented in
   `docs/operations.md`). Mitigation: backup before the first XTx. A drain tool
   is a reasonable follow-up, not a blocker.
2. **Mixed-version replication needs ordered upgrade.** Followers first, then
   leader; a leader that commits an XTx before followers upgrade gets typed
   refusals on those followers (they stop, they do not corrupt).
3. **Scope limits (by design).** Single node / single data directory; point
   reads and writes only (no scans or phantom protection inside a transaction);
   no distributed or XA transactions.
4. **Soak depth.** Soaks are bounded (≈160 distinct seeds × up to 500 steps plus
   the CI fixed seeds). Not run: multi-hour soaks, Windows/macOS kill -9
   (SIGKILL tests are non-Windows by construction), real-disk power-loss
   testing (fault-injecting FS only), and multi-node network-partition testing
   beyond the in-process failover matrix.
5. **Segments can exceed `SegmentMaxSize`.** Behaviour documented above; sizing
   alerts keyed to the limit should allow for a transaction-sized overshoot.
6. **#165 merged by hand.** The integration-branch merge of #165 bypassed the
   daemon land endpoint after timeouts (green CI and mergeability verified by the
   manager); the content is covered by the full suite above.

## Not run to completion

The 300-iteration-per-mode kill -9 soak (`SCRIVA_KILL_SOAK=1`, one directory
carried across iterations, ~5 s/iteration, several modes) is too long to finish
inside this task. It was stopped manually while `TestKill9_WriterRecovery` was at
seed 1184 of 1000-1299, with **no assertion failure or panic logged** in that
range. The default kill -9 set (6 iterations per mode, including the XTx modes)
passes under `make test` with the race detector. An earlier attempt hit the
20-minute `go test` timeout (a timeout, not an assertion failure). Recommended
before tagging, on an idle machine:
`SCRIVA_KILL_SOAK=1 go test ./engine -race -count=1 -run TestKill9 -timeout 3h`.

