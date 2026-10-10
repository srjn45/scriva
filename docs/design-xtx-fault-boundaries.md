# XTx fault-boundary checklist and design fixtures

**Status:** design / test scaffolding. Nothing here is enabled in `engine.Open`.
**Companion to:** [`design-cross-collection-transactions.md`](design-cross-collection-transactions.md)
(`§` references below point there).
**Executable form:** `internal/xtxspec` — `Boundaries()` is this table,
`Resolve()` is the §7.2 truth table, and the fixtures in
`internal/xtxspec/testdata/` are the frozen wire format.

## 1. What the scaffolding is (and is not)

| Piece | File | Purpose |
|---|---|---|
| Stamped-entry codec, v1/v2 checksum | `wire.go` | validates §3.1–§3.2; v1 input is pinned to `store.Encode` by `TestV1ChecksumMatchesStore` |
| Journal codec + `ScanJournal` | `journal.go` | validates §4.1–§4.3 (torn tail vs fail-closed) and decision folding |
| `xtx.format` codec + gate | `format.go` | validates §11.2 L2 |
| `Resolve`, `EntryVisible` | `recovery.go` | the §7.2 truth table and §3.3 predicate as pure functions |
| `Boundaries()` | `boundaries.go` | one record per write/fsync/rename, each with deterministic post-crash states |
| Golden fixtures | `testdata/*` | byte-exact; regenerate only deliberately: `go test ./internal/xtxspec -update-xtx-fixtures` |

It is **not** imported by `engine`, `server`, or any command
(`TestNotImportedByProduction`), writes no files, and does not change any
existing byte format. The real implementation (R0/R1, §11.3) must re-use or
replace these parsers and keep the fixtures passing.

## 2. Fixtures

All fixtures describe one transaction `4f9c2a7e11b0d3a5-0000000000000012`
(participants `orders` n=2, `stock` n=1; fixed timestamp, no clock/RNG).

| File | Asserts |
|---|---|
| `entries_run_orders.ndjson` | valid v2 run; `store.Decode` (every released reader) **rejects** each line |
| `entry_v1crc_spliced.ndjson` | stamp spliced onto a v1-checksummed line ⇒ rejected (`ErrV1CRCOnStamp`) |
| `entry_stamp_stripped.ndjson` | tool that drops unknown keys ⇒ v2 crc fails v1 ⇒ never silently committed |
| `journal_clean.ndjson` | header + one `COMMIT`; `parts` canonical (bytewise), `pd` verified |
| `journal_commit_retire.ndjson` | `RETIRE(commit)` supersedes `COMMIT` |
| `journal_torn_tail.ndjson`, `journal_zero_tail.ndjson` | torn/zero tail ⇒ truncate to last good `\n` (§4.3) |
| `journal_corrupt_middle.ndjson` | complete record with bad CRC followed by a good one ⇒ **fail closed**, never presume abort |
| `journal_decision_conflict.ndjson` | `COMMIT`+`ABORT` same txid ⇒ `xtx-decision-conflict` |
| `journal_header_only.ndjson` | fresh journal, zero records |
| `xtx.format.json`, `xtx.format.too_new.json` | version gate (`min_reader` 1 ok, 99 ⇒ `ErrFormatTooNew`), crc-protected |

Clarification recorded here (to fold into the design at ratification): the
`len(tx.t)` prefix in the v2 checksum is **one byte**, as `tx.t` ≤ 64 bytes.

## 3. Boundary checklist

Legend — *Kind*: `write` append/pwrite, `fsync`, `rename`, `create` (needs a
directory fsync). *Crash outcome* is the exact, required result of recovery;
"may be acked" means a client could already hold a success reply, so the only
legal outcome is **committed** (I3). Every row is asserted by
`TestBoundaryCrashOutcomes`.

| ID | Step | Kind | Target | Boundary | Crash outcome (truth-table row) | May be acked |
|---|---|---|---|---|---|---|
| F1 | F1 | create | `xtx.format` | create + write format file | nothing (row 1): no stamped byte exists yet | no |
| F2 | F2 | fsync | `xtx.format` | fsync format file | nothing (row 1) | no |
| F3 | F3 | fsync | root dir | fsync dir after format create | nothing (row 1); format may vanish, runs not yet written | no |
| J1 | J1 | create | `xtx.journal` | create + write header | nothing (row 1); torn header rewritten at open | no |
| J2 | J2 | fsync | root dir | fsync journal file + dir | nothing (row 1) | no |
| S1a | S1 | write | participant segment | stamped line torn mid-line | aborted (row 2): torn tail truncated by existing open path, run truncated | no |
| S1b | S1 | write | participant segment | run *k* complete, *k+1* not started | aborted (row 2) | no |
| S1c | S1 | write | all segments | all appended, none fsynced | aborted (row 2) or nothing (row 1), depending on write-back | no |
| S2a | S2 | fsync | participant segment | crash inside *k*-th fsync | aborted (row 2) | no |
| S2b | S2 | fsync | participant segment | fsync returns error | aborted (row 2); segment poisoned until restart; client gets `ErrXTxDurability` | no |
| S2c | S2 | fsync | participant dir | dir fsync for segment created in XTx | aborted (row 2) | no |
| S3a | S3 | write | `xtx.journal` | COMMIT torn (no `\n` / zero fill) | aborted (row 13): tail truncated, runs complete, no decision | no |
| S3b | S3 | write | `xtx.journal` | COMMIT whole in page cache, unsynced | **either** aborted (row 2) or committed (row 3) — atomic in every participant; no ack was sent | no |
| S4a | S4 | fsync | `xtx.journal` | fsync error | same as S3b; process reports `ErrXTxOutcomeUnknown`, journal poisoned | no |
| S4b | S4 | fsync | `xtx.journal` | COMMIT durable, indexes stale | committed (row 3), tail-replayed | yes |
| S5 | S5 | write | in-memory indexes | crash while applying | committed (row 3), indexes rebuilt | yes |
| S7 | S7 | write | client conn | crash around ack | committed (row 3) | yes |
| A1 | — | write | `xtx.journal` | advisory ABORT lost/kept | aborted (row 2 / row 8) | no |
| R1 | — | write | `xtx.journal` | RETIRE torn | committed (row 3); COMMIT still authoritative | yes |
| R2 | — | fsync | `xtx.journal` | RETIRE fsync | committed (row 9, no evidence needed) or row 3 | yes |
| R3 | — | rename | participant segments | compaction swap that de-stamps retired entries | committed (row 4); old-or-new whole via existing compaction recovery | yes |
| G1 | — | write | `xtx.journal.tmp` | write GC'd journal | old journal intact; tmp deleted at open | yes |
| G2 | — | fsync | `xtx.journal.tmp` | fsync tmp | as G1 | yes |
| G3 | — | rename | `xtx.journal` | rename tmp over journal | old-or-new journal whole, never mixed | yes |
| G4 | — | fsync | root dir | fsync dir after rename | may revert to old whole journal | yes |
| H1 | — | write | `xtx.journal` | journal lost, stamped runs present | **refuse open** (row 12), never presume abort | n/a |
| H2 | — | write | participant segment | COMMIT durable, a run lost | **refuse open** (row 5), never commit a subset | n/a |

### 3.1 Findings the checklist surfaced

1. **§0's "every unacknowledged XTx is fully absent" is too strong.** S3b/S4a
   show an unacked XTx may legitimately recover as committed (the kernel wrote
   the record back before the crash). The enforceable contract is *all-or-nothing
   in every participant, and every acked XTx present*. Recommended wording fix
   at ratification.
2. **Standalone lock/fsync order is fixed by S2 < S3 < S4 < S5 < S7**; each
   boundary above is that order's cut point, so the future crash matrix
   (`TestXTxCrashMatrix_*`, §14.1) can iterate `Boundaries()` rather than
   re-derive the list.
3. **Fail-closed rows (H1, H2, `journal_corrupt_middle`)** are the only places
   Open refuses; everything reachable by a clean crash resolves without
   operator action.

## 4. How the later stages use this

- R0 (fence): production parsers must pass `TestFixturesGolden`-equivalent
  byte comparisons against `testdata/` and the old-reader rejection test.
- Crash matrix: drive `faultfs_test.go` with one injected fault per `ID`, then
  call the real recovery and compare with `CrashState.Want`.
- Fixture changes require a design amendment: the wire format is frozen.
