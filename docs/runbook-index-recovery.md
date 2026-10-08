# Runbook: index / data integrity recovery

For operators running `scriva serve` and for Go programs embedding ScrivaDB.
The segments (`*.ndjson`) are the **source of truth**; primary and secondary
indexes, the id counter, and compaction manifests are *derived* and can be
rebuilt from them. This runbook is about telling those two cases apart and
recovering without losing data.

Commands below are those of the `scriva` binary (`scriva verify --help`,
`scriva repair --help`). They operate directly on a data directory — no server,
no network, no API key.

---

## 1. Symptoms

| Symptom | Likely cause |
|---|---|
| `scriva serve` refuses to start; error matches `engine.ErrIntegrity` and names a finding | Open-time scan found damaged segments or ambiguous history (default `--integrity-policy fail`) |
| Server/embedder fails with `engine.ErrDatabaseLocked` ("database already open") | Another process (or another `Open` in the same process) holds the directory lock |
| Lookups or queries miss records that exist, return deleted records, or return the wrong record | Index disagrees with segments (`index-*`, `sidx-*` findings) |
| `scriva_integrity_findings_total` or `scriva_integrity_open_total{outcome="failed"}` increasing | Open-time scan reported findings |
| `scriva_append_errors_total{reason="poisoned"}` / `scriva_segment_poisoned_total` increasing | A write could not be rolled back; the segment stopped accepting appends |
| `scriva_recovery_total{kind="rebuild"}` or `spotcheck_fail` after restarts | The persisted index was untrusted at open and was rebuilt |

---

## 2. Decision tree

**Stop the writer first** (section 3, step 1), then run:

```bash
scriva verify --data ./data --mode full
echo $?
```

`--mode full` scans every segment and compares the indexes against the
ground truth rebuilt from them. `--mode quick` (bounded spot check) is fine for
routine health checks but is not enough to clear a suspected problem. Add
`--json` for machine-readable output and `--collection NAME` to narrow it.

| Exit | Meaning | Action |
|---|---|---|
| `0` | Clean | Directory is healthy. Look elsewhere (client, network, config). Restart. |
| `1` | **Repairable** — only derived structures disagree (severity `repairable-index`: `index-*`, `sidx-*`, `idcounter-behind`, `compaction-manifest-*`, leftover temp files) | Safe to `repair`. No record data is lost; indexes are rebuilt from segments. |
| `2` | **Data corruption** (`segment-bad-region`, `segment-glued-line`, `segment-unreadable`, …) and/or **conflicts** (`conflict-duplicate-id`, `conflict-id-reuse-after-delete`, `conflict-write-after-delete`, `conflict-revision-regression`) | Do **not** expect an automatic fix. Take a backup, then follow 2a / 2b below. |
| `3` | Usage error, unreadable directory, or locked directory (`lock-held`) | Fix the path/permissions, or find and stop the process holding the lock. |

Finding severities, lowest to highest: `info` < `repairable-index` <
`data-corruption` < `conflict`. The exit code reflects the worst one.

**2a. Data corruption (exit 2, `data-corruption`).** `repair --dry-run` shows
what would happen. Plain `repair` rebuilds indexes but leaves damaged bytes
alone. `repair --salvage` moves the valid records out of damaged segments into
a new segment. The damaged originals are never deleted: they are moved
byte-for-byte to `<collection>/quarantine/<run>/` (with a `MANIFEST.json` of
sizes and SHA-256 hashes; open, verify and rebuild ignore that directory) and a
second copy stays in the repair backup. Review the dry-run, and
prefer restoring a known-good backup if the lost region matters.

**2b. Conflicts (exit 2, `conflict`).** Conflicting history is **never
resolved automatically**. `repair` only reports it (default
`--on-conflict report`), or refuses before touching anything with
`--on-conflict abort`. Decide the correct resolution yourself from the
evidence (section 5) or restore from backup.

A *torn, never-acknowledged tail* on the newest segment (`segment-torn-tail`)
is trimmed automatically at open and is not data loss.

---

## 3. Safe sequence: stop → backup → dry-run → repair → reverify → restart

1. **Stop the writer.** Send `SIGTERM` (or `SIGINT`) to `scriva serve`; it
   drains in-flight RPCs and closes the engine. Confirm the process is gone.
   `repair` refuses a directory that is open elsewhere (exit `3`).
2. **Take your own file-level backup**, in addition to the one `repair` makes:
   ```bash
   cp -a ./data ./data.pre-repair-$(date -u +%Y%m%dT%H%M%SZ)
   ```
   Put it on a different volume if you can. (`scriva-cli backup` needs a
   running server; use a plain copy for a stopped directory.)
3. **Plan the repair without changing anything:**
   ```bash
   scriva repair --data ./data --dry-run
   ```
   Exit `1` means repairs would be applied; `0` means nothing to do; `2` means
   corruption/conflicts remain that repair will not fix on its own.
4. **Repair.** Add `--salvage` only if you decided so in 2a, and
   `--on-conflict abort` if you want any conflict to stop it before it touches
   anything:
   ```bash
   scriva repair --data ./data [--salvage] [--on-conflict abort]
   ```
   Before its first change `repair` writes a verified backup named
   `repair-backup-<UTC time>` next to the data directory (override the parent
   with `--backup-dir`, which must be outside `--data`). It prints the backup
   path and next steps — keep that output.
5. **Re-verify:**
   ```bash
   scriva verify --data ./data --mode full
   ```
   Expect exit `0`. Anything else: stop, do not restart, and go to section 5.
6. **Restart** `scriva serve` and watch the metrics (section 7). Keep both
   backups until you have confirmed the application is healthy.

---

## 4. Do not

- **Do not delete segment files** to "make the error go away". Segments are the
  data. Orphan/leftover files are reported by `verify` and handled by `repair`.
- **Do not run two writers on one directory** — two `scriva serve` processes,
  a server plus an embedded program, or a server plus `repair`. The directory
  lock (`ErrDatabaseLocked`, `lock-held`) exists to prevent this; never work
  around it by removing the lock file or copying a live directory and running
  the copy against shared storage.
- **Do not hand-edit index files** (primary/secondary index, id counter,
  compaction manifests). They are derived and carry coverage fingerprints; edits
  turn a repairable problem into an untrusted one. Let `repair` rebuild them.
- **Do not run `repair` without a verified backup** or on a directory you have
  not just verified.
- **Do not set `--integrity-policy report` as a fix.** It opens past damage and
  counts it; it hides, not repairs, the problem. Use it only for deliberate
  read-mostly salvage, and fix forward.

---

## 5. Privacy-safe evidence collection

When opening an issue or asking for help, you usually do not need to share
records. Collect:

```bash
scriva version
scriva verify --data ./data --mode full --json > verify-report.json
scriva repair --data ./data --dry-run --json > repair-plan.json
```

- The reports carry finding **codes, severities, collection names and
  segment/offset locations** — not record contents. Skim them before sharing
  and redact collection names if those are sensitive.
- Also useful: the exact server/embedder error line (it names the first
  finding), the relevant `scriva_integrity_*`, `scriva_recovery_*`,
  `scriva_append_errors_total` and `scriva_dir_lock_total` values, the Go
  version/OS/filesystem (network filesystems are a common cause of
  lock/flush surprises), and what happened just before (crash, `kill -9`,
  disk full, restore from backup).
- Do **not** attach segment files, the data directory, or the repair backup.
  They contain your data (and, with encryption at rest, are still sensitive
  metadata). If a maintainer needs a sample, build a minimal reproducing
  directory with synthetic records.

---

## 6. Embedder guidance (Go programs using `scriva.Open` / `engine.Open`)

- **One owner per directory.** Exactly one `Open` per data directory at a time
  across all processes. A second open fails fast with `engine.ErrDatabaseLocked`
  (`errors.Is`). Treat it as a deployment bug (overlapping rollout, two
  replicas on shared storage), not something to retry in a loop.
- **Always `Close`.** `Close` flushes and releases the directory lock. Trap
  `SIGTERM`/`SIGINT` in your program and call `Close` before exiting, after
  your own in-flight work has drained. A hard kill is survivable (open replays
  the tail and rebuilds the index if needed) but costs startup time and is the
  main source of recoveries.
- **Handle `engine.ErrIntegrity`.** With the default policy, `Open` fails with
  an error matching `engine.ErrIntegrity` (typed `*engine.OpenIntegrityError`,
  which names the first finding). Do not retry, and do not auto-delete files.
  Fail the instance, alert, and run section 3 with the program stopped. The
  façade options `scriva.WithIntegrityPolicy`, `scriva.WithOnIntegrity` and
  `scriva.WithLogger` expose findings and logs; `engine.VerifyDir` inspects a
  directory without opening it.
  ```go
  db, err := scriva.Open(dir, opts...)
  switch {
  case errors.Is(err, engine.ErrDatabaseLocked):
      // another owner: do not retry blindly; page the owner of the rollout
  case errors.Is(err, engine.ErrIntegrity):
      // damaged data: stop, alert, follow the recovery runbook
  case err != nil:
      // other open failure
  }
  ```
- **Wire up metrics.** The engine exposes hooks (for example
  `CollectionConfig.OnLock`) rather than calling Prometheus directly; export
  the same series `scriva serve` does (section 7) so alerts behave identically.
- **Do not reach into the directory from another process.** Other tools —
  including `scriva verify` on a live directory (findings may reflect in-flight
  writes) and any `repair` — should run only while your program is stopped.
  If other processes or services need the data, expose it through a
  `scriva serve` daemon and use its gRPC/REST API (or the SDKs) instead of
  opening the same directory directly.

---

## 7. Metrics and a sample alert

Relevant series (see [Prometheus metrics](getting-started.md#prometheus-metrics)):

| Metric | Use |
|---|---|
| `scriva_integrity_open_total{outcome}` | `failed` = open refused; `reported` = opened past findings |
| `scriva_integrity_findings_total{severity,code}` | Which findings, how bad |
| `scriva_recovery_total{kind}` | `rebuild`/`spotcheck_fail` = index was untrusted at open |
| `scriva_append_errors_total{reason}` / `scriva_segment_poisoned_total` | Write-path damage |
| `scriva_dir_lock_total{result}` | `contended` = a second opener was refused |

Sample Prometheus rules:

```yaml
groups:
  - name: scriva-integrity
    rules:
      - alert: ScrivaIntegrityFindings
        expr: sum by (collection, severity) (increase(scriva_integrity_findings_total{severity!="info"}[15m])) > 0
        for: 0m
        labels: { severity: page }
        annotations:
          summary: "ScrivaDB integrity findings in {{ $labels.collection }} ({{ $labels.severity }})"
          runbook: "docs/runbook-index-recovery.md"
      - alert: ScrivaOpenRefused
        expr: increase(scriva_integrity_open_total{outcome="failed"}[15m]) > 0
        labels: { severity: page }
        annotations:
          summary: "ScrivaDB refused to open a collection (integrity)"
          runbook: "docs/runbook-index-recovery.md"
      - alert: ScrivaSegmentPoisoned
        expr: increase(scriva_segment_poisoned_total[15m]) > 0
        labels: { severity: page }
        annotations:
          summary: "ScrivaDB segment poisoned in {{ $labels.collection }}; restart and run verify"
      - alert: ScrivaDirLockContended
        expr: increase(scriva_dir_lock_total{result="contended"}[15m]) > 0
        labels: { severity: ticket }
        annotations:
          summary: "A second process tried to open a ScrivaDB data directory"
```

Note: a process that failed to open cannot always serve metrics; also alert on
the process being down (`up == 0`) and on the error in its logs.
