---
title: Durability & backup
description: Choose an fsync policy, understand crash safety and checksums, and run online backups with restore.
---

## Durability modes

ScrivaDB lets you trade write throughput against your crash-loss window with the
`--sync` flag:

| Mode | Behaviour | Trade-off |
|---|---|---|
| `none` | Rely on the OS to flush pages. | Fastest; loses the last unflushed writes on a crash. |
| `interval` | fsync on a timer (`--sync-interval`, default `1s`). | Balanced; bounded loss window. |
| `always` | fsync on every write. | Safest; slowest. |

The embedded façade (`scriva.Open`) defaults to `interval` (~1s) — a sensible
middle ground for in-process use.

## Crash safety & integrity

- Writes are **append-only** — nothing is modified in place, so a crash mid-write
  can only ever leave a torn trailing line, never corrupt existing data.
- Every segment entry carries a **CRC32C checksum**. On read, a mismatch is
  reported rather than silently returning wrong data — so on-disk bit-rot is
  caught, not propagated.
- The in-memory `id` index is persisted with its own checksum **and per-segment
  coverage fingerprints**. After a crash, open checks the file against the
  segments and replays only the bytes written since the last persist; anything it
  cannot prove current is rebuilt from the segments, which are the source of
  truth. A one-time rebuild happens when upgrading index files from older
  releases.
- A failed write is rolled back to the last good byte. If even the rollback
  fails, the segment is *poisoned* (`ErrSegmentPoisoned`) and refuses appends
  until restart, rather than risk writing after garbage.
- Open is **fail-closed**: damaged segments or ambiguous history stop the server
  from starting (`--integrity-policy fail`, the default) instead of serving
  wrong answers. Only one process may own a data directory (an exclusive `LOCK`
  file; use a local filesystem).
- `kill -9` loses nothing that was acknowledged; **power loss** can lose writes
  not yet flushed, bounded by your sync mode above.

## Verify and repair

With the server stopped, check a data directory and repair derived state
(indexes, id counter) from the segments:

```bash
scriva verify --data ./data                 # exit 0 clean, 1 repairable, 2 corruption/conflict, 3 usage
scriva repair --data ./data --dry-run       # show the plan, change nothing
scriva repair --data ./data                 # verified backup first, then rebuild
scriva verify --data ./data                 # confirm exit 0 before restarting
```

`repair` never edits segment bytes unless you pass `--salvage`, and never
resolves conflicting history on its own. See the
[index recovery runbook](https://github.com/srjn45/scriva/blob/main/docs/runbook-index-recovery.md)
for the full decision tree.

## Online backup

`scriva-cli backup` streams a **consistent gzip snapshot** of the live database
— no need to stop the server:

```bash
scriva-cli backup --out scriva-$(date +%F).tar.gz --api-key dev-key
```

### Restore

Restore is deliberately boring — it's just a tarball:

```bash
tar xzf scriva-2026-07-10.tar.gz -C ./restored-data
scriva serve --data ./restored-data --api-key dev-key
```

:::tip[Encrypted backups for free]
If a collection uses [encryption at rest](/scriva/guides/encryption/), its
segment files are already ciphertext — so the backup tarball is encrypted with
no extra step, and a leaked archive is useless without the key. Just remember to
back up the **key separately**: a backup without it is unrecoverable.
:::

## Compaction

A background goroutine per collection merges and deduplicates sealed segments,
reclaiming space from superseded and expired records. It kicks in on an interval
(`--compact-interval`, default `5m`) or when the dirty ratio crosses
`--compact-dirty` (default `0.30`). Operators can also force a synchronous pass:

```bash
scriva-cli compact users --api-key dev-key
```

## Next

- [Replication & failover](/scriva/guides/replication/) — scale reads and
  survive a leader loss.
- [Configuration](/scriva/reference/configuration/) — every server flag.
