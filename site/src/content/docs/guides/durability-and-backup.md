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

The embedded façade (`filedb.Open`) defaults to `interval` (~1s) — a sensible
middle ground for in-process use.

## Crash safety & integrity

- Writes are **append-only** — nothing is modified in place, so a crash mid-write
  can only ever leave a torn trailing line, never corrupt existing data.
- Every segment entry carries a **CRC32C checksum**. On read, a mismatch is
  reported rather than silently returning wrong data — so on-disk bit-rot is
  caught, not propagated.
- The in-memory `id` index is persisted with its own checksum for fast, verified
  restarts.

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
