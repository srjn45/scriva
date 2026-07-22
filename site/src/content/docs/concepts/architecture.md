---
title: Architecture
description: How ScrivaDB stores data — segment files, the write and read paths, indexes, compaction, and crash safety.
---

ScrivaDB is built around one idea: **an append-only log of JSON, made queryable**.

## Storage model

Each collection is a directory of **NDJSON segment files** — one JSON object per
line, appended in order:

```
data/
  users/
    000001.ndjson      # sealed segment
    000002.ndjson      # active segment (being appended to)
    index/             # persisted id + secondary indexes
```

A segment is **sealed** once it reaches `--segment-size` (default 4 MiB) and a
fresh one is opened. Sealed segments are immutable until the compactor rewrites
them.

Every line carries the record's `id`, `key`, `rev`, your `data`, and a **CRC32C**
`crc`.

## Write path

1. The record is serialized to a single NDJSON line.
2. It's **appended** to the active segment — inserts, updates, and deletes are
   all just new lines (a delete is a tombstone line).
3. Indexes are updated in memory.
4. The write is durably flushed according to the [`--sync` mode](/scriva/guides/durability-and-backup/).

Because writes never modify existing bytes, a crash can at worst leave a torn
trailing line, which is detected and skipped on restart.

## Read path

1. The **in-memory `id` index** (or a **secondary index**) locates candidate
   records without scanning the whole collection.
2. Each candidate line's **CRC is verified** — a mismatch is surfaced, not
   silently returned.
3. The engine applies the filter and **streams** matching rows, respecting
   `limit`, `offset`, ordering, and the keyset cursor.

The latest `rev` for a given `id`/`key` wins, so superseded lines and tombstones
are transparently skipped.

## Indexes

- **`id` index** — in memory for O(1) lookup by id; persisted with a checksum so
  restarts are fast and verified.
- **Secondary indexes** — per-field, giving O(1) equality lookups and O(matches)
  range queries. Maintained automatically on write and persisted.

## Compaction

A background goroutine per collection periodically **merges and deduplicates**
sealed segments: it keeps the newest `rev` per record, drops tombstones and
expired (TTL) records, and reclaims space. It triggers on `--compact-interval`
or when the dirty ratio crosses `--compact-dirty`; operators can force a
synchronous pass with `scriva-cli compact`.

## Replication

Committed writes are assigned a monotonic **global LSN**. A follower bootstraps
from a snapshot and then tails the leader by LSN, applying each write through the
normal write path — so a follower's segments and indexes match the leader
exactly. See [Replication & failover](/scriva/guides/replication/).

## Observability

- **Prometheus metrics** — per-collection gauges, compaction histograms, gRPC
  request duration, per-query rows-scanned.
- **Structured logging** — leveled `log/slog`, one record per RPC.
- **Distributed tracing** — opt-in OpenTelemetry spans (gateway → gRPC →
  `engine.scan` / `engine.compaction`). The embeddable engine gains **no** OTel
  dependency.

For the authoritative deep-dive, see
[`docs/architecture.md`](https://github.com/srjn45/scriva/blob/main/docs/architecture.md).
