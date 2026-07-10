---
title: Roadmap
description: What's shipped in FileDB v2 and where it's headed.
---

FileDB v2 reached its **v1.0.0 API freeze** — the gRPC/REST surface is stable
and backwards compatibility is maintained from here.

## Shipped

- **Storage** — append-only NDJSON segments, CRC32C integrity, background
  compaction, online backup + restore.
- **Durability** — `none` / `interval` / `always` fsync modes.
- **Indexing** — in-memory `id` index, per-field secondary indexes (equality +
  range).
- **Queries** — streaming `Find` with limit/offset, multi-field ordering, keyset
  pagination, and field projection; server-side `Aggregate` (count/sum/avg/min/max,
  optional group-by); live `Watch`.
- **Records** — caller-supplied string keys, monotonic revisions,
  compare-and-swap, upsert, TTL, optimistic transactions.
- **APIs** — dual gRPC + REST from one binary, generated OpenAPI, and seven
  hand-written client SDKs.
- **Distribution** — standalone server (Homebrew / apt / GHCR / release
  binaries) **and** an embeddable Go engine with zero heavy dependencies.
- **Replication** — leader→follower with read replicas and manual `Promote`
  failover.
- **Operations** — Prometheus metrics, structured logging, slow-query log, audit
  log, OpenTelemetry tracing, health/readiness probes, scoped API keys, TLS/mTLS,
  backpressure and rate limiting, per-tenant quotas & limits.

## Direction

The authoritative, always-current roadmap lives in the repo at
[`ROADMAP.md`](https://github.com/srjn45/FileDBv2/blob/main/ROADMAP.md). Because
the v1 API is frozen, ongoing work focuses on performance, operability, and the
client ecosystem rather than breaking API changes.

Have a use case that isn't covered? [Open an issue](https://github.com/srjn45/FileDBv2/issues).
