---
title: What is ScrivaDB?
description: A lightweight, append-only, file-based document database — one binary, human-readable NDJSON storage, gRPC + REST, and an embeddable Go engine.
---

**ScrivaDB** is a lightweight, append-only, file-based document database. It
stores each collection as a set of **NDJSON segment files** — one JSON object
per line, appended in order. There is no binary format, no external runtime, and
no hidden state. You can inspect, back up, or migrate your data with any text
editor or Unix tool.

```bash
$ scriva-cli insert users '{"name":"alice","age":30}' --api-key dev-key
  ✓ id=01J8...  key=01J8...  rev=1

$ tail -1 data/users/000001.ndjson      # your data is just text
  {"id":"01J8...","key":"01J8...","rev":1,"data":{"name":"alice","age":30},"crc":"a1b2"}
```

## How it's delivered

ScrivaDB ships as **two binaries built from one repo**:

- **`scriva`** — the server. Serves a dual **gRPC + REST** API from a single
  process, on `:5433` (gRPC) and `:8080` (REST) by default.
- **`scriva-cli`** — the client. Subcommands plus an interactive REPL; talks to
  a local server over a Unix domain socket.

And a third distribution channel — the **embeddable engine**. `go get` the
`scriva`/`engine` packages and run the database **in-process**, with no gRPC,
no network, and no separate daemon.

## What makes it different

- **Human-readable on disk** — NDJSON segments you can `cat`, `grep`, and
  `tar`. Backups are a gzip stream; restore is `tar xzf`.
- **Append-only writes** — inserts, updates, and deletes are always new lines;
  nothing is modified in place. A background compactor merges and deduplicates
  sealed segments.
- **End-to-end integrity** — every entry carries a CRC32C checksum, so silent
  bit-rot is caught on read instead of returning wrong data.
- **Real query engine** — secondary indexes for O(1) equality and range
  lookups, streaming `Find` with keyset pagination, and server-side
  aggregations.
- **More than key-value** — caller-supplied string keys, per-record revisions
  with compare-and-swap, upsert, TTL, optimistic transactions, and live `Watch`
  subscriptions.

## When to use it

ScrivaDB is the right tool when:

- You need persistence without standing up PostgreSQL or MongoDB.
- Your data fits on one machine and you want human-readable files.
- You're building CLI tools, local services, IoT daemons, or small web apps.
- You want a simple HTTP/gRPC API you can call from any language.

It is **not** the right tool for datasets too large to compact on a single
machine, complex joins, or automatic multi-node consensus. Replication is
leader→follower with **manual** failover — deliberately simple.

## Next steps

- [Install](/scriva/start/install/) the server and CLI.
- Run through the [Quickstart](/scriva/start/quickstart/).
- Learn the [data model](/scriva/guides/data-model/) — keys, revisions, CAS.
