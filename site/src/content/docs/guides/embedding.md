---
title: Embedding (Go library)
description: Run FileDB in-process as a Go library — keyed CRUD, compare-and-swap, secondary indexes, and in-process Watch, with zero heavy dependencies.
---

FileDB v2's storage engine is a plain Go library, so you can skip the server
entirely and run the database **in-process** — no gRPC, no network, no separate
daemon. This is the right choice when your program is the only writer and you
want FileDB's durability and query model directly inside your binary.

## Install

```bash
go get github.com/srjn45/filedbv2/filedb   # the ergonomic façade (recommended)
go get github.com/srjn45/filedbv2/engine   # the lower-level storage engine
```

## Usage

```go
import "github.com/srjn45/filedbv2/filedb"

db, _ := filedb.Open("./data")            // embedded durability defaults (fsync ~1s)
defer db.Close()

sessions := db.MustCollection("sessions")

id, _, _ := sessions.InsertWithKey("sess-1", map[string]any{"status": "open"})
rec, _   := sessions.GetByKey("sess-1")   // caller-supplied string keys

// optimistic compare-and-swap on the record's revision
_, _ = sessions.UpdateIfRev("sess-1", rec.Rev, map[string]any{"status": "closed"})
```

## What's included

The embedded surface gives you:

- Caller-supplied **string keys** and keyed find/update/delete.
- Per-record **revisions** with **compare-and-swap** (`UpdateIfRev`).
- **Upsert**, **count**, and **exists**.
- **Secondary indexes** for fast equality and range queries.
- In-process **`Watch`** subscriptions with a documented overflow contract.
- A **`LoadJSONL`** bulk-import path.

## Zero heavy dependencies

The `engine` package pulls in **no** gRPC, protobuf, Prometheus, cobra, or
OpenTelemetry dependencies — a **CI gate enforces this**. Embedding FileDB won't
drag a server framework into your binary.

## Two distribution channels

Embedding is a distinct channel from the server:

| Channel | How you get it | For |
|---|---|---|
| **Embedded** | `go get` the module | In-process use inside a Go program. |
| **Server** | Homebrew / apt / GHCR / release binaries | A standalone database over gRPC + REST. |

Both build from the same repo. See the full API reference, durability modes, the
Watch overflow contract, and the versioning/stability policy in
[`docs/embedding.md`](https://github.com/srjn45/FileDBv2/blob/main/docs/embedding.md).

## Next

- [Data model](/FileDBv2/guides/data-model/) — the same keys/revisions/CAS model.
- [Client SDKs](/FileDBv2/guides/clients/) — if you'd rather talk to a server
  from another language.
