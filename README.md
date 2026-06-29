# FileDB v2

A lightweight, append-only, file-based document database — single binary, zero dependencies, human-readable storage.

[![CI](https://github.com/srjn45/filedbv2/actions/workflows/ci.yml/badge.svg)](https://github.com/srjn45/filedbv2/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/srjn45/filedbv2)](https://goreportcard.com/report/github.com/srjn45/filedbv2)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Quick start

```bash
# 1. Start the server
filedb serve --data ./data --api-key dev-key

# 2. Insert a record
filedb-cli insert users '{"name":"alice","age":30}' --api-key dev-key

# 3. Query it
filedb-cli find users '{"field":"name","op":"eq","value":"alice"}' --api-key dev-key
```

Or use the interactive REPL:

```bash
filedb-cli repl --api-key dev-key
```

---

## What it is

FileDB v2 stores each collection as a set of **NDJSON segment files** — one JSON object per line, appended in order. There is no binary format, no external runtime, and no hidden state. You can inspect, backup, or migrate your data with any text editor or Unix tool.

Key properties:

- **Append-only writes** — inserts, updates, and deletes are always new lines; no in-place modification
- **Configurable durability** — choose `none` (OS flush), `always` (fsync per write), or `interval` (fsync on a timer) to trade throughput against crash-loss window
- **Background compaction** — a goroutine per collection merges and deduplicates sealed segments
- **In-memory index** — O(1) lookup by id, persisted with a checksum for fast restarts
- **Secondary indexes** — per-field inverted indexes for O(1) equality lookups; automatically maintained and persisted
- **Transactions** — optimistic multi-operation transactions via `BeginTx` / `CommitTx` / `RollbackTx`
- **gRPC + REST** — dual API served from one binary; CLI uses the Unix socket when local
- **OpenAPI spec** — `docs/openapi/filedb.swagger.json` generated from the proto; generate clients for any language with [openapi-generator](https://openapi-generator.tech/)
- **Optional TLS** — TCP gRPC listener can be secured with a cert/key pair; CLI verifies via `--tls-ca`
- **YAML config file** — `--config filedb.yaml` with CLI flag overrides always winning
- **Prometheus metrics** — per-collection gauges, compaction histograms, and gRPC request duration at `--metrics-addr`
- **Single binary** — no JVM, no Python, no config files required to get started
- **Web admin UI** — browser-based collection and record manager at `clients/web/` (React + Vite, talks to the REST gateway)

---

## When to use it

FileDB v2 is the right tool when:

- You need persistence without standing up PostgreSQL or MongoDB
- Your data fits on one machine and you want human-readable files
- You're building CLI tools, local services, IoT daemons, or small web apps
- You want a simple HTTP/gRPC API you can call from any language

It is **not** the right tool for multi-node replication, complex joins, or datasets too large to compact on a single machine.

---

## Documentation

| Document | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Install, run, first queries, TLS, config file, secondary indexes, metrics |
| [Architecture](docs/architecture.md) | Storage model, write/read paths, compaction, secondary indexes, crash safety |

---

## Build from source

```bash
git clone https://github.com/srjn45/filedbv2
cd filedbv2
make build          # produces bin/filedb and bin/filedb-cli
make test           # run tests with race detector
make proto          # regenerate gRPC code (requires buf)
```

---

## License

MIT — see [LICENSE](LICENSE).

---

*Rebuilt from [FileDB PHP](https://github.com/srjn45/FileDB-php) — original college project, now in Go.*
