---
title: Configuration
description: Server flags, the YAML config file, environment variables, TLS, scoped API keys, and operational limits.
---

ScrivaDB is configured by CLI flags, an optional YAML file, and a few environment
variables. **CLI flags always win** over the config file.

## Starting the server

```bash
scriva serve \
  --data ./data \
  --api-key my-secret-key \
  --grpc-addr :5433 \
  --rest-addr :8080
```

Or via a config file:

```bash
scriva serve --config scriva.yaml
```

See the fully commented
[`scriva.example.yaml`](https://github.com/srjn45/scriva/blob/main/scriva.example.yaml).

## Core flags

| Flag | Default | Description |
|---|---|---|
| `--data` | `./data` | Data directory. |
| `--grpc-addr` | `:5433` | TCP gRPC listen address. |
| `--rest-addr` | `:8080` | REST gateway listen address. |
| `--socket` | `/tmp/scriva.sock` | Unix domain socket path. |
| `--api-key` | `$SCRIVA_API_KEY` | API key (empty = no auth). |
| `--metrics-addr` | `:9090` | Prometheus metrics address (empty = disabled). |
| `--segment-size` | `4194304` | Max segment file size in bytes (4 MiB). |
| `--compact-interval` | `5m` | Compaction interval. |
| `--compact-dirty` | `0.30` | Dirty-ratio threshold to trigger compaction. |
| `--sync` | `none` | Durability mode: `none`, `always`, or `interval`. |
| `--sync-interval` | `1s` | Flush cadence when `--sync=interval`. |
| `--tx-timeout` | `5m` | Idle timeout before an open transaction is reaped (`0` = off). |
| `--default-ttl` | `0` | Default expiry for inserted records (`0` = never), e.g. `24h`. |
| `--watch-buffer` | `64` | Per-subscriber Watch buffer; overflow signals a slow subscriber. |

## Security

| Flag | Description |
|---|---|
| `--tls-cert` / `--tls-key` | Secure the TCP gRPC listener with a cert/key pair. |
| `--tls-client-ca` | PEM CA bundle that signs trusted client certs (enables mTLS). |
| `--tls-client-auth` | Client-cert policy: `off`, `require`, or `verify-if-given`. |

**Scoped API keys** — configure multiple named keys with `read` or `read-write`
scope. A read-only key is refused on writes, and keys **hot-reload on `SIGHUP`**
for rotation without a restart.

## Observability & limits

| Flag | Description |
|---|---|
| `--log-level` | `debug`, `info`, `warn`, or `error`. |
| `--log-format` | `text` or `json`. |
| `--audit-log` | Append-only NDJSON audit file of mutating/admin RPCs and auth failures. |
| `--slow-query-ms` | Log any `Find` slower than N ms at `WARN`, with scan stats (`0` = off). |
| `--max-inflight` | Server-wide in-flight RPC ceiling; excess gets `RESOURCE_EXHAUSTED` (`0` = off). |
| `--rate-limit` | Per-API-key rate limit (req/s); over-budget gets `RESOURCE_EXHAUSTED` (`0` = off). |
| `--max-concurrent-streams` | Max HTTP/2 streams per gRPC connection (`0` = library default). |
| `--otlp-endpoint` | OTLP/gRPC collector for OpenTelemetry tracing (empty = off). |
| `--otlp-sample-ratio` | Trace sampling ratio. |

## Health & readiness

- gRPC `grpc.health.v1.Health` (`SERVING` → `NOT_SERVING` on graceful shutdown).
- HTTP `/healthz` (liveness) and `/readyz` (DB open + data dir writable).

## Next

- [Durability & backup](/scriva/guides/durability-and-backup/)
- [API & OpenAPI](/scriva/reference/api/)
