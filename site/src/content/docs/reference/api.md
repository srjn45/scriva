---
title: API & OpenAPI
description: The dual gRPC + REST API, the proto source of truth, the generated OpenAPI spec, and health endpoints.
---

ScrivaDB serves a **dual API from one binary**: gRPC (default `127.0.0.1:5433`) and a
grpc-gateway REST bridge (default `127.0.0.1:8080`). The CLI prefers the Unix socket when it's
talking to a local server.

## Source of truth

The entire API is defined in a single proto file,
[`proto/scriva.proto`](https://github.com/srjn45/scriva/blob/main/proto/scriva.proto).
The gRPC stubs, the REST gateway, and the OpenAPI spec are all generated from it.

## RPC surface

| Group | RPCs |
|---|---|
| Collections | `CreateCollection`, `DropCollection`, `ListCollections`, `CollectionStats` |
| CRUD | `Insert`, `InsertMany`, `Update`, `Delete`, `FindById` |
| Keyed | `FindByKey`, `UpdateByKey`, `DeleteByKey`, `Upsert`, `UpdateIfRev` (a caller key is set on `Insert`; there is no `InsertWithKey` RPC) |
| Query | `Find` (streaming), `Aggregate` (streaming), `Watch` (streaming) |
| Indexes | `EnsureIndex`, `DropIndex`, `ListIndexes` |
| Transactions | `BeginTx`, `CommitTx`, `RollbackTx` (**gRPC only**, no REST route) |
| Cross-collection transactions | `BeginXTx`, `StageXTx`, `GetXTx`, `CommitXTx`, `RollbackXTx`, `XTxStatus` |
| Admin | `Compact`, `ReplicationStatus`, `Promote` |
| Backup / replication | `Snapshot` (streaming), `Replicate` (streaming) — **gRPC only**, no REST route |
| Health | `grpc.health.v1.Health` |

Online backup is the `Snapshot` RPC (`scriva-cli backup`); there is no `Backup` RPC.

Errors use standard gRPC status codes — e.g. a duplicate key is `AlreadyExists`,
a missing record is `NotFound`, a write to a read replica is
`FAILED_PRECONDITION`, and shed load is `RESOURCE_EXHAUSTED`.

For the XTx lifecycle, isolation limits, retry rules, and authorization model,
see [Cross-collection transactions](/scriva/guides/cross-collection-transactions/).

## REST examples

```bash
# insert
curl -H "x-api-key: dev-key" \
  -d '{"data":{"name":"alice","age":30}}' \
  http://localhost:8080/v1/users/records

# find
curl -H "x-api-key: dev-key" \
  -d '{"filter":{"field":"age","op":"gt","value":18}}' \
  http://localhost:8080/v1/users/records/find
```

## OpenAPI

A generated OpenAPI/Swagger spec lives at
[`docs/openapi/scriva.swagger.json`](https://github.com/srjn45/scriva/blob/main/docs/openapi/scriva.swagger.json)
and covers the 29 REST-annotated operations. The five gRPC-only RPCs —
`BeginTx`, `CommitTx`, `RollbackTx`, `Snapshot` and `Replicate` — have no REST
route and are not in the spec; use the generated gRPC stubs for them. Generate a client for any language with
[openapi-generator](https://openapi-generator.tech/), or use one of the
[hand-written SDKs](/scriva/guides/clients/) (which wrap most, but not all, RPCs — see that page for the exceptions).

## Health endpoints

| Endpoint | Meaning |
|---|---|
| gRPC `Health/Check` | `SERVING` until graceful shutdown. |
| HTTP `GET /healthz` | Liveness. |
| HTTP `GET /readyz` | Readiness — DB open and data dir writable. |

## Stability

As of **v1.0.0**, the API is **frozen** — see the
[roadmap](/scriva/reference/roadmap/) and the project
[CHANGELOG](https://github.com/srjn45/scriva/blob/main/CHANGELOG.md).
