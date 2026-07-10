---
title: API & OpenAPI
description: The dual gRPC + REST API, the proto source of truth, the generated OpenAPI spec, and health endpoints.
---

FileDB serves a **dual API from one binary**: gRPC on `:5433` and a
grpc-gateway REST bridge on `:8080`. The CLI prefers the Unix socket when it's
talking to a local server.

## Source of truth

The entire API is defined in a single proto file,
[`proto/filedb.proto`](https://github.com/srjn45/FileDBv2/blob/main/proto/filedb.proto).
The gRPC stubs, the REST gateway, and the OpenAPI spec are all generated from it.

## RPC surface

| Group | RPCs |
|---|---|
| CRUD | `Insert`, `Update`, `Delete`, `FindById` |
| Keyed | `InsertWithKey`, `FindByKey`, `UpdateByKey`, `DeleteByKey`, `Upsert`, `UpdateIfRev` |
| Query | `Find` (streaming), `Aggregate` (streaming), `Watch` (streaming) |
| Transactions | `BeginTx`, `CommitTx`, `RollbackTx` |
| Admin | `Compact`, `Backup`, `Promote`, `ReplicationStatus` |
| Health | `grpc.health.v1.Health` |

Errors use standard gRPC status codes — e.g. a duplicate key is `AlreadyExists`,
a missing record is `NotFound`, a write to a read replica is
`FAILED_PRECONDITION`, and shed load is `RESOURCE_EXHAUSTED`.

## REST examples

```bash
# insert
curl -H "x-api-key: dev-key" \
  -d '{"data":{"name":"alice","age":30}}' \
  http://localhost:8080/v1/users/records

# find
curl -H "x-api-key: dev-key" \
  -d '{"filter":{"field":"age","op":"gt","value":18}}' \
  http://localhost:8080/v1/users/records:find
```

## OpenAPI

A generated OpenAPI/Swagger spec lives at
[`docs/openapi/filedb.swagger.json`](https://github.com/srjn45/FileDBv2/blob/main/docs/openapi/filedb.swagger.json)
and covers every RPC. Generate a client for any language with
[openapi-generator](https://openapi-generator.tech/), or use one of the
[hand-written SDKs](/FileDBv2/guides/clients/).

## Health endpoints

| Endpoint | Meaning |
|---|---|
| gRPC `Health/Check` | `SERVING` until graceful shutdown. |
| HTTP `GET /healthz` | Liveness. |
| HTTP `GET /readyz` | Readiness — DB open and data dir writable. |

## Stability

As of **v1.0.0**, the API is **frozen** — see the
[roadmap](/FileDBv2/reference/roadmap/) and the project
[CHANGELOG](https://github.com/srjn45/FileDBv2/blob/main/CHANGELOG.md).
