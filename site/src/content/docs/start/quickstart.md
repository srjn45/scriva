---
title: Quickstart
description: Start the server, insert your first record, query it, and open the REPL — in under a minute.
---

Once you've [installed](/scriva/start/install/) `scriva` and `scriva-cli`,
you're ready to go.

## 1. Start the server

```bash
scriva serve --data ./data --api-key dev-key
```

This opens gRPC on `:5433`, the REST gateway on `:8080`, and a Unix socket at
`/tmp/scriva.sock` (which the CLI prefers when it's local).

## 2. Insert a record

```bash
scriva-cli insert users '{"name":"alice","age":30}' --api-key dev-key
```

Every record comes back with an `id`, a caller-visible `key`, and a monotonic
`rev`.

## 3. Query it

Filters are small JSON objects (`field` / `op` / `value`):

```bash
scriva-cli find users '{"field":"name","op":"eq","value":"alice"}' --api-key dev-key
scriva-cli find users '{"field":"age","op":"gt","value":18}'       --api-key dev-key
```

## 4. Use the REST gateway

Anything the CLI can do is also available over HTTP on `:8080`:

```bash
curl -H "x-api-key: dev-key" \
  -d '{"data":{"name":"bob","age":41}}' \
  http://localhost:8080/v1/users/records
```

## 5. Open the REPL

For interactive exploration:

```bash
scriva-cli repl --api-key dev-key
```

```
scriva> use users
scriva> insert {"name":"carol","age":27}
scriva> find {"field":"age","op":"lt","value":30}
scriva> aggregate --field age --aggs count,avg
```

## What just happened

Your data now lives in `./data/users/` as append-only NDJSON — go ahead and
`cat` it. From here:

- Understand [keys, revisions, and CAS](/scriva/guides/data-model/).
- Learn [queries, indexes, and aggregations](/scriva/guides/queries/).
- Tune [durability and set up backups](/scriva/guides/durability-and-backup/).
