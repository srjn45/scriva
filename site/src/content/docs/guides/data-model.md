---
title: Data model
description: Collections, records, caller-supplied keys, monotonic revisions, upsert, compare-and-swap, TTL, and transactions.
---

FileDB is a **document** database. You organize data into **collections**, and
each collection holds JSON **records**.

## Records

Every record carries three pieces of engine metadata alongside your `data`:

| Field | Meaning |
|---|---|
| `id` | Immutable, engine-assigned identity for the record. |
| `key` | A caller-supplied (or auto-generated) **string key** — your natural key. |
| `rev` | A **monotonic revision** that increments on every update. |

Plus a `crc` checksum, which the engine verifies on read.

## Keyed CRUD

You can address records by their natural `key`, not just the internal `id`:

```bash
filedb-cli insert users '{"name":"alice"}' --key alice --api-key dev-key
filedb-cli get    users --key alice --api-key dev-key
filedb-cli update users --key alice '{"name":"alice","age":31}' --api-key dev-key
filedb-cli delete users --key alice --api-key dev-key
```

Duplicate keys are rejected with `AlreadyExists`; missing keys return
`NotFound`.

## Upsert

Insert-or-replace in one call — idempotent by key:

```bash
filedb-cli upsert users '{"name":"alice","age":31}' --key alice --api-key dev-key
```

## Compare-and-swap (CAS)

Because every record has a `rev`, you get optimistic concurrency for free.
`update-if-rev` only applies your change if the stored revision still matches —
otherwise it fails, so two writers can't silently clobber each other.

```bash
# read gives you rev=4; only update if it's still 4
filedb-cli update-if-rev users --key alice --rev 4 '{"name":"alice","age":32}' --api-key dev-key
```

## TTL / expiring records

Records can carry a deadline after which they're hidden from reads and reclaimed
by compaction:

- Per record: `--ttl 24h` on insert/update.
- Per collection default: `create-collection --default-ttl ...` (persisted).
- Server-wide default: `--default-ttl` on `filedb serve`.

Expired records disappear from reads **immediately**; the on-disk space is
reclaimed on the next compaction pass.

## Transactions

For multi-operation atomicity, FileDB offers **optimistic** transactions:

```
BeginTx  →  (insert / update / delete ...)  →  CommitTx
                                             ↘  RollbackTx
```

A commit succeeds only if none of the records the transaction touched changed
underneath it. Idle transactions are reaped after `--tx-timeout` (default 5m).

## Next

- [Queries & indexes](/FileDBv2/guides/queries/) — filters, pagination,
  secondary indexes, aggregations.
- [Embedding](/FileDBv2/guides/embedding/) — the same model as an in-process Go
  API.
