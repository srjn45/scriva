---
title: Queries & indexes
description: Filters, streaming Find, keyset pagination, field projection, secondary indexes, and server-side aggregations.
---

## Filters

A filter is a small JSON object: a `field`, an `op`, and a `value`.

```json
{"field": "age", "op": "gt", "value": 18}
```

Supported operators include `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, plus
composition for multi-condition queries.

```bash
filedb-cli find users '{"field":"name","op":"eq","value":"alice"}' --api-key dev-key
```

## Streaming Find

`Find` **streams** results as it reads. It pushes `limit`, `offset`, a
multi-field `order_by_fields` sort, and a keyset `page_token` cursor **into the
engine** — so a limited query is bounded by the page size, not the collection
size, and it honours client cancellation.

```bash
filedb-cli find users '{"field":"age","op":"gte","value":21}' \
  --limit 50 --order-by age --api-key dev-key
```

### Keyset pagination

Pass the `page_token` returned by one page into the next. Cursor pagination
seeks past already-returned rows in **O(page)** — concatenated pages cover every
row exactly once, with no duplicates or gaps.

### Field projection

Return only the fields you need (`id`, `key`, and `rev` are always included):

```bash
filedb-cli find users '{"field":"age","op":"gt","value":18}' \
  --fields name,age --api-key dev-key
```

## Secondary indexes

Per-field indexes give **O(1) equality lookups** and **O(matches) range
queries**. They're automatically maintained on write and persisted across
restarts, alongside the in-memory `id` index (which provides O(1) lookup by id
and is checksummed for fast restart).

When a query can use an index, the engine scans only matching rows instead of
the whole collection. Turn on the [slow-query log](/FileDBv2/reference/configuration/)
(`--slow-query-ms`) to surface unindexed hot queries — it reports the filter
shape, rows scanned vs returned, and whether an index was used.

## Aggregations

`Aggregate` computes `count` and numeric `sum` / `avg` / `min` / `max` over the
**same filter** as `Find`, optionally grouped by a field. It streams one result
per group and folds records **in the engine** — so memory is bounded by the
number of distinct groups, not the number of rows, and clients total server-side
instead of pulling the whole collection.

```bash
filedb-cli aggregate users \
  --group-by country \
  --field age \
  --aggs count,sum,avg,min,max \
  --api-key dev-key
```

## Watch

Subscribe to live changes on a collection with `Watch`. Each subscriber has a
bounded buffer (`--watch-buffer`, default 64); a subscriber too slow to keep up
receives an explicit `OVERFLOW` signal rather than blocking writers.

## Next

- [Durability & backup](/FileDBv2/guides/durability-and-backup/)
- [Architecture](/FileDBv2/concepts/architecture/) — how the read/write paths
  and compaction actually work.
