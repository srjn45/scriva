---
title: Atomic batches & upgrading roots
description: Make multi-write changes in one collection crash-atomic, and safely convert an existing data directory.
---

Use an atomic batch when one change updates several records in **one collection**.
On a new or converted **atomic-batch root**, `InsertMany`, a
single-collection `CommitTx` with two or more writes, and bulk loads either
become visible together or do not become visible at all after a crash or power
loss. For one decision spanning collections, use
[cross-collection transactions](/scriva/guides/cross-collection-transactions/)
instead.

:::caution[Limits and cost]
Atomic batches are limited to **1,000 operations or 64 MiB** and are never split
by ScrivaDB. Each batch forces both a data fsync and a decision fsync, regardless
of `--sync`; relaxed sync modes therefore do not make these batches cheaper. A
larger workload must be split by the application, and each resulting batch is an
independent atomic decision.
:::

## Which data directories are atomic

| Root | Batch behavior after a crash |
|---|---|
| New directory created by this release | Atomic from its first write. |
| Existing directory with `xtx.format` | Atomic; it already has coordinator state. |
| Legacy directory with collections but no `xtx.format` | **Not crash-atomic.** A crash can replay a complete prefix of a multi-write batch. Merely upgrading the binary does not change it. |
| Follower directory | Initialized by replicated leader envelopes; do not convert it locally. |

Atomicity also covers reader visibility for one call: a scan, `Find`, aggregate,
or `Count` sees the batch as a whole or not at all. Separate calls do not form a
shared snapshot.

## Convert a legacy root

Conversion enables the coordinator that records a durable all-or-nothing
decision. It is **one-way**: a converted root cannot be opened by releases from
before cross-collection transactions. There is no drain or in-place undo.

1. Take and verify a backup first. Use `scriva repair`'s verified backup, or
   stop the server and copy its complete data directory. Restoring that backup
   is the rollback path.
2. If the database replicates, upgrade followers first. A follower that cannot
   understand transaction envelopes is refused when the first batch reaches it.
3. Start the leader once with the conversion option:

```bash
scriva serve --data ./data --enable-atomic-batches
```

```yaml
# scriva.yaml
enable_atomic_batches: true
```

The log line `converted legacy root to atomic batches` confirms success. The
flag may remain configured or be removed after that start; the root itself
retains the format marker. For rollback, recovery, downgrade order, and the
full operational procedure, follow the [canonical upgrade and recovery
playbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md#atomic-single-collection-batches-rollout-and-recovery).

## Recovery and uncertain outcomes

ScrivaDB writes the batch data, makes it durable, then records and fsyncs its
commit decision. A crash before that decision recovers the old state; a durable
decision recovers the complete new state, even if the client never received a
reply. It never exposes a partial batch on an atomic root.

Do not blindly retry a batch whose result is unknown. A failed data phase is a
proven abort and can be retried. But a failed decision append or fsync can leave
the outcome unknown; use the transaction ID reported with the error to resolve
it with `XTxStatus` before taking another action. A post-decision rotation error
means the batch committed, while the collection is fenced pending restart.

If recovery cannot prove the coordinator evidence, open fails closed rather
than serving a prefix. Run the [verify and repair guidance](/scriva/guides/durability-and-backup/#verify-and-repair), then use the [atomic-batch recovery
runbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md#atomic-single-collection-batches-rollout-and-recovery).

## Choose the right boundary

- Use an atomic batch for multiple writes to one collection.
- Use a [cross-collection transaction](/scriva/guides/cross-collection-transactions/) for multiple collections, its point-read conflict checks, and idempotency keys.
- Use ordinary single-record writes when no multi-record invariant is involved.

For the wire API and generated client stubs, see [API & OpenAPI](/scriva/reference/api/). The existing hand-written SDKs cover single-collection transactions; see [Client SDKs](/scriva/guides/clients/) for their exact surface.
