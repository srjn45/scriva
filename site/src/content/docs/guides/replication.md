---
title: Replication & failover
description: Leader→follower replication, read replicas, staleness bounds, and manual promotion after a leader loss.
---

ScrivaDB supports **leader→follower** replication for read scaling and standby
redundancy. It is intentionally simple: asynchronous, with **manual** failover.
Automatic election is out of scope.

## Starting a follower

Start a hot standby that replicates from a leader:

```bash
scriva serve --data ./follower-data \
  --replicate-from leader-host:5433 \
  --api-key dev-key
```

The follower:

1. **Bootstraps** from a snapshot of the leader.
2. **Tails** the leader's committed writes — each tagged with a monotonic global
   **LSN** — and applies them through the normal write path, so its indexes end
   up byte-for-byte consistent.
3. **Resumes** after a disconnect from its persisted applied-LSN, with **no gaps
   or duplicates**.

Replication is async, so the follower runs at a bounded lag behind the leader.

## Cross-collection transactions

Cross-collection transactions replicate as a single **transaction record** (protocol
v2) carrying every operation under one LSN, so a follower never sees a partial
transaction. Followers built from this release advertise protocol v2
automatically. A leader will **not** downgrade a transaction to per-entry records:
an older follower that does not speak v2 keeps receiving ordinary writes, but is
refused with `FAILED_PRECONDITION` (`REPLICATION_PROTOCOL_UNSUPPORTED`) the moment
a transaction must be shipped — upgrade the follower first.

The follower applies each transaction **atomically and durably** through its own
cross-collection coordinator: readers see all of its operations or none, and the
follower's applied LSN advances only once the whole transaction is committed
locally. After a crash or reconnect an already-applied transaction is recognised
and skipped; a redelivery whose content differs from what was applied stops the
follower with a divergence error (re-bootstrap it from a snapshot). Missing
participant collections are created on the follower as legacy replication does.
A promotion waits for an in-flight transaction apply, so a new leader never
starts from a partially applied transaction. See
[design-xtx-replication.md](https://github.com/srjn45/scriva/blob/main/docs/design-xtx-replication.md).

## Read replicas

A follower serves reads — `Find`, `FindById`, `FindByKey`, `Aggregate` — from
its applied state, and **refuses writes** with a typed `FAILED_PRECONDITION`. So
you can scale read traffic horizontally by adding followers.

### Bounding staleness

Every server exposes `ReplicationStatus` (leader LSN, per-follower lag). Bound
how stale a replica read may be by diffing a follower's `applied_lsn` against the
leader's `leader_lsn`.

## Manual failover

After a leader loss, promote a caught-up follower with the admin `Promote` RPC:

```bash
scriva-cli promote --api-key dev-key
```

Promotion:

- Stops replicating and **lifts the read-only guard**, so the node accepts writes.
- Is guarded by a **lag check** — a stale replica is refused (override with
  `--force`).
- Is **one-way**. There is no automatic election; re-point the old leader as a
  follower of the new one if you want it back.

See the [operator runbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md)
for the full failover procedure.

## Next

- [Configuration](/scriva/reference/configuration/)
- [Architecture](/scriva/concepts/architecture/)
