---
title: Replication & failover
description: Leader→follower replication, read replicas, staleness bounds, and manual promotion after a leader loss.
---

FileDB supports **leader→follower** replication for read scaling and standby
redundancy. It is intentionally simple: asynchronous, with **manual** failover.
Automatic election is out of scope.

## Starting a follower

Start a hot standby that replicates from a leader:

```bash
filedb serve --data ./follower-data \
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
filedb-cli promote --api-key dev-key
```

Promotion:

- Stops replicating and **lifts the read-only guard**, so the node accepts writes.
- Is guarded by a **lag check** — a stale replica is refused (override with
  `--force`).
- Is **one-way**. There is no automatic election; re-point the old leader as a
  follower of the new one if you want it back.

See the [operator runbook](https://github.com/srjn45/FileDBv2/blob/main/docs/operations.md)
for the full failover procedure.

## Next

- [Configuration](/FileDBv2/reference/configuration/)
- [Architecture](/FileDBv2/concepts/architecture/)
