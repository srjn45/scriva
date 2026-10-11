# Operations

Operator runbooks for running ScrivaDB in production. Start with
[getting-started.md](getting-started.md) for setup and
[architecture.md](architecture.md) for how the internals work.

---

## Manual failover (promoting a follower)

ScrivaDB replicates a **leader** to one or more read-only **followers** (see
[Replication](getting-started.md#replication-leader--follower)). Replication is
asynchronous and there is **no automatic leader election** — recovering write
availability after a leader loss is a deliberate operator action: promote a
caught-up follower to leader with the admin **`Promote`** RPC.

Promotion is **one-way**. A promoted node becomes an ordinary leader; the old
leader must not come back as a leader against the same data (see
[Preventing split-brain](#preventing-split-brain)).

### Prerequisites

- A follower running with `--replicate-from <old-leader>`.
- A **read-write** API key for the follower (`Promote` is an admin operation).
- Network reachability from clients (and any surviving followers) to the
  follower you intend to promote.

### Runbook

**1. Detect leader loss.**

Confirm the leader is actually down (not a transient network blip) before failing
over — promoting while the old leader still takes writes causes split-brain. Check
liveness/readiness and that clients are erroring:

```bash
curl -sf http://<leader>:8080/healthz   # liveness; fails/refuses when down
curl -sf http://<leader>:8080/readyz    # readiness
```

**2. Pick the most caught-up follower and verify it is current.**

If you run several followers, promote the one with the highest applied LSN (the
least data loss). Read each follower's applied LSN:

```bash
curl -s -H 'x-api-key: <key>' http://<follower>:8080/v1/replication/status
# {"appliedLsn":"1234", ...}
```

Ideally the chosen follower's `appliedLsn` equals the leader's last-known
`leaderLsn` (**lag 0** — no data loss). The follower tracks the last leader LSN it
observed; the `Promote` call itself reports the lag it measured.

**3. Promote.**

```bash
scriva-cli --host <follower>:5433 --api-key <key> promote
# promoted: role=leader lsn=1234 lag=0
```

or over REST:

```bash
curl -s -H 'x-api-key: <key>' http://<follower>:8080/v1/replication/promote -d '{}'
# {"role":"leader","lsn":"1234","lag":"0"}
```

On success the follower stops replicating from its old upstream, lifts its
read-only guard, and begins accepting writes at the LSN reported in the response
(new writes are tagged with strictly greater LSNs, so no LSN is ever reused).

**4. Repoint writers and surviving followers.**

- Point write clients at the new leader's address.
- Re-point any other followers by restarting them with
  `--replicate-from <new-leader>`. A follower that was tailing the old leader will
  **not** automatically follow the new one. If a follower's data has diverged from
  the new leader (it applied writes the new leader never had, or vice-versa),
  re-bootstrap it: stop it, wipe its data directory, and restart it against the
  new leader so it takes a fresh snapshot.

**5. Rebuild the old leader as a follower (optional).**

When the old node returns, do **not** let it serve as a leader. Wipe its data
directory and restart it with `--replicate-from <new-leader>` so it re-bootstraps
and rejoins as a follower.

### The lag guard

Promotion refuses to silently discard data. If the chosen follower's replication
lag (**last-known leader LSN − applied LSN**) exceeds the configured ceiling, the
call fails with `FAILED_PRECONDITION` and reports the lag:

```
FAILED_PRECONDITION: replica lag exceeds promotion threshold: applied_lsn=1200 last_known_leader_lsn=1234 lag=34 threshold=0
```

The ceiling is `--promote-max-lag` (config `promote_max_lag`), **default 0** — the
follower must be fully caught up with every write it knows the leader committed.
Raise it if you can tolerate a bounded amount of lost tail, or wait for the
follower to catch up and retry.

When the leader is **unrecoverable** and you accept losing its un-replicated
tail, force the promotion past the guard:

```bash
scriva-cli --host <follower>:5433 --api-key <key> promote --force
# or REST: curl ... /v1/replication/promote -d '{"force":true}'
```

A forced promotion of a lagging replica **permanently discards** the writes the
old leader committed but never shipped. Use it only when the alternative (no
leader at all) is worse.

### Preventing split-brain

There is no coordinator to fence the old leader, so **you** must ensure only one
leader takes writes at a time:

- Only promote once you have confirmed the old leader is down (or has been
  stopped / had its write traffic cut off).
- After failover, never restart the old node as a leader against its old data —
  rebuild it as a follower of the new leader (step 5).
- Steer clients with a single indirection (DNS name, load-balancer target, or
  config) you can flip atomically, rather than hard-coding leader addresses in
  every client.

### Scope & limitations

- **Manual only.** There is no automatic failure detection or election. Promotion
  is an explicit, audited operator action.
- **One-way.** `Promote` on a node that is already a leader is refused
  (`FAILED_PRECONDITION`, nothing to promote).
- **Async replication.** A non-forced promotion of a fully-caught-up follower
  loses no acknowledged data; a forced promotion of a lagging follower may.
- **Auth.** `Promote` requires a read-write key. Finer per-key admin ACLs (an
  admin scope) arrive with the S3 milestone; until then a read-write key is the
  admin boundary. The replication link itself uses plain gRPC — run it inside a
  trusted network (mutual TLS is a later milestone).

---

## Replicating cross-collection transactions (protocol v2)

A cross-collection transaction (`CommitXTx`) is replicated as **one record with
one LSN**: a follower applies it atomically through its own durable coordinator,
so a read on a follower sees either none or all of a committed leader
transaction — including across follower crashes, reconnects and snapshot
bootstraps. Design: [design-xtx-replication.md](design-xtx-replication.md).

### Upgrade order (protocol negotiation)

Upgrade **followers first, then the leader**. Followers advertise protocol
`{1, 2}`; the leader never downgrades a transaction to per-entry records. A
follower that does not speak v2 and would need a transaction envelope is cut off
with `FAILED_PRECONDITION` / `REPLICATION_PROTOCOL_UNSUPPORTED`
(`server.IsReplicationProtocolError`): upgrade that follower and restart it.
Single-entry traffic keeps flowing to old followers until the first transaction.

### Monitoring lag

- `ReplicationStatus` RPC / `GET /v1/replication/status`: on the leader
  `leader_lsn` and per-follower `lag`; on a follower `applied_lsn`. A transaction
  is one LSN, and `applied_lsn` never covers a partially applied transaction.
- Prometheus (follower): `scriva_replication_lag_lsn`,
  `scriva_replication_applied_lsn`, `scriva_replication_tx_apply_total{outcome}`,
  `scriva_replication_tx_apply_duration_seconds`, `scriva_replication_tx_ops_total`.
- Alert on: `scriva_replication_lag_lsn` growing; any
  `tx_apply_total{outcome="diverged"|"failed"}`; the follower process logging
  `replication stopped` or `replication needs re-bootstrap`.

### What a follower does when…

| symptom | meaning | action |
|---|---|---|
| `FAILED_PRECONDITION` "re-bootstrap" | fell out of the leader's ring, or the leader restarted/crashed (its ring is empty and a recovered transaction is never republished) | wipe the follower's data dir and restart with `--replicate-from`; it snapshot-bootstraps |
| `ErrEnvelopeApplyFailed` wrapping `ErrReplicationDivergence` | the follower's history and the leader's disagree for an applied LSN/txid | re-bootstrap the follower; investigate how it diverged (e.g. it was promoted, or pointed at a different leader) |
| `ErrEnvelopeDigest` / `ErrEnvelopeInvalid` / unknown record version | corrupted or newer-protocol record | nothing was applied; check versions and the network path, then restart |
| `ErrReplicationPromoted` | the node was promoted and refuses replicated writes | expected after failover; stop the old apply loop |

### Failover with transactions

Use the [manual failover runbook](#manual-failover-promoting-a-follower) as is.
Transaction-specific guarantees:

- `Promote` waits for an in-flight transaction apply; the LSN it reports covers
  whole transactions only, and the new leader assigns LSNs from `applied + 1`.
- A follower that crashed between the transaction's journal COMMIT and its
  `replication.json` write recovers the transaction and its LSN at open;
  promoting it immediately is safe.
- Re-pointing surviving followers at the new leader always re-bootstraps them
  (the new leader's ring is empty), so none can be left with half of a
  transaction.
- A leader crash after a transaction's COMMIT but before it reached any follower
  is not lost: leader startup recovery commits it, and every follower
  re-bootstraps from a snapshot that contains it.

---

## Audit log

`--audit-log <path>` turns on a durable, append-only trail of every
state-mutating and admin RPC and every rejected authentication attempt. It is the
system of record for "who did what" — including the `Promote`, `Compact`,
`CreateCollection`, and `DropCollection` admin actions — and is kept deliberately
separate from the request log so it can have its own retention and be shipped to
an append-only or write-once store. It is off by default. See
[Audit log](getting-started.md#audit-log) for the record format and field
reference.

### How it works

The audit trail is a single gRPC interceptor chained **outside** the auth
interceptor, so a call rejected by auth is still recorded (an inner interceptor
would never run for a rejected call). Because the resolved principal lives on a
context the auth interceptor derives *downstream*, the audit interceptor installs
a small principal "sink" that the auth interceptor fills in on a successful
authentication; on a rejected call the sink stays empty and the record is
attributed to `unauthenticated`. Writes to the file are serialized by the JSON
handler, so records are whole lines even under concurrent RPCs. One record is
emitted per RPC — never zero, never two — and it is written before the RPC's
response reaches the client.

### Runbook

- **Placement.** Point `--audit-log` at a file on a volume with its own retention
  and, ideally, append-only/immutable semantics (e.g. a WORM mount or a shipper
  that tails the file into a tamper-evident store). Keeping it off the data
  directory avoids coupling audit retention to database backups.
- **Rotation.** ScrivaDB opens the path with `O_APPEND` and holds it open for the
  process lifetime; it does not rotate the file itself. Use `copytruncate`-style
  rotation (e.g. `logrotate --copytruncate`) so the open descriptor keeps writing
  to the same inode, or rotate during a restart. A plain `rename`+`create` leaves
  ScrivaDB writing to the renamed (still-open) file until the next restart.
- **Volume.** Only mutating/admin RPCs and auth failures are recorded, so a
  read-heavy workload produces a modest, predictable audit volume; a write-heavy
  one produces roughly one line per write. Size retention accordingly.
- **What is not covered.** Successful reads are not audited (use the request log
  or tracing). The audit log records the *fact and target* of a mutation, not the
  record payload, so it does not leak stored data.

---

## Cross-collection transactions (XTx)

Cross-collection transactions coordinate multi-collection atomic mutations via the root coordinator journal (`xtx.journal`).

### Metrics & monitoring

Four Prometheus metrics expose transaction activity and recovery:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `scriva_xtx_total` | Counter | `outcome` | Transaction attempts by outcome (`begin`, `commit`, `abort`, `conflict`, `expired`, `unknown`) |
| `scriva_xtx_duration_seconds` | Histogram | `outcome` | Duration of transaction commit execution by outcome |
| `scriva_xtx_conflicts_total` | Counter | `kind` | Conflicts by category (`write`, `read`, `constraint`) |
| `scriva_xtx_recovery_total` | Counter | `kind` | Recovery outcomes at open (`committed`, `aborted`, `unresolved`) |

### Operational guidance & alerts

1. **Alert on `increase(scriva_xtx_total{outcome="unknown"}[15m]) > 0`:**
   An `unknown` outcome indicates an fsync failure or network ambiguity on the coordinator journal decision record (S4). Clients encountering `XTX_OUTCOME_UNKNOWN` must query `XTxStatus` rather than retrying blindly.
2. **Alert on `increase(scriva_xtx_recovery_total{kind="unresolved"}[15m]) > 0`:**
   Unresolved transactions at open indicate journal corruption or missing participant segment runs. The server fails closed with `ErrXTxRecoveryRequired` or `OpenIntegrityError` to prevent partial application. Follow the [index recovery runbook](runbook-index-recovery.md) to inspect with `scriva verify` and recover with `scriva repair`.
3. **Monitor `rate(scriva_xtx_conflicts_total[5m])`:**
   Elevated conflict rates indicate concurrent contending transactions touching the same documents or unique keys. Ensure clients implement exponential backoff and jitter on retry.

