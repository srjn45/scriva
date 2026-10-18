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

### Upgrade, downgrade and recovery playbook

**Upgrading from a pre-transaction release (≤ v1.4).** A data directory written
before cross-collection transactions is a *legacy root*: it has no `xtx.format`
and no `xtx.journal`. The new binary opens, verifies and serves it unchanged and
writes no `xtx.*` file until the first `CommitXTx`. That first transaction
upgrades the root in place — `xtx.format` (file + directory fsync), then
`xtx.journal`, then stamped runs — and never rewrites existing sealed segments.
Verified against a real v1.4.0 directory by `TestXTxLifecycle_Upgraded`
(fixture: `engine/testdata/upgrade/v1.4.0`, read-only input, regenerate only by
running the v1.4.0 tag's engine; see its `README.md`).

**Downgrading.** Once a root holds transaction state it is **not** downgradable
to a binary that predates the fence:

| Reader | Root has `xtx.*` | Behavior |
|---|---|---|
| pre-fence binary | live stamped runs | fails closed on the first stamped line (`ErrCorruptEntry`); **do not run that binary's `repair`** — it would quarantine the stamped segments |
| this release, `xtx.format` `min_reader` too new | any | `Open`, `VerifyDir` and `Repair` refuse (`ErrFormatTooNew`, or `ErrXTxUnsupported` for an unknown feature) and modify nothing |
| this release | `xtx.journal` without `xtx.format` | `ErrXTxUnsupported`, nothing modified |
| this release | stamped runs, `xtx.journal` missing (with or without `xtx.format`) | `ErrXTxJournalMissing`; runs are **never** presumed aborted, because the journal is the only record of which were committed |

To roll back to an older binary, restore the pre-upgrade backup. There is no
in-place drain command yet.

**Recovery.** `Open` resolves every undecided run itself (presumed abort only
when no COMMIT exists, never when the evidence was lost). For anything it
refuses: take a copy of the directory first, run `scriva verify` (read-only; it
never takes the lock), then `scriva repair` — which backs up and verifies the
affected collections before mutating anything and refuses transaction roots it
cannot reconcile. Restore the missing `xtx.journal`/`xtx.format` from backup
rather than deleting them. Never create an empty `xtx.journal` to silence
`ErrXTxJournalMissing`.

**Mixed versions in replication.** See [Upgrade order](#upgrade-order-protocol-negotiation):
a follower that cannot speak protocol v2 is refused with
`REPLICATION_PROTOCOL_UNSUPPORTED` when the first transaction would reach it,
never fed per-entry records.

---

### Atomic single-collection batches: rollout and recovery

`InsertMany`, single-collection `CommitTx` and bulk loads are crash-atomic on a
root in atomic-batch mode (design: [architecture](architecture.md#atomic-single-collection-batches)).

**Which roots.** A new data directory is atomic from its first write. A root that
already has `xtx.format` stays atomic. A *legacy* root (collections, no
`xtx.format`) is **not** changed by upgrading the binary: it logs
`legacy root opened: atomic batches disabled; run serve --enable-atomic-batches
to convert` and keeps the old prefix-visible batch behaviour. A follower root is
initialised by the leader's envelopes, never locally.

**Converting a legacy root** (one-way):

1. Take a verified backup: `scriva repair`'s backup, or stop the server and copy
   the directory. This is the only rollback; there is no drain tool.
2. Upgrade every follower first (a v1-only follower is refused with
   `REPLICATION_PROTOCOL_UNSUPPORTED` when the first batch reaches it).
3. Start once with `scriva serve --enable-atomic-batches` (or
   `enable_atomic_batches: true`). The log line `converted legacy root to atomic
   batches` confirms it; the flag may stay set or be removed afterwards.

**Cost.** Each batch forces a data fsync and a decision fsync whatever
`--sync-mode` is, so relaxed modes lose their batching advantage. Measured
numbers are in [release-readiness-batch.md](release-readiness-batch.md#performance).

**Client errors.**

| Error | Meaning | Action |
|---|---|---|
| `ErrXTxDurability` (aborted) | proven abort before the decision, nothing applied | safe to retry |
| `ErrXTxOutcomeUnknown` | decision fsync/append failed; the journal is fenced | do **not** retry; `XTxStatus(txid)` after restart; the answer is final |
| `*PostCommitError` (`rotate`) | batch committed; rotation afterwards failed and the collection is fenced | treat as success; restart per the fenced-collection runbook below |
| `ErrXTxTooLarge` | over 1000 ops / 64 MiB | split the batch yourself, accepting that the parts are independent |
| `ErrBatchNotAtomic` | embedded collection opened without a DB | open through `engine.Open`/`scriva.Open` or set `AllowNonAtomicBatch` |

**Recovery.** `Open` resolves every batch run itself: no COMMIT means the run is
truncated (full old state), a COMMIT with a matching run is replayed (full new
state), including a batch whose caller never got an acknowledgment. If the
evidence is damaged `Open` refuses (`ErrXTxIncomplete`, `ErrXTxJournalMissing`,
`ErrXTxRecoveryRequired`) and modifies nothing; follow the recovery paragraph of
the [playbook](#upgrade-downgrade-and-recovery-playbook) (verify, then repair
from a copy; restore `xtx.journal` from backup; never create an empty one).
`snapshot` and `repair --backup` carry `xtx.journal`/`xtx.format`, so a restored
snapshot of an atomic root is atomic.

**Known limit.** Atomicity here is about crashes. A scan running concurrently
with a batch may see part of it (hardening §3.5, tracked separately).

---

### Runbook: `DropCollection` returns `PARTICIPANT_RETAINED`

**Symptom.** `DropCollection` fails with gRPC `FAILED_PRECONDITION`, reason
`PARTICIPANT_RETAINED` (embedded: `engine.ErrParticipantRetained`), with the
blocking transaction ids in the error details.

**What it means.** The coordinator journal (`xtx.journal`) holds committed
cross-collection transactions that name this collection and are not retired. The
journal needs the collection's files as recovery evidence: deleting them would
make the database fail to reopen. The refusal is a fail-safe, taken under the
exclusive DB lock before any side effect.

**What is preserved.** Everything. Nothing is closed, renamed, removed or
journaled; the collection stays open, readable and writable, the directory is
byte-identical, and the refusal persists across restart and in restored
snapshots.

**What is not available yet.** Retirement of journal decisions at drop (which
would let the drop proceed) is not implemented, and there is deliberately no
force flag. Do **not** delete the directory by hand or edit `xtx.journal`; the
root will fail to reopen (`ErrXTxRecoveryRequired` / `OpenIntegrityError`).

**Safe options.**
1. Keep the collection (empty or delete its records with normal deletes if the
   data must go; the collection itself remains).
2. If space is the concern, run compaction after deleting records.
3. To discard the whole database, take it offline and remove the entire data
   directory; do not remove individual participants.
4. If a different collection that never took part in an XTx must go, drop it
   normally; the refusal is per collection.

Verify the root any time with `scriva verify`.

## Segment rotation errors and fenced collections

This section provides operator guidance and a runbook for managing segment rotation failures, post-commit rotation warnings, and fenced collections.

### Understanding rotation errors vs. fencing

Segment rotation is an ancillary step executed *after* a write has durably committed and updated indexes:
- **Ancillary Warning (`*PostCommitError` / `Scriva-Post-Commit-Warning`)**:
  - The write was **durable and committed**.
  - If new segment creation fails but fallback reopen of the old segment succeeds, or if `meta.json` persist fails, the collection is **NOT fenced**. It continues accepting subsequent reads and writes.
  - Clients observe `codes.OK` with the `scriva-post-commit-warning` trailer (REST: HTTP 200 with `Scriva-Post-Commit-Warning` header; CLI: exit 0 with stderr warning).
  - Operator action: investigate disk space, file permissions, or OS limits before an unrecoverable failure occurs.
- **Fail-Stop Fencing (`COLLECTION_FENCED`)**:
  - Occurs when rotation hits an unrecoverable state:
    1. Active segment seal sync failure (`Sync()` error).
    2. Unrecoverable rotation failure (new segment creation failed AND fallback reopen failed).
    3. Directory fsync failure (`fsyncDir` failed after installing new segment).
  - The collection transitions to **fenced**: all subsequent reads and writes on that collection fail closed with `FAILED_PRECONDITION` / `COLLECTION_FENCED` (`retry_safe: false`).
  - Read-only admin operations (`Close`, `Stats`, `Verify`) remain permitted; compaction, index changes, and snapshot are refused.
  - Other unaffected collections continue operating normally.

### Monitoring & alerts

| Metric / Symptom | Severity | Meaning | Operator Action |
|---|---|---|---|
| `scriva_write_outcome_unknown_total{op="seal"}` | High / Alert | Segment seal sync failed; collection fenced | Trigger failover or run restart runbook below |
| `scriva_write_outcome_unknown_total{op="rotate"}` | High / Alert | Directory fsync failed or fallback reopen failed; collection fenced | Resolve disk/system error and restart |
| `Scriva-Post-Commit-Warning` header / trailer | Warning | Ancillary rotation failure (fallback or meta persist) | Check disk capacity and file permissions |
| Client receives `COLLECTION_FENCED` (exit code 4) | High / Alert | Traffic to collection is blocked fail-stop | Restart server after remediating storage |

### Operator Runbook: Recovering a Fenced Collection

When a collection is fenced due to a rotation or seal failure (`op="seal"` or `op="rotate"`):

**1. Identify the failing collection and error cause:**
Check the server log for the initial fence event:
```
level=ERROR msg="collection fenced" collection=users op=seal err="input/output error"
# or
level=ERROR msg="collection fenced" collection=orders op=rotate err="no space left on device"
```

**2. Inspect storage health and host resources:**
Inspect the storage volume housing the ScrivaDB data directory:
```bash
# Check disk space
df -h /var/lib/scriva/data

# Check inode usage
df -i /var/lib/scriva/data

# Check kernel dmesg for hardware / filesystem errors
dmesg -T | grep -E "I/O error|EXT4|XFS|btrfs"

# Check open file descriptor counts and ulimits
lsof -p $(pgrep scriva) | wc -l
ulimit -n
```

**3. Remediate the underlying system fault:**
- **No space left on device (`ENOSPC`):** Expand the storage volume or purge non-essential files on the volume.
- **Permission denied / read-only filesystem:** Restore write permissions or remount the volume read-write (`mount -o remount,rw ...`).
- **File descriptor exhaustion (`EMFILE`):** Raise `LimitNOFILE` in the systemd unit or container spec.
- **I/O hardware error (`EIO`):** If running a replicated cluster, initiate [manual failover](#manual-failover-promoting-a-follower) to a healthy follower.

**4. Restart ScrivaDB:**
Once the underlying storage fault is resolved, restart the ScrivaDB process:
```bash
systemctl restart scriva
# or
docker restart scriva
```
During startup:
- ScrivaDB validates all segment files and verifies CRC32C checksums.
- Any incomplete or torn final lines from un-synced writes are trimmed.
- The highest segment is reopened as the active segment.
- Even if `meta.json` was missing or stale, startup recovery scans segment tails to restore `maxID`, preventing ID counter reuse.
- The fail-stop fence is cleared, restoring full read/write availability.

**5. Post-restart verification:**
Run `scriva verify` offline or against the active data directory to confirm segment and index integrity:
```bash
scriva-cli --data /var/lib/scriva/data --collection <collection-name> verify --mode full
```
Confirm the collection serves reads and writes:
```bash
scriva-cli --host 127.0.0.1:5433 collection stats <collection-name>
```


