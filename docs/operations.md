# Operations

Operator runbooks for running FileDB in production. Start with
[getting-started.md](getting-started.md) for setup and
[architecture.md](architecture.md) for how the internals work.

---

## Manual failover (promoting a follower)

FileDB replicates a **leader** to one or more read-only **followers** (see
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
filedb-cli --host <follower>:5433 --api-key <key> promote
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
filedb-cli --host <follower>:5433 --api-key <key> promote --force
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
