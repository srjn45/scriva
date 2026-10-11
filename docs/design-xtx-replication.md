# Replicating cross-collection transactions — protocol v2

Status: implemented and tested end to end (wire format, leader publish, follower
decode/verify, atomic follower apply, crash/failover matrix, status accounting,
metrics — §7, §9, §10, §11). The operator runbook is in
[operations.md](operations.md#replicating-cross-collection-transactions-protocol-v2).

This document supersedes the per-entry-LSN wording of §12.1/§12.2 of
[design-cross-collection-transactions.md](design-cross-collection-transactions.md):
a cross-collection transaction is replicated as **one record with one LSN**, not
as N per-entry records.

## 1. Why a new record

Legacy replication (v1) ships one `ReplicationRecord` per committed entry. A
cross-collection transaction touches several collections; shipping it entry by
entry lets a follower observe — or crash with — a prefix of the transaction.
Protocol v2 adds a *transaction envelope*: the whole committed transaction as a
single record that a follower must apply atomically or not at all.

## 2. Wire format

`proto/scriva.proto`:

- `ReplicateRequest.protocol_versions` (`repeated uint32`, field 3). Empty means a
  legacy follower, equivalent to `{1}`. Current followers send `{1, 2}`.
- `ReplicationRecord.record_version` (field 9). `0`/`1` = legacy single entry,
  wire-identical to before. `2` = transaction envelope.
- `ReplicationRecord.tx` (field 10, `ReplicationTx`): `tx_id`, `key`
  (idempotency key, may be empty), `ops` (`ReplicationTxOp`: `collection`, `op`,
  `id`, `rev`, `data`, `expires_at`, `ts`, `epoch`) and `digest`.

An envelope record sets `lsn` (field 1) to the transaction's single global LSN;
the single-entry fields 2–8 are unset.

### Canonical order

Ops are ordered by collection name ascending (bytewise); each collection's ops are
contiguous and keep staging order. `TxEnvelope.Participants()` yields the
per-collection run lengths (the `tx.n` stamp each participant's segment carries).

### Digest

`digest` is SHA-256 over:

```
"SCRIVA-XTX-ENV\x00" u32le(2) lp(tx_id) lp(key) u32le(nOps)
per op: u32le(index) lp(collection) lp(op) u64le(id) u64le(rev)
        i64le(expires_at) i64le(ts.sec) u32le(ts.nsec) u64le(epoch)
        byte(hasData) lp(canonical-json(data))
```

(`lp(x)` = `u32le(len)` + bytes.) The LSN is **excluded**: it is stream framing
assigned under the broker lock, which lets the digest be computed before that lock
is taken, and it keeps the digest identical on leader and follower. `data` is
normalised through a JSON round trip (numbers become `float64`) when the envelope
is built so both sides hash the same bytes. A receiver must call
`engine.TxEnvelope.Verify` (done in `server.txEnvelopeFromProto`); a mismatch is
`engine.ErrEnvelopeDigest`.

## 3. Negotiation (no silent downgrade)

- A follower that advertises v2 receives legacy records as v1 and transactions as
  v2 envelopes.
- A follower that does **not** advertise v2 keeps receiving legacy single entries
  unchanged. When the leader would have to send it an envelope, it fails the
  stream with gRPC `FAILED_PRECONDITION` and an `errdetails.ErrorInfo`
  (`domain=scriva.replication`, `reason=REPLICATION_PROTOCOL_UNSUPPORTED`,
  metadata `required_version`, `lsn`, `tx_id`). `server.IsReplicationProtocolError`
  recognises it. The leader never downgrades a transaction to per-entry records,
  and a participant write of a cross-collection transaction is **never** published
  as a legacy entry.
- A follower that sees an unknown `record_version` stops without applying anything
  (`server.ErrUnknownRecordVersion`).

## 4. Publish point and recovery

In `commitXTxLocked`, after the journal COMMIT is durable (S4) and the indexes are
applied, still holding every participant lock, the leader calls
`broker.publishTx(env)` exactly once. An aborted transaction is never published.
The envelope is built (and its size checked) *before* a txid is allocated or
anything is written, so a transaction that commits is always publishable.
`DefaultMaxEnvelopeBytes` (32 MiB) bounds it; larger transactions fail with
`engine.ErrXTxTooLarge` before any write (only when replication is enabled).

Startup recovery does **not** republish: the ring is in memory and empty after a
restart. A follower behind the restart watermark gets `FAILED_PRECONDITION` and
re-bootstraps from a snapshot, which already contains the recovered transaction.

This relies on the restored LSN being **strictly above** every caught-up
follower's resume point. Before the COMMIT becomes durable the coordinator
therefore calls `broker.reserveNext()`, which persists the LSN watermark block
covering the LSN the transaction will receive. Without it, a leader that was
cleanly closed (watermark == last LSN), restarted, committed a transaction and
crashed between the COMMIT and `publishTx` would restore the old LSN, serve a
follower caught up at that LSN from an empty ring, and the follower would
silently miss a transaction the leader had committed.

### LSN safety across crashes

The broker persists `leader_lsn` ahead of the LSN it hands out, in blocks of 4096
(`lsnReserveBlock`). A crashed leader therefore restores an LSN ≥ any LSN it ever
published and never re-issues one; a clean `Close` writes the exact value.

If S4 fsync fails (`ErrXTxOutcomeUnknown`) the outcome is unknown, so the broker
runs `resyncBarrier`: it burns an LSN, empties the ring and cuts off every
subscriber, forcing all followers to re-bootstrap.

## 5. LSN accounting

One transaction consumes exactly one LSN. Legacy entries and envelopes share one
monotonic sequence. `CurrentLSN`, `ReplicationStatus.leader_lsn`, the follower's
applied LSN (`replication.json`) and promotion (`setLSN`) all use that single
sequence.

## 6. Retention and backpressure

The ring stores whole records but its capacity is counted in *entries*
(`weight`: legacy = 1, envelope = number of ops). Eviction drops whole records from
the head; a record heavier than the ring is retained alone. `subscribe(fromLSN)`
returns whole records only, so an envelope is never split.

Subscriber buffers are bounded by record count and by weight; a subscriber that
overflows is cut off (it re-requests from its applied LSN or re-bootstraps) rather
than receiving a partial envelope. A heavy envelope is always admitted to an empty
buffer. The server calls `ReplicationStream.Consumed` for every live record it
drains.

## 7. Follower apply

`Follower.stream` decodes and verifies each v2 record, then calls the seam

```go
func (f *Follower) applyEnvelope(lsn uint64, env *engine.TxEnvelope) error
```

which calls `engine.DB.ApplyReplicatedTx(lsn, env) (applied bool, err error)`.
It returns nil only after the **whole** transaction is committed locally through
the cross-collection coordinator (or was already applied). Any failure is wrapped
in `server.ErrEnvelopeApplyFailed`; `Follower.Run` stops and returns it (it also
stops on envelope-invalid/digest errors and unknown versions). A failed apply
leaves nothing visible and the applied LSN unchanged.

Steps (all under `DB.xtxSnapshotMu.RLock`, `DB.mu.RLock` and every participant's
write lock in canonical order, so readers see none or all):

1. Verify the envelope; create participant collections that do not exist locally
   (same behaviour as legacy apply; an empty collection left by a failed apply is
   harmless); ensure the `_key` index for keyed data.
2. Idempotency/divergence check (§9.2).
3. Drop operations whose effect is already present (insert/update at an
   equal-or-newer revision, delete of an absent id) — the revision-level
   idempotency legacy apply has, needed for the bootstrap watermark overlap.
4. Shared coordinator steps S1–S4: stamped runs with the leader's exact ids,
   revisions, timestamps, expiries and epochs; unconditional fsync; journal
   COMMIT (carrying the binding, §9.1).
5. Apply to primary/secondary indexes, bump id counters, release locks, rotate
   segments / persist meta, emit watch events.
6. `SetAppliedLSN(lsn)` (best-effort persist: §9.3 makes it recoverable).

Unique constraints are not re-checked (the leader validated them). The follower's
own broker is **not** published to: a replicated transaction is not a new leader
LSN, exactly like legacy `ApplyReplication`. `Promote` calls `setLSN(applied)`
so the new leader continues strictly above every replicated LSN.

## 9. LSN binding, idempotency and recovery

### 9.1 Binding

The journal COMMIT record gains optional fields `rl` (replication LSN), `rt`
(leader txid) and `rd` (hex envelope digest) — all or none; absent on locally
originated commits. They are inside the record CRC, written and fsynced in the
same append as the COMMIT, so the transaction's durability and its position are
one atomic decision. ABORT/RETIRE records never carry them. A second commit
binding the same LSN is a decision conflict. The follower allocates its own
local txid (its journal epoch may differ from the leader's) and keeps the
client's idempotency key, so a retry after promotion is still idempotent.

### 9.2 Idempotency and divergence

Looked up under the participant locks:

- A commit bound to this LSN or this leader txid, with the same LSN, txid and
  digest → no-op success (`applied=false`); the LSN still advances.
- Bound to this LSN or txid but with a different digest or LSN →
  `engine.ErrReplicationDivergence`: nothing applied, LSN not advanced, the run
  stops. The follower must be re-bootstrapped.
- A commit with the leader's own txid and no binding (the snapshot shipped the
  leader's journal) whose participant digests match → no-op; differing →
  divergence.
- Otherwise per-operation revision filtering (step 3 above).

### 9.3 Recovery and the applied-LSN rule

The applied LSN advances only after the COMMIT is durable and the indexes are
applied. Crash points:

| crash | recovery | resume |
|---|---|---|
| before the COMMIT is durable (before/after PREPARE) | presumed abort, runs truncated; none of the tx | envelope redelivered and applied |
| after COMMIT, before indexes / before `replication.json` | tx committed by journal; applied LSN raised to the highest bound `rl` | redelivery recognised, no-op |
| after `replication.json` | as normal | next LSN |

Raising the applied LSN is sound because a follower applies strictly in LSN order:
every LSN below a bound one was applied before it. Journal records are never
retired or dropped while they bind an LSN above the persisted applied LSN
(currently nothing retires them).

### 9.4 Promotion interlock

`ApplyReplicatedTx` and `Promote` share `DB.replApplyMu`. Promote waits for an
in-flight apply, reads the applied LSN, flips the role and marks the node
promoted; later replicated applies fail with `ErrReplicationPromoted`.
`ReplicationLag` and `AppliedLSN` therefore never cover a half-applied
transaction.

### 9.5 Test seams

`engine.XTxHookReplApplied` (committed and visible, before the applied LSN is
advanced) joins the existing `XTxHookLocked/Prepared/Decided/Applied` points
(`SetTestXTxHook`; the hook key is the envelope's idempotency key). Crash images
are taken by copying the data directory from inside a hook.

## 10. Status accounting and metrics

- `ReplicationStatus.applied_lsn`, `DB.AppliedLSN` and `DB.ReplicationLag` only
  advance after the whole transaction is committed and visible (§7 step 6, §9.3);
  a half-applied transaction is never reported applied, not even across a crash
  (the applied LSN is re-derived from journal bindings, never from partial runs).
- Lag is counted in LSNs; a transaction is one LSN regardless of its size.
- Follower-side Prometheus metrics (fed through `Follower.SetEnvelopeApplyObserver`,
  never from the engine):

| metric | meaning |
|---|---|
| `scriva_replication_tx_apply_total{outcome}` | envelopes processed: `applied`, `duplicate` (idempotent redelivery / snapshot overlap), `diverged`, `promoted`, `failed` |
| `scriva_replication_tx_apply_duration_seconds{outcome}` | apply latency |
| `scriva_replication_tx_ops_total` | operations in applied envelopes |
| `scriva_replication_applied_lsn` | follower's fully applied LSN |
| `scriva_replication_lag_lsn` | last-known leader LSN − applied LSN |

## 11. Verified failure matrix

`server/replication_failover_test.go` (real leader/follower engines, in-process
gRPC, race detector):

| scenario | result |
|---|---|
| leader crash after COMMIT, before publish | recovery commits the tx; caught-up follower gets `FAILED_PRECONDITION` and re-bootstraps; never silently misses it |
| leader crash before COMMIT | presumed abort; no node ever shows any part |
| follower crash: before PREPARE / after PREPARE | none of the tx; envelope reapplied on resume |
| follower crash: after COMMIT / before LSN persist | all of the tx; applied LSN restored to the bound LSN; no re-apply |
| network disconnect mid-stream | reconnect resumes from applied LSN, no loss or duplication |
| redelivery of an applied tx | no-op, LSN unchanged |
| same LSN/txid, different content | `ErrReplicationDivergence` (wrapped in `ErrEnvelopeApplyFailed`); run stops, nothing applied |
| ring overflow | `FAILED_PRECONDITION`; snapshot bootstrap is atomic; snapshot overlap redelivery is a no-op |
| promote while caught up / in flight / after crash recovery | new leader continues at applied LSN + 1; in-flight apply completes before promotion; later replicated applies fail `ErrReplicationPromoted` |

## 12. Not covered here

- Single-collection `CommitTx` still publishes per entry (one LSN each) and is not
  atomic on followers.
- A legacy single-entry write that is durable but crashes before publish has the
  same unpublished window on a leader that was just cleanly restarted; the
  transaction path closes it, the legacy path is unchanged.
- The client SDK proto copies under `clients/` are not regenerated.
