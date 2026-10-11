# Replicating cross-collection transactions — protocol v2

Status: implemented (wire format, leader publish, follower decode/verify). Atomic
follower apply is the next stage and plugs into the seam described in §7.

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

## 7. Follower apply contract (next stage)

`Follower.stream` decodes and verifies each v2 record, then calls the seam

```go
func (f *Follower) applyEnvelope(lsn uint64, env *engine.TxEnvelope) error
```

in `server/replication_tx.go`. Today it returns `ErrEnvelopeApplyUnsupported`:
nothing is applied, the applied LSN does not advance and `Follower.Run` returns the
error (it also stops on unknown versions and on the typed protocol refusal).

The implementation must return nil only after the **whole** transaction is
committed locally and atomically through the cross-collection coordinator; the
caller then advances the applied LSN. Re-delivery (resume from a stale applied LSN,
or a snapshot that already contains the tx) must be a no-op, keyed on `env.TxID` /
`env.Key` and entry revisions.

## 8. Not covered here

- Single-collection `CommitTx` still publishes per entry (one LSN each) and is not
  atomic on followers.
- Crash/failover test matrix, replication status for transactions and metrics
  belong to the integration stage.
- The client SDK proto copies under `clients/` are not regenerated.
