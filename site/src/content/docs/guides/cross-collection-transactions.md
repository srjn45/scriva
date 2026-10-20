---
title: Cross-collection transactions
description: Atomically commit point writes across collections, with explicit isolation limits and safe recovery from uncertain commits.
---

Use a cross-collection transaction (XTx) when one decision must update records in more than one collection of the **same database**. It stages work first, then makes every staged write visible together at commit — or applies none of them.

:::caution[Important limits]
XTx is not general serializable isolation. It validates point reads and writes optimistically at commit; it provides **no predicate or phantom protection**. `Find`, scans, filters, aggregates, and index lookups are unavailable in an XTx. Transactions have at most 16 declared participants and 1,000 staged operations.
:::

## Declare participants, then stage work

Name every collection the transaction may touch at `BeginXTx`; all must already exist. The list is fixed. Beginning takes no locks and writes nothing. `StageXTx` buffers inserts, full-replacement updates, and deletes; other readers cannot see them before commit.

```bash
# The key identifies this business operation across retries and restarts.
X=$(scriva-cli begin-xtx accounts ledger --key transfer-42)

# Get is a point read and sees this transaction's own staged writes.
scriva-cli xtx-get    "$X" accounts 1
scriva-cli xtx-update "$X" accounts 1 '{"balance":60}'
scriva-cli xtx-insert "$X" ledger   '{"account":1,"delta":-40}'
scriva-cli commit-xtx "$X"
```

An update or delete can include an expected revision. More importantly, every `GetXTx` and every staged update/delete records the version (or absence) it observed. At commit ScrivaDB rechecks those observations under the participant locks. If another writer changed one, commit returns `ABORTED` / `XTX_CONFLICT`, applies nothing, and the caller can begin a fresh transaction, re-read, and retry with backoff and jitter.

The reads are not one cross-collection snapshot: point reads of different documents can come from different moments. Because no predicate reads are admitted, the database cannot promise that a range or query stayed unchanged. Design workflows around known document IDs, or enforce the invariant with a materialized record that you point-read and update.

## Idempotency and uncertain commits

Always supply a unique business-operation idempotency key when the caller might retry, and persist it with the business operation so retries after a restart reuse the same value. If that key has already committed, a later commit returns the original outcome with `replayed: true` and applies nothing. The key-to-transaction decision survives a server restart.

Do not blindly retry a commit that returns `XTX_OUTCOME_UNKNOWN`, or whose response is lost to a timeout, reset, or proxy error. The commit might already be durable. Resolve it first:

```bash
scriva-cli xtx-status transfer-42
# COMMITTED tx:...  -> done; do not apply the transfer again
# ABORTED / EXPIRED -> begin a new transaction
# PENDING           -> wait, then ask again
# UNKNOWN           -> not proof of abort; investigate or keep resolving, never blind-retry
```

`XTxStatus` accepts the idempotency key, coordinator transaction ID, or handle ID. Use the `status_ref` supplied in error details when present. A replay after a restart has the exact transaction ID, but its individual revision results are placeholders; re-read records if those revisions matter.

## Access paths

The six XTx operations are available through generated gRPC clients, the REST gateway, the CLI, and the embedded Go API. The hand-written SDKs do not wrap them; use generated stubs or REST for network clients.

| Operation | REST route | What it does |
|---|---|---|
| `BeginXTx` | `POST /v1/xtx` | declares `collections` and optional `idempotency_key` |
| `StageXTx` | `POST /v1/xtx/{xtx_id}/ops` | stages an insert, update, or delete |
| `GetXTx` | `GET /v1/xtx/{xtx_id}/records/{collection}/{id}` | transactional point read |
| `CommitXTx` | `POST /v1/xtx/{xtx_id}/commit` | validates and atomically applies the staged work |
| `RollbackXTx` | `POST /v1/xtx/{xtx_id}/rollback` | discards staged work; idempotent |
| `XTxStatus` | `GET /v1/xtx/status?ref=…` | resolves an outcome by key, transaction ID, or handle ID |

For an embedded program, use `db.Transact` for a scoped workflow or `db.BeginXTx` for manual control. `Transact` does not retry for you; on a conflict, rerun the whole function so it reads current values, and on an unknown outcome call `db.XTxStatus`.

## Authorization and lifecycle

When authentication is configured, `BeginXTx`, `StageXTx`, `GetXTx`, `CommitXTx`, and `RollbackXTx` require a read-write principal, authenticated by an API key or verified client certificate. `GetXTx` is intentionally a write-class operation because it extends the transaction read set and idle lifetime. `XTxStatus` is read-only. A collection-restricted key must be allowed for **every** declared participant, and the handle is bound to the principal that opened it; another principal cannot commit or roll it back. Read-only followers reject handle operations; use the leader.

The handle ID is process-local and does not survive a restart. A handle idle past `--tx-timeout` (default five minutes) is discarded with no staged data applied; per-transaction idle and lifetime limits are also available. `RollbackXTx` cannot undo a commit decision.

## Further reading

- [API & OpenAPI](/scriva/reference/api/) for generated-client and protocol references.
- [Client SDKs](/scriva/guides/clients/) for the distinction between hand-written SDKs and generated stubs.
- [XTx operational runbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md#cross-collection-transactions-xtx) for metrics, upgrade, recovery, and incident handling.
- [Architecture: transaction handle](https://github.com/srjn45/scriva/blob/main/docs/architecture.md#transaction-handle-dbbeginxtx) for the exact concurrency and durability model.
