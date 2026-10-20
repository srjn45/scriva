# Handwritten SDK cross-collection transaction contract

This is the implementation contract for the ten handwritten SDKs. It is a
successor API: existing single-collection `BeginTx`/`CommitTx`/`RollbackTx`
methods remain unchanged. The source of truth is `proto/scriva.proto` and the
server's XTx handlers and integration tests, not the shape of a generated
client.

## Common API and lifecycle

Expose the six RPCs as first-class client methods, rather than asking callers
to assemble generated protobuf requests. Keep an escape hatch to the generated
stub where that is an established SDK convention.

| Operation | SDK request / result contract |
| --- | --- |
| `beginXTx(collections, options?)` | `collections` is a non-empty collection list. Options contain `idempotencyKey`, `idleTimeoutMs`, and `maxLifetimeMs`. Return `{ xtxId, collections, idempotencyKey }`; returned collections are in server canonical lock order. |
| `stageXTx(xtxId, operation)` | `operation` contains `collection`, `op` (`insert`, `update`, or `delete`), `id`, `data`, and `expectedRev`. Return the (possibly reserved) `id`. |
| `getXTx(xtxId, collection, id)` | Return the transaction-view `Record`, including its `rev`. This is a point read and participates in optimistic validation. |
| `commitXTx(xtxId, options?)` | Options contain `lockTimeoutMs`. Return `{ txId, ops, replayed }`, where each operation has `collection`, `id`, `rev`, and `op`. |
| `rollbackXTx(xtxId)` | Return `true`/`ok`. |
| `xTxStatus(ref)` | `ref` is an `xtxId`, durable coordinator `txId`, or idempotency key. Return `{ status, txId }`. |

`xtxId` is an unguessable, process-local handle; it does not survive a server
restart. `txId` is a commit-decision identifier. Choose and retain an
idempotency key when a commit must be recoverable across a restart: it is at
most 128 bytes, is echoed by begin, and is a durable `xTxStatus` reference.
Do not silently generate or rotate one on a retry.

An XTx declares all participant collections at begin. Stage only those
collections. `insert` requires `id = 0` and reserves an ID; `update` and
`delete` require an ID; insert/update require a complete `data` replacement;
SDKs should omit `data` for a delete. `expectedRev = 0` means no explicit revision
check. A nonzero expected revision is checked at stage time against the
transaction's observed revision; a mismatch is `XTX_CONFLICT` and leaves the
handle open. Reads and staged update/deletes are revalidated atomically at
commit. There are no scans, filters, aggregates, or index lookups in XTx, and
ordinary CRUD must never be tagged with `x-xtx-id`: use `stageXTx` and
`getXTx` only.

An insert ID and all staged data are provisional and invisible until commit.
`getXTx` has read-your-writes; a staged insert has revision zero and a staged
update reports the revision on which it is based. Commit returns committed
revisions (including a delete tombstone revision). For a replay after a server
restart, `replayed=true` means the `txId` is exact but operation revisions are
placeholders; re-read records when revisions matter. An empty-write commit
validates reads and has an empty `txId`.

## Errors, retries, and recovery

Each XTx failure carries `google.rpc.ErrorInfo` with domain `scriva.xtx`.
SDKs must expose or wrap the transport error so callers can inspect `reason`
and metadata; preserve gRPC status code, `retry_safe`, `status_ref`, `tx_id`,
and conflict metadata. REST-capable SDKs must preserve equivalent structured
details. Never decide by message text.

| Code | Reason | Required SDK behavior |
| --- | --- | --- |
| `ABORTED` | `XTX_CONFLICT` | Nothing applied. Expose conflict collection/id/kind/expected/actual revision; a caller may start a **new** transaction. |
| `CANCELLED` / `DEADLINE_EXCEEDED` | `XTX_CANCELED_BEFORE_PREPARE` | Nothing applied and handle remains open. Commit again or rollback. |
| `UNKNOWN` | `XTX_OUTCOME_UNKNOWN` | **Never blind retry commit.** Call `xTxStatus(metadata.status_ref)`; `COMMITTED` is success, `ABORTED`/`EXPIRED` permits a new transaction, and `PENDING` requires polling/backoff. |
| `UNAVAILABLE` | `XTX_IN_PROGRESS` | Do not issue another commit. Resolve with `xTxStatus(metadata.status_ref)` and poll if pending. |
| `UNAVAILABLE` | `XTX_DURABILITY` | Nothing applied; retrying as a new transaction is safe. |
| `FAILED_PRECONDITION` | `XTX_EXPIRED`, `XTX_FINISHED`, `XTX_READ_ONLY`, `XTX_UNSUPPORTED`, `COLLECTION_FENCED` | Do not reuse the handle. `EXPIRED` permits a new transaction; a fenced collection needs server recovery/reopen. |
| `NOT_FOUND` | `XTX_NOT_FOUND`, `XTX_DOC_NOT_FOUND`, `XTX_COLLECTION_NOT_FOUND` | Preserve the typed distinction; an unknown handle after restart is not evidence that a commit failed—look up the retained idempotency key. |
| `INVALID_ARGUMENT` | `XTX_INVALID`, `XTX_NOT_PARTICIPANT`, `XTX_TOO_LARGE` | Correct the request; do not retry unchanged. |
| `UNIMPLEMENTED` | `XTX_SCAN_UNSUPPORTED` | Do not emulate a scan within the transaction. |
| `RESOURCE_EXHAUSTED`, `ALREADY_EXISTS`, `DATA_LOSS`, `INTERNAL` | `XTX_RESOURCE_EXHAUSTED`, `XTX_DUPLICATE_KEY`, `XTX_DATA_LOSS`, `XTX_INTERNAL` | Surface the reason and `retry_safe`; only start a new transaction when it is explicitly `true`. |

`retry_safe` means that re-running the logical operation as a new transaction
cannot double-apply; it does not authorize reusing a finished handle. The
default for unknown/unclassified errors is unsafe. A lost client response to a
successful `CommitXTx` has the same rule as `XTX_OUTCOME_UNKNOWN`: status
lookup before any retry. SDK convenience helpers may implement this polling,
but must return a result that identifies whether commit was replayed and must
not hide the ambiguity.

## Per-SDK successor matrix

Add the names below to the existing high-level client. They follow each SDK's
current single-collection transaction naming and async model; do not rename or
change existing methods. `Begin`, `Stage`, `Get`, `Commit`, `Rollback`, and
`Status` in this table mean all six operations in the common contract above.

| SDK | Concrete successor methods | Result/error and test guidance |
| --- | --- | --- |
| Python | `begin_xtx`, `stage_xtx`, `get_xtx`, `commit_xtx`, `rollback_xtx`, `xtx_status` on `ScrivaDB` | Return dataclasses (or documented mappings) for begin/commit/status; translate `grpc.RpcError` into an inspectable XTx error retaining `ErrorInfo`. Add pytest coverage with the existing synchronous client style. |
| JavaScript/TypeScript | `beginXTx`, `stageXTx`, `getXTx`, `commitXTx`, `rollbackXTx`, `xTxStatus` returning `Promise` | Export discriminated result and XTx error types from `src/types.ts`; parse gRPC status details rather than only rejecting with an `Error` string. Add TypeScript integration tests. |
| Rust | async `begin_xtx`, `stage_xtx`, `get_xtx`, `commit_xtx`, `rollback_xtx`, `xtx_status` on `ScrivaDb` | Add public request/result structs and a typed `ScrivaDbError` XTx-detail accessor. Test with Tokio, including no blind commit retry after an unknown outcome. |
| Ruby | `begin_xtx`, `stage_xtx`, `get_xtx`, `commit_xtx`, `rollback_xtx`, `xtx_status` | Return Ruby value objects/hashes consistently with current client records and retain `GRPC::BadStatus` details in an XTx error. Add Minitest/RSpec coverage in the repository's chosen test layout. |
| PHP | `beginXTx`, `stageXTx`, `getXTx`, `commitXTx`, `rollbackXTx`, `xTxStatus` on `ScrivaDB` | Convert generated responses to documented arrays/value objects and retain decoded `ErrorInfo` in `ScrivaDBException`. Add PHPUnit integration tests. |
| C# | async `BeginXTxAsync`, `StageXTxAsync`, `GetXTxAsync`, `CommitXTxAsync`, `RollbackXTxAsync`, `XTxStatusAsync` with `CancellationToken` | Use public DTOs and an exception/detail helper over `RpcException`; keep existing `BeginTxAsync` signatures. Add xUnit coverage for cancellation-before-prepare and status resolution. |
| Java | `beginXTx`, `stageXTx`, `getXTx`, `commitXTx`, `rollbackXTx`, `xTxStatus` on `ScrivaDBClient` | Add immutable public DTOs and a typed status-detail helper around `StatusRuntimeException`; preserve blocking-client semantics. Add JUnit integration tests. |
| Kotlin | suspend `beginXTx`, `stageXTx`, `getXTx`, `commitXTx`, `rollbackXTx`, `xTxStatus` | Add Kotlin data classes and decode `StatusException` details without blocking; retain the current suspend API style. Add coroutine tests. |
| Scala | `beginXTx`, `stageXTx`, `getXTx`, `commitXTx`, `rollbackXTx`, `xTxStatus`, all `Future`-returning | Model results as case classes and expose typed error details while preserving `Future`/execution-context behavior. Extend ScalaTest fake-stub and integration coverage. |
| Clojure | `begin-xtx`, `stage-xtx`, `get-xtx`, `commit-xtx`, `rollback-xtx`, `xtx-status` | Return idiomatic maps with kebab-case keys; preserve `ex-info`/gRPC cause and attach parsed XTx reason/metadata. Add `clojure.test` coverage. |

Every SDK's test suite must cover: an atomic write across two collections;
read-your-writes and invisibility before commit; insert/update/delete and their
returned IDs/revisions; expected-revision conflict with no writes; explicit
idempotency-key replay (`replayed=true`); rollback idempotence and rollback
after commit; status by handle, key, and tx ID; expiration; and the
`XTX_OUTCOME_UNKNOWN`/lost-response flow proving that it calls `XTxStatus`
before a new commit. Include generated-proto regeneration or vendoring only
where that SDK already owns generated sources; this contract does not require
SDK implementations to change the server wire protocol.
