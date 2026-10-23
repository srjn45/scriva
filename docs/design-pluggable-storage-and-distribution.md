# Technical design: pluggable collection engines, partitioning, and replication

**Status:** proposed architecture. This is not implemented. It is deliberately
separate from the current `engine.Collection`, which is an append-only NDJSON
implementation tightly coupled to segments, indexes, compaction, snapshotting,
and the current replication feed.

## 1. Scope and non-goals

### Scope

- Multiple named storage engines inside one `engine.DB`.
- A durable catalog that maps a logical collection to its engine, engine format,
  shards, and placement policy.
- A capability contract that makes unsupported combinations fail before write.
- Logical replication that can be applied by a different physical engine.
- Local partitioning before distributed placement.
- A migration protocol from one engine to another.

### Explicit non-goals for the first implementation

- User-supplied dynamic plugins, out-of-process engine code, or arbitrary shared
  libraries.
- A distributed transaction manager across databases, clusters, or regions.
- Automatic leader election built on the present asynchronous replication
  stream.
- Transparent global secondary indexes, foreign keys, joins, or uniqueness over
  several shards.
- Online engine conversion in the first release; the first migration is a
  planned, write-fenced operation.

## 2. Current-state constraints

Today a `DB` opens collection directories directly. `Collection` owns NDJSON
segments, in-memory and persisted indexes, compaction, `meta.json`, and the
append callback that feeds replication. The cross-collection transaction (XTx)
coordinator is one root-level journal that assumes all participants are local
collections of one process and one data directory. Current replication carries
committed entries (or transaction envelopes) and a monotonic leader LSN.

The design therefore introduces a boundary **inside** the engine before changing
the public API. It must not reinterpret existing collection directories or make
an old binary silently open a new engine format.

## 3. Persistent catalog and collection manifest

### 3.1 Root catalog

Add a root-level catalog, atomically written with the same temp-file, fsync, and
rename discipline as other durable metadata:

```json
{
  "format": 1,
  "database_id": "db_…",
  "catalog_epoch": 7,
  "collections": {
    "users": {
      "id": "col_01…",
      "engine": {"id": "append-v1", "format": 1},
      "partitioning": {"kind": "none"},
      "shards": [{"id": "s0", "state": "active", "path": "users"}]
    }
  }
}
```

Suggested files are `catalog.json`, `catalog.json.tmp`, and a format fence such
as `catalog.format`. A pre-catalog database is imported exactly once: opening a
root without the catalog discovers existing collection directories and writes a
catalog entry with `engine=append-v1`, `format=1`, `partitioning=none`. The
operation must be recoverable and idempotent. A binary that cannot understand
the catalog version refuses to open it.

The collection ID, not its mutable display name, is used in replication,
snapshots, routing metadata, and engine directories. Name-to-ID resolution is
part of a catalog read snapshot.

### 3.2 Manifests and state transitions

Each collection also has an engine-owned manifest under its directory. The root
catalog is authoritative for routing; the collection manifest binds that route
to local files and detects an accidental directory swap. Both record engine ID,
format version, collection ID, and catalog epoch.

Catalog changes use an explicit state machine:

```text
creating -> active -> migrating -> cutover-pending -> active
                         |                 |
                         +-> failed -------+-> rollback-required
deleting -> tombstoned -> removed
```

No state is inferred from a partially created directory. Startup either resumes
the recorded operation or refuses with an operator-visible recovery state.

## 4. Engine contract

The exact Go names are illustrative; final interfaces should be kept internal
until at least two engines pass the conformance suite.

```go
type EngineFactory interface {
    ID() string                 // e.g. "append-v1"
    FormatVersion() uint32
    Capabilities() Capabilities
    Open(OpenRequest) (ShardStore, error)
    Create(CreateRequest) (ShardStore, error)
    Restore(RestoreRequest) (ShardStore, error)
}

type ShardStore interface {
    Get(ctx context.Context, key RecordKey) (Record, error)
    Apply(ctx context.Context, mutation Mutation) (ApplyResult, error)
    Scan(ctx context.Context, plan ScanPlan, emit func(Record) error) error
    Snapshot(ctx context.Context, sink SnapshotSink) (SnapshotDescriptor, error)
    Verify(ctx context.Context, mode VerifyMode) (VerifyReport, error)
    Close() error
}
```

`Mutation` is a canonical, engine-independent operation: collection ID, shard
ID, mutation ID, operation kind, record key/ID, revision, document, expiry,
logical timestamp, encryption epoch, and precondition. It is stable enough to
be journaled, replayed, and replicated. An engine may use a completely different
physical record layout but must give the same result when it applies the same
mutation twice.

The outer collection service owns API validation, auth, quotas, encryption
policy, revision/CAS semantics, idempotency, watch publication, metrics, and
routing. The engine owns physical persistence, local indexes, durable recovery,
and implementation-specific compaction. This avoids every engine reimplementing
security and wire semantics.

### 4.1 Capability contract

```go
type Capabilities struct {
    Durable              bool
    PointReads           bool
    OrderedScans         bool
    SecondaryIndexes     bool
    OnlineSnapshot       bool
    LogicalApply         bool
    PrepareRecover       bool
    EncryptionAtRest     bool
    TTL                  bool
    Watchable            bool
    LocalPartitions      bool
}
```

Capabilities are persisted in no user data; they are derived from the compiled
engine version and checked at create, alter, restore, transaction, and server
startup. The catalog stores required features for a collection so a downgraded
binary fails closed instead of opening with fewer guarantees.

Examples:

- `memory-v1` is non-durable and cannot join a durable transaction or a durable
  replica group.
- `append-v1` supports durable snapshots and logical apply through an adapter
  around the current segment implementation.
- An engine without `PrepareRecover` is excluded from cross-engine atomic XTx.
- An engine without ordered scans rejects range queries instead of doing an
  accidental unbounded scan unless the caller explicitly permits it.

## 5. Built-in engine plan

### `append-v1`

The current append-only NDJSON collection becomes the reference implementation.
It keeps its on-disk files and integrity behaviour. An adapter lifts the current
entry append/recovery functions into `ShardStore`; this is a refactor, not a
format conversion.

### `memory-v1`

An in-process map plus deterministic ordered index. It is useful for unit tests,
benchmarks, and explicitly ephemeral collections. Its manifest must state
`durable=false`; server startup rejects it where durable replication, backup
guarantees, or durable XTx are requested.

### First durable alternative: choose, do not accumulate

Implement either a page-oriented B+ tree or an LSM implementation after a
published benchmark decision record:

- **B+ tree:** preferable for frequent replacement of individual documents,
  bounded point lookup latency, and ordered scans. It needs checksummed pages,
  copy-on-write or WAL recovery, page reclamation, and crash tests.
- **LSM:** preferable for sustained ingest where sequential writes amortise
  better. It needs a WAL, immutable memtables, SSTable format, compaction
  scheduler, tombstone retention, and read-amplification controls.

Do not begin both. Every durable engine carries the full burden of backup,
verify/repair posture, encryption, logical apply, recovery fuzzing, crash tests,
and format fences.

### Specialised engines are later

Time-series and columnar engines deserve independent RFCs because their schema,
retention, scan, and aggregation semantics differ. Full-text and vector search
should start as rebuildable secondary-index modules backed by an authoritative
durable collection; they must not become the only copy of a document.

## 6. Logical replication

### 6.1 Protocol

The present replication protocol is safe for its current append-oriented model,
including atomic transaction envelopes. It should gain a new record generation,
not mutate existing fields:

```text
ReplicationChange v3
  lsn, database_id, catalog_epoch
  collection_id, shard_id, engine_id, engine_format
  mutation_id, mutation or transaction change-set
  canonical SHA-256 digest
```

The follower verifies the digest, catalog identity, allowed engine format, and
shard epoch before applying it. `Apply` is idempotent by mutation ID/revision.
An engine mismatch is not translated into physical bytes; it triggers a typed
`REPLICATION_ENGINE_UNSUPPORTED` error and snapshot/bootstrap path.

Protocol v1/v2 remain supported for existing append-only databases. A leader
does not emit v3 to a follower that did not negotiate it. Once a collection has
a non-append engine or partitioned placement, its replica group requires v3;
there is no lossy downgrade.

### 6.2 Snapshots

Snapshots contain a signed/hashed manifest with database ID, catalog epoch,
collection/shard IDs, engine ID/format, LSN boundary, encryption metadata, and
every payload digest. The receiving node stages and verifies the complete
snapshot before one atomic install. Engine formats are opaque payloads only
after the top-level manifest has been validated.

### 6.3 Consistency modes

- **Async follower (existing model):** leader acknowledgement is local
  durability; a follower can lag. Promotion stays manual and guarded by LSN.
- **Quorum replica group (future):** acknowledgement requires durable quorum
  replication; every shard has one elected leader and a membership/configuration
  log. This requires consensus, leader leases/fencing, and joint consensus for
  membership changes.

Never label asynchronous replica promotion as automatic HA. During a network
partition, two writers must not both accept writes for a shard.

## 7. Partitioning and routing

### 7.1 Shard map

Each active collection has a versioned shard map in the catalog. A request is
routed using the catalog epoch captured at its start; a changed epoch retries
routing before any mutation is applied.

```json
{
  "kind": "hash",
  "key_path": "tenant_id",
  "epoch": 4,
  "shards": [
    {"id": "s-a", "hash_range": [0, 2147483647]},
    {"id": "s-b", "hash_range": [2147483648, 4294967295]}
  ]
}
```

The first release supports `none`, `hash`, `range`, and `time` only. A partition
key is immutable for a record. For hash partitions, point writes and key reads
must include it; otherwise the server rejects the request instead of broadcasting
a write. Range and time partitioning have explicit boundary and late-write
policies.

### 7.2 Query behaviour

- A point lookup with a partition key reaches one shard.
- Range predicates that align with range/time boundaries reach only relevant
  shards.
- A query without a routable predicate is a scatter/gather request. It has a
  configurable fan-out and deadline, returns a typed partial-result status on
  shard failures, and exposes shards scanned in metrics.
- Global ordering/limit requires a merge stage and continuation token encoding
  the catalog epoch and per-shard cursor. An epoch mismatch invalidates the
  token safely.

Global secondary indexes and globally unique keys are intentionally deferred.
The first version permits local secondary indexes only. Application-level keys
should include the partition key where global uniqueness matters.

### 7.3 Resharding

Do not hide resharding behind an automatic rebalance loop. It is a durable
state machine: create target shard -> snapshot source range -> replay changes ->
verify counts/digests -> install dual-read route -> fence source writes -> atomically
advance shard-map epoch -> retire source after retention. Failed states are
recoverable from the catalog and observable in `scriva shard status`.

## 8. Transactions

The existing root-level XTx journal remains valid only for local collections
whose stores implement `PrepareRecover`. The coordinator is generalised behind
an internal participant interface:

```go
type PreparedParticipant interface {
    Prepare(ctx context.Context, tx PreparedChangeSet) (PreparedHandle, error)
    CommitPrepared(ctx context.Context, h PreparedHandle, decision Decision) error
    AbortPrepared(ctx context.Context, h PreparedHandle) error
    RecoverPrepared(ctx context.Context, decisionLookup DecisionLookup) error
}
```

The durable root decision remains the sole commit point for local multi-engine
transactions. A participant is visible only after it can recover that decision.
If any participant lacks `PrepareRecover`, the operation fails with
`ENGINE_TRANSACTION_UNSUPPORTED` before preparing any other participant.

For distributed shards, phase one permits only single-shard transactions.
Cross-shard transactions require a separate consensus-aware transaction RFC;
they must not reuse a local file journal across node failures.

## 9. Engine migration

The first migration is offline/maintenance-window only:

1. Validate the source collection and take a verified backup/snapshot.
2. Add `migration` state and target engine information to the catalog.
3. Fence writes to the source collection and record its source LSN/epoch.
4. Build the target store in a separate staging path from a consistent snapshot.
5. Verify record count, keys, revisions, expiry state, index expectations, and
   a canonical content digest.
6. Atomically switch the catalog entry to the target path/engine and fsync the
   catalog directory.
7. Reopen and verify the target before acknowledging completion; retain source
   data until explicit post-cutover retention expires.

An online migration may later dual-write canonical mutations to source and target
and compare their reads, but it is explicitly postponed. It multiplies failure
and rollback states and should be designed only after the offline process is
well exercised.

## 10. Public API and configuration evolution

Existing `CreateCollection(name)` remains a shorthand for an append-only,
unpartitioned collection. New optional fields are additive:

```yaml
collections:
  telemetry:
    engine: timeseries-v1
    partitioning:
      kind: time
      field: observed_at
      interval: 24h
    replication:
      mode: async
      replicas: 2
```

The proto/API should add a `CollectionSpec` message and return a
`CollectionDescription` including engine, capabilities, partitioning, and
placement. Feature negotiation is versioned; old clients can still create and
use default collections but cannot accidentally request an unknown engine.

Administrative operations are separate from ordinary CRUD: `CreateCollection`
with spec, `DescribeCollection`, `BeginEngineMigration`, `MigrationStatus`,
`CreateShard`, `ReshardStatus`, and eventually replica-group membership calls.
All require an explicit administrative authorization capability.

## 11. Implementation phases and acceptance gates

### Phase A — foundations

- Introduce catalog read/write/recovery and import existing roots as
  `append-v1`/single shard.
- Encapsulate today’s collection behind the internal engine adapter.
- Add engine IDs, format fences, capability checks, and metrics.
- Preserve existing public API, storage files, snapshots, and v1/v2 replication
  byte-for-byte for default collections.

**Gate:** all current crash, verify/repair, XTx, snapshot, and replication tests
pass unchanged; an old data directory opens and produces an equivalent catalog;
an old binary rejects a catalog-bearing root safely.

### Phase B — conformance and `memory-v1`

- Define a reusable conformance suite: CRUD/CAS/revision, TTL, indexes,
  scan ordering, snapshots, idempotent apply, cancellation, close/reopen, and
  advertised-capability rejection.
- Implement memory engine and mixed-engine catalog lifecycle tests.

**Gate:** no durable claim can be made for memory collections; every forbidden
backup/replication/XTx combination returns a stable typed error.

### Phase C — logical replication

- Add v3 negotiation and canonical logical change sets for `append-v1`.
- Add engine-aware snapshot manifests and staged restore.
- Prove duplicate, gap, digest mismatch, catalog epoch mismatch, engine format
  mismatch, follower crash, and bootstrap recovery behaviour.

**Gate:** no follower accepts a change for a wrong collection/shard/engine;
mixed protocol versions fail closed rather than silently losing mutations.

### Phase D — one durable alternative

- Publish benchmark corpus and choose B-tree or LSM by workload target.
- Implement it with the full conformance suite, fault injection, format fence,
  verify/repair story, snapshot, encryption, and logical replication.
- Implement offline append-to-target migration.

**Gate:** recovery, backup/restore, and replicated convergence are demonstrated
under crash injection for both engines; migration has a tested rollback path.

### Phase E — local partitions

- Implement hash/range/time shard map, request routing, scatter/gather limits,
  merge cursors, and local shard lifecycle.
- Limit transactions to one shard unless all participants are local and use the
  prepared-participant coordinator.

**Gate:** deterministic routing across restart; no duplicate/omitted records
through split/retry tests; resharding has crash-resume tests at every state.

### Phase F — distributed replica groups

- Write a separate consensus, membership, bootstrap, and disaster-recovery RFC.
- Implement one per-shard consensus group with leader fencing and quorum commit.
- Add observability for quorum health, leader term, membership, replication lag,
  snapshot install, and recovery.

**Gate:** partition, leader crash, stale leader, membership change, and
snapshot-join tests prove no split-brain acknowledged writes. Only after this
gate may automatic failover be advertised.

## 12. Observability and operational requirements

Every engine/shard must report engine ID/format, catalog and shard-map epoch,
durability mode, compaction/backlog, snapshot age, replication position, and
capability failures. Operators need `scriva collection describe`, `scriva shard
status`, `scriva migration status`, and machine-readable equivalents before
multi-engine or partitioned operation is considered production-ready.

Backups must retain catalog, collection manifests, root transaction state, shard
map, engine payloads, encryption metadata, and checksums as one verified unit.
Restoring only a collection directory without its catalog entry is an explicit
advanced recovery flow, never normal restore behaviour.

