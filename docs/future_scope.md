# ScrivaDB future scope: collection engines, replication, and partitioning

**Status:** proposal; no items in this document are implemented or a promise of
delivery. It is the product and architecture direction for work after the v1.6
hardening and transaction releases.

## Goal

Keep ScrivaDB simple by default while allowing one database to use different
storage layouts where that is useful. A database should be able to contain, for
example, an inspectable append-only audit collection, an update-oriented user
collection, and a high-ingest time-series collection without treating them as
separate products.

```text
database
  users          engine=btree-v1       partitions=none       replicas=1
  audit_events   engine=append-v1      partitions=time(month) replicas=1
  telemetry      engine=timeseries-v1  partitions=time(day)   replicas=3
  sessions       engine=memory-v1      partitions=none       replicas=1
```

`append-v1` is the existing NDJSON-segment implementation. It remains the
default: human-readable, portable, and with no extra runtime dependency.

## Terms

| Term | Meaning |
|---|---|
| **Storage engine** | The implementation that persists and indexes one collection shard. |
| **Logical collection** | The collection visible through the Scriva API. It may have one or more shards. |
| **Shard / partition** | A disjoint subset of a logical collection, selected by a routing rule. |
| **Replica** | Another copy of the *same* shard. Replication improves availability/read scale; it does not split data. |
| **Placement** | Where a shard and its replicas run: initially local process only, later a node group. |
| **Capability** | An explicit engine guarantee, such as durable prepare/recover, range scans, or online snapshots. |

Partitioning and replication solve different problems. Partitioning spreads data
and write load; replication keeps copies of that partition. A replicated,
unpartitioned collection still has one write bottleneck. A partitioned,
unreplicated collection still has one failure domain per shard.

## Candidate engines

| Engine | Good fit | Important cost / limit | Priority |
|---|---|---|---|
| `append-v1` | audit trails, local-first data, simple deployments | update/delete amplification and compaction | Existing default |
| `memory-v1` | tests, caches, temporary sessions | no restart durability | First new engine |
| `btree-v1` | point reads, range scans, frequent document replacement | binary page format and complex recovery | Evaluate after benchmarks |
| `lsm-v1` | sustained write-heavy ingestion, large working sets | read/compaction amplification and tuning | Alternative to B-tree, not an automatic second choice |
| `timeseries-v1` | timestamp-first telemetry, retention and downsampling | specialised schema/query model | Only if this is a product focus |
| `columnar-v1` | analytical scans and aggregates | poor row-level update behaviour | Research, likely analytical companion |
| search/vector index | full-text or semantic retrieval | should be a derived index, not authoritative storage | Later optional module |
| SQLite adapter | interoperability with SQLite tooling | changes the product’s dependency and storage story | Explicit product decision, not a default |

There should not be a generic “load arbitrary engine plugin” mechanism in the
first version. Engines run in-process and are compiled, reviewed implementations
with a durable format identifier and conformance tests. This preserves the
single-binary distribution model and prevents an engine from silently weakening
durability or recovery guarantees.

## Product rules

1. **Safe default.** `CreateCollection` without an engine keeps creating
   `append-v1` collections with today’s behaviour and files.
2. **Engine identity is durable.** A collection manifest records an engine ID
   and format version. Opening a collection with an unknown or too-new ID fails
   closed; Scriva must never guess how to interpret its files.
3. **Capabilities are explicit.** The server validates a requested operation
   against the collection’s capabilities before doing work. It never silently
   degrades an atomic write, snapshot, encryption policy, or replication mode.
4. **Migration is a first-class operation.** Changing engine is not a config
   edit. It is an export/build/verify/cutover workflow with a backup and an
   auditable state record.
5. **Engine-specific physical logs stay local.** Replication carries canonical,
   versioned logical mutations and snapshots, not NDJSON lines, B-tree pages, or
   LSM internals.
6. **No fake distributed transactions.** Until a shard/engine has durable
   prepare, decision, and recovery semantics, a transaction spanning it is
   rejected explicitly.

## Replication and availability direction

The current product is single-writer, leader-to-follower asynchronous
replication with snapshot bootstrap and manual promotion. It is useful and
should remain available for local disaster recovery/read replicas, but it is
not automatic failover and cannot safely make two partition leaders accept
writes during a network partition.

The evolution is:

1. Generalise the existing replication stream to logical mutation envelopes,
   while retaining protocol compatibility with existing `append-v1` followers.
2. Support local multi-shard routing first; it exercises the catalog, placement,
   snapshot, and transaction boundaries without distributed failure modes.
3. Add a per-shard replica group only with quorum/consensus semantics (for
   example Raft): one elected leader, a durable membership/configuration log,
   majority acknowledgement for strong writes, and fencing of old leaders.
4. Make automatic failover an opt-in deployment mode after membership changes,
   bootstrap, split-brain recovery, observability, and operator procedures are
   proven. It is not a flag layered over asynchronous replication.

## Partitioning direction

The initial partition modes should be intentionally small:

| Mode | Route | Suitable use |
|---|---|---|
| `none` | one shard | default and all existing collections |
| `hash` | stable hash of an explicit partition key | even distribution for keyed writes |
| `range` | ordered key boundaries | range queries and locality |
| `time` | timestamp bucket | events, retention, and pruning |

Cross-partition queries must state their cost. Point queries with the partition
key route to one shard. Broadcast scans and global aggregates fan out with
bounded concurrency and explicit partial-failure behaviour. Global secondary
indexes, joins, and arbitrary cross-shard uniqueness are deferred; they need
their own consistency and recovery design.

## Recommended delivery order

1. Publish benchmarks and workload targets for the existing engine; use them to
   decide whether B-tree or LSM work is justified.
2. Introduce a catalog and internal engine capability boundary while preserving
   the existing `append-v1` on-disk format and public API.
3. Ship `memory-v1` plus a conformance suite. This validates mixed-engine
   collection lifecycle without introducing a second durable format.
4. Ship one durable alternative (`btree-v1` *or* `lsm-v1`), including offline
   migration, backup/restore, encryption, and logical replication support.
5. Ship local hash/range/time partitioning with no automatic placement.
6. Design and prove per-shard consensus replication, then consider managed
   placement and automatic failover.

The detailed technical design and acceptance criteria are in
[`design-pluggable-storage-and-distribution.md`](design-pluggable-storage-and-distribution.md).

