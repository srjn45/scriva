<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Scriva\V1;

/**
 * ---------------------------------------------------------------------------
 * Service
 * ---------------------------------------------------------------------------
 *
 */
class ScrivaClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Scriva\V1\CreateCollectionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CreateCollection(\Scriva\V1\CreateCollectionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/CreateCollection',
        $argument,
        ['\Scriva\V1\CreateCollectionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\DropCollectionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function DropCollection(\Scriva\V1\DropCollectionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/DropCollection',
        $argument,
        ['\Scriva\V1\DropCollectionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\ListCollectionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ListCollections(\Scriva\V1\ListCollectionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/ListCollections',
        $argument,
        ['\Scriva\V1\ListCollectionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- CRUD ---
     *
     * @param \Scriva\V1\InsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Insert(\Scriva\V1\InsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Insert',
        $argument,
        ['\Scriva\V1\InsertResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\InsertManyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function InsertMany(\Scriva\V1\InsertManyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/InsertMany',
        $argument,
        ['\Scriva\V1\InsertManyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\FindByIdRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function FindById(\Scriva\V1\FindByIdRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/FindById',
        $argument,
        ['\Scriva\V1\FindResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Server-streaming: results are streamed back one by one.
     * @param \Scriva\V1\FindRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Find(\Scriva\V1\FindRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/scriva.v1.Scriva/Find',
        $argument,
        ['\Scriva\V1\FindResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\UpdateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Update(\Scriva\V1\UpdateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Update',
        $argument,
        ['\Scriva\V1\UpdateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\DeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Delete(\Scriva\V1\DeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Delete',
        $argument,
        ['\Scriva\V1\DeleteResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Keyed CRUD, Upsert & compare-and-swap (N1) ---
     *
     * These map straight onto the embedded engine's keyed operations, giving
     * network clients natural (caller-supplied) string keys, upsert, and
     * optimistic-concurrency updates keyed on a per-record revision (`rev`).
     *
     * Upsert inserts data under key if no live record carries it, or replaces the
     * existing record's data if one does — atomically. Returns the resulting
     * record with its (incremented on replace) revision. This is the keyed-insert
     * path: there is no separate InsertWithKey RPC.
     * @param \Scriva\V1\UpsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Upsert(\Scriva\V1\UpsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Upsert',
        $argument,
        ['\Scriva\V1\UpsertResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * FindByKey returns the record carrying the caller-supplied string key.
     * A missing key yields NOT_FOUND.
     * @param \Scriva\V1\FindByKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function FindByKey(\Scriva\V1\FindByKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/FindByKey',
        $argument,
        ['\Scriva\V1\FindResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * UpdateByKey overwrites the record carrying key, preserving the key itself.
     * A missing key yields NOT_FOUND.
     * @param \Scriva\V1\UpdateByKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function UpdateByKey(\Scriva\V1\UpdateByKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/UpdateByKey',
        $argument,
        ['\Scriva\V1\UpdateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * DeleteByKey removes the record carrying key. A missing key yields NOT_FOUND.
     * @param \Scriva\V1\DeleteByKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function DeleteByKey(\Scriva\V1\DeleteByKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/DeleteByKey',
        $argument,
        ['\Scriva\V1\DeleteResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * UpdateIfRev conditionally updates the record carrying key: the write is
     * applied only if the record's current revision equals expected_rev. A stale
     * revision (or a missing key) is a clean no-op reported as swapped=false, not
     * an error.
     * @param \Scriva\V1\UpdateIfRevRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function UpdateIfRev(\Scriva\V1\UpdateIfRevRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/UpdateIfRev',
        $argument,
        ['\Scriva\V1\UpdateIfRevResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Secondary indexes ---
     *
     * @param \Scriva\V1\EnsureIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function EnsureIndex(\Scriva\V1\EnsureIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/EnsureIndex',
        $argument,
        ['\Scriva\V1\EnsureIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\DropIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function DropIndex(\Scriva\V1\DropIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/DropIndex',
        $argument,
        ['\Scriva\V1\DropIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\ListIndexesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ListIndexes(\Scriva\V1\ListIndexesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/ListIndexes',
        $argument,
        ['\Scriva\V1\ListIndexesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Transactions ---
     *
     * @param \Scriva\V1\BeginTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function BeginTx(\Scriva\V1\BeginTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/BeginTx',
        $argument,
        ['\Scriva\V1\BeginTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\CommitTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CommitTx(\Scriva\V1\CommitTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/CommitTx',
        $argument,
        ['\Scriva\V1\CommitTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Scriva\V1\RollbackTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function RollbackTx(\Scriva\V1\RollbackTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/RollbackTx',
        $argument,
        ['\Scriva\V1\RollbackTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Cross-collection transactions (XTx) ---
     *
     * A cross-collection transaction stages inserts, updates and deletes on
     * several collections of one database and applies them atomically at commit.
     *
     * Isolation is optimistic validation of point reads and writes: every GetXTx
     * and every staged update/delete records the revision (or absence) of the
     * document it touched, and CommitXTx re-checks those observations under the
     * participant locks. A changed observation aborts the commit with ABORTED
     * (reason XTX_CONFLICT) and nothing is applied. There are no predicate reads
     * and therefore no phantom protection: scans, range filters, aggregations and
     * index lookups are not available inside a transaction and are rejected.
     *
     * Every failure carries a google.rpc.ErrorInfo detail (domain "scriva.xtx")
     * whose reason is machine-readable and whose metadata["retry_safe"] says
     * whether re-running the transaction can never double-apply. The one
     * ambiguous result is reason XTX_OUTCOME_UNKNOWN (code UNKNOWN): the commit
     * may or may not have been applied, so the client must call XTxStatus with
     * metadata["status_ref"] instead of retrying.
     *
     * BeginXTx opens a transaction over an explicit list of participant
     * collections, which must already exist. Nothing is locked or written. The
     * returned xtx_id is a process-local, unguessable handle id that addresses
     * every later call; it does not survive a server restart.
     * @param \Scriva\V1\BeginXTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function BeginXTx(\Scriva\V1\BeginXTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/BeginXTx',
        $argument,
        ['\Scriva\V1\BeginXTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * StageXTx buffers one insert, update or delete on a participant collection.
     * Nothing is written and no other reader can see it before CommitXTx.
     * @param \Scriva\V1\StageXTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function StageXTx(\Scriva\V1\StageXTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/StageXTx',
        $argument,
        ['\Scriva\V1\StageXTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * GetXTx is a point read through the transaction. It returns the
     * transaction's own staged write when there is one (read-your-writes),
     * otherwise the committed document, which is added to the read set that
     * CommitXTx validates. Because it changes the transaction's state it is
     * classified as a write RPC: it needs a read-write key and is refused on a
     * read-only follower.
     * @param \Scriva\V1\GetXTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function GetXTx(\Scriva\V1\GetXTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/GetXTx',
        $argument,
        ['\Scriva\V1\GetXTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * CommitXTx validates the transaction and applies it atomically. It is
     * idempotent: repeating it on a committed transaction returns the original
     * result, and a transaction whose idempotency key already committed returns
     * that outcome with replayed=true and applies nothing.
     * @param \Scriva\V1\CommitXTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CommitXTx(\Scriva\V1\CommitXTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/CommitXTx',
        $argument,
        ['\Scriva\V1\CommitXTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * RollbackXTx discards everything the transaction staged. It is idempotent.
     * It cannot undo a decision: a committed transaction yields
     * FAILED_PRECONDITION (XTX_FINISHED).
     * @param \Scriva\V1\RollbackXTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function RollbackXTx(\Scriva\V1\RollbackXTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/RollbackXTx',
        $argument,
        ['\Scriva\V1\RollbackXTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * XTxStatus reports the outcome of a transaction by coordinator tx_id,
     * idempotency key or xtx_id. It is the only safe way to resolve an
     * XTX_OUTCOME_UNKNOWN commit or a lost commit response. Read-only.
     * @param \Scriva\V1\XTxStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function XTxStatus(\Scriva\V1\XTxStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/XTxStatus',
        $argument,
        ['\Scriva\V1\XTxStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Watch (server-streaming change feed) ---
     *
     * @param \Scriva\V1\WatchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Watch(\Scriva\V1\WatchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/scriva.v1.Scriva/Watch',
        $argument,
        ['\Scriva\V1\WatchEvent', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Aggregations (N4) ---
     *
     * Aggregate computes count and numeric aggregations (sum/avg/min/max) over the
     * live records matching the same Filter as Find, optionally grouped by a field.
     * It server-streams one message per group; a plain (ungrouped) aggregation
     * streams a single message. Aggregation runs entirely in the engine — the
     * collection is never materialised on the client.
     * @param \Scriva\V1\AggregateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Aggregate(\Scriva\V1\AggregateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/scriva.v1.Scriva/Aggregate',
        $argument,
        ['\Scriva\V1\AggregateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Stats ---
     *
     * @param \Scriva\V1\CollectionStatsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CollectionStats(\Scriva\V1\CollectionStatsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/CollectionStats',
        $argument,
        ['\Scriva\V1\CollectionStatsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Admin ---
     *
     * Compact runs a forced, synchronous compaction pass on a collection and
     * returns only after it completes.
     * @param \Scriva\V1\CompactRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Compact(\Scriva\V1\CompactRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Compact',
        $argument,
        ['\Scriva\V1\CompactResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Snapshot streams a consistent, gzip-compressed tar archive of the whole
     * database. Restore by extracting it into a data directory. gRPC-only:
     * binary streaming does not map cleanly onto the REST gateway.
     * @param \Scriva\V1\SnapshotRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Snapshot(\Scriva\V1\SnapshotRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/scriva.v1.Scriva/Snapshot',
        $argument,
        ['\Scriva\V1\SnapshotChunk', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Replication (R1) ---
     *
     * Replicate is the leader's log-shipping feed: a follower tails committed
     * segment entries, each tagged with a monotonic global sequence number (LSN),
     * and applies them through the normal write path to stay consistent. The
     * follower asks to resume from the last LSN it applied (from_lsn); the leader
     * streams every committed entry with lsn > from_lsn — first any recent history
     * still buffered, then live writes as they commit. A follower too far behind
     * for the leader's in-memory buffer gets FAILED_PRECONDITION and must
     * re-bootstrap from a Snapshot. gRPC-only (binary, long-lived stream).
     * @param \Scriva\V1\ReplicateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Replicate(\Scriva\V1\ReplicateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/scriva.v1.Scriva/Replicate',
        $argument,
        ['\Scriva\V1\ReplicationRecord', 'decode'],
        $metadata, $options);
    }

    /**
     * ReplicationStatus reports the leader's current LSN and, for each connected
     * follower, the last LSN shipped to it and its lag. Used for observability and
     * to read the leader's LSN watermark before bootstrapping a fresh follower.
     * @param \Scriva\V1\ReplicationStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ReplicationStatus(\Scriva\V1\ReplicationStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/ReplicationStatus',
        $argument,
        ['\Scriva\V1\ReplicationStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Failover (R3) ---
     *
     * Promote flips a caught-up follower into a leader: it stops replicating from
     * its upstream, lifts the read-only guard, and begins accepting writes.
     * Promotion is refused with FAILED_PRECONDITION when the node is not a
     * follower, or when its replication lag (last-known leader LSN minus applied
     * LSN) exceeds the server's configured threshold — pass force to override that
     * guard when the leader is unrecoverable and some divergence is acceptable.
     * This is an admin operation and requires a read-write API key. Promotion is a
     * one-way transition; automatic leader election is out of scope.
     * @param \Scriva\V1\PromoteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Promote(\Scriva\V1\PromoteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/scriva.v1.Scriva/Promote',
        $argument,
        ['\Scriva\V1\PromoteResponse', 'decode'],
        $metadata, $options);
    }

}
