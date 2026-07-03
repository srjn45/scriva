<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Filedb\V1;

/**
 * ---------------------------------------------------------------------------
 * Service
 * ---------------------------------------------------------------------------
 *
 */
class FileDBClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Filedb\V1\CreateCollectionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CreateCollection(\Filedb\V1\CreateCollectionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/CreateCollection',
        $argument,
        ['\Filedb\V1\CreateCollectionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\DropCollectionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function DropCollection(\Filedb\V1\DropCollectionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/DropCollection',
        $argument,
        ['\Filedb\V1\DropCollectionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\ListCollectionsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ListCollections(\Filedb\V1\ListCollectionsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/ListCollections',
        $argument,
        ['\Filedb\V1\ListCollectionsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- CRUD ---
     *
     * @param \Filedb\V1\InsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Insert(\Filedb\V1\InsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/Insert',
        $argument,
        ['\Filedb\V1\InsertResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\InsertManyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function InsertMany(\Filedb\V1\InsertManyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/InsertMany',
        $argument,
        ['\Filedb\V1\InsertManyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\FindByIdRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function FindById(\Filedb\V1\FindByIdRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/FindById',
        $argument,
        ['\Filedb\V1\FindResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Server-streaming: results are streamed back one by one.
     * @param \Filedb\V1\FindRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Find(\Filedb\V1\FindRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/filedb.v1.FileDB/Find',
        $argument,
        ['\Filedb\V1\FindResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\UpdateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Update(\Filedb\V1\UpdateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/Update',
        $argument,
        ['\Filedb\V1\UpdateResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\DeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Delete(\Filedb\V1\DeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/Delete',
        $argument,
        ['\Filedb\V1\DeleteResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Secondary indexes ---
     *
     * @param \Filedb\V1\EnsureIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function EnsureIndex(\Filedb\V1\EnsureIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/EnsureIndex',
        $argument,
        ['\Filedb\V1\EnsureIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\DropIndexRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function DropIndex(\Filedb\V1\DropIndexRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/DropIndex',
        $argument,
        ['\Filedb\V1\DropIndexResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\ListIndexesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ListIndexes(\Filedb\V1\ListIndexesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/ListIndexes',
        $argument,
        ['\Filedb\V1\ListIndexesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Transactions ---
     *
     * @param \Filedb\V1\BeginTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function BeginTx(\Filedb\V1\BeginTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/BeginTx',
        $argument,
        ['\Filedb\V1\BeginTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\CommitTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CommitTx(\Filedb\V1\CommitTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/CommitTx',
        $argument,
        ['\Filedb\V1\CommitTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Filedb\V1\RollbackTxRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function RollbackTx(\Filedb\V1\RollbackTxRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/RollbackTx',
        $argument,
        ['\Filedb\V1\RollbackTxResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Watch (server-streaming change feed) ---
     *
     * @param \Filedb\V1\WatchRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Watch(\Filedb\V1\WatchRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/filedb.v1.FileDB/Watch',
        $argument,
        ['\Filedb\V1\WatchEvent', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Stats ---
     *
     * @param \Filedb\V1\CollectionStatsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function CollectionStats(\Filedb\V1\CollectionStatsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/CollectionStats',
        $argument,
        ['\Filedb\V1\CollectionStatsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * --- Admin ---
     *
     * Compact runs a forced, synchronous compaction pass on a collection and
     * returns only after it completes.
     * @param \Filedb\V1\CompactRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Compact(\Filedb\V1\CompactRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/filedb.v1.FileDB/Compact',
        $argument,
        ['\Filedb\V1\CompactResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Snapshot streams a consistent, gzip-compressed tar archive of the whole
     * database. Restore by extracting it into a data directory. gRPC-only:
     * binary streaming does not map cleanly onto the REST gateway.
     * @param \Filedb\V1\SnapshotRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Snapshot(\Filedb\V1\SnapshotRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/filedb.v1.FileDB/Snapshot',
        $argument,
        ['\Filedb\V1\SnapshotChunk', 'decode'],
        $metadata, $options);
    }

}
