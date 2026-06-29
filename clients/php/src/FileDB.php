<?php

declare(strict_types=1);

namespace FileDBv2;

use Filedb\V1\AndFilter;
use Filedb\V1\BeginTxRequest;
use Filedb\V1\CollectionStatsRequest;
use Filedb\V1\CommitTxRequest;
use Filedb\V1\CreateCollectionRequest;
use Filedb\V1\DeleteRequest;
use Filedb\V1\DropCollectionRequest;
use Filedb\V1\DropIndexRequest;
use Filedb\V1\EnsureIndexRequest;
use Filedb\V1\FieldFilter;
use Filedb\V1\FileDBClient as GrpcStub;
use Filedb\V1\Filter;
use Filedb\V1\FilterOp;
use Filedb\V1\FindByIdRequest;
use Filedb\V1\FindRequest;
use Filedb\V1\InsertManyRequest;
use Filedb\V1\InsertRequest;
use Filedb\V1\ListCollectionsRequest;
use Filedb\V1\ListIndexesRequest;
use Filedb\V1\OrFilter;
use Filedb\V1\RollbackTxRequest;
use Filedb\V1\UpdateRequest;
use Filedb\V1\WatchRequest;
use Google\Protobuf\Struct;
use Google\Protobuf\Value;
use Google\Protobuf\ListValue;
use Google\Protobuf\NullValue;

/**
 * PHP gRPC client for FileDB v2.
 *
 * A thin, idiomatic wrapper over the gRPC API defined in proto/filedb.proto.
 * Every RPC is exposed as a camelCase method; records are returned as plain
 * PHP arrays and filters are plain associative arrays (see find()).
 *
 * Example:
 *
 *   $db = new FileDB('localhost', 5433, 'dev-key');
 *   $db->createCollection('users');
 *   $id = $db->insert('users', ['name' => 'Alice', 'age' => 30]);
 *   $record = $db->findById('users', $id);
 *   $admins = $db->find('users', ['field' => 'role', 'op' => 'eq', 'value' => 'admin']);
 *   $db->update('users', $id, ['name' => 'Alice', 'age' => 31]);
 *   $db->delete('users', $id);
 *   $db->dropCollection('users');
 */
class FileDB
{
    private GrpcStub $stub;

    private array $metadata;

    private static array $OP_MAP = [
        'eq'       => FilterOp::EQ,
        'neq'      => FilterOp::NEQ,
        'gt'       => FilterOp::GT,
        'gte'      => FilterOp::GTE,
        'lt'       => FilterOp::LT,
        'lte'      => FilterOp::LTE,
        'contains' => FilterOp::CONTAINS,
        'regex'    => FilterOp::REGEX,
    ];

    private static array $WATCH_OP_NAMES = [
        0 => 'UNSPECIFIED',
        1 => 'INSERTED',
        2 => 'UPDATED',
        3 => 'DELETED',
    ];

    /**
     * @param string      $host       Server host (e.g. 'localhost')
     * @param int         $port       gRPC port (default server port: 5433)
     * @param string      $apiKey     API key — sent as x-api-key metadata on every call
     * @param string|null $tlsCaCert  Path to PEM CA certificate file for TLS, or null for plaintext
     */
    public function __construct(
        string $host = 'localhost',
        int $port = 5433,
        string $apiKey = '',
        ?string $tlsCaCert = null
    ) {
        $target = $host . ':' . $port;

        if ($tlsCaCert !== null) {
            $certData = file_get_contents($tlsCaCert);
            if ($certData === false) {
                throw new \RuntimeException("Cannot read TLS CA cert: $tlsCaCert");
            }
            $creds = \Grpc\ChannelCredentials::createSsl($certData);
        } else {
            $creds = \Grpc\ChannelCredentials::createInsecure();
        }

        $this->stub = new GrpcStub($target, ['credentials' => $creds]);
        $this->metadata = [['x-api-key', $apiKey]];
    }

    // -------------------------------------------------------------------------
    // Collection management
    // -------------------------------------------------------------------------

    /**
     * Create a new collection. Returns the collection name.
     */
    public function createCollection(string $name): string
    {
        $req = new CreateCollectionRequest();
        $req->setName($name);
        [$resp, $status] = $this->stub->CreateCollection($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getName();
    }

    /**
     * Drop a collection and all its data. Returns true on success.
     */
    public function dropCollection(string $name): bool
    {
        $req = new DropCollectionRequest();
        $req->setName($name);
        [$resp, $status] = $this->stub->DropCollection($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    /**
     * List all collection names.
     *
     * @return string[]
     */
    public function listCollections(): array
    {
        $req = new ListCollectionsRequest();
        [$resp, $status] = $this->stub->ListCollections($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return iterator_to_array($resp->getNames());
    }

    // -------------------------------------------------------------------------
    // CRUD
    // -------------------------------------------------------------------------

    /**
     * Insert one record. Returns the assigned integer ID.
     *
     * @param array<string, mixed> $data
     */
    public function insert(string $collection, array $data): int
    {
        $req = new InsertRequest();
        $req->setCollection($collection);
        $req->setData($this->arrayToStruct($data));
        [$resp, $status] = $this->stub->Insert($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return (int) $resp->getId();
    }

    /**
     * Insert multiple records. Returns the assigned IDs in insertion order.
     *
     * @param array<array<string, mixed>> $records
     * @return int[]
     */
    public function insertMany(string $collection, array $records): array
    {
        $req = new InsertManyRequest();
        $req->setCollection($collection);
        $structs = array_map([$this, 'arrayToStruct'], $records);
        $req->setRecords($structs);
        [$resp, $status] = $this->stub->InsertMany($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return array_map('intval', iterator_to_array($resp->getIds()));
    }

    /**
     * Fetch a single record by ID.
     *
     * @return array<string, mixed>
     */
    public function findById(string $collection, int $id): array
    {
        $req = new FindByIdRequest();
        $req->setCollection($collection);
        $req->setId($id);
        [$resp, $status] = $this->stub->FindById($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $this->recordToArray($resp->getRecord());
    }

    /**
     * Query records. The Find RPC is server-streaming; results are collected
     * into an array.
     *
     * @param array<string, mixed>|null $filter  See filterToProto() for the filter format
     * @return array<array<string, mixed>>
     */
    public function find(
        string $collection,
        ?array $filter = null,
        int $limit = 0,
        int $offset = 0,
        string $orderBy = '',
        bool $descending = false
    ): array {
        $req = new FindRequest();
        $req->setCollection($collection);
        $req->setLimit($limit);
        $req->setOffset($offset);
        $req->setOrderBy($orderBy);
        $req->setDescending($descending);
        if ($filter !== null) {
            $req->setFilter($this->filterToProto($filter));
        }

        $call = $this->stub->Find($req, $this->metadata);
        $results = [];
        foreach ($call->responses() as $resp) {
            $results[] = $this->recordToArray($resp->getRecord());
        }
        $status = $call->getStatus();
        $this->checkStatus($status);
        return $results;
    }

    /**
     * Update a record by ID. Returns the updated ID.
     *
     * @param array<string, mixed> $data
     */
    public function update(string $collection, int $id, array $data): int
    {
        $req = new UpdateRequest();
        $req->setCollection($collection);
        $req->setId($id);
        $req->setData($this->arrayToStruct($data));
        [$resp, $status] = $this->stub->Update($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return (int) $resp->getId();
    }

    /**
     * Delete a record by ID. Returns true if the record existed.
     */
    public function delete(string $collection, int $id): bool
    {
        $req = new DeleteRequest();
        $req->setCollection($collection);
        $req->setId($id);
        [$resp, $status] = $this->stub->Delete($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    // -------------------------------------------------------------------------
    // Secondary indexes
    // -------------------------------------------------------------------------

    /**
     * Create a secondary index on a field (no-op if it already exists).
     */
    public function ensureIndex(string $collection, string $field): void
    {
        $req = new EnsureIndexRequest();
        $req->setCollection($collection);
        $req->setField($field);
        [, $status] = $this->stub->EnsureIndex($req, $this->metadata)->wait();
        $this->checkStatus($status);
    }

    /**
     * Drop a secondary index. Returns true if the index existed.
     */
    public function dropIndex(string $collection, string $field): bool
    {
        $req = new DropIndexRequest();
        $req->setCollection($collection);
        $req->setField($field);
        [$resp, $status] = $this->stub->DropIndex($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    /**
     * List all indexed field names for a collection.
     *
     * @return string[]
     */
    public function listIndexes(string $collection): array
    {
        $req = new ListIndexesRequest();
        $req->setCollection($collection);
        [$resp, $status] = $this->stub->ListIndexes($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return iterator_to_array($resp->getFields());
    }

    // -------------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------------

    /**
     * Begin a transaction on a collection. Returns the transaction ID.
     */
    public function beginTx(string $collection): string
    {
        $req = new BeginTxRequest();
        $req->setCollection($collection);
        [$resp, $status] = $this->stub->BeginTx($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getTxId();
    }

    /**
     * Commit a transaction. Returns true on success.
     */
    public function commitTx(string $txId): bool
    {
        $req = new CommitTxRequest();
        $req->setTxId($txId);
        [$resp, $status] = $this->stub->CommitTx($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    /**
     * Roll back a transaction. Returns true on success.
     */
    public function rollbackTx(string $txId): bool
    {
        $req = new \Filedb\V1\RollbackTxRequest();
        $req->setTxId($txId);
        [$resp, $status] = $this->stub->RollbackTx($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    // -------------------------------------------------------------------------
    // Watch (server-streaming change feed)
    // -------------------------------------------------------------------------

    /**
     * Subscribe to change events on a collection.
     *
     * Returns a Generator that yields event arrays, each shaped like:
     *
     *   ['op' => 'INSERTED', 'collection' => 'users',
     *    'record' => [...], 'ts' => '2026-06-29T...']
     *
     * 'op' is one of INSERTED / UPDATED / DELETED.
     * Stop watching by breaking out of the foreach loop.
     *
     * @param array<string, mixed>|null $filter
     * @return \Generator<int, array<string, mixed>>
     */
    public function watch(string $collection, ?array $filter = null): \Generator
    {
        $req = new WatchRequest();
        $req->setCollection($collection);
        if ($filter !== null) {
            $req->setFilter($this->filterToProto($filter));
        }
        $call = $this->stub->Watch($req, $this->metadata);
        foreach ($call->responses() as $event) {
            $out = [
                'op'         => self::$WATCH_OP_NAMES[$event->getOp()] ?? 'UNSPECIFIED',
                'collection' => $event->getCollection(),
                'record'     => $this->recordToArray($event->getRecord()),
            ];
            if ($event->hasTs()) {
                $out['ts'] = $event->getTs()->toDateTime()->format(\DateTimeInterface::ATOM);
            }
            yield $out;
        }
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    /**
     * Return collection statistics.
     *
     * @return array{collection: string, record_count: int, segment_count: int, dirty_entries: int, size_bytes: int}
     */
    public function stats(string $collection): array
    {
        $req = new CollectionStatsRequest();
        $req->setCollection($collection);
        [$resp, $status] = $this->stub->CollectionStats($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return [
            'collection'    => $resp->getCollection(),
            'record_count'  => (int) $resp->getRecordCount(),
            'segment_count' => (int) $resp->getSegmentCount(),
            'dirty_entries' => (int) $resp->getDirtyEntries(),
            'size_bytes'    => (int) $resp->getSizeBytes(),
        ];
    }

    // -------------------------------------------------------------------------
    // Filter builder
    // -------------------------------------------------------------------------

    /**
     * Convert a plain PHP array filter to a proto Filter.
     *
     * Field filter:
     *   ['field' => 'age', 'op' => 'gt', 'value' => '30']
     *
     * AND composite:
     *   ['and' => [
     *       ['field' => 'age',  'op' => 'gte', 'value' => '18'],
     *       ['field' => 'city', 'op' => 'eq',  'value' => 'Berlin'],
     *   ]]
     *
     * OR composite: same shape but key is 'or'.
     *
     * Supported op values: eq neq gt gte lt lte contains regex
     */
    public function filterToProto(array $filter): Filter
    {
        if (isset($filter['and'])) {
            $children = array_map([$this, 'filterToProto'], $filter['and']);
            $and = new AndFilter();
            $and->setFilters($children);
            $f = new Filter();
            $f->setAnd($and);
            return $f;
        }
        if (isset($filter['or'])) {
            $children = array_map([$this, 'filterToProto'], $filter['or']);
            $or = new OrFilter();
            $or->setFilters($children);
            $f = new Filter();
            $f->setOr($or);
            return $f;
        }

        $opName = strtolower((string) ($filter['op'] ?? ''));
        if (!isset(self::$OP_MAP[$opName])) {
            throw new \InvalidArgumentException(
                "Unknown filter op '$opName'; expected one of: " . implode(', ', array_keys(self::$OP_MAP))
            );
        }
        $ff = new FieldFilter();
        $ff->setField((string) $filter['field']);
        $ff->setOp(self::$OP_MAP[$opName]);
        $ff->setValue((string) $filter['value']);

        $f = new Filter();
        $f->setField($ff);
        return $f;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Convert a plain PHP array to a google.protobuf.Struct.
     *
     * @param array<string, mixed> $data
     */
    private function arrayToStruct(array $data): Struct
    {
        $struct = new Struct();
        $fields = [];
        foreach ($data as $key => $val) {
            $fields[(string) $key] = $this->toValue($val);
        }
        $struct->setFields($fields);
        return $struct;
    }

    /**
     * Convert a PHP scalar/array/null to google.protobuf.Value.
     *
     * @param mixed $val
     */
    private function toValue($val): Value
    {
        $v = new Value();
        if ($val === null) {
            $v->setNullValue(NullValue::NULL_VALUE);
        } elseif (is_bool($val)) {
            $v->setBoolValue($val);
        } elseif (is_int($val) || is_float($val)) {
            $v->setNumberValue((float) $val);
        } elseif (is_string($val)) {
            $v->setStringValue($val);
        } elseif (is_array($val)) {
            if (array_is_list($val)) {
                $lv = new ListValue();
                $lv->setValues(array_map([$this, 'toValue'], $val));
                $v->setListValue($lv);
            } else {
                $v->setStructValue($this->arrayToStruct($val));
            }
        } else {
            $v->setStringValue((string) $val);
        }
        return $v;
    }

    /**
     * Convert a google.protobuf.Struct to a plain PHP array.
     *
     * @return array<string, mixed>
     */
    private function structToArray(Struct $struct): array
    {
        $out = [];
        foreach ($struct->getFields() as $key => $value) {
            $out[$key] = $this->fromValue($value);
        }
        return $out;
    }

    /**
     * Convert a google.protobuf.Value to a PHP scalar/array/null.
     *
     * @return mixed
     */
    private function fromValue(Value $v)
    {
        switch ($v->getKind()) {
            case 'null_value':   return null;
            case 'bool_value':   return $v->getBoolValue();
            case 'number_value': return $v->getNumberValue();
            case 'string_value': return $v->getStringValue();
            case 'struct_value': return $this->structToArray($v->getStructValue());
            case 'list_value':
                return array_map([$this, 'fromValue'], iterator_to_array($v->getListValue()->getValues()));
            default: return null;
        }
    }

    /**
     * Convert a proto Record to a plain PHP array.
     *
     * @return array<string, mixed>
     */
    private function recordToArray(\Filedb\V1\Record $record): array
    {
        $out = [
            'id'   => (string) $record->getId(),
            'data' => $record->hasData() ? $this->structToArray($record->getData()) : [],
        ];
        if ($record->hasDateAdded()) {
            $out['date_added'] = $record->getDateAdded()->toDateTime()->format(\DateTimeInterface::ATOM);
        }
        if ($record->hasDateModified()) {
            $out['date_modified'] = $record->getDateModified()->toDateTime()->format(\DateTimeInterface::ATOM);
        }
        return $out;
    }

    /**
     * Throw a RuntimeException if the gRPC status is not OK.
     */
    private function checkStatus(\stdClass $status): void
    {
        if ($status->code !== \Grpc\STATUS_OK) {
            throw new \RuntimeException(
                sprintf('gRPC error %d: %s', $status->code, $status->details)
            );
        }
    }
}
