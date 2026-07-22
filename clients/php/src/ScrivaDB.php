<?php

declare(strict_types=1);

namespace ScrivaDB;

use Scriva\V1\AggregateOp;
use Scriva\V1\AggregateRequest;
use Scriva\V1\AndFilter;
use Scriva\V1\BeginTxRequest;
use Scriva\V1\CollectionStatsRequest;
use Scriva\V1\CommitTxRequest;
use Scriva\V1\CompactRequest;
use Scriva\V1\CreateCollectionRequest;
use Scriva\V1\DeleteByKeyRequest;
use Scriva\V1\DeleteRequest;
use Scriva\V1\DropCollectionRequest;
use Scriva\V1\DropIndexRequest;
use Scriva\V1\EnsureIndexRequest;
use Scriva\V1\FieldFilter;
use Scriva\V1\ScrivaClient as GrpcStub;
use Scriva\V1\Filter;
use Scriva\V1\FilterOp;
use Scriva\V1\FindByIdRequest;
use Scriva\V1\FindByKeyRequest;
use Scriva\V1\FindRequest;
use Scriva\V1\InsertManyRequest;
use Scriva\V1\InsertRequest;
use Scriva\V1\ListCollectionsRequest;
use Scriva\V1\ListIndexesRequest;
use Scriva\V1\OrderBy;
use Scriva\V1\OrFilter;
use Scriva\V1\RollbackTxRequest;
use Scriva\V1\SnapshotRequest;
use Scriva\V1\UpdateByKeyRequest;
use Scriva\V1\UpdateIfRevRequest;
use Scriva\V1\UpdateRequest;
use Scriva\V1\UpsertRequest;
use Scriva\V1\WatchRequest;
use Google\Protobuf\Struct;
use Google\Protobuf\Value;
use Google\Protobuf\ListValue;
use Google\Protobuf\NullValue;

/**
 * PHP gRPC client for ScrivaDB.
 *
 * A thin, idiomatic wrapper over the gRPC API defined in proto/scriva.proto.
 * Every RPC is exposed as a camelCase method; records are returned as plain
 * PHP arrays and filters are plain associative arrays (see find()).
 *
 * Example:
 *
 *   $db = new ScrivaDB('localhost', 5433, 'dev-key');
 *   $db->createCollection('users');
 *   $id = $db->insert('users', ['name' => 'Alice', 'age' => 30]);
 *   $record = $db->findById('users', $id);
 *   $admins = $db->find('users', ['field' => 'role', 'op' => 'eq', 'value' => 'admin']);
 *   $db->update('users', $id, ['name' => 'Alice', 'age' => 31]);
 *   $db->delete('users', $id);
 *   $db->dropCollection('users');
 */
class ScrivaDB
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

    private static array $AGG_OP_MAP = [
        'count' => AggregateOp::AGG_COUNT,
        'sum'   => AggregateOp::AGG_SUM,
        'avg'   => AggregateOp::AGG_AVG,
        'min'   => AggregateOp::AGG_MIN,
        'max'   => AggregateOp::AGG_MAX,
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
     *
     * @param int $defaultTtlSeconds Default per-record TTL for the collection, in
     *     seconds. When > 0, records inserted without an explicit ttl_seconds
     *     expire this many seconds after they are written. 0 (the default) means
     *     records never expire unless the server was started with --default-ttl.
     */
    public function createCollection(string $name, int $defaultTtlSeconds = 0): string
    {
        $req = new CreateCollectionRequest();
        $req->setName($name);
        $req->setDefaultTtlSeconds($defaultTtlSeconds);
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
     * @param int $ttlSeconds Per-record TTL in seconds. When > 0, overrides the
     *     collection default and expires the record this many seconds from now.
     *     0 (the default) inherits the collection's default TTL.
     * @param string $key Optional caller-supplied string primary key (keyed
     *     Create, N1). When non-empty the record is inserted under this key; a key
     *     already held by a live record throws AlreadyExistsException. A keyed
     *     insert ignores $ttlSeconds and does not participate in transactions.
     *     Empty (the default) preserves the server-assigned-id behaviour.
     */
    public function insert(string $collection, array $data, int $ttlSeconds = 0, string $key = ''): int
    {
        $req = new InsertRequest();
        $req->setCollection($collection);
        $req->setData($this->arrayToStruct($data));
        $req->setTtlSeconds($ttlSeconds);
        $req->setKey($key);
        [$resp, $status] = $this->stub->Insert($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return (int) $resp->getId();
    }

    /**
     * Insert multiple records. Returns the assigned IDs in insertion order.
     *
     * @param array<array<string, mixed>> $records
     * @param int $ttlSeconds Per-record TTL applied to every record in the batch.
     *     Same semantics as insert(): > 0 overrides the collection default,
     *     0 (the default) inherits it.
     * @return int[]
     */
    public function insertMany(string $collection, array $records, int $ttlSeconds = 0): array
    {
        $req = new InsertManyRequest();
        $req->setCollection($collection);
        $structs = array_map([$this, 'arrayToStruct'], $records);
        $req->setRecords($structs);
        $req->setTtlSeconds($ttlSeconds);
        [$resp, $status] = $this->stub->InsertMany($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return array_map('intval', iterator_to_array($resp->getIds()));
    }

    /**
     * Fetch a single record by ID.
     *
     * @param string[] $fields Optional field projection (N2): when non-empty, only
     *     these top-level fields are returned in the record's data. id, key and rev
     *     are always included; an unknown field is silently omitted. Empty (the
     *     default) returns the full record.
     * @return array<string, mixed>
     */
    public function findById(string $collection, int $id, array $fields = []): array
    {
        $req = new FindByIdRequest();
        $req->setCollection($collection);
        $req->setId($id);
        if ($fields !== []) {
            $req->setFields($fields);
        }
        [$resp, $status] = $this->stub->FindById($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $this->recordToArray($resp->getRecord());
    }

    /**
     * Query records. The Find RPC is server-streaming; results are collected
     * into an array.
     *
     * Supports field projection (N2), keyset pagination (N3) and multi-field
     * ordering (N3). To page through results with a stable keyset cursor, use
     * findPage() instead, which also returns the next-page token.
     *
     * @param array<string, mixed>|null $filter  See filterToProto() for the filter format
     * @param int    $limit       0 = no limit
     * @param int    $offset      Rows to skip (ignored when $pageToken is set for keyset paging)
     * @param string $orderBy     Deprecated single-field sort; honoured only when
     *                            $orderByFields is empty. Prefer $orderByFields.
     * @param bool   $descending  Direction for the deprecated $orderBy
     * @param string[] $fields    Field projection (N2): only these top-level fields
     *                            are returned in each record's data (id/key/rev
     *                            always included). Empty = full records.
     * @param array<array{field: string, desc?: bool}>|null $orderByFields
     *                            Multi-field, per-field-directional sort (N3). When
     *                            non-empty it supersedes $orderBy/$descending. Each
     *                            entry is ['field' => 'name', 'desc' => true|false].
     * @param string $pageToken   Opaque keyset cursor (N3) from a previous findPage()
     *                            call; empty requests the first page.
     * @return array<array<string, mixed>>
     */
    public function find(
        string $collection,
        ?array $filter = null,
        int $limit = 0,
        int $offset = 0,
        string $orderBy = '',
        bool $descending = false,
        array $fields = [],
        ?array $orderByFields = null,
        string $pageToken = ''
    ): array {
        return $this->findPage(
            $collection,
            $filter,
            $limit,
            $offset,
            $orderBy,
            $descending,
            $fields,
            $orderByFields,
            $pageToken
        )['records'];
    }

    /**
     * Query records and also return the next-page keyset cursor (N3).
     *
     * Identical arguments to find(), but returns:
     *
     *   ['records' => [...record arrays...], 'page_token' => '<cursor>']
     *
     * When 'page_token' is a non-empty string more rows remain under the requested
     * ordering — pass it back as $pageToken (with the same filter, ordering and
     * limit) to fetch the next page. An empty 'page_token' means the last page was
     * reached. Keyset paging is only meaningful with an ordering; combine it with
     * offset = 0.
     *
     * @param array<string, mixed>|null $filter
     * @param string[] $fields
     * @param array<array{field: string, desc?: bool}>|null $orderByFields
     * @return array{records: array<array<string, mixed>>, page_token: string}
     */
    public function findPage(
        string $collection,
        ?array $filter = null,
        int $limit = 0,
        int $offset = 0,
        string $orderBy = '',
        bool $descending = false,
        array $fields = [],
        ?array $orderByFields = null,
        string $pageToken = ''
    ): array {
        $req = new FindRequest();
        $req->setCollection($collection);
        $req->setLimit($limit);
        $req->setOffset($offset);
        $req->setOrderBy($orderBy);
        $req->setDescending($descending);
        $req->setPageToken($pageToken);
        if ($fields !== []) {
            $req->setFields($fields);
        }
        if ($orderByFields !== null && $orderByFields !== []) {
            $req->setOrderByFields(array_map([$this, 'orderByToProto'], $orderByFields));
        }
        if ($filter !== null) {
            $req->setFilter($this->filterToProto($filter));
        }

        $call = $this->stub->Find($req, $this->metadata);
        $results = [];
        $nextToken = '';
        foreach ($call->responses() as $resp) {
            // The cursor rides on the final streamed message, which may carry no
            // record of its own — only collect a record when one is present.
            if ($resp->hasRecord()) {
                $results[] = $this->recordToArray($resp->getRecord());
            }
            if ($resp->getPageToken() !== '') {
                $nextToken = $resp->getPageToken();
            }
        }
        $this->checkStatus($call->getStatus());
        return ['records' => $results, 'page_token' => $nextToken];
    }

    /**
     * Update a record by ID. Returns the updated ID.
     *
     * @param array<string, mixed> $data
     * @param int $ttlSeconds Per-record TTL in seconds. When > 0, resets the
     *     record's expiry to this many seconds from now. 0 (the default) is
     *     sticky — it leaves any existing expiry deadline in place.
     */
    public function update(string $collection, int $id, array $data, int $ttlSeconds = 0): int
    {
        $req = new UpdateRequest();
        $req->setCollection($collection);
        $req->setId($id);
        $req->setData($this->arrayToStruct($data));
        $req->setTtlSeconds($ttlSeconds);
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
    // Keyed CRUD, Upsert & compare-and-swap (N1)
    // -------------------------------------------------------------------------

    /**
     * Insert-or-replace a record under a caller-supplied string key, atomically.
     *
     * If no live record carries $key the data is inserted; if one does, its data
     * is replaced and its revision incremented. Returns the resulting record array
     * (including its 'key' and 'rev').
     *
     * @param array<string, mixed> $data
     * @return array<string, mixed>
     */
    public function upsert(string $collection, string $key, array $data): array
    {
        $req = new UpsertRequest();
        $req->setCollection($collection);
        $req->setKey($key);
        $req->setData($this->arrayToStruct($data));
        [$resp, $status] = $this->stub->Upsert($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $this->recordToArray($resp->getRecord());
    }

    /**
     * Fetch the record carrying a caller-supplied string key.
     *
     * @param string[] $fields Optional field projection (N2); see findById().
     * @return array<string, mixed>
     * @throws NotFoundException if no live record carries $key.
     */
    public function findByKey(string $collection, string $key, array $fields = []): array
    {
        $req = new FindByKeyRequest();
        $req->setCollection($collection);
        $req->setKey($key);
        if ($fields !== []) {
            $req->setFields($fields);
        }
        [$resp, $status] = $this->stub->FindByKey($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $this->recordToArray($resp->getRecord());
    }

    /**
     * Overwrite the record carrying $key, preserving the key itself. Returns a
     * result array: ['id' => ..., 'key' => ..., 'rev' => ..., 'date_modified' => ...].
     *
     * @param array<string, mixed> $data
     * @return array<string, mixed>
     * @throws NotFoundException if no live record carries $key.
     */
    public function updateByKey(string $collection, string $key, array $data): array
    {
        $req = new UpdateByKeyRequest();
        $req->setCollection($collection);
        $req->setKey($key);
        $req->setData($this->arrayToStruct($data));
        [$resp, $status] = $this->stub->UpdateByKey($req, $this->metadata)->wait();
        $this->checkStatus($status);
        $out = [
            'id'  => (string) $resp->getId(),
            'key' => $resp->getKey(),
            'rev' => (int) $resp->getRev(),
        ];
        if ($resp->getDateModified() !== '') {
            $out['date_modified'] = $resp->getDateModified();
        }
        return $out;
    }

    /**
     * Delete the record carrying $key. Returns true on success.
     *
     * @throws NotFoundException if no live record carries $key.
     */
    public function deleteByKey(string $collection, string $key): bool
    {
        $req = new DeleteByKeyRequest();
        $req->setCollection($collection);
        $req->setKey($key);
        [$resp, $status] = $this->stub->DeleteByKey($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    /**
     * Optimistic-concurrency update (compare-and-swap on the record's revision).
     *
     * The write is applied only if the record carrying $key currently has revision
     * $expectedRev. A stale revision, or a missing key, is a clean no-op — not an
     * error — reported as ['swapped' => false, 'record' => null].
     *
     * Returns ['swapped' => bool, 'record' => array|null]; when swapped is true the
     * record array carries the new (incremented) revision.
     *
     * @param array<string, mixed> $data
     * @return array{swapped: bool, record: array<string, mixed>|null}
     */
    public function updateIfRev(string $collection, string $key, int $expectedRev, array $data): array
    {
        $req = new UpdateIfRevRequest();
        $req->setCollection($collection);
        $req->setKey($key);
        $req->setExpectedRev($expectedRev);
        $req->setData($this->arrayToStruct($data));
        [$resp, $status] = $this->stub->UpdateIfRev($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return [
            'swapped' => $resp->getSwapped(),
            'record'  => $resp->hasRecord() ? $this->recordToArray($resp->getRecord()) : null,
        ];
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
        $req = new \Scriva\V1\RollbackTxRequest();
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
    // Aggregations (N4)
    // -------------------------------------------------------------------------

    /**
     * Compute count and numeric aggregations (sum/avg/min/max) over the live
     * records matching $filter, optionally grouped by a field. The Aggregate RPC
     * is server-streaming (one message per group); results are collected into an
     * array of group result arrays, each shaped like:
     *
     *   [
     *     'group_value' => 'admin',  // the group_by value (null for the whole set)
     *     'count'       => 3,
     *     'sum'         => 90.0,     // only meaningful when 'numeric' is true
     *     'avg'         => 30.0,
     *     'min'         => 25.0,
     *     'max'         => 35.0,
     *     'numeric'     => true,
     *   ]
     *
     * For an ungrouped request the array holds exactly one element with
     * 'group_value' => null.
     *
     * @param array<string, mixed>|null $filter  Same filter format as find()
     * @param string   $groupBy       Group-by field; empty aggregates the whole set
     * @param string   $field         Numeric field for sum/avg/min/max (required for those)
     * @param string[] $aggregations  Any of 'sum','avg','min','max' (count is always
     *                                returned); empty yields count-only
     * @return array<array<string, mixed>>
     */
    public function aggregate(
        string $collection,
        ?array $filter = null,
        string $groupBy = '',
        string $field = '',
        array $aggregations = []
    ): array {
        $req = new AggregateRequest();
        $req->setCollection($collection);
        $req->setGroupBy($groupBy);
        $req->setField($field);
        if ($filter !== null) {
            $req->setFilter($this->filterToProto($filter));
        }
        if ($aggregations !== []) {
            $ops = [];
            foreach ($aggregations as $name) {
                $key = strtolower((string) $name);
                if (!isset(self::$AGG_OP_MAP[$key])) {
                    throw new \InvalidArgumentException(
                        "Unknown aggregation '$name'; expected one of: "
                        . implode(', ', array_keys(self::$AGG_OP_MAP))
                    );
                }
                $ops[] = self::$AGG_OP_MAP[$key];
            }
            $req->setAggregations($ops);
        }

        $call = $this->stub->Aggregate($req, $this->metadata);
        $groups = [];
        foreach ($call->responses() as $resp) {
            $group = [
                'group_value' => $resp->hasGroupValue() ? $this->fromValue($resp->getGroupValue()) : null,
                'count'       => (int) $resp->getCount(),
                'numeric'     => $resp->getNumeric(),
            ];
            if ($resp->getNumeric()) {
                $group['sum'] = $resp->getSum();
                $group['avg'] = $resp->getAvg();
                $group['min'] = $resp->getMin();
                $group['max'] = $resp->getMax();
            }
            $groups[] = $group;
        }
        $this->checkStatus($call->getStatus());
        return $groups;
    }

    /**
     * Convenience count: the number of live records matching $filter. Empty
     * $filter counts the whole collection.
     *
     * @param array<string, mixed>|null $filter
     */
    public function count(string $collection, ?array $filter = null): int
    {
        $groups = $this->aggregate($collection, $filter);
        return $groups === [] ? 0 : (int) $groups[0]['count'];
    }

    /**
     * Convenience group-by: aggregate over $field, bucketed by distinct values of
     * $groupBy. Returns one result array per group (see aggregate()).
     *
     * @param string[] $aggregations Any of 'sum','avg','min','max' (count always returned)
     * @param array<string, mixed>|null $filter
     * @return array<array<string, mixed>>
     */
    public function groupBy(
        string $collection,
        string $groupBy,
        string $field = '',
        array $aggregations = [],
        ?array $filter = null
    ): array {
        return $this->aggregate($collection, $filter, $groupBy, $field, $aggregations);
    }

    // -------------------------------------------------------------------------
    // Maintenance
    // -------------------------------------------------------------------------

    /**
     * Force a synchronous compaction of a collection, merging dirty segments and
     * reclaiming space from deleted/overwritten records. Returns true on success.
     */
    public function compact(string $collection): bool
    {
        $req = new CompactRequest();
        $req->setCollection($collection);
        [$resp, $status] = $this->stub->Compact($req, $this->metadata)->wait();
        $this->checkStatus($status);
        return $resp->getOk();
    }

    /**
     * Stream a consistent snapshot (a gzipped tar archive) of the entire
     * database. The Snapshot RPC is server-streaming; this yields the raw byte
     * chunks in order.
     *
     * Concatenating every yielded chunk reproduces the backup archive exactly.
     *
     * @return \Generator<int, string>
     */
    public function snapshot(): \Generator
    {
        $req = new SnapshotRequest();
        $call = $this->stub->Snapshot($req, $this->metadata);
        foreach ($call->responses() as $chunk) {
            yield $chunk->getData();
        }
        $this->checkStatus($call->getStatus());
    }

    /**
     * Stream a snapshot straight to a file on disk. Returns the number of bytes
     * written.
     */
    public function snapshotToFile(string $path): int
    {
        $fh = fopen($path, 'wb');
        if ($fh === false) {
            throw new \RuntimeException("Cannot open snapshot file for writing: $path");
        }
        try {
            $total = 0;
            foreach ($this->snapshot() as $chunk) {
                $written = fwrite($fh, $chunk);
                if ($written === false) {
                    throw new \RuntimeException("Failed writing snapshot to: $path");
                }
                $total += $written;
            }
            return $total;
        } finally {
            fclose($fh);
        }
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

    /**
     * Convert a plain ['field' => 'name', 'desc' => bool] array to a proto OrderBy.
     *
     * @param array{field: string, desc?: bool} $spec
     */
    private function orderByToProto(array $spec): OrderBy
    {
        $ob = new OrderBy();
        $ob->setField((string) ($spec['field'] ?? ''));
        $ob->setDesc((bool) ($spec['desc'] ?? false));
        return $ob;
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
    private function recordToArray(\Scriva\V1\Record $record): array
    {
        $out = [
            'id'   => (string) $record->getId(),
            'data' => $record->hasData() ? $this->structToArray($record->getData()) : [],
        ];
        // Keyed CRUD surface (N1): the caller-supplied string key (present only
        // for keyed records) and the monotonic per-record revision.
        $key = $record->getKey();
        if ($key !== '') {
            $out['key'] = $key;
        }
        $out['rev'] = (int) $record->getRev();
        if ($record->hasDateAdded()) {
            $out['date_added'] = $record->getDateAdded()->toDateTime()->format(\DateTimeInterface::ATOM);
        }
        if ($record->hasDateModified()) {
            $out['date_modified'] = $record->getDateModified()->toDateTime()->format(\DateTimeInterface::ATOM);
        }
        return $out;
    }

    /**
     * Throw on a non-OK gRPC status, mapping the common keyed-CRUD codes to
     * idiomatic exception subclasses: NOT_FOUND -> NotFoundException,
     * ALREADY_EXISTS -> AlreadyExistsException, anything else -> ScrivaDBException.
     * All three extend \RuntimeException.
     */
    private function checkStatus(\stdClass $status): void
    {
        if ($status->code === \Grpc\STATUS_OK) {
            return;
        }
        $message = sprintf('gRPC error %d: %s', $status->code, $status->details);
        switch ($status->code) {
            case \Grpc\STATUS_NOT_FOUND:
                throw new NotFoundException($message, $status->code);
            case \Grpc\STATUS_ALREADY_EXISTS:
                throw new AlreadyExistsException($message, $status->code);
            default:
                throw new ScrivaDBException($message, $status->code);
        }
    }
}
