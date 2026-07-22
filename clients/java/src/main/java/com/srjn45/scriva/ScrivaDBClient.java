package com.srjn45.scriva;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import scriva.v1.ScrivaGrpc;
import scriva.v1.ScrivaOuterClass.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.ByteString;

import javax.net.ssl.SSLException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Java client for ScrivaDB.
 *
 * <p>A thin, ergonomic wrapper over the gRPC API defined in {@code proto/scriva.proto}.
 * Records are returned as {@link Record} value objects; filters and order-by
 * specs are plain Java structures.
 *
 * <p>Usage:
 * <pre>
 *   ScrivaDBClient db = new ScrivaDBClient("localhost", 5433, "dev-key");
 *   db.createCollection("users");
 *   long id = db.insert("users", Map.of("name", "Alice", "age", 30));
 *   ScrivaDBClient.Record record = db.findById("users", id);
 *   String name = (String) record.get("name");
 *   db.close();
 * </pre>
 */
public class ScrivaDBClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ScrivaGrpc.ScrivaBlockingStub blockingStub;
    private final ScrivaGrpc.ScrivaStub asyncStub;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Connect without TLS (plaintext). */
    public ScrivaDBClient(String host, int port, String apiKey) {
        this(buildPlaintextChannel(host, port), apiKey);
    }

    /** Connect with TLS, verifying the server against the supplied CA certificate. */
    public ScrivaDBClient(String host, int port, String apiKey, File tlsCaCert) throws SSLException {
        this(buildTlsChannel(host, port, tlsCaCert), apiKey);
    }

    private ScrivaDBClient(ManagedChannel channel, String apiKey) {
        this.channel = channel;
        ClientInterceptor authInterceptor = buildAuthInterceptor(apiKey);
        Channel intercepted = ClientInterceptors.intercept(channel, authInterceptor);
        this.blockingStub = ScrivaGrpc.newBlockingStub(intercepted);
        this.asyncStub    = ScrivaGrpc.newStub(intercepted);
    }

    // -----------------------------------------------------------------------
    // Channel factories
    // -----------------------------------------------------------------------

    private static ManagedChannel buildPlaintextChannel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    private static ManagedChannel buildTlsChannel(String host, int port, File caCert) throws SSLException {
        return NettyChannelBuilder.forAddress(host, port)
                .sslContext(GrpcSslContexts.forClient().trustManager(caCert).build())
                .build();
    }

    // -----------------------------------------------------------------------
    // Auth interceptor
    // -----------------------------------------------------------------------

    private static final Metadata.Key<String> API_KEY_HEADER =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    private static ClientInterceptor buildAuthInterceptor(String apiKey) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(API_KEY_HEADER, apiKey);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    // -----------------------------------------------------------------------
    // Error translation — map engine gRPC status codes to typed exceptions
    // -----------------------------------------------------------------------

    /**
     * Translate a gRPC {@link StatusRuntimeException} into a typed ScrivaDB
     * exception. {@code NOT_FOUND} becomes {@link NotFoundException} and
     * {@code ALREADY_EXISTS} becomes {@link AlreadyExistsException}; every other
     * status code is returned unchanged so it propagates as-is.
     */
    private static RuntimeException translate(StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        String msg = e.getStatus().getDescription();
        if (msg == null) {
            msg = e.getMessage();
        }
        switch (code) {
            case NOT_FOUND:      return new NotFoundException(msg, e);
            case ALREADY_EXISTS: return new AlreadyExistsException(msg, e);
            default:             return e;
        }
    }

    // -----------------------------------------------------------------------
    // Collection management
    // -----------------------------------------------------------------------

    public String createCollection(String name) {
        return createCollection(name, 0L);
    }

    /**
     * Create a collection with a default per-record TTL, in seconds.
     *
     * <p>Records inserted without an explicit {@code ttlSeconds} then expire this
     * many seconds after being written. Pass {@code 0} to inherit the server-wide
     * default (equivalent to {@link #createCollection(String)}).
     */
    public String createCollection(String name, long defaultTtlSeconds) {
        CreateCollectionResponse resp = blockingStub.createCollection(
                CreateCollectionRequest.newBuilder()
                        .setName(name)
                        .setDefaultTtlSeconds(defaultTtlSeconds)
                        .build());
        return resp.getName();
    }

    public boolean dropCollection(String name) {
        DropCollectionResponse resp = blockingStub.dropCollection(
                DropCollectionRequest.newBuilder().setName(name).build());
        return resp.getOk();
    }

    public List<String> listCollections() {
        ListCollectionsResponse resp = blockingStub.listCollections(
                ListCollectionsRequest.newBuilder().build());
        return resp.getNamesList();
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    public long insert(String collection, Map<String, Object> data) {
        return insert(collection, data, 0L, "");
    }

    /**
     * Insert one record with an explicit per-record TTL, in seconds.
     *
     * <p>{@code ttlSeconds > 0} expires the record that long after insertion,
     * overriding any collection default; {@code 0} applies the collection default.
     */
    public long insert(String collection, Map<String, Object> data, long ttlSeconds) {
        return insert(collection, data, ttlSeconds, "");
    }

    /**
     * Insert one record, optionally under a caller-supplied string primary key.
     *
     * <p>When {@code key} is non-empty the record is inserted under that key
     * (keyed create); a key already held by a live record raises
     * {@link AlreadyExistsException}. A keyed insert does not participate in
     * transactions or per-record TTL. {@code ttlSeconds > 0} expires the record
     * that long after insertion (keyless inserts only). Returns the assigned ID.
     */
    public long insert(String collection, Map<String, Object> data, long ttlSeconds, String key) {
        try {
            InsertResponse resp = blockingStub.insert(InsertRequest.newBuilder()
                    .setCollection(collection)
                    .setData(mapToStruct(data))
                    .setTtlSeconds(ttlSeconds)
                    .setKey(key != null ? key : "")
                    .build());
            return resp.getId();
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /** Convenience: keyed insert under {@code key}. Raises {@link AlreadyExistsException} if taken. */
    public long insertKeyed(String collection, String key, Map<String, Object> data) {
        return insert(collection, data, 0L, key);
    }

    public List<Long> insertMany(String collection, List<Map<String, Object>> records) {
        return insertMany(collection, records, 0L);
    }

    /** Insert many records, applying the same per-record TTL (seconds) to each. */
    public List<Long> insertMany(String collection, List<Map<String, Object>> records, long ttlSeconds) {
        InsertManyRequest.Builder req = InsertManyRequest.newBuilder()
                .setCollection(collection)
                .setTtlSeconds(ttlSeconds);
        for (Map<String, Object> r : records) {
            req.addRecords(mapToStruct(r));
        }
        InsertManyResponse resp = blockingStub.insertMany(req.build());
        return resp.getIdsList();
    }

    /** Fetch a single record by ID. */
    public Record findById(String collection, long id) {
        return findById(collection, id, null);
    }

    /**
     * Fetch a single record by ID, optionally projecting its data (N2).
     *
     * <p>When {@code fields} is non-empty only those top-level fields are returned
     * in the record's {@code data} ({@code id}, {@code key} and {@code rev} are
     * always included). Pass {@code null}/empty for the full record.
     */
    public Record findById(String collection, long id, List<String> fields) {
        FindByIdRequest.Builder req = FindByIdRequest.newBuilder()
                .setCollection(collection)
                .setId(id);
        if (fields != null) {
            req.addAllFields(fields);
        }
        try {
            FindResponse resp = blockingStub.findById(req.build());
            return recordFromProto(resp.getRecord());
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /** Convenience overload — no filter, no ordering, no pagination. */
    public List<Record> find(String collection) {
        return find(collection, null, 0, 0, "", false);
    }

    /** Convenience overload — filter only. */
    public List<Record> find(String collection, Map<String, Object> filter) {
        return find(collection, filter, 0, 0, "", false);
    }

    /**
     * Find records, sorting on a single field (the deprecated scalar sort).
     *
     * <p>Superseded by {@link #find(String, Map, int, int, List, List, String)}
     * which supports multi-field ordering, projection and keyset pagination.
     *
     * @param collection collection name
     * @param filter     plain map filter (see {@link #filterToProto(Map)}), or null
     * @param limit      max results (0 = no limit)
     * @param offset     skip first N results
     * @param orderBy    field name to sort by, or empty string
     * @param descending sort descending when true
     */
    public List<Record> find(String collection, Map<String, Object> filter,
                             int limit, int offset, String orderBy, boolean descending) {
        FindRequest req = buildFindRequest(collection, filter, limit, offset,
                null, orderBy, descending, null, "");
        return drain(req).records();
    }

    /**
     * Find records with multi-field ordering (N3), field projection (N2) and an
     * optional keyset page token (N3).
     *
     * @param collection collection name
     * @param filter     plain map filter, or null
     * @param limit      max results per page (0 = no limit)
     * @param offset     skip first N results (use 0 with {@code pageToken})
     * @param orderBy    ordered list of {@link Order} sort keys, or null/empty
     * @param fields     top-level data fields to project, or null/empty for all
     * @param pageToken  keyset cursor from a prior {@link #findPage}, or ""
     */
    public List<Record> find(String collection, Map<String, Object> filter,
                             int limit, int offset, List<Order> orderBy,
                             List<String> fields, String pageToken) {
        return findPage(collection, filter, limit, offset, orderBy, fields, pageToken).records();
    }

    /**
     * Fetch one keyset page, returning the records plus a next-page cursor (N3).
     *
     * <p>Pass an ordering and a {@code limit}, then feed the returned
     * {@link Page#nextPageToken()} back as {@code pageToken} on the next call to
     * walk the collection page by page in O(page) time. An empty next-page token
     * means the last page was reached. Keep the same filter, ordering and limit on
     * every page.
     */
    public Page findPage(String collection, Map<String, Object> filter,
                         int limit, int offset, List<Order> orderBy,
                         List<String> fields, String pageToken) {
        FindRequest req = buildFindRequest(collection, filter, limit, offset,
                orderBy, null, false, fields, pageToken);
        return drain(req);
    }

    @SuppressWarnings("deprecation")
    private FindRequest buildFindRequest(String collection, Map<String, Object> filter,
                                         int limit, int offset, List<Order> orderBy,
                                         String legacyOrderBy, boolean descending,
                                         List<String> fields, String pageToken) {
        FindRequest.Builder req = FindRequest.newBuilder()
                .setCollection(collection)
                .setLimit(limit)
                .setOffset(offset)
                .setPageToken(pageToken != null ? pageToken : "");
        if (filter != null) {
            req.setFilter(filterToProto(filter));
        }
        if (fields != null) {
            req.addAllFields(fields);
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            for (Order o : orderBy) {
                req.addOrderByFields(OrderBy.newBuilder()
                        .setField(o.field())
                        .setDesc(o.desc())
                        .build());
            }
        } else if (legacyOrderBy != null && !legacyOrderBy.isEmpty()) {
            // Deprecated single-field path — honoured only when order_by_fields is empty.
            req.setOrderBy(legacyOrderBy);
            req.setDescending(descending);
        }
        return req.build();
    }

    private Page drain(FindRequest req) {
        List<Record> records = new ArrayList<>();
        String nextToken = "";
        try {
            Iterator<FindResponse> iter = blockingStub.find(req);
            while (iter.hasNext()) {
                FindResponse resp = iter.next();
                records.add(recordFromProto(resp.getRecord()));
                if (!resp.getPageToken().isEmpty()) {
                    nextToken = resp.getPageToken();
                }
            }
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
        return new Page(records, nextToken);
    }

    public long update(String collection, long id, Map<String, Object> data) {
        return update(collection, id, data, 0L);
    }

    /**
     * Update a record, resetting its TTL to {@code ttlSeconds} from now.
     *
     * <p>{@code ttlSeconds > 0} overrides the collection default and resets the
     * expiry deadline; {@code 0} (the default) is sticky — it leaves any existing
     * deadline untouched.
     */
    public long update(String collection, long id, Map<String, Object> data, long ttlSeconds) {
        UpdateResponse resp = blockingStub.update(UpdateRequest.newBuilder()
                .setCollection(collection)
                .setId(id)
                .setData(mapToStruct(data))
                .setTtlSeconds(ttlSeconds)
                .build());
        return resp.getId();
    }

    public boolean delete(String collection, long id) {
        DeleteResponse resp = blockingStub.delete(DeleteRequest.newBuilder()
                .setCollection(collection)
                .setId(id)
                .build());
        return resp.getOk();
    }

    // -----------------------------------------------------------------------
    // Keyed CRUD, upsert & compare-and-swap (N1)
    // -----------------------------------------------------------------------

    /**
     * Insert {@code data} under {@code key}, or replace the existing keyed record.
     *
     * <p>Atomic: if no live record carries {@code key} it is inserted; otherwise
     * the existing record's data is replaced and its {@code rev} incremented.
     * Returns the resulting record (including its key and rev).
     */
    public Record upsert(String collection, String key, Map<String, Object> data) {
        try {
            UpsertResponse resp = blockingStub.upsert(UpsertRequest.newBuilder()
                    .setCollection(collection)
                    .setKey(key)
                    .setData(mapToStruct(data))
                    .build());
            return recordFromProto(resp.getRecord());
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /** Fetch the record carrying {@code key}. Raises {@link NotFoundException} if none. */
    public Record findByKey(String collection, String key) {
        return findByKey(collection, key, null);
    }

    /**
     * Fetch the record carrying {@code key}, optionally projecting its data (N2).
     *
     * <p>Raises {@link NotFoundException} if no live record carries {@code key}.
     * When {@code fields} is non-empty only those top-level fields are returned in
     * {@code data} ({@code id}/{@code key}/{@code rev} always included).
     */
    public Record findByKey(String collection, String key, List<String> fields) {
        FindByKeyRequest.Builder req = FindByKeyRequest.newBuilder()
                .setCollection(collection)
                .setKey(key);
        if (fields != null) {
            req.addAllFields(fields);
        }
        try {
            FindResponse resp = blockingStub.findByKey(req.build());
            return recordFromProto(resp.getRecord());
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /**
     * Overwrite the record carrying {@code key}, preserving the key itself.
     *
     * <p>Raises {@link NotFoundException} if no live record carries {@code key}.
     * Returns the write's outcome — id, key, rev (after the write) and the
     * modified timestamp.
     */
    public UpdateResult updateByKey(String collection, String key, Map<String, Object> data) {
        try {
            UpdateResponse resp = blockingStub.updateByKey(UpdateByKeyRequest.newBuilder()
                    .setCollection(collection)
                    .setKey(key)
                    .setData(mapToStruct(data))
                    .build());
            return new UpdateResult(resp.getId(), resp.getKey(), resp.getRev(), resp.getDateModified());
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /**
     * Delete the record carrying {@code key}. Returns {@code true} on success.
     *
     * <p>Raises {@link NotFoundException} if no live record carries {@code key}.
     */
    public boolean deleteByKey(String collection, String key) {
        try {
            DeleteResponse resp = blockingStub.deleteByKey(DeleteByKeyRequest.newBuilder()
                    .setCollection(collection)
                    .setKey(key)
                    .build());
            return resp.getOk();
        } catch (StatusRuntimeException e) {
            throw translate(e);
        }
    }

    /**
     * Compare-and-swap update on {@code key}, conditional on {@code expectedRev}.
     *
     * <p>The write is applied only if the record's current {@code rev} equals
     * {@code expectedRev}. A stale revision (or a missing key) is a clean no-op —
     * never an error. Returns a {@link CasResult}; its {@code record} is populated
     * only when {@link CasResult#swapped()} is {@code true}.
     */
    public CasResult updateIfRev(String collection, String key, long expectedRev, Map<String, Object> data) {
        UpdateIfRevResponse resp = blockingStub.updateIfRev(UpdateIfRevRequest.newBuilder()
                .setCollection(collection)
                .setKey(key)
                .setExpectedRev(expectedRev)
                .setData(mapToStruct(data))
                .build());
        Record record = (resp.getSwapped() && resp.hasRecord())
                ? recordFromProto(resp.getRecord())
                : null;
        return new CasResult(resp.getSwapped(), record);
    }

    // -----------------------------------------------------------------------
    // Secondary indexes
    // -----------------------------------------------------------------------

    public void ensureIndex(String collection, String field) {
        blockingStub.ensureIndex(EnsureIndexRequest.newBuilder()
                .setCollection(collection)
                .setField(field)
                .build());
    }

    public boolean dropIndex(String collection, String field) {
        DropIndexResponse resp = blockingStub.dropIndex(DropIndexRequest.newBuilder()
                .setCollection(collection)
                .setField(field)
                .build());
        return resp.getOk();
    }

    public List<String> listIndexes(String collection) {
        ListIndexesResponse resp = blockingStub.listIndexes(ListIndexesRequest.newBuilder()
                .setCollection(collection)
                .build());
        return resp.getFieldsList();
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    public String beginTx(String collection) {
        BeginTxResponse resp = blockingStub.beginTx(BeginTxRequest.newBuilder()
                .setCollection(collection)
                .build());
        return resp.getTxId();
    }

    public boolean commitTx(String txId) {
        CommitTxResponse resp = blockingStub.commitTx(CommitTxRequest.newBuilder()
                .setTxId(txId)
                .build());
        return resp.getOk();
    }

    public boolean rollbackTx(String txId) {
        RollbackTxResponse resp = blockingStub.rollbackTx(RollbackTxRequest.newBuilder()
                .setTxId(txId)
                .build());
        return resp.getOk();
    }

    // -----------------------------------------------------------------------
    // Watch (async streaming)
    // -----------------------------------------------------------------------

    /**
     * Subscribe to change events on a collection.
     *
     * <p>Events are delivered to {@code observer} on the gRPC executor thread.
     * Call {@link io.grpc.ClientCall#cancel} or close the client to stop the stream.
     *
     * @param collection collection to watch
     * @param filter     optional filter (only matching events are delivered), or null
     * @param observer   receives {@link WatchEvent} notifications
     */
    public void watch(String collection, Map<String, Object> filter, StreamObserver<WatchEvent> observer) {
        WatchRequest.Builder req = WatchRequest.newBuilder().setCollection(collection);
        if (filter != null) {
            req.setFilter(filterToProto(filter));
        }
        asyncStub.watch(req.build(), observer);
    }

    // -----------------------------------------------------------------------
    // Aggregations (N4)
    // -----------------------------------------------------------------------

    /**
     * Compute count + numeric aggregations over the filtered live records.
     *
     * <p>The {@code Aggregate} RPC is server-streaming — one message per group;
     * this collects them into a list. Each {@link AggResult} carries {@code group}
     * (the group-by value, {@code null} for the whole-set group), {@code count},
     * and — when the group held at least one numeric {@code field} value —
     * sum/avg/min/max with {@link AggResult#numeric()} {@code true}.
     *
     * @param collection   collection name
     * @param aggregations which numeric aggregations to compute: any of
     *                     {@code count sum avg min max}. {@code count} is always
     *                     returned; the rest require {@code field}. Null = count only.
     * @param field        numeric field for {@code sum/avg/min/max}, or ""
     * @param groupBy      optional group-by field — one result per distinct value, or ""
     * @param filter       the same plain-map filter as {@link #find}, or null
     */
    public List<AggResult> aggregate(String collection, List<String> aggregations,
                                     String field, String groupBy, Map<String, Object> filter) {
        AggregateRequest.Builder req = AggregateRequest.newBuilder()
                .setCollection(collection)
                .setField(field != null ? field : "")
                .setGroupBy(groupBy != null ? groupBy : "");
        if (filter != null) {
            req.setFilter(filterToProto(filter));
        }
        if (aggregations != null) {
            for (String a : aggregations) {
                req.addAggregations(parseAgg(a));
            }
        }
        List<AggResult> out = new ArrayList<>();
        Iterator<AggregateResponse> iter = blockingStub.aggregate(req.build());
        while (iter.hasNext()) {
            AggregateResponse r = iter.next();
            out.add(new AggResult(
                    valueToObject(r.getGroupValue()),
                    r.getCount(),
                    r.getNumeric(),
                    r.getSum(), r.getAvg(), r.getMin(), r.getMax()));
        }
        return out;
    }

    /** Count all live records in a collection. */
    public long count(String collection) {
        return count(collection, null);
    }

    /** Count the live records matching {@code filter} (or all records when null). */
    public long count(String collection, Map<String, Object> filter) {
        List<AggResult> groups = aggregate(collection, null, "", "", filter);
        return groups.isEmpty() ? 0L : groups.get(0).count();
    }

    /**
     * Group live records by {@code field} and aggregate each group.
     *
     * <p>Convenience wrapper over {@link #aggregate}. {@code field} is the group-by
     * field; {@code metric} is the numeric field for {@code sum/avg/min/max} (pass
     * those names in {@code aggregations}). Returns one {@link AggResult} per
     * distinct group value.
     */
    public List<AggResult> groupBy(String collection, String field,
                                   List<String> aggregations, String metric,
                                   Map<String, Object> filter) {
        return aggregate(collection, aggregations, metric, field, filter);
    }

    private static AggregateOp parseAgg(String op) {
        switch (op.toLowerCase(Locale.ROOT)) {
            case "count": return AggregateOp.AGG_COUNT;
            case "sum":   return AggregateOp.AGG_SUM;
            case "avg":   return AggregateOp.AGG_AVG;
            case "min":   return AggregateOp.AGG_MIN;
            case "max":   return AggregateOp.AGG_MAX;
            default:
                throw new IllegalArgumentException(
                        "unknown aggregation '" + op + "'; expected one of "
                                + "[avg, count, max, min, sum]");
        }
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    public Map<String, Object> stats(String collection) {
        CollectionStatsResponse resp = blockingStub.collectionStats(
                CollectionStatsRequest.newBuilder().setCollection(collection).build());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("collection",    resp.getCollection());
        m.put("record_count",  resp.getRecordCount());
        m.put("segment_count", resp.getSegmentCount());
        m.put("dirty_entries", resp.getDirtyEntries());
        m.put("size_bytes",    resp.getSizeBytes());
        return m;
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /**
     * Force a synchronous compaction of a collection — merges dirty segments and
     * reclaims space from deleted/overwritten records. Returns only after the
     * compaction completes.
     *
     * @return true on success
     */
    public boolean compact(String collection) {
        CompactResponse resp = blockingStub.compact(CompactRequest.newBuilder()
                .setCollection(collection)
                .build());
        return resp.getOk();
    }

    /**
     * Stream a consistent, gzip-compressed tar snapshot of the whole database.
     *
     * <p>Each element is a chunk of the archive; concatenate them in order to
     * reconstruct it. Restore with {@code tar xzf backup.tar.gz}. gRPC-only —
     * this does not map onto the REST gateway.
     */
    public Iterator<SnapshotChunk> snapshot() {
        return blockingStub.snapshot(SnapshotRequest.newBuilder().build());
    }

    /**
     * Stream a whole-database snapshot straight to a file.
     *
     * @param path destination file (written as a gzip-compressed tar archive)
     * @return the number of bytes written
     */
    public long snapshotToFile(String path) throws IOException {
        Iterator<SnapshotChunk> chunks = snapshot();
        long total = 0;
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(path))) {
            while (chunks.hasNext()) {
                ByteString data = chunks.next().getData();
                data.writeTo(out);
                total += data.size();
            }
        }
        return total;
    }

    // -----------------------------------------------------------------------
    // Filter builder
    // -----------------------------------------------------------------------

    /**
     * Convert a plain Java map to a proto {@link Filter}.
     *
     * <p>Field filter:
     * <pre>
     *   Map.of("field", "age", "op", "gt", "value", "30")
     * </pre>
     *
     * <p>AND composite:
     * <pre>
     *   Map.of("and", List.of(
     *       Map.of("field", "age",  "op", "gte", "value", "18"),
     *       Map.of("field", "name", "op", "contains", "value", "alice")
     *   ))
     * </pre>
     *
     * <p>OR composite: same but key is {@code "or"}.
     *
     * <p>Supported {@code op} values: {@code eq neq gt gte lt lte contains regex}
     */
    @SuppressWarnings("unchecked")
    public Filter filterToProto(Map<String, Object> filter) {
        if (filter.containsKey("and")) {
            AndFilter.Builder and = AndFilter.newBuilder();
            for (Map<String, Object> child : (List<Map<String, Object>>) filter.get("and")) {
                and.addFilters(filterToProto(child));
            }
            return Filter.newBuilder().setAnd(and).build();
        }
        if (filter.containsKey("or")) {
            OrFilter.Builder or = OrFilter.newBuilder();
            for (Map<String, Object> child : (List<Map<String, Object>>) filter.get("or")) {
                or.addFilters(filterToProto(child));
            }
            return Filter.newBuilder().setOr(or).build();
        }
        // Field filter
        String field = (String) filter.get("field");
        String op    = (String) filter.get("op");
        String value = String.valueOf(filter.get("value"));
        return Filter.newBuilder()
                .setField(FieldFilter.newBuilder()
                        .setField(field)
                        .setOp(parseOp(op))
                        .setValue(value))
                .build();
    }

    private static FilterOp parseOp(String op) {
        switch (op.toLowerCase(Locale.ROOT)) {
            case "eq":       return FilterOp.EQ;
            case "neq":      return FilterOp.NEQ;
            case "gt":       return FilterOp.GT;
            case "gte":      return FilterOp.GTE;
            case "lt":       return FilterOp.LT;
            case "lte":      return FilterOp.LTE;
            case "contains": return FilterOp.CONTAINS;
            case "regex":    return FilterOp.REGEX;
            default:         return FilterOp.FILTER_OP_UNSPECIFIED;
        }
    }

    // -----------------------------------------------------------------------
    // Struct / Record conversion helpers
    // -----------------------------------------------------------------------

    /** Convert a proto {@link scriva.v1.ScrivaOuterClass.Record} into a {@link Record}. */
    static Record recordFromProto(scriva.v1.ScrivaOuterClass.Record r) {
        Map<String, Object> data = r.hasData() ? structToMap(r.getData()) : new LinkedHashMap<>();
        String dateAdded    = r.hasDateAdded()    ? timestampToIso(r.getDateAdded())    : null;
        String dateModified = r.hasDateModified() ? timestampToIso(r.getDateModified()) : null;
        return new Record(r.getId(), r.getKey(), r.getRev(), data, dateAdded, dateModified);
    }

    private static String timestampToIso(com.google.protobuf.Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()).toString();
    }

    /** Convert a plain Java map to a protobuf {@link Struct}. */
    public static Struct mapToStruct(Map<String, Object> map) {
        Struct.Builder builder = Struct.newBuilder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            builder.putFields(entry.getKey(), objectToValue(entry.getValue()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static Value objectToValue(Object obj) {
        if (obj == null) {
            return Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
        }
        if (obj instanceof Boolean) {
            return Value.newBuilder().setBoolValue((Boolean) obj).build();
        }
        if (obj instanceof Number) {
            return Value.newBuilder().setNumberValue(((Number) obj).doubleValue()).build();
        }
        if (obj instanceof String) {
            return Value.newBuilder().setStringValue((String) obj).build();
        }
        if (obj instanceof Map) {
            return Value.newBuilder().setStructValue(mapToStruct((Map<String, Object>) obj)).build();
        }
        if (obj instanceof List) {
            com.google.protobuf.ListValue.Builder list = com.google.protobuf.ListValue.newBuilder();
            for (Object item : (List<?>) obj) {
                list.addValues(objectToValue(item));
            }
            return Value.newBuilder().setListValue(list).build();
        }
        return Value.newBuilder().setStringValue(obj.toString()).build();
    }

    /** Convert a protobuf {@link Struct} to a plain Java map. */
    public static Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
            result.put(entry.getKey(), valueToObject(entry.getValue()));
        }
        return result;
    }

    private static Object valueToObject(Value value) {
        switch (value.getKindCase()) {
            case NULL_VALUE:   return null;
            case BOOL_VALUE:   return value.getBoolValue();
            case NUMBER_VALUE: return value.getNumberValue();
            case STRING_VALUE: return value.getStringValue();
            case STRUCT_VALUE: return structToMap(value.getStructValue());
            case LIST_VALUE: {
                List<Object> list = new ArrayList<>();
                for (Value v : value.getListValue().getValuesList()) {
                    list.add(valueToObject(v));
                }
                return list;
            }
            default: return null;
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void close() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    // =======================================================================
    // Value types
    // =======================================================================

    /**
     * A record returned from the engine.
     *
     * <p>{@link #id()} is the server-assigned numeric id; {@link #key()} is the
     * caller-supplied string key ({@code ""} for keyless records); {@link #rev()}
     * is the monotonic per-record revision (starts at 1, bumped on every write);
     * {@link #data()} is the decoded document. {@link #get(String)} is a shortcut
     * to a top-level {@code data} field.
     */
    public static final class Record {
        private final long id;
        private final String key;
        private final long rev;
        private final Map<String, Object> data;
        private final String dateAdded;
        private final String dateModified;

        Record(long id, String key, long rev, Map<String, Object> data,
               String dateAdded, String dateModified) {
            this.id = id;
            this.key = key;
            this.rev = rev;
            this.data = data;
            this.dateAdded = dateAdded;
            this.dateModified = dateModified;
        }

        public long id() { return id; }
        /** Caller-supplied string key, or {@code ""} for a keyless record. */
        public String key() { return key; }
        /** True when this record carries a caller-supplied key. */
        public boolean hasKey() { return key != null && !key.isEmpty(); }
        public long rev() { return rev; }
        public Map<String, Object> data() { return data; }
        /** ISO-8601 creation timestamp, or {@code null} when unset. */
        public String dateAdded() { return dateAdded; }
        /** ISO-8601 last-modified timestamp, or {@code null} when unset. */
        public String dateModified() { return dateModified; }

        /** Shortcut for {@code data().get(field)}. */
        public Object get(String field) { return data.get(field); }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Record{id=").append(id);
            if (hasKey()) sb.append(", key=").append(key);
            sb.append(", rev=").append(rev).append(", data=").append(data).append('}');
            return sb.toString();
        }
    }

    /**
     * The outcome of an {@link #updateByKey} write: the affected record's id, key,
     * revision (after the write) and last-modified timestamp.
     */
    public static final class UpdateResult {
        private final long id;
        private final String key;
        private final long rev;
        private final String dateModified;

        UpdateResult(long id, String key, long rev, String dateModified) {
            this.id = id;
            this.key = key;
            this.rev = rev;
            this.dateModified = dateModified;
        }

        public long id() { return id; }
        public String key() { return key; }
        public long rev() { return rev; }
        /** ISO-8601 last-modified timestamp string as returned by the engine. */
        public String dateModified() { return dateModified; }

        @Override
        public String toString() {
            return "UpdateResult{id=" + id + ", key=" + key + ", rev=" + rev
                    + ", dateModified=" + dateModified + '}';
        }
    }

    /**
     * The outcome of an {@link #updateIfRev} compare-and-swap. {@link #record()}
     * is populated only when {@link #swapped()} is {@code true}.
     */
    public static final class CasResult {
        private final boolean swapped;
        private final Record record;

        CasResult(boolean swapped, Record record) {
            this.swapped = swapped;
            this.record = record;
        }

        public boolean swapped() { return swapped; }
        /** The resulting record when {@link #swapped()} is true; {@code null} otherwise. */
        public Record record() { return record; }

        @Override
        public String toString() {
            return "CasResult{swapped=" + swapped + ", record=" + record + '}';
        }
    }

    /**
     * One keyset page from {@link #findPage}: the records plus the next-page
     * cursor. An empty {@link #nextPageToken()} means the last page was reached.
     */
    public static final class Page {
        private final List<Record> records;
        private final String nextPageToken;

        Page(List<Record> records, String nextPageToken) {
            this.records = records;
            this.nextPageToken = nextPageToken;
        }

        public List<Record> records() { return records; }
        public String nextPageToken() { return nextPageToken; }
        /** True when a further page remains under the requested ordering. */
        public boolean hasNextPage() { return nextPageToken != null && !nextPageToken.isEmpty(); }
    }

    /**
     * One group's aggregation result (N4). {@link #group()} is the group-by value
     * ({@code null} for the whole-set group); the numeric aggregates are
     * meaningful only when {@link #numeric()} is {@code true}.
     */
    public static final class AggResult {
        private final Object group;
        private final long count;
        private final boolean numeric;
        private final double sum;
        private final double avg;
        private final double min;
        private final double max;

        AggResult(Object group, long count, boolean numeric,
                  double sum, double avg, double min, double max) {
            this.group = group;
            this.count = count;
            this.numeric = numeric;
            this.sum = sum;
            this.avg = avg;
            this.min = min;
            this.max = max;
        }

        /** The group-by field's value (number/string/bool), or {@code null} for the whole set. */
        public Object group() { return group; }
        public long count() { return count; }
        /** True when at least one record in the group carried a numeric aggregate field. */
        public boolean numeric() { return numeric; }
        public double sum() { return sum; }
        public double avg() { return avg; }
        public double min() { return min; }
        public double max() { return max; }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("AggResult{group=").append(group)
                    .append(", count=").append(count);
            if (numeric) {
                sb.append(", sum=").append(sum).append(", avg=").append(avg)
                        .append(", min=").append(min).append(", max=").append(max);
            }
            return sb.append('}').toString();
        }
    }

    /**
     * A single sort key for a multi-field order-by (N3): a field name and a
     * direction. Use {@link #asc(String)} / {@link #desc(String)} to build one.
     */
    public static final class Order {
        private final String field;
        private final boolean desc;

        public Order(String field, boolean desc) {
            this.field = field;
            this.desc = desc;
        }

        public static Order asc(String field)  { return new Order(field, false); }
        public static Order desc(String field) { return new Order(field, true); }

        public String field() { return field; }
        public boolean desc() { return desc; }
    }
}
