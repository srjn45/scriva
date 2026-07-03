package com.srjn45.filedbv2;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import filedb.v1.FileDBGrpc;
import filedb.v1.Filedb.*;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Java client for FileDB v2.
 *
 * <p>Usage:
 * <pre>
 *   FileDBClient db = new FileDBClient("localhost", 5433, "dev-key");
 *   db.createCollection("users");
 *   long id = db.insert("users", Map.of("name", "Alice", "age", 30));
 *   Map<String, Object> record = db.findById("users", id);
 *   db.close();
 * </pre>
 */
public class FileDBClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final FileDBGrpc.FileDBBlockingStub blockingStub;
    private final FileDBGrpc.FileDBStub asyncStub;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Connect without TLS (plaintext). */
    public FileDBClient(String host, int port, String apiKey) {
        this(buildPlaintextChannel(host, port), apiKey);
    }

    /** Connect with TLS, verifying the server against the supplied CA certificate. */
    public FileDBClient(String host, int port, String apiKey, File tlsCaCert) throws SSLException {
        this(buildTlsChannel(host, port, tlsCaCert), apiKey);
    }

    private FileDBClient(ManagedChannel channel, String apiKey) {
        this.channel = channel;
        ClientInterceptor authInterceptor = buildAuthInterceptor(apiKey);
        Channel intercepted = ClientInterceptors.intercept(channel, authInterceptor);
        this.blockingStub = FileDBGrpc.newBlockingStub(intercepted);
        this.asyncStub    = FileDBGrpc.newStub(intercepted);
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
        return insert(collection, data, 0L);
    }

    /**
     * Insert one record with an explicit per-record TTL, in seconds.
     *
     * <p>{@code ttlSeconds > 0} expires the record that long after insertion,
     * overriding any collection default; {@code 0} applies the collection default.
     */
    public long insert(String collection, Map<String, Object> data, long ttlSeconds) {
        InsertResponse resp = blockingStub.insert(InsertRequest.newBuilder()
                .setCollection(collection)
                .setData(mapToStruct(data))
                .setTtlSeconds(ttlSeconds)
                .build());
        return resp.getId();
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

    public Map<String, Object> findById(String collection, long id) {
        FindResponse resp = blockingStub.findById(FindByIdRequest.newBuilder()
                .setCollection(collection)
                .setId(id)
                .build());
        return structToMap(resp.getRecord().getData());
    }

    /**
     * Find records matching the given options. All parameters are optional.
     *
     * @param collection collection name
     * @param filter     plain map filter (see {@link #filterToProto(Map)}), or null
     * @param limit      max results (0 = no limit)
     * @param offset     skip first N results
     * @param orderBy    field name to sort by, or empty string
     * @param descending sort descending when true
     * @return list of record data maps
     */
    public List<Map<String, Object>> find(String collection, Map<String, Object> filter,
                                          int limit, int offset, String orderBy, boolean descending) {
        FindRequest.Builder req = FindRequest.newBuilder()
                .setCollection(collection)
                .setLimit(limit)
                .setOffset(offset)
                .setOrderBy(orderBy != null ? orderBy : "")
                .setDescending(descending);
        if (filter != null) {
            req.setFilter(filterToProto(filter));
        }
        Iterator<FindResponse> iter = blockingStub.find(req.build());
        List<Map<String, Object>> results = new ArrayList<>();
        while (iter.hasNext()) {
            results.add(structToMap(iter.next().getRecord().getData()));
        }
        return results;
    }

    /** Convenience overload — no filter, no ordering, no pagination. */
    public List<Map<String, Object>> find(String collection) {
        return find(collection, null, 0, 0, "", false);
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
    // Struct conversion helpers
    // -----------------------------------------------------------------------

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
}
