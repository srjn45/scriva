package io.github.srjn45.scriva

import io.github.srjn45.scriva.Conversions.filterToProto
import io.github.srjn45.scriva.Conversions.mapToStruct
import io.github.srjn45.scriva.Conversions.recordFromProto
import io.github.srjn45.scriva.Conversions.valueToObject
import io.github.srjn45.scriva.Conversions.watchOpFromProto
import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.MetadataUtils
import java.io.BufferedOutputStream
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import scriva.v1.ScrivaGrpcKt
import scriva.v1.ScrivaOuterClass.*

/**
 * Idiomatic Kotlin client for ScrivaDB.
 *
 * Unary RPCs are `suspend` functions; the server-streaming `find`, `aggregate`,
 * `watch` and `snapshot` calls return a [Flow]. Every call carries the
 * `x-api-key` metadata automatically.
 *
 * ```
 * ScrivaClient.connect("localhost", 5433, "dev-key").use { db ->
 *     db.createCollection("users")
 *     val id = db.insert("users", mapOf("name" to "Alice", "age" to 30))
 *     val record = db.findById("users", id)
 *     println(record["name"])
 * }
 * ```
 */
class ScrivaClient internal constructor(
    private val channel: ManagedChannel,
    apiKey: String,
) : Closeable {

    private val stub: ScrivaGrpcKt.ScrivaCoroutineStub

    init {
        val md = Metadata().apply { put(API_KEY_HEADER, apiKey) }
        val intercepted: Channel =
            ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md))
        stub = ScrivaGrpcKt.ScrivaCoroutineStub(intercepted)
    }

    companion object {
        private val API_KEY_HEADER: Metadata.Key<String> =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)

        /** Connect over plaintext (no TLS). */
        fun connect(host: String, port: Int, apiKey: String): ScrivaClient {
            val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
            return ScrivaClient(channel, apiKey)
        }

        /** Connect over TLS, verifying the server against [tlsCaCert]. */
        fun connectTls(host: String, port: Int, apiKey: String, tlsCaCert: File): ScrivaClient {
            val channel = NettyChannelBuilder.forAddress(host, port)
                .sslContext(GrpcSslContexts.forClient().trustManager(tlsCaCert).build())
                .build()
            return ScrivaClient(channel, apiKey)
        }
    }

    // -- Error translation -------------------------------------------------

    private fun translate(e: Throwable): Throwable {
        val (code, desc) = when (e) {
            is StatusException -> e.status.code to e.status.description
            is StatusRuntimeException -> e.status.code to e.status.description
            else -> return e
        }
        val msg = desc ?: e.message
        return when (code) {
            Status.Code.NOT_FOUND -> NotFoundException(msg, e)
            Status.Code.ALREADY_EXISTS -> AlreadyExistsException(msg, e)
            else -> e
        }
    }

    private inline fun <T> translating(block: () -> T): T =
        try {
            block()
        } catch (e: StatusException) {
            throw translate(e)
        } catch (e: StatusRuntimeException) {
            throw translate(e)
        }

    // -- Collection management ---------------------------------------------

    suspend fun createCollection(name: String, defaultTtlSeconds: Long = 0): String = translating {
        stub.createCollection(
            CreateCollectionRequest.newBuilder()
                .setName(name).setDefaultTtlSeconds(defaultTtlSeconds).build()
        ).name
    }

    suspend fun dropCollection(name: String): Boolean = translating {
        stub.dropCollection(DropCollectionRequest.newBuilder().setName(name).build()).ok
    }

    suspend fun listCollections(): List<String> = translating {
        stub.listCollections(ListCollectionsRequest.getDefaultInstance()).namesList
    }

    // -- CRUD --------------------------------------------------------------

    suspend fun insert(
        collection: String,
        data: Map<String, Any?>,
        ttlSeconds: Long = 0,
        key: String = "",
    ): Long = translating {
        stub.insert(
            InsertRequest.newBuilder()
                .setCollection(collection).setData(mapToStruct(data))
                .setTtlSeconds(ttlSeconds).setKey(key).build()
        ).id
    }

    /** Keyed insert under [key]; raises [AlreadyExistsException] if taken. */
    suspend fun insertKeyed(collection: String, key: String, data: Map<String, Any?>): Long =
        insert(collection, data, 0, key)

    suspend fun insertMany(
        collection: String,
        records: List<Map<String, Any?>>,
        ttlSeconds: Long = 0,
    ): List<Long> = translating {
        val req = InsertManyRequest.newBuilder().setCollection(collection).setTtlSeconds(ttlSeconds)
        records.forEach { req.addRecords(mapToStruct(it)) }
        stub.insertMany(req.build()).idsList
    }

    suspend fun findById(collection: String, id: Long, fields: List<String>? = null): Record =
        translating {
            val req = FindByIdRequest.newBuilder().setCollection(collection).setId(id)
            fields?.let { req.addAllFields(it) }
            recordFromProto(stub.findById(req.build()).record)
        }

    /**
     * Stream records matching the query. Multi-field [orderBy] (N3), field
     * projection [fields] (N2) and an opaque [pageToken] (N3) are all optional.
     */
    fun find(
        collection: String,
        filter: Filter? = null,
        limit: Int = 0,
        offset: Int = 0,
        orderBy: List<Order> = emptyList(),
        fields: List<String>? = null,
        pageToken: String = "",
    ): Flow<Record> =
        stub.find(buildFindRequest(collection, filter, limit, offset, orderBy, fields, pageToken))
            .catch { throw translate(it) }
            .map { recordFromProto(it.record) }

    /** Fetch one keyset page: the records plus a next-page cursor (N3). */
    suspend fun findPage(
        collection: String,
        filter: Filter? = null,
        limit: Int = 0,
        offset: Int = 0,
        orderBy: List<Order> = emptyList(),
        fields: List<String>? = null,
        pageToken: String = "",
    ): Page = translating {
        val req = buildFindRequest(collection, filter, limit, offset, orderBy, fields, pageToken)
        val records = ArrayList<Record>()
        var nextToken = ""
        stub.find(req).toList().forEach {
            records.add(recordFromProto(it.record))
            if (it.pageToken.isNotEmpty()) nextToken = it.pageToken
        }
        Page(records, nextToken)
    }

    private fun buildFindRequest(
        collection: String, filter: Filter?, limit: Int, offset: Int,
        orderBy: List<Order>, fields: List<String>?, pageToken: String,
    ): FindRequest {
        val req = FindRequest.newBuilder()
            .setCollection(collection).setLimit(limit).setOffset(offset).setPageToken(pageToken)
        filter?.let { req.setFilter(filterToProto(it)) }
        fields?.let { req.addAllFields(it) }
        orderBy.forEach {
            req.addOrderByFields(OrderBy.newBuilder().setField(it.field).setDesc(it.desc).build())
        }
        return req.build()
    }

    suspend fun update(
        collection: String,
        id: Long,
        data: Map<String, Any?>,
        ttlSeconds: Long = 0,
    ): Long = translating {
        stub.update(
            UpdateRequest.newBuilder()
                .setCollection(collection).setId(id).setData(mapToStruct(data))
                .setTtlSeconds(ttlSeconds).build()
        ).id
    }

    suspend fun delete(collection: String, id: Long): Boolean = translating {
        stub.delete(DeleteRequest.newBuilder().setCollection(collection).setId(id).build()).ok
    }

    // -- Keyed CRUD, upsert & compare-and-swap (N1) ------------------------

    suspend fun upsert(collection: String, key: String, data: Map<String, Any?>): Record =
        translating {
            recordFromProto(
                stub.upsert(
                    UpsertRequest.newBuilder()
                        .setCollection(collection).setKey(key).setData(mapToStruct(data)).build()
                ).record
            )
        }

    suspend fun findByKey(collection: String, key: String, fields: List<String>? = null): Record =
        translating {
            val req = FindByKeyRequest.newBuilder().setCollection(collection).setKey(key)
            fields?.let { req.addAllFields(it) }
            recordFromProto(stub.findByKey(req.build()).record)
        }

    suspend fun updateByKey(collection: String, key: String, data: Map<String, Any?>): UpdateResult =
        translating {
            val resp = stub.updateByKey(
                UpdateByKeyRequest.newBuilder()
                    .setCollection(collection).setKey(key).setData(mapToStruct(data)).build()
            )
            UpdateResult(resp.id, resp.key, resp.rev, resp.dateModified)
        }

    suspend fun deleteByKey(collection: String, key: String): Boolean = translating {
        stub.deleteByKey(
            DeleteByKeyRequest.newBuilder().setCollection(collection).setKey(key).build()
        ).ok
    }

    /**
     * Compare-and-swap on [key], conditional on [expectedRev]. A stale revision
     * (or missing key) is a clean no-op reported as `swapped = false`.
     */
    suspend fun updateIfRev(
        collection: String,
        key: String,
        expectedRev: Long,
        data: Map<String, Any?>,
    ): CasResult = translating {
        val resp = stub.updateIfRev(
            UpdateIfRevRequest.newBuilder()
                .setCollection(collection).setKey(key).setExpectedRev(expectedRev)
                .setData(mapToStruct(data)).build()
        )
        val record = if (resp.swapped && resp.hasRecord()) recordFromProto(resp.record) else null
        CasResult(resp.swapped, record)
    }

    // -- Secondary indexes -------------------------------------------------

    suspend fun ensureIndex(collection: String, field: String) {
        translating {
            stub.ensureIndex(
                EnsureIndexRequest.newBuilder().setCollection(collection).setField(field).build()
            )
        }
    }

    suspend fun dropIndex(collection: String, field: String): Boolean = translating {
        stub.dropIndex(
            DropIndexRequest.newBuilder().setCollection(collection).setField(field).build()
        ).ok
    }

    suspend fun listIndexes(collection: String): List<String> = translating {
        stub.listIndexes(ListIndexesRequest.newBuilder().setCollection(collection).build()).fieldsList
    }

    // -- Transactions ------------------------------------------------------

    suspend fun beginTx(collection: String): String = translating {
        stub.beginTx(BeginTxRequest.newBuilder().setCollection(collection).build()).txId
    }

    suspend fun commitTx(txId: String): Boolean = translating {
        stub.commitTx(CommitTxRequest.newBuilder().setTxId(txId).build()).ok
    }

    suspend fun rollbackTx(txId: String): Boolean = translating {
        stub.rollbackTx(RollbackTxRequest.newBuilder().setTxId(txId).build()).ok
    }

    // -- Watch (server-streaming change feed) ------------------------------

    /** Subscribe to change events on [collection] as a [Flow]. */
    fun watch(collection: String, filter: Filter? = null): Flow<WatchEvent> {
        val req = WatchRequest.newBuilder().setCollection(collection)
        filter?.let { req.setFilter(filterToProto(it)) }
        return stub.watch(req.build())
            .catch { throw translate(it) }
            .map { ev ->
                WatchEvent(
                    op = watchOpFromProto(ev.op),
                    collection = ev.collection,
                    record = if (ev.hasRecord()) recordFromProto(ev.record) else null,
                    ts = if (ev.hasTs()) Instant.ofEpochSecond(ev.ts.seconds, ev.ts.nanos.toLong()).toString() else null,
                )
            }
    }

    // -- Aggregations (N4) -------------------------------------------------

    /**
     * Compute count + numeric aggregations over the filtered live records.
     * One [AggResult] per group (or a single whole-set group when [groupBy] is "").
     */
    suspend fun aggregate(
        collection: String,
        aggregations: List<String> = emptyList(),
        field: String = "",
        groupBy: String = "",
        filter: Filter? = null,
    ): List<AggResult> = translating {
        val req = AggregateRequest.newBuilder()
            .setCollection(collection).setField(field).setGroupBy(groupBy)
        filter?.let { req.setFilter(filterToProto(it)) }
        aggregations.forEach { req.addAggregations(parseAgg(it)) }
        stub.aggregate(req.build()).toList().map {
            AggResult(valueToObject(it.groupValue), it.count, it.numeric, it.sum, it.avg, it.min, it.max)
        }
    }

    /** Count all live records, or those matching [filter]. */
    suspend fun count(collection: String, filter: Filter? = null): Long {
        val groups = aggregate(collection, emptyList(), "", "", filter)
        return if (groups.isEmpty()) 0 else groups.first().count
    }

    /** Group live records by [field] and aggregate [metric] per group. */
    suspend fun groupBy(
        collection: String,
        field: String,
        aggregations: List<String>,
        metric: String,
        filter: Filter? = null,
    ): List<AggResult> = aggregate(collection, aggregations, metric, field, filter)

    private fun parseAgg(op: String): AggregateOp = when (op.lowercase()) {
        "count" -> AggregateOp.AGG_COUNT
        "sum" -> AggregateOp.AGG_SUM
        "avg" -> AggregateOp.AGG_AVG
        "min" -> AggregateOp.AGG_MIN
        "max" -> AggregateOp.AGG_MAX
        else -> throw IllegalArgumentException(
            "unknown aggregation '$op'; expected one of [avg, count, max, min, sum]"
        )
    }

    // -- Stats -------------------------------------------------------------

    suspend fun stats(collection: String): Stats = translating {
        val r = stub.collectionStats(
            CollectionStatsRequest.newBuilder().setCollection(collection).build()
        )
        Stats(r.collection, r.recordCount, r.segmentCount, r.dirtyEntries, r.sizeBytes)
    }

    // -- Maintenance -------------------------------------------------------

    suspend fun compact(collection: String): Boolean = translating {
        stub.compact(CompactRequest.newBuilder().setCollection(collection).build()).ok
    }

    /** Stream the raw gzip-tar snapshot archive chunk by chunk. */
    fun snapshot(): Flow<com.google.protobuf.ByteString> =
        stub.snapshot(SnapshotRequest.getDefaultInstance())
            .catch { throw translate(it) }
            .map { it.data }

    /** Stream a whole-database snapshot straight to [path]; returns bytes written. */
    suspend fun snapshotToFile(path: String): Long {
        var total = 0L
        BufferedOutputStream(FileOutputStream(path)).use { out ->
            snapshot().collect { data ->
                data.writeTo(out)
                total += data.size()
            }
        }
        return total
    }

    // -- Lifecycle ---------------------------------------------------------

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}
