package io.github.srjn45.scriva

import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.protobuf.StatusProto
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import scriva.v1.ScrivaGrpcKt
import scriva.v1.ScrivaOuterClass
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Hermetic tests: an in-process gRPC server backed by an in-memory fake exercises
 * client construction, message round-tripping and streaming — no external server,
 * no TCP ports, parallel-safe.
 */
class ScrivaClientTest {

    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var db: ScrivaClient
    private lateinit var fake: FakeScriva

    @BeforeTest
    fun setUp() {
        val name = InProcessServerBuilder.generateName()
        fake = FakeScriva()
        server = InProcessServerBuilder.forName(name).directExecutor().addService(fake).build().start()
        channel = InProcessChannelBuilder.forName(name).directExecutor().build()
        db = ScrivaClient(channel, "test-key")
    }

    @AfterTest
    fun tearDown() {
        db.close()
        channel.shutdownNow()
        server.shutdownNow()
    }

    // -- Pure conversion round-trip (no server) ----------------------------

    @Test
    fun structRoundTripPreservesNestedValues() {
        val original: Map<String, Any?> = mapOf(
            "name" to "Alice",
            "age" to 30.0,
            "active" to true,
            "missing" to null,
            "tags" to listOf("a", "b"),
            "nested" to mapOf("x" to 1.0, "y" to listOf(true, false)),
        )
        val roundTripped = Conversions.structToMap(Conversions.mapToStruct(original))
        assertEquals(original, roundTripped)
    }

    // -- Client construction + unary round-trip ----------------------------

    @Test
    fun createInsertAndFindById() = runTest {
        assertEquals("users", db.createCollection("users"))
        val id = db.insert("users", mapOf("name" to "Alice", "age" to 30))
        assertTrue(id > 0)
        val record = db.findById("users", id)
        assertEquals("Alice", record["name"])
        assertEquals(30.0, record["age"])
        assertEquals(id, record.id)
        assertEquals(1L, record.rev)
    }

    @Test
    fun findStreamsRecordsAndConvertsFilter() = runTest {
        db.createCollection("users")
        db.insert("users", mapOf("name" to "Alice", "role" to "admin"))
        db.insert("users", mapOf("name" to "Bob", "role" to "user"))

        val records = db.find(
            "users",
            filter = and(field("age", FilterOp.GT, 18), field("role", FilterOp.EQ, "admin")),
        ).toList()

        assertEquals(2, records.size)
        // The filter was converted to the proto shape and reached the server.
        val f = fake.lastFilter!!
        assertTrue(f.hasAnd())
        assertEquals("age", f.and.getFilters(0).field.field)
        assertEquals(ScrivaOuterClass.FilterOp.GT, f.and.getFilters(0).field.op)
    }

    @Test
    fun watchMapsEventsToFlow() = runTest {
        val events = db.watch("users").toList()
        assertEquals(2, events.size)
        assertEquals(WatchOp.INSERTED, events[0].op)
        assertEquals(WatchOp.DELETED, events[1].op)
        assertNull(events[1].record)
    }

    @Test
    fun notFoundIsTranslated() = runTest {
        try {
            db.findByKey("users", "ghost")
            assertFalse(true, "expected NotFoundException")
        } catch (e: NotFoundException) {
            assertTrue(e.message?.contains("no such key") == true)
        }
    }

    @Test
    fun xTxUsesExactWireFieldsAndNeverRetriesCommit() = runTest {
        val tx = db.beginXTx(listOf("ledger", "accounts"), XTxBeginOptions("key-1", 20, 40))
        assertEquals(listOf("accounts", "ledger"), tx.collections)
        assertEquals(8, db.stageXTx(tx.xtxId, XTxOperation("accounts", XTxOp.UPDATE, 8, mapOf("balance" to 9), 3)))
        assertEquals(3, db.getXTx(tx.xtxId, "accounts", 8).rev)
        assertEquals("decision-1", db.commitXTx(tx.xtxId, XTxCommitOptions(50)).txId)
        assertTrue(db.rollbackXTx(tx.xtxId))
        assertEquals("decision-1", db.xTxStatus("key-1").txId)
        assertEquals(1, fake.commitCalls, "an outcome must be resolved through XTxStatus, not a blind commit retry")
    }

    @Test
    fun xTxUnknownOutcomePreservesStructuredRecoveryMetadata() {
        val info = com.google.rpc.ErrorInfo.newBuilder().setDomain("scriva.xtx").setReason("XTX_OUTCOME_UNKNOWN")
            .putMetadata("status_ref", "key-1").putMetadata("retry_safe", "false").build()
        val error = StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
            .setCode(io.grpc.Status.Code.UNKNOWN.value()).addDetails(com.google.protobuf.Any.pack(info)).build())
        val detail = xTxErrorDetail(error)!!
        assertEquals("XTX_OUTCOME_UNKNOWN", detail.reason)
        assertEquals("key-1", detail.metadata["status_ref"])
        assertEquals("false", detail.metadata["retry_safe"])
    }

    @Test
    fun unknownCommitIsResolvedByStatusBeforeRetry() = runTest {
        val tx = db.beginXTx(listOf("a", "b"), XTxBeginOptions("idem-unknown"))
        fake.failNextCommit = true
        val error = try { db.commitXTx(tx.xtxId); error("expected unknown outcome") }
        catch (e: io.grpc.StatusException) { e }
        val detail = xTxErrorDetail(error)!!
        assertEquals("XTX_OUTCOME_UNKNOWN", detail.reason)
        assertEquals("idem-unknown", detail.metadata["status_ref"])
        assertEquals("false", detail.metadata["retry_safe"])
        assertEquals("XTX_STATUS_COMMITTED", db.xTxStatus(detail.metadata.getValue("status_ref")).status)
        assertEquals(1, fake.commitCalls)
        assertEquals(1, fake.statusCalls)
        assertEquals("idem-unknown", fake.lastStatusRef)
    }

    /** Minimal in-memory implementation of just the RPCs the tests touch. */
    private class FakeScriva : ScrivaGrpcKt.ScrivaCoroutineImplBase() {
        private val ids = AtomicLong(0)
        private val store = HashMap<String, MutableList<ScrivaOuterClass.Record>>()
        var lastFilter: ScrivaOuterClass.Filter? = null
        var commitCalls = 0
        var statusCalls = 0
        var failNextCommit = false
        var lastStatusRef = ""

        override suspend fun createCollection(
            request: ScrivaOuterClass.CreateCollectionRequest,
        ): ScrivaOuterClass.CreateCollectionResponse {
            store.getOrPut(request.name) { mutableListOf() }
            return ScrivaOuterClass.CreateCollectionResponse.newBuilder().setName(request.name).build()
        }

        override suspend fun insert(
            request: ScrivaOuterClass.InsertRequest,
        ): ScrivaOuterClass.InsertResponse {
            val id = ids.incrementAndGet()
            val rec = ScrivaOuterClass.Record.newBuilder()
                .setId(id).setRev(1).setData(request.data).build()
            store.getOrPut(request.collection) { mutableListOf() }.add(rec)
            return ScrivaOuterClass.InsertResponse.newBuilder().setId(id).setRev(1).build()
        }

        override suspend fun findById(
            request: ScrivaOuterClass.FindByIdRequest,
        ): ScrivaOuterClass.FindResponse {
            val rec = store[request.collection]?.firstOrNull { it.id == request.id }
                ?: throw io.grpc.Status.NOT_FOUND.withDescription("no such id").asException()
            return ScrivaOuterClass.FindResponse.newBuilder().setRecord(rec).build()
        }

        override fun find(request: ScrivaOuterClass.FindRequest): Flow<ScrivaOuterClass.FindResponse> {
            lastFilter = if (request.hasFilter()) request.filter else null
            val recs = store[request.collection].orEmpty().toList()
            return flow {
                recs.forEach { emit(ScrivaOuterClass.FindResponse.newBuilder().setRecord(it).build()) }
            }
        }

        override suspend fun findByKey(
            request: ScrivaOuterClass.FindByKeyRequest,
        ): ScrivaOuterClass.FindResponse =
            throw io.grpc.Status.NOT_FOUND.withDescription("no such key").asException()

        override fun watch(request: ScrivaOuterClass.WatchRequest): Flow<ScrivaOuterClass.WatchEvent> = flow {
            emit(
                ScrivaOuterClass.WatchEvent.newBuilder()
                    .setOp(ScrivaOuterClass.WatchOp.INSERTED)
                    .setCollection(request.collection)
                    .setRecord(ScrivaOuterClass.Record.newBuilder().setId(1).setRev(1).build())
                    .build()
            )
            emit(
                ScrivaOuterClass.WatchEvent.newBuilder()
                    .setOp(ScrivaOuterClass.WatchOp.DELETED)
                    .setCollection(request.collection)
                    .build()
            )
        }

        override suspend fun beginXTx(request: ScrivaOuterClass.BeginXTxRequest) =
            ScrivaOuterClass.BeginXTxResponse.newBuilder().setXtxId("xtx-1")
                .addAllCollections(request.collectionsList.sorted()).setIdempotencyKey(request.idempotencyKey).build()
        override suspend fun stageXTx(request: ScrivaOuterClass.StageXTxRequest) =
            ScrivaOuterClass.StageXTxResponse.newBuilder().setId(request.id).build()
        override suspend fun getXTx(request: ScrivaOuterClass.GetXTxRequest) =
            ScrivaOuterClass.GetXTxResponse.newBuilder().setRecord(ScrivaOuterClass.Record.newBuilder().setId(request.id).setRev(3)).build()
        override suspend fun commitXTx(request: ScrivaOuterClass.CommitXTxRequest): ScrivaOuterClass.CommitXTxResponse {
            commitCalls++
            if (failNextCommit) {
                failNextCommit = false
                val info = com.google.rpc.ErrorInfo.newBuilder().setDomain("scriva.xtx").setReason("XTX_OUTCOME_UNKNOWN")
                    .putMetadata("status_ref", "idem-unknown").putMetadata("retry_safe", "false").build()
                throw StatusProto.toStatusException(com.google.rpc.Status.newBuilder().setCode(io.grpc.Status.Code.UNKNOWN.value()).addDetails(com.google.protobuf.Any.pack(info)).build())
            }
            return ScrivaOuterClass.CommitXTxResponse.newBuilder().setTxId("decision-1")
                .addOps(ScrivaOuterClass.XTxOpResult.newBuilder().setCollection("accounts").setId(8).setRev(4).setOp(ScrivaOuterClass.XTxOpKind.XTX_OP_UPDATE)).build()
        }
        override suspend fun rollbackXTx(request: ScrivaOuterClass.RollbackXTxRequest) = ScrivaOuterClass.RollbackXTxResponse.newBuilder().setOk(true).build()
        override suspend fun xTxStatus(request: ScrivaOuterClass.XTxStatusRequest): ScrivaOuterClass.XTxStatusResponse { statusCalls++; lastStatusRef=request.ref; return ScrivaOuterClass.XTxStatusResponse.newBuilder().setStatus(ScrivaOuterClass.XTxState.XTX_STATUS_COMMITTED).setTxId("decision-1").build() }
    }
}
