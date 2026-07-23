package io.github.srjn45.scriva

import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
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

    /** Minimal in-memory implementation of just the RPCs the tests touch. */
    private class FakeScriva : ScrivaGrpcKt.ScrivaCoroutineImplBase() {
        private val ids = AtomicLong(0)
        private val store = HashMap<String, MutableList<ScrivaOuterClass.Record>>()
        var lastFilter: ScrivaOuterClass.Filter? = null

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
    }
}
