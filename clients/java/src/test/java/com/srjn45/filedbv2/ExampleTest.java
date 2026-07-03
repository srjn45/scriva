package com.srjn45.filedbv2;

import filedb.v1.Filedb.WatchEvent;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link FileDBClient}.
 *
 * Requires a running FileDB server on localhost:5433 with api-key "dev-key".
 * Start it with:
 *   make run   (from the repo root)
 *
 * Run with:
 *   ./gradlew test
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExampleTest {

    private static final String HOST    = System.getenv().getOrDefault("FILEDB_HOST",    "localhost");
    private static final int    PORT    = Integer.parseInt(System.getenv().getOrDefault("FILEDB_PORT",    "5433"));
    private static final String API_KEY = System.getenv().getOrDefault("FILEDB_API_KEY", "dev-key");
    private static final String COL     = "test_java_integration";

    private static FileDBClient db;

    private static long insertedId1;
    private static long insertedId2;
    private static long insertedId3;

    @BeforeAll
    static void connect() {
        db = new FileDBClient(HOST, PORT, API_KEY);
        // Clean up from any previous failed run
        try { db.dropCollection(COL); } catch (Exception ignored) {}
    }

    @AfterAll
    static void disconnect() throws InterruptedException {
        try { db.dropCollection(COL); } catch (Exception ignored) {}
        db.close();
    }

    // -----------------------------------------------------------------------
    // Collection management
    // -----------------------------------------------------------------------

    @Test
    @Order(1)
    void createCollection() {
        String name = db.createCollection(COL);
        assertEquals(COL, name);
    }

    @Test
    @Order(2)
    void listCollections() {
        List<String> names = db.listCollections();
        assertTrue(names.contains(COL), "Expected " + COL + " in " + names);
    }

    // -----------------------------------------------------------------------
    // Indexes
    // -----------------------------------------------------------------------

    @Test
    @Order(3)
    void ensureAndListIndex() {
        db.ensureIndex(COL, "name");
        List<String> indexes = db.listIndexes(COL);
        assertTrue(indexes.contains("name"));
    }

    @Test
    @Order(4)
    void dropIndex() {
        db.ensureIndex(COL, "age");
        boolean ok = db.dropIndex(COL, "age");
        assertTrue(ok);
        assertFalse(db.listIndexes(COL).contains("age"));
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    @Test
    @Order(5)
    void insertSingle() {
        insertedId1 = db.insert(COL, Map.of("name", "Alice", "age", 30, "role", "admin"));
        assertTrue(insertedId1 > 0);
    }

    @Test
    @Order(6)
    void insertMany() {
        List<Long> ids = db.insertMany(COL, List.of(
                Map.of("name", "Bob",   "age", 25, "role", "user"),
                Map.of("name", "Carol", "age", 35, "role", "user")
        ));
        assertEquals(2, ids.size());
        ids.forEach(id -> assertTrue(id > 0));
        insertedId2 = ids.get(0);
        insertedId3 = ids.get(1);
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    @Test
    @Order(7)
    void findById() {
        Map<String, Object> record = db.findById(COL, insertedId1);
        assertEquals("Alice", record.get("name"));
    }

    @Test
    @Order(8)
    void findAll() {
        List<Map<String, Object>> records = db.find(COL);
        assertEquals(3, records.size());
    }

    @Test
    @Order(9)
    void findWithFieldFilter() {
        List<Map<String, Object>> users = db.find(COL,
                Map.of("field", "role", "op", "eq", "value", "user"),
                0, 0, "name", false);
        assertEquals(2, users.size());
    }

    @Test
    @Order(10)
    void findWithAndFilter() {
        List<Map<String, Object>> results = db.find(COL,
                Map.of("and", List.of(
                        Map.of("field", "age",  "op", "gt", "value", "25"),
                        Map.of("field", "role", "op", "eq", "value", "user")
                )), 0, 0, "", false);
        assertEquals(1, results.size());
        assertEquals("Carol", results.get(0).get("name"));
    }

    @Test
    @Order(11)
    void findWithOrFilter() {
        List<Map<String, Object>> results = db.find(COL,
                Map.of("or", List.of(
                        Map.of("field", "role", "op", "eq", "value", "admin"),
                        Map.of("field", "name", "op", "eq", "value", "Carol")
                )), 0, 0, "", false);
        assertEquals(2, results.size());
    }

    @Test
    @Order(12)
    void findWithPagination() {
        List<Map<String, Object>> page1 = db.find(COL, null, 2, 0, "name", false);
        List<Map<String, Object>> page2 = db.find(COL, null, 2, 2, "name", false);
        assertEquals(2, page1.size());
        assertEquals(1, page2.size());
    }

    // -----------------------------------------------------------------------
    // Update
    // -----------------------------------------------------------------------

    @Test
    @Order(13)
    void update() {
        long updatedId = db.update(COL, insertedId2, Map.of("name", "Bob", "age", 26, "role", "moderator"));
        assertEquals(insertedId2, updatedId);
        Map<String, Object> record = db.findById(COL, insertedId2);
        assertEquals("moderator", record.get("role"));
        assertEquals(26.0, record.get("age"));
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    @Test
    @Order(14)
    void delete() {
        boolean ok = db.delete(COL, insertedId3);
        assertTrue(ok);
        List<Map<String, Object>> all = db.find(COL);
        assertEquals(2, all.size());
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    @Test
    @Order(15)
    void transactionCommit() {
        String txId = db.beginTx(COL);
        assertNotNull(txId);
        assertFalse(txId.isEmpty());
        assertTrue(db.commitTx(txId));
    }

    @Test
    @Order(16)
    void transactionRollback() {
        String txId = db.beginTx(COL);
        assertTrue(db.rollbackTx(txId));
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    @Test
    @Order(17)
    void stats() {
        Map<String, Object> s = db.stats(COL);
        assertEquals(COL, s.get("collection"));
        assertTrue(((Number) s.get("record_count")).longValue() >= 2);
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    @Test
    @Order(18)
    void compact() {
        assertTrue(db.compact(COL));
    }

    @Test
    @Order(19)
    void perRecordTtl() {
        long ttlId = db.insert(COL, Map.of("name", "Ephemeral", "role", "temp"), 3600L);
        assertTrue(ttlId > 0);
        // ttlSeconds = 0 (default overload) is sticky — the record stays reachable
        db.update(COL, ttlId, Map.of("name", "Ephemeral", "role", "temp", "touched", true));
        Map<String, Object> record = db.findById(COL, ttlId);
        assertEquals(Boolean.TRUE, record.get("touched"));
    }

    @Test
    @Order(20)
    void snapshotToFile() throws Exception {
        java.io.File backup = java.io.File.createTempFile("filedb_java_snapshot_test", ".tar.gz");
        try {
            long bytes = db.snapshotToFile(backup.getAbsolutePath());
            assertTrue(bytes > 0, "Expected a non-empty snapshot archive");
        } finally {
            backup.delete();
        }
    }

    // -----------------------------------------------------------------------
    // Watch
    // -----------------------------------------------------------------------

    @Test
    @Order(21)
    void watchReceivesInsertEvents() throws InterruptedException {
        String watchCol = COL + "_watch";
        db.createCollection(watchCol);

        List<WatchEvent> events = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);

        db.watch(watchCol, null, new StreamObserver<WatchEvent>() {
            @Override
            public void onNext(WatchEvent event) {
                events.add(event);
                latch.countDown();
            }
            @Override public void onError(Throwable t)  {}
            @Override public void onCompleted()          {}
        });

        Thread.sleep(100);
        db.insert(watchCol, Map.of("x", 1));
        db.insert(watchCol, Map.of("x", 2));

        boolean received = latch.await(5, TimeUnit.SECONDS);
        db.dropCollection(watchCol);

        assertTrue(received, "Did not receive expected watch events within timeout");
        assertEquals(2, events.size());
    }

    // -----------------------------------------------------------------------
    // Drop collection
    // -----------------------------------------------------------------------

    @Test
    @Order(22)
    void dropCollection() {
        boolean ok = db.dropCollection(COL);
        assertTrue(ok);
        assertFalse(db.listCollections().contains(COL));
    }
}
