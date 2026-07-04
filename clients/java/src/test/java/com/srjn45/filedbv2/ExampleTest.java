package com.srjn45.filedbv2;

import com.srjn45.filedbv2.FileDBClient.AggResult;
import com.srjn45.filedbv2.FileDBClient.CasResult;
import com.srjn45.filedbv2.FileDBClient.Page;
import com.srjn45.filedbv2.FileDBClient.Record;
import com.srjn45.filedbv2.FileDBClient.UpdateResult;
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
        Record record = db.findById(COL, insertedId1);
        assertEquals("Alice", record.get("name"));
        assertEquals(insertedId1, record.id());
        // Every live record carries a revision, starting at 1.
        assertTrue(record.rev() >= 1);
    }

    @Test
    @Order(8)
    void findByIdWithProjection() {
        // N2: only the projected field appears in data; id/rev still present.
        Record record = db.findById(COL, insertedId1, List.of("name"));
        assertEquals("Alice", record.get("name"));
        assertFalse(record.data().containsKey("age"), "age should be projected out");
        assertEquals(insertedId1, record.id());
    }

    @Test
    @Order(9)
    void findAll() {
        List<Record> records = db.find(COL);
        assertEquals(3, records.size());
    }

    @Test
    @Order(10)
    void findWithFieldFilter() {
        List<Record> users = db.find(COL,
                Map.of("field", "role", "op", "eq", "value", "user"),
                0, 0, "name", false);
        assertEquals(2, users.size());
    }

    @Test
    @Order(11)
    void findWithAndFilter() {
        List<Record> results = db.find(COL,
                Map.of("and", List.of(
                        Map.of("field", "age",  "op", "gt", "value", "25"),
                        Map.of("field", "role", "op", "eq", "value", "user")
                )), 0, 0, "", false);
        assertEquals(1, results.size());
        assertEquals("Carol", results.get(0).get("name"));
    }

    @Test
    @Order(12)
    void findWithOrFilter() {
        List<Record> results = db.find(COL,
                Map.of("or", List.of(
                        Map.of("field", "role", "op", "eq", "value", "admin"),
                        Map.of("field", "name", "op", "eq", "value", "Carol")
                )), 0, 0, "", false);
        assertEquals(2, results.size());
    }

    @Test
    @Order(13)
    void findWithPagination() {
        List<Record> page1 = db.find(COL, null, 2, 0, "name", false);
        List<Record> page2 = db.find(COL, null, 2, 2, "name", false);
        assertEquals(2, page1.size());
        assertEquals(1, page2.size());
    }

    // -----------------------------------------------------------------------
    // Update
    // -----------------------------------------------------------------------

    @Test
    @Order(14)
    void update() {
        long updatedId = db.update(COL, insertedId2, Map.of("name", "Bob", "age", 26, "role", "moderator"));
        assertEquals(insertedId2, updatedId);
        Record record = db.findById(COL, insertedId2);
        assertEquals("moderator", record.get("role"));
        assertEquals(26.0, record.get("age"));
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    @Test
    @Order(15)
    void delete() {
        boolean ok = db.delete(COL, insertedId3);
        assertTrue(ok);
        List<Record> all = db.find(COL);
        assertEquals(2, all.size());
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    @Test
    @Order(16)
    void transactionCommit() {
        String txId = db.beginTx(COL);
        assertNotNull(txId);
        assertFalse(txId.isEmpty());
        assertTrue(db.commitTx(txId));
    }

    @Test
    @Order(17)
    void transactionRollback() {
        String txId = db.beginTx(COL);
        assertTrue(db.rollbackTx(txId));
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    @Test
    @Order(18)
    void stats() {
        Map<String, Object> s = db.stats(COL);
        assertEquals(COL, s.get("collection"));
        assertTrue(((Number) s.get("record_count")).longValue() >= 2);
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    @Test
    @Order(19)
    void compact() {
        assertTrue(db.compact(COL));
    }

    @Test
    @Order(20)
    void perRecordTtl() {
        long ttlId = db.insert(COL, Map.of("name", "Ephemeral", "role", "temp"), 3600L);
        assertTrue(ttlId > 0);
        // ttlSeconds = 0 (default overload) is sticky — the record stays reachable
        db.update(COL, ttlId, Map.of("name", "Ephemeral", "role", "temp", "touched", true));
        Record record = db.findById(COL, ttlId);
        assertEquals(Boolean.TRUE, record.get("touched"));
    }

    @Test
    @Order(21)
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
    // Keyed CRUD, upsert & compare-and-swap (N1)
    // -----------------------------------------------------------------------

    @Test
    @Order(30)
    void keyedCrudAndCas() {
        final String col = COL + "_keyed";
        db.createCollection(col);
        try {
            // upsert inserts on first call (rev 1) ...
            Record created = db.upsert(col, "user:1", Map.of("name", "Alice", "plan", "free"));
            assertEquals("user:1", created.key());
            assertEquals(1L, created.rev());
            assertEquals("free", created.get("plan"));

            // ... and replaces on the second, bumping rev.
            Record replaced = db.upsert(col, "user:1", Map.of("name", "Alice", "plan", "pro"));
            assertEquals(2L, replaced.rev());
            assertEquals("pro", replaced.get("plan"));

            // findByKey returns the current record + projection (N2).
            Record fetched = db.findByKey(col, "user:1");
            assertEquals("pro", fetched.get("plan"));
            Record projected = db.findByKey(col, "user:1", List.of("plan"));
            assertFalse(projected.data().containsKey("name"), "name should be projected out");
            assertEquals("pro", projected.get("plan"));

            // CAS with the current rev swaps and returns the new record.
            CasResult ok = db.updateIfRev(col, "user:1", replaced.rev(),
                    Map.of("name", "Alice", "plan", "enterprise"));
            assertTrue(ok.swapped());
            assertNotNull(ok.record());
            assertEquals("enterprise", ok.record().get("plan"));
            assertEquals(3L, ok.record().rev());

            // CAS with a stale rev is a clean no-op — never an error.
            CasResult stale = db.updateIfRev(col, "user:1", 1L,
                    Map.of("name", "Alice", "plan", "nope"));
            assertFalse(stale.swapped());
            assertNull(stale.record());
            assertEquals("enterprise", db.findByKey(col, "user:1").get("plan"));

            // updateByKey overwrites, preserving the key and returning the new rev.
            UpdateResult upd = db.updateByKey(col, "user:1", Map.of("name", "Alice", "plan", "team"));
            assertEquals("user:1", upd.key());
            assertEquals(4L, upd.rev());
            assertEquals("team", db.findByKey(col, "user:1").get("plan"));

            // A keyed insert onto a live key raises AlreadyExistsException.
            assertThrows(AlreadyExistsException.class,
                    () -> db.insertKeyed(col, "user:1", Map.of("name", "clash")));

            // deleteByKey removes it; a subsequent lookup raises NotFoundException.
            assertTrue(db.deleteByKey(col, "user:1"));
            assertThrows(NotFoundException.class, () -> db.findByKey(col, "user:1"));
            assertThrows(NotFoundException.class, () -> db.updateByKey(col, "missing", Map.of("x", 1)));
            assertThrows(NotFoundException.class, () -> db.deleteByKey(col, "missing"));
        } finally {
            db.dropCollection(col);
        }
    }

    // -----------------------------------------------------------------------
    // Keyset pagination + multi-field ordering (N3)
    // -----------------------------------------------------------------------

    @Test
    @Order(31)
    void keysetPaginationAndMultiFieldOrder() {
        final String col = COL + "_paging";
        db.createCollection(col);
        try {
            db.insertMany(col, List.of(
                    Map.of("team", "red",  "score", 10),
                    Map.of("team", "blue", "score", 20),
                    Map.of("team", "red",  "score", 30),
                    Map.of("team", "blue", "score", 40),
                    Map.of("team", "red",  "score", 50)
            ));

            // Multi-field sort: team ascending, then score descending.
            List<FileDBClient.Order> order =
                    List.of(FileDBClient.Order.asc("team"), FileDBClient.Order.desc("score"));

            // Walk the collection two records at a time via keyset tokens.
            List<Record> all = new java.util.ArrayList<>();
            String token = "";
            int pages = 0;
            do {
                Page p = db.findPage(col, null, 2, 0, order, null, token);
                all.addAll(p.records());
                token = p.nextPageToken();
                pages++;
                assertTrue(pages <= 10, "keyset pagination did not terminate");
            } while (!token.isEmpty());

            assertEquals(5, all.size());
            // blue group first (asc team), scores descending within each group.
            assertEquals("blue", all.get(0).get("team"));
            assertEquals(40.0, all.get(0).get("score"));
            assertEquals(20.0, all.get(1).get("score"));
            assertEquals("red", all.get(2).get("team"));
            assertEquals(50.0, all.get(2).get("score"));
            assertEquals(30.0, all.get(3).get("score"));
            assertEquals(10.0, all.get(4).get("score"));
        } finally {
            db.dropCollection(col);
        }
    }

    // -----------------------------------------------------------------------
    // Aggregations (N4)
    // -----------------------------------------------------------------------

    @Test
    @Order(32)
    void aggregations() {
        final String col = COL + "_agg";
        db.createCollection(col);
        try {
            db.insertMany(col, List.of(
                    Map.of("team", "red",  "score", 10),
                    Map.of("team", "blue", "score", 20),
                    Map.of("team", "red",  "score", 30),
                    Map.of("team", "blue", "score", 40),
                    Map.of("team", "red",  "score", 50)
            ));

            // count — whole set and filtered.
            assertEquals(5L, db.count(col));
            assertEquals(3L, db.count(col,
                    Map.of("field", "team", "op", "eq", "value", "red")));

            // group-by team with numeric aggregates over score.
            List<AggResult> groups = db.groupBy(col, "team",
                    List.of("sum", "avg", "min", "max"), "score", null);
            assertEquals(2, groups.size());

            AggResult red = groups.stream()
                    .filter(g -> "red".equals(g.group()))
                    .findFirst().orElseThrow();
            assertEquals(3L, red.count());
            assertTrue(red.numeric());
            assertEquals(90.0, red.sum());
            assertEquals(30.0, red.avg());
            assertEquals(10.0, red.min());
            assertEquals(50.0, red.max());

            AggResult blue = groups.stream()
                    .filter(g -> "blue".equals(g.group()))
                    .findFirst().orElseThrow();
            assertEquals(2L, blue.count());
            assertEquals(60.0, blue.sum());
            assertEquals(30.0, blue.avg());

            // Ungrouped aggregate yields a single whole-set group (null group).
            List<AggResult> whole = db.aggregate(col, List.of("sum"), "score", "", null);
            assertEquals(1, whole.size());
            assertNull(whole.get(0).group());
            assertEquals(5L, whole.get(0).count());
            assertEquals(150.0, whole.get(0).sum());
        } finally {
            db.dropCollection(col);
        }
    }

    // -----------------------------------------------------------------------
    // Watch
    // -----------------------------------------------------------------------

    @Test
    @Order(40)
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
    @Order(50)
    void dropCollection() {
        boolean ok = db.dropCollection(COL);
        assertTrue(ok);
        assertFalse(db.listCollections().contains(COL));
    }
}
