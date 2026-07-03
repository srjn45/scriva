package com.srjn45.filedbv2.examples;

import com.srjn45.filedbv2.FileDBClient;
import filedb.v1.Filedb.WatchEvent;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end example: connect, create a collection, insert, query, update, delete, watch.
 *
 * Run with:
 *   ./gradlew run -PmainClass=com.srjn45.filedbv2.examples.BasicExample
 *
 * Or after building:
 *   java -cp build/libs/filedbv2-client-0.1.0.jar com.srjn45.filedbv2.examples.BasicExample
 */
public class BasicExample {

    public static void main(String[] args) throws Exception {
        String host   = System.getenv().getOrDefault("FILEDB_HOST",    "localhost");
        int    port   = Integer.parseInt(System.getenv().getOrDefault("FILEDB_PORT",    "5433"));
        String apiKey = System.getenv().getOrDefault("FILEDB_API_KEY", "dev-key");

        try (FileDBClient db = new FileDBClient(host, port, apiKey)) {
            runBasicExample(db);
            runWatchExample(db);
        }
    }

    private static void runBasicExample(FileDBClient db) {
        final String col = "test_java";

        // ---- Create collection ----
        System.out.println("=== Collection management ===");
        String created = db.createCollection(col);
        System.out.println("Created: " + created);
        System.out.println("Collections: " + db.listCollections());

        // ---- Ensure index ----
        db.ensureIndex(col, "name");
        System.out.println("Indexes: " + db.listIndexes(col));

        // ---- Insert records ----
        System.out.println("\n=== Insert ===");
        long id1 = db.insert(col, Map.of("name", "Alice", "age", 30, "role", "admin"));
        long id2 = db.insert(col, Map.of("name", "Bob",   "age", 25, "role", "user"));
        long id3 = db.insert(col, Map.of("name", "Carol", "age", 35, "role", "user"));
        System.out.printf("Inserted ids: %d, %d, %d%n", id1, id2, id3);

        // ---- Insert many ----
        List<Long> ids = db.insertMany(col, List.of(
                Map.of("name", "Dave", "age", 28, "role", "user"),
                Map.of("name", "Eve",  "age", 22, "role", "admin")
        ));
        System.out.println("InsertMany ids: " + ids);

        // ---- Find by id ----
        System.out.println("\n=== FindById ===");
        Map<String, Object> record = db.findById(col, id1);
        System.out.println("FindById(" + id1 + "): " + record);

        // ---- Find with filter ----
        System.out.println("\n=== Find (role=user) ===");
        List<Map<String, Object>> users = db.find(col,
                Map.of("field", "role", "op", "eq", "value", "user"),
                0, 0, "name", false);
        users.forEach(r -> System.out.println("  " + r));

        // ---- Find with AND filter ----
        System.out.println("\n=== Find (age > 25 AND role = user) ===");
        List<Map<String, Object>> filtered = db.find(col,
                Map.of("and", List.of(
                        Map.of("field", "age",  "op", "gt",  "value", "25"),
                        Map.of("field", "role", "op", "eq",  "value", "user")
                )), 0, 0, "", false);
        filtered.forEach(r -> System.out.println("  " + r));

        // ---- Update ----
        System.out.println("\n=== Update ===");
        long updated = db.update(col, id2, Map.of("name", "Bob", "age", 26, "role", "moderator"));
        System.out.println("Updated id: " + updated);
        System.out.println("After update: " + db.findById(col, id2));

        // ---- Delete ----
        System.out.println("\n=== Delete ===");
        boolean deleted = db.delete(col, id3);
        System.out.println("Deleted id " + id3 + ": " + deleted);

        // ---- Stats ----
        System.out.println("\n=== Stats ===");
        Map<String, Object> stats = db.stats(col);
        stats.forEach((k, v) -> System.out.printf("  %s: %s%n", k, v));

        // ---- Compaction ----
        System.out.println("\n=== Compact ===");
        boolean compacted = db.compact(col);
        System.out.println("Compacted: " + compacted);

        // ---- Per-record TTL ----
        System.out.println("\n=== Per-record TTL ===");
        long ttlId = db.insert(col, Map.of("name", "Ephemeral", "role", "temp"), 3600L);
        System.out.println("Inserted #" + ttlId + " with a 3600s TTL");
        // ttlSeconds = 0 (default overload) is sticky — keeps the existing deadline
        db.update(col, ttlId, Map.of("name", "Ephemeral", "role", "temp", "touched", true));
        System.out.println("Updated the TTL record (deadline preserved)");

        // ---- Snapshot (whole-database backup) ----
        System.out.println("\n=== Snapshot ===");
        try {
            java.io.File backup = java.io.File.createTempFile("filedb_java_snapshot", ".tar.gz");
            long bytes = db.snapshotToFile(backup.getAbsolutePath());
            System.out.printf("Wrote %d bytes to %s%n", bytes, backup.getAbsolutePath());
            backup.delete();
        } catch (java.io.IOException e) {
            System.err.println("Snapshot failed: " + e.getMessage());
        }

        // ---- Drop collection ----
        System.out.println("\n=== Drop collection ===");
        boolean dropped = db.dropCollection(col);
        System.out.println("Dropped: " + dropped);
        System.out.println("Collections after drop: " + db.listCollections());
    }

    private static void runWatchExample(FileDBClient db) throws InterruptedException {
        final String col = "watch_java";
        db.createCollection(col);

        CountDownLatch latch = new CountDownLatch(3);

        System.out.println("\n=== Watch ===");
        db.watch(col, null, new StreamObserver<WatchEvent>() {
            @Override
            public void onNext(WatchEvent event) {
                System.out.printf("  [%s] %s id=%d%n",
                        event.getOp(),
                        event.getCollection(),
                        event.getRecord().getId());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Watch error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Watch stream closed.");
            }
        });

        // Trigger a few inserts so the watch observer fires
        Thread.sleep(200);
        db.insert(col, Map.of("event", "first"));
        db.insert(col, Map.of("event", "second"));
        db.insert(col, Map.of("event", "third"));

        boolean received = latch.await(5, TimeUnit.SECONDS);
        System.out.println("Received all watch events: " + received);

        db.dropCollection(col);
    }
}
