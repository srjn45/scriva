package com.srjn45.scriva.examples;

import com.srjn45.scriva.ScrivaDBClient;
import com.srjn45.scriva.ScrivaDBClient.AggResult;
import com.srjn45.scriva.ScrivaDBClient.CasResult;
import com.srjn45.scriva.ScrivaDBClient.Order;
import com.srjn45.scriva.ScrivaDBClient.Page;
import com.srjn45.scriva.ScrivaDBClient.Record;
import com.srjn45.scriva.ScrivaDBClient.UpdateResult;
import com.srjn45.scriva.NotFoundException;
import scriva.v1.ScrivaOuterClass.WatchEvent;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end example: connect, create a collection, insert, query, update, delete,
 * plus the v0.7.0 wire API — keyed CRUD & CAS (N1), field projection (N2), keyset
 * pagination + multi-field ordering (N3), and aggregations (N4) — and watch.
 *
 * Run with:
 *   ./gradlew run -PmainClass=com.srjn45.scriva.examples.BasicExample
 *
 * Or after building:
 *   java -cp build/libs/scriva-client-0.1.0.jar com.srjn45.scriva.examples.BasicExample
 */
public class BasicExample {

    public static void main(String[] args) throws Exception {
        String host   = System.getenv().getOrDefault("SCRIVA_HOST",    "localhost");
        int    port   = Integer.parseInt(System.getenv().getOrDefault("SCRIVA_PORT",    "5433"));
        String apiKey = System.getenv().getOrDefault("SCRIVA_API_KEY", "dev-key");

        try (ScrivaDBClient db = new ScrivaDBClient(host, port, apiKey)) {
            runBasicExample(db);
            runKeyedExample(db);
            runPagingAndAggExample(db);
            runWatchExample(db);
        }
    }

    private static void runBasicExample(ScrivaDBClient db) {
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
        Record record = db.findById(col, id1);
        System.out.println("FindById(" + id1 + "): " + record);

        // ---- Field projection (N2) ----
        System.out.println("\n=== FindById with projection (name only) ===");
        Record projected = db.findById(col, id1, List.of("name"));
        System.out.println("Projected data: " + projected.data());

        // ---- Find with filter ----
        System.out.println("\n=== Find (role=user) ===");
        List<Record> users = db.find(col,
                Map.of("field", "role", "op", "eq", "value", "user"),
                0, 0, "name", false);
        users.forEach(r -> System.out.println("  " + r));

        // ---- Find with AND filter ----
        System.out.println("\n=== Find (age > 25 AND role = user) ===");
        List<Record> filtered = db.find(col,
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

        // ---- Snapshot (whole-database backup) ----
        System.out.println("\n=== Snapshot ===");
        try {
            java.io.File backup = java.io.File.createTempFile("scriva_java_snapshot", ".tar.gz");
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

    /** Keyed CRUD, upsert and optimistic concurrency (N1). */
    private static void runKeyedExample(ScrivaDBClient db) {
        final String col = "keyed_java";
        db.createCollection(col);

        System.out.println("\n=== Keyed CRUD (N1) ===");

        // Upsert inserts on first call, replaces (bumping rev) thereafter.
        Record created = db.upsert(col, "user:1", Map.of("name", "Alice", "plan", "free"));
        System.out.println("Upsert insert: " + created + " (rev=" + created.rev() + ")");

        Record replaced = db.upsert(col, "user:1", Map.of("name", "Alice", "plan", "pro"));
        System.out.println("Upsert replace: " + replaced + " (rev=" + replaced.rev() + ")");

        // Fetch by key.
        Record fetched = db.findByKey(col, "user:1");
        System.out.println("FindByKey: " + fetched);

        // Compare-and-swap: only applies when the expected rev still matches.
        CasResult ok = db.updateIfRev(col, "user:1", replaced.rev(),
                Map.of("name", "Alice", "plan", "enterprise"));
        System.out.println("CAS (fresh rev): swapped=" + ok.swapped() + " -> " + ok.record());

        CasResult stale = db.updateIfRev(col, "user:1", 1L,
                Map.of("name", "Alice", "plan", "downgrade"));
        System.out.println("CAS (stale rev): swapped=" + stale.swapped() + " (no-op)");

        // Update / delete by key.
        UpdateResult upd = db.updateByKey(col, "user:1", Map.of("name", "Alice", "plan", "team"));
        System.out.println("UpdateByKey: id=" + upd.id() + " key=" + upd.key() + " rev=" + upd.rev());

        // Keyed insert rejects a duplicate key.
        try {
            db.insertKeyed(col, "user:1", Map.of("name", "clash"));
        } catch (com.srjn45.scriva.AlreadyExistsException e) {
            System.out.println("Keyed insert on a taken key -> AlreadyExistsException (expected)");
        }

        boolean gone = db.deleteByKey(col, "user:1");
        System.out.println("DeleteByKey: " + gone);
        try {
            db.findByKey(col, "user:1");
        } catch (NotFoundException e) {
            System.out.println("FindByKey after delete -> NotFoundException (expected)");
        }

        db.dropCollection(col);
    }

    /** Keyset pagination + multi-field ordering (N3) and aggregations (N4). */
    private static void runPagingAndAggExample(ScrivaDBClient db) {
        final String col = "analytics_java";
        db.createCollection(col);

        db.insertMany(col, List.of(
                Map.of("name", "Alice", "team", "red",  "score", 10),
                Map.of("name", "Bob",   "team", "blue", "score", 20),
                Map.of("name", "Carol", "team", "red",  "score", 30),
                Map.of("name", "Dave",  "team", "blue", "score", 40),
                Map.of("name", "Eve",   "team", "red",  "score", 50)
        ));

        // ---- Keyset pagination (N3): page by page with a stable multi-field sort ----
        System.out.println("\n=== Keyset pagination + multi-field order (N3) ===");
        List<Order> order = List.of(Order.asc("team"), Order.desc("score"));
        String token = "";
        int page = 0;
        do {
            Page p = db.findPage(col, null, 2, 0, order, null, token);
            System.out.println("Page " + (++page) + ":");
            p.records().forEach(r -> System.out.println("  " + r.get("team") + " " + r.get("score")));
            token = p.nextPageToken();
        } while (!token.isEmpty());

        // ---- Aggregations (N4) ----
        System.out.println("\n=== Aggregations (N4) ===");
        System.out.println("count(all) = " + db.count(col));
        System.out.println("count(team=red) = "
                + db.count(col, Map.of("field", "team", "op", "eq", "value", "red")));

        List<AggResult> byTeam = db.groupBy(col, "team",
                List.of("sum", "avg", "min", "max"), "score", null);
        System.out.println("group by team (sum/avg/min/max of score):");
        for (AggResult g : byTeam) {
            System.out.printf("  %s -> count=%d sum=%.0f avg=%.1f min=%.0f max=%.0f%n",
                    g.group(), g.count(), g.sum(), g.avg(), g.min(), g.max());
        }

        db.dropCollection(col);
    }

    private static void runWatchExample(ScrivaDBClient db) throws InterruptedException {
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
