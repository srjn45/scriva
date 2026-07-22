# ScrivaDB — Java Client

Java 11+ gRPC client for [ScrivaDB](../../README.md).

**Maven coordinates:** `com.srjn45:scriva-client:0.1.0`

---

## Requirements

- Java 11+
- Gradle 8+ (wrapper included)
- A running ScrivaDB server (`make run` from the repo root)

---

## Build

```bash
cd clients/java
./gradlew build           # compile + run tests (requires live server)
./gradlew compileJava     # compile only
```

Proto stubs are generated automatically by the `com.google.protobuf` Gradle plugin during compilation. No manual `protoc` invocation is needed.

---

## Install (Maven / Gradle)

> **Note:** Replace `0.1.0` with the published version once released to Maven Central.

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("com.srjn45:scriva-client:0.1.0")
}
```

### Maven

```xml
<dependency>
  <groupId>com.srjn45</groupId>
  <artifactId>scriva-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

---

## Quick start

```java
import com.srjn45.scriva.ScrivaDBClient;
import java.util.List;
import java.util.Map;

try (ScrivaDBClient db = new ScrivaDBClient("localhost", 5433, "dev-key")) {

    // Collection management
    db.createCollection("users");

    // Insert
    long id = db.insert("users", Map.of("name", "Alice", "age", 30, "role", "admin"));

    // Find by id — returns a Record value object
    ScrivaDBClient.Record record = db.findById("users", id);
    System.out.println(record.get("name")); // Alice
    System.out.println(record.id() + " rev=" + record.rev());

    // Find with filter
    List<ScrivaDBClient.Record> admins = db.find("users",
            Map.of("field", "role", "op", "eq", "value", "admin"),
            0, 0, "name", false);

    // Update
    db.update("users", id, Map.of("name", "Alice", "age", 31, "role", "superadmin"));

    // Delete
    db.delete("users", id);

    // Stats
    Map<String, Object> stats = db.stats("users");

    // Drop
    db.dropCollection("users");
}
```

---

## API reference

### Constructor

```java
// Plaintext (no TLS)
ScrivaDBClient db = new ScrivaDBClient(String host, int port, String apiKey);

// TLS — verifies server against the supplied CA certificate
ScrivaDBClient db = new ScrivaDBClient(String host, int port, String apiKey, File tlsCaCert);
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

### Records

Reads return `ScrivaDBClient.Record` value objects rather than raw maps:

```java
ScrivaDBClient.Record r = db.findById("users", id);

long   id           = r.id();            // server-assigned numeric id
String key          = r.key();           // caller-supplied key ("" when keyless)
boolean keyed       = r.hasKey();
long   rev          = r.rev();           // per-record revision (starts at 1)
Map<String,Object> data = r.data();      // the decoded document
Object name         = r.get("name");     // shortcut for data().get("name")
String created      = r.dateAdded();     // ISO-8601, may be null
String modified     = r.dateModified();  // ISO-8601, may be null
```

---

### Collection management

```java
String name          = db.createCollection("col");
boolean ok           = db.dropCollection("col");
List<String> names   = db.listCollections();

// Give the collection a default per-record TTL (seconds). Records inserted
// without an explicit ttl then expire this many seconds after being written.
String sessions      = db.createCollection("sessions", 3600L);
```

---

### CRUD

```java
// Insert one record — returns the assigned id
long id = db.insert("col", Map.of("field", "value"));

// Insert multiple records — returns list of ids in insertion order
List<Long> ids = db.insertMany("col", List.of(
        Map.of("name", "Alice"),
        Map.of("name", "Bob")
));

// Find by id — returns a Record
ScrivaDBClient.Record record = db.findById("col", id);

// Find with options (all optional — pass null/0/""/false to omit)
List<ScrivaDBClient.Record> results = db.find(
        "col",
        filter,       // Map<String,Object> filter or null
        limit,        // int — 0 = no limit
        offset,       // int
        "name",       // String orderBy — "" = no ordering
        false         // boolean descending
);

// Convenience overloads
List<ScrivaDBClient.Record> all      = db.find("col");
List<ScrivaDBClient.Record> filtered = db.find("col", filter);

// Update — returns updated id
long updatedId = db.update("col", id, Map.of("name", "new value"));

// Delete — returns true if record existed
boolean deleted = db.delete("col", id);
```

#### Per-record TTL

`insert`, `insertMany`, and `update` each have an overload taking a `long ttlSeconds`:

```java
// Expire this record 60 seconds from now, regardless of the collection default.
long id = db.insert("sessions", Map.of("token", "abc"), 60L);

// Same TTL applied to every record in the batch.
List<Long> ids = db.insertMany("sessions",
        List.of(Map.of("token", "a"), Map.of("token", "b")), 60L);

// On update, ttlSeconds > 0 resets the expiry; the no-ttl overload (or 0) is
// sticky and leaves the existing deadline untouched.
db.update("sessions", id, Map.of("token", "abc", "seen", true), 120L);
```

`ttlSeconds = 0` inherits the collection's default TTL on insert; a value greater
than 0 overrides it. Negative values are rejected by the server.

---

### Keyed CRUD, upsert & compare-and-swap (N1)

Records can carry a caller-supplied string primary **key**, and every live record
has a monotonic **revision** (`rev`, starting at 1, bumped on each write). These
power keyed lookups and optimistic-concurrency updates.

```java
// Keyed insert — rejected with AlreadyExistsException if the key is taken
long id = db.insertKeyed("users", "user:42", Map.of("name", "Alice"));
// (equivalently: db.insert("users", data, 0L, "user:42"))

// Upsert — insert under a key, or replace the existing keyed record (bumping rev)
ScrivaDBClient.Record r = db.upsert("users", "user:42", Map.of("name", "Alice", "plan", "pro"));

// Fetch by key — raises NotFoundException when no live record carries the key
ScrivaDBClient.Record found = db.findByKey("users", "user:42");

// Overwrite by key, preserving the key — returns id/key/rev/dateModified
ScrivaDBClient.UpdateResult upd = db.updateByKey("users", "user:42", Map.of("name", "Alice", "plan", "team"));
System.out.println(upd.rev());

// Delete by key — raises NotFoundException if absent
boolean gone = db.deleteByKey("users", "user:42");

// Compare-and-swap: apply the write only if the current rev matches expectedRev.
// A stale rev (or missing key) is a clean no-op — never an error.
ScrivaDBClient.CasResult cas = db.updateIfRev("users", "user:42", r.rev(),
        Map.of("name", "Alice", "plan", "enterprise"));
if (cas.swapped()) {
    System.out.println("new rev = " + cas.record().rev());
}
```

Typed exceptions (both extend `ScrivaDBException`, a `RuntimeException`):

| gRPC status | Exception |
|---|---|
| `NOT_FOUND` | `NotFoundException` |
| `ALREADY_EXISTS` | `AlreadyExistsException` |

---

### Field projection (N2)

`findById`, `findByKey` and `find`/`findPage` accept an optional list of top-level
fields to return in each record's `data`. `id`, `key` and `rev` are always
included; unknown fields are silently omitted.

```java
ScrivaDBClient.Record r = db.findById("users", id, List.of("name", "email"));
ScrivaDBClient.Record k = db.findByKey("users", "user:42", List.of("name"));

// find/findPage take fields as the 6th argument (see below)
```

---

### Keyset pagination & multi-field ordering (N3)

`findPage` walks a collection page by page using an opaque keyset cursor —
O(page) rather than O(offset). Ordering is a list of `ScrivaDBClient.Order` sort
keys applied lexicographically; the record id is always the final tiebreaker, so
pagination is stable.

```java
List<ScrivaDBClient.Order> order = List.of(
        ScrivaDBClient.Order.asc("team"),
        ScrivaDBClient.Order.desc("score")
);

String token = "";
do {
    ScrivaDBClient.Page page = db.findPage(
            "scores",
            null,        // filter
            50,          // limit (page size)
            0,           // offset — keep 0 with a page token
            order,       // multi-field ordering
            null,        // fields projection (N2), or null for all
            token        // keyset cursor, "" for the first page
    );
    for (ScrivaDBClient.Record r : page.records()) {
        // ...
    }
    token = page.nextPageToken();   // "" once the last page is reached
} while (!token.isEmpty());
```

`find(collection, filter, limit, offset, List<Order>, fields, pageToken)` is the
same call when you only want the records and not the next-page cursor. Keep the
same filter, ordering and limit on every page.

---

### Aggregations (N4)

```java
// Count all live records, or only those matching a filter
long total   = db.count("orders");
long shipped = db.count("orders", Map.of("field", "status", "op", "eq", "value", "shipped"));

// Group by a field with numeric aggregates over another field
List<ScrivaDBClient.AggResult> byRegion = db.groupBy(
        "orders",
        "region",                             // group-by field
        List.of("sum", "avg", "min", "max"),  // which aggregations
        "total",                              // numeric metric field
        null                                  // optional filter
);
for (ScrivaDBClient.AggResult g : byRegion) {
    System.out.printf("%s: count=%d sum=%.2f avg=%.2f%n",
            g.group(), g.count(), g.sum(), g.avg());
}

// Or call aggregate directly (group() is null for the whole-set group)
List<ScrivaDBClient.AggResult> whole = db.aggregate(
        "orders", List.of("sum"), "total", "" /* no group-by */, null);
```

Each `AggResult` carries `group()`, `count()` and `numeric()`; the `sum()`,
`avg()`, `min()`, `max()` accessors are meaningful only when `numeric()` is true
(i.e. the group held at least one numeric value for the aggregate field).

---

### Secondary indexes

```java
db.ensureIndex("col", "fieldName");
boolean ok          = db.dropIndex("col", "fieldName");
List<String> fields = db.listIndexes("col");
```

---

### Transactions

```java
String txId = db.beginTx("col");
boolean committed  = db.commitTx(txId);
boolean rolledBack = db.rollbackTx(txId);
```

---

### Watch (streaming change feed)

```java
import scriva.v1.Scriva.WatchEvent;
import io.grpc.stub.StreamObserver;

db.watch("col", null /* optional filter */, new StreamObserver<WatchEvent>() {
    @Override
    public void onNext(WatchEvent event) {
        System.out.println(event.getOp() + " id=" + event.getRecord().getId());
    }
    @Override public void onError(Throwable t)  { t.printStackTrace(); }
    @Override public void onCompleted()          { System.out.println("done"); }
});
```

Events are delivered on the gRPC executor thread. The stream runs until the server closes it or the channel is shut down.

---

### Stats

```java
Map<String, Object> stats = db.stats("col");
// Keys: collection, record_count, segment_count, dirty_entries, size_bytes
```

---

### Maintenance

```java
// Force a synchronous compaction of a collection — merges dirty segments and
// reclaims space from deleted/overwritten records. Returns true on success.
boolean ok = db.compact("col");

// Stream a consistent gzip-compressed tar snapshot of the whole database
// straight to a file. Returns the number of bytes written; restore with
// `tar xzf backup.tar.gz`.
long bytes = db.snapshotToFile("backup.tar.gz");

// Or consume the raw archive chunks yourself (Snapshot is server-streaming):
Iterator<SnapshotChunk> chunks = db.snapshot();
while (chunks.hasNext()) {
    ByteString data = chunks.next().getData();
    // ...
}
```

---

## Filter syntax

Filters are plain `Map<String, Object>` values that mirror the proto `Filter` message.

### Field filter

```java
Map.of("field", "age", "op", "gt", "value", "30")
```

### AND composite

```java
Map.of("and", List.of(
    Map.of("field", "age",  "op", "gte", "value", "18"),
    Map.of("field", "name", "op", "contains", "value", "alice")
))
```

### OR composite

```java
Map.of("or", List.of(
    Map.of("field", "status", "op", "eq", "value", "active"),
    Map.of("field", "role",   "op", "eq", "value", "admin")
))
```

### Supported `op` values

| op | Meaning |
|---|---|
| `eq` | equal |
| `neq` | not equal |
| `gt` | greater than |
| `gte` | greater than or equal |
| `lt` | less than |
| `lte` | less than or equal |
| `contains` | string contains (substring) |
| `regex` | regular expression match |

---

## TLS

```java
File caCert = new File("/path/to/ca.crt");
ScrivaDBClient db = new ScrivaDBClient("myserver.example.com", 5433, "my-api-key", caCert);
```

When no CA cert is supplied the client connects over plaintext (insecure channel).

---

## Running the example

```bash
cd clients/java
SCRIVA_API_KEY=dev-key ./gradlew run -PmainClass=com.srjn45.scriva.examples.BasicExample
```

---

## Running the tests

Tests connect to a live ScrivaDB server. Start it first:

```bash
# From repo root
make run
```

Then in a separate terminal:

```bash
cd clients/java
./gradlew test
```
