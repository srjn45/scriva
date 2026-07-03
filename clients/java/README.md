# FileDB v2 — Java Client

Java 11+ gRPC client for [FileDB v2](../../README.md).

**Maven coordinates:** `com.srjn45:filedbv2-client:0.1.0`

---

## Requirements

- Java 11+
- Gradle 8+ (wrapper included)
- A running FileDB v2 server (`make run` from the repo root)

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
    implementation("com.srjn45:filedbv2-client:0.1.0")
}
```

### Maven

```xml
<dependency>
  <groupId>com.srjn45</groupId>
  <artifactId>filedbv2-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

---

## Quick start

```java
import com.srjn45.filedbv2.FileDBClient;
import java.util.List;
import java.util.Map;

try (FileDBClient db = new FileDBClient("localhost", 5433, "dev-key")) {

    // Collection management
    db.createCollection("users");

    // Insert
    long id = db.insert("users", Map.of("name", "Alice", "age", 30, "role", "admin"));

    // Find by id
    Map<String, Object> record = db.findById("users", id);
    System.out.println(record); // {name=Alice, age=30.0, role=admin}

    // Find with filter
    List<Map<String, Object>> admins = db.find("users",
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
FileDBClient db = new FileDBClient(String host, int port, String apiKey);

// TLS — verifies server against the supplied CA certificate
FileDBClient db = new FileDBClient(String host, int port, String apiKey, File tlsCaCert);
```

`x-api-key` is attached as gRPC metadata on every call automatically.

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

// Find by id
Map<String, Object> record = db.findById("col", id);

// Find with options (all optional — pass null/0/""/false to omit)
List<Map<String, Object>> results = db.find(
        "col",
        filter,       // Map<String,Object> filter or null
        limit,        // int — 0 = no limit
        offset,       // int
        "name",       // String orderBy — "" = no ordering
        false         // boolean descending
);

// Convenience overload — returns all records with no filter or ordering
List<Map<String, Object>> all = db.find("col");

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
import filedb.v1.Filedb.WatchEvent;
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
FileDBClient db = new FileDBClient("myserver.example.com", 5433, "my-api-key", caCert);
```

When no CA cert is supplied the client connects over plaintext (insecure channel).

---

## Running the example

```bash
cd clients/java
FILEDB_API_KEY=dev-key ./gradlew run -PmainClass=com.srjn45.filedbv2.examples.BasicExample
```

---

## Running the tests

Tests connect to a live FileDB server. Start it first:

```bash
# From repo root
make run
```

Then in a separate terminal:

```bash
cd clients/java
./gradlew test
```
