# ScrivaDB — Kotlin Client

Kotlin gRPC client for [ScrivaDB](../../README.md), built on `grpc-kotlin` +
coroutines. Unary calls are `suspend` functions; the streaming `Find`, `Watch`,
`Aggregate` and `Snapshot` RPCs return a `Flow`.

**Maven coordinates:** `io.github.srjn45:scriva-client-kotlin:1.2.1`

---

## Requirements

- JDK 11+ (targets JVM 11 bytecode)
- Gradle 8+ (wrapper included)
- A running ScrivaDB server (`make run` from the repo root) for the example / live tests

---

## Build

```bash
cd clients/kotlin
./gradlew build            # generate stubs + compile + run the hermetic tests
./gradlew compileKotlin    # compile only
```

Proto stubs are generated automatically from `../../proto/scriva.proto` by the
`com.google.protobuf` Gradle plugin (grpc-java + grpc-kotlin). No manual `protoc`.

---

## Install (Gradle / Maven)

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.github.srjn45:scriva-client-kotlin:1.2.1")
}
```

### Maven

```xml
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>scriva-client-kotlin</artifactId>
  <version>1.2.1</version>
</dependency>
```

---

## Quick start

```kotlin
import io.github.srjn45.scriva.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

runBlocking {
    ScrivaClient.connect("localhost", 5433, "dev-key").use { db ->
        db.createCollection("users")

        val id = db.insert("users", mapOf("name" to "Alice", "age" to 30, "role" to "admin"))

        val record = db.findById("users", id)
        println(record["name"])                 // Alice
        println("${record.id} rev=${record.rev}")

        // Streaming find returns a Flow<Record>
        val admins: List<Record> = db.find(
            "users",
            filter = field("role", FilterOp.EQ, "admin"),
        ).toList()

        db.update("users", id, mapOf("name" to "Alice", "age" to 31, "role" to "superadmin"))
        db.delete("users", id)
        db.dropCollection("users")
    }
}
```

---

## API reference

### Connecting

```kotlin
// Plaintext
val db = ScrivaClient.connect(host = "localhost", port = 5433, apiKey = "dev-key")

// TLS — verifies the server against the supplied CA certificate
val db = ScrivaClient.connectTls("host", 5433, "dev-key", java.io.File("/path/ca.crt"))
```

`x-api-key` is attached as gRPC metadata on every call. `ScrivaClient` is
`Closeable` — use `use { }` or call `close()`.

### Records

Reads return an immutable `Record` data class:

```kotlin
val r: Record = db.findById("users", id)
r.id            // Long — server-assigned id
r.key           // String — caller-supplied key ("" when keyless)
r.hasKey        // Boolean
r.rev           // Long — per-record revision (starts at 1)
r.data          // Map<String, Any?> — the decoded document
r["name"]       // shortcut for r.data["name"]
r.dateAdded     // String? ISO-8601
r.dateModified  // String?
```

### Collections, CRUD & TTL

```kotlin
db.createCollection("col")
db.createCollection("sessions", defaultTtlSeconds = 3600)
db.dropCollection("col")
db.listCollections()

val id  = db.insert("col", mapOf("field" to "value"))
val id2 = db.insert("sessions", mapOf("token" to "abc"), ttlSeconds = 60)
val ids = db.insertMany("col", listOf(mapOf("name" to "Alice"), mapOf("name" to "Bob")))
db.update("col", id, mapOf("name" to "new"))
db.delete("col", id)
```

### Keyed CRUD, upsert & compare-and-swap (N1)

```kotlin
val id = db.insertKeyed("users", "user:42", mapOf("name" to "Alice"))  // AlreadyExistsException if taken
val r  = db.upsert("users", "user:42", mapOf("plan" to "pro"))
val f  = db.findByKey("users", "user:42")                              // NotFoundException if absent
val u  = db.updateByKey("users", "user:42", mapOf("plan" to "team"))
db.deleteByKey("users", "user:42")

val cas = db.updateIfRev("users", "user:42", r.rev, mapOf("plan" to "enterprise"))
if (cas.swapped) println(cas.record?.rev)
```

Errors map to typed exceptions: `NotFoundException` (`NOT_FOUND`) and
`AlreadyExistsException` (`ALREADY_EXISTS`), both `ScrivaException`.

### Field projection (N2), ordering & keyset pagination (N3)

```kotlin
val r = db.findById("users", id, fields = listOf("name", "email"))

var token = ""
do {
    val page: Page = db.findPage(
        "scores", limit = 50,
        orderBy = listOf(Order.asc("team"), Order.desc("score")),
        pageToken = token,
    )
    page.records.forEach { /* ... */ }
    token = page.nextPageToken
} while (token.isNotEmpty())
```

### Aggregations (N4)

```kotlin
val total   = db.count("orders")
val shipped = db.count("orders", field("status", FilterOp.EQ, "shipped"))

val byRegion = db.groupBy("orders", "region", listOf("sum", "avg", "min", "max"), "total")
byRegion.forEach { g -> println("${g.group}: count=${g.count} sum=${g.sum}") }
```

### Watch (streaming change feed)

```kotlin
db.watch("col").collect { event ->
    println("${event.op} id=${event.record?.id}")
}
```

`watch` returns `Flow<WatchEvent>`; collect it inside a coroutine and cancel the
job (or the enclosing scope) to stop the stream.

### Indexes, transactions, stats & maintenance

```kotlin
db.ensureIndex("col", "name"); db.dropIndex("col", "name"); db.listIndexes("col")

val tx = db.beginTx("col"); db.commitTx(tx); db.rollbackTx(tx)

val s: Stats = db.stats("col")            // recordCount, segmentCount, dirtyEntries, sizeBytes
db.compact("col")
val bytes = db.snapshotToFile("backup.tar.gz")
```

---

## Filters

Filters are an idiomatic sealed hierarchy. Build them with `field`, `and`, `or`:

```kotlin
field("age", FilterOp.GT, 30)

and(
    field("age", FilterOp.GTE, 18),
    field("name", FilterOp.CONTAINS, "alice"),
)

or(
    field("status", FilterOp.EQ, "active"),
    field("role", FilterOp.EQ, "admin"),
)
```

`FilterOp` values: `EQ NEQ GT GTE LT LTE CONTAINS REGEX`.

---

## Running the example

```bash
# From the repo root, start a server:
make run
# Then:
cd clients/kotlin
SCRIVA_API_KEY=dev-key ./gradlew run
```

---

## Running the tests

The bundled tests are hermetic — they spin up an in-process gRPC server, so no
external ScrivaDB is required:

```bash
cd clients/kotlin
./gradlew test
```
