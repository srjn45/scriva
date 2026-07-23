# ScrivaDB — Scala Client

Scala gRPC client for [ScrivaDB](../../README.md), built on
[ScalaPB](https://scalapb.github.io/). Unary RPCs return
`scala.concurrent.Future`; the streaming `Find` is offered both collected
(`find`) and lazily (`findStream: Iterator`), and `Watch` uses a cancellable
callback.

**Coordinates:** `io.github.srjn45:scriva-client-scala_2.13:1.2.1`

---

## Requirements

- JDK 11+
- sbt 1.x (the build pins `sbt.version=1.10.7` in `project/build.properties`)
- Scala 2.13
- A running ScrivaDB server (`make run` from the repo root) for the example / live use

---

## Build

```bash
cd clients/scala
sbt compile        # generate stubs + compile
sbt test           # run the hermetic in-process tests
```

Scala message classes and gRPC stubs are generated from `../../proto/scriva.proto`
by ScalaPB during compilation — no manual `protoc`.

---

## Install (sbt / Maven)

### sbt

```scala
libraryDependencies += "io.github.srjn45" %% "scriva-client-scala" % "1.2.1"
```

### Maven

```xml
<dependency>
  <groupId>io.github.srjn45</groupId>
  <artifactId>scriva-client-scala_2.13</artifactId>
  <version>1.2.1</version>
</dependency>
```

---

## Quick start

```scala
import io.github.srjn45.scriva._
import scala.concurrent.ExecutionContext.Implicits.global

val db = ScrivaClient.connect("localhost", 5433, "dev-key")

for {
  _  <- db.createCollection("users")
  id <- db.insert("users", Map("name" -> "Alice", "age" -> 30, "role" -> "admin"))
  r  <- db.findById("users", id)
  admins <- db.find("users", filter = Some(Filter.field("role", FilterOp.Eq, "admin")))
} yield {
  println(r("name"))            // Alice
  println(s"${r.id} rev=${r.rev}")
  println(admins)
}

// ... later
db.close()
```

`x-api-key` is attached as gRPC metadata on every call. `ScrivaClient` is
`AutoCloseable`.

---

## API reference

### Connecting

```scala
val db = ScrivaClient.connect(host = "localhost", port = 5433, apiKey = "dev-key")

// TLS — verifies the server against the supplied CA certificate
val tls = ScrivaClient.connectTls("host", 5433, "dev-key", new java.io.File("/path/ca.crt"))
```

### Records

Reads return an immutable `Record` case class:

```scala
val r: Record = /* from findById / findByKey / upsert */
r.id            // Long
r.key           // String ("" when keyless)
r.hasKey        // Boolean
r.rev           // Long — per-record revision (starts at 1)
r.data          // Map[String, Any]
r("name")       // shortcut for r.data("name"); r.get("name") returns an Option
r.dateAdded     // Option[String] (ISO-8601)
```

### Collections, CRUD & TTL

```scala
db.createCollection("col")
db.createCollection("sessions", defaultTtlSeconds = 3600)
db.dropCollection("col")
db.listCollections()

db.insert("col", Map("field" -> "value"))
db.insert("sessions", Map("token" -> "abc"), ttlSeconds = 60)
db.insertMany("col", Seq(Map("name" -> "Alice"), Map("name" -> "Bob")))
db.update("col", id, Map("name" -> "new"))
db.delete("col", id)
```

### Keyed CRUD, upsert & compare-and-swap (N1)

```scala
db.insertKeyed("users", "user:42", Map("name" -> "Alice"))   // AlreadyExistsException if taken
db.upsert("users", "user:42", Map("plan" -> "pro"))
db.findByKey("users", "user:42")                             // NotFoundException if absent
db.updateByKey("users", "user:42", Map("plan" -> "team"))
db.deleteByKey("users", "user:42")

db.updateIfRev("users", "user:42", expectedRev = 2L, Map("plan" -> "enterprise"))
  .map(cas => if (cas.swapped) println(cas.record.map(_.rev)))
```

`NOT_FOUND` / `ALREADY_EXISTS` become failed Futures carrying
`NotFoundException` / `AlreadyExistsException` (both extend `ScrivaException`).

### Ordering, projection & keyset pagination (N2/N3)

```scala
db.findById("users", id, fields = Seq("name", "email"))

def walk(token: String): Future[Unit] =
  db.findPage("scores", limit = 50,
      orderBy = Seq(Order.asc("team"), Order.desc("score")), pageToken = token).flatMap { page =>
    page.records.foreach(r => /* ... */)
    if (page.hasNextPage) walk(page.nextPageToken) else Future.unit
  }
```

### Aggregations (N4)

```scala
db.count("orders")
db.count("orders", Some(Filter.field("status", FilterOp.Eq, "shipped")))
db.groupBy("orders", "region", Seq("sum", "avg", "min", "max"), "total")
  .map(_.foreach(g => println(s"${g.group}: count=${g.count} sum=${g.sum}")))
```

### Watch (streaming change feed)

```scala
val handle = db.watch("col") { event =>
  println(s"${event.op} id=${event.record.map(_.id)}")
}
// ... later
handle.close()   // cancels the subscription
```

### Indexes, transactions, stats & maintenance

```scala
db.ensureIndex("col", "name"); db.dropIndex("col", "name"); db.listIndexes("col")
db.beginTx("col"); db.commitTx(txId); db.rollbackTx(txId)
db.stats("col")                 // Stats(collection, recordCount, segmentCount, dirtyEntries, sizeBytes)
db.compact("col")
db.snapshotToFile("backup.tar.gz")
```

---

## Filters

```scala
Filter.field("age", FilterOp.Gt, 30)

Filter.and(
  Filter.field("age", FilterOp.Gte, 18),
  Filter.field("name", FilterOp.Contains, "alice"),
)

Filter.or(
  Filter.field("status", FilterOp.Eq, "active"),
  Filter.field("role", FilterOp.Eq, "admin"),
)
```

`FilterOp` values: `Eq Neq Gt Gte Lt Lte Contains Regex`.

---

## Running the example

```bash
# From the repo root, start a server:
make run
# Then:
cd clients/scala
SCRIVA_API_KEY=dev-key sbt "runMain io.github.srjn45.scriva.examples.BasicExample"
```

---

## Running the tests

Tests are hermetic — an in-process gRPC server is started per suite, so no
external ScrivaDB is required:

```bash
cd clients/scala
sbt test
```
