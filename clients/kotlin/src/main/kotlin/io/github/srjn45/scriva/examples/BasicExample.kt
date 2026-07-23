package io.github.srjn45.scriva.examples

import io.github.srjn45.scriva.FilterOp
import io.github.srjn45.scriva.Order
import io.github.srjn45.scriva.ScrivaClient
import io.github.srjn45.scriva.and
import io.github.srjn45.scriva.field
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * End-to-end example mirroring the Java client's BasicExample: connect, manage
 * collections, CRUD, keyed CRUD + CAS (N1), projection (N2), keyset pagination +
 * multi-field ordering (N3), aggregations (N4), and a Flow-based watch.
 *
 * Requires a running server (`make run` from the repo root). Run with:
 *   ./gradlew run
 */
fun main() = runBlocking {
    val host = System.getenv("SCRIVA_HOST") ?: "localhost"
    val port = (System.getenv("SCRIVA_PORT") ?: "5433").toInt()
    val apiKey = System.getenv("SCRIVA_API_KEY") ?: "dev-key"

    ScrivaClient.connect(host, port, apiKey).use { db ->
        val col = "test_kotlin"
        println("=== Collection management ===")
        println("Created: ${db.createCollection(col)}")
        db.ensureIndex(col, "name")
        println("Indexes: ${db.listIndexes(col)}")

        println("\n=== Insert ===")
        val id1 = db.insert(col, mapOf("name" to "Alice", "age" to 30, "role" to "admin"))
        db.insertMany(col, listOf(
            mapOf("name" to "Bob", "age" to 25, "role" to "user"),
            mapOf("name" to "Carol", "age" to 35, "role" to "user"),
        ))
        println("Inserted id1=$id1")

        println("\n=== FindById ===")
        println(db.findById(col, id1))

        println("\n=== Find (age > 25 AND role = user) ===")
        db.find(col, and(
            field("age", FilterOp.GT, 25),
            field("role", FilterOp.EQ, "user"),
        )).toList().forEach { println("  $it") }

        println("\n=== Keyed CRUD + CAS (N1) ===")
        val created = db.upsert(col + "_k", "user:1", mapOf("plan" to "free"))
        val replaced = db.upsert(col + "_k", "user:1", mapOf("plan" to "pro"))
        val cas = db.updateIfRev(col + "_k", "user:1", replaced.rev, mapOf("plan" to "enterprise"))
        println("CAS swapped=${cas.swapped} -> ${cas.record}")

        println("\n=== Keyset pagination + multi-field order (N3) ===")
        db.createCollection(col + "_p")
        db.insertMany(col + "_p", listOf(
            mapOf("team" to "red", "score" to 10),
            mapOf("team" to "blue", "score" to 20),
            mapOf("team" to "red", "score" to 30),
        ))
        var token = ""
        val order = listOf(Order.asc("team"), Order.desc("score"))
        do {
            val page = db.findPage(col + "_p", limit = 2, orderBy = order, pageToken = token)
            page.records.forEach { println("  ${it["team"]} ${it["score"]}") }
            token = page.nextPageToken
        } while (token.isNotEmpty())

        println("\n=== Aggregations (N4) ===")
        println("count(all in _p) = ${db.count(col + "_p")}")
        db.groupBy(col + "_p", "team", listOf("sum", "avg"), "score")
            .forEach { println("  ${it.group} -> count=${it.count} sum=${it.sum}") }

        println("\n=== Watch ===")
        val watchCol = col + "_w"
        db.createCollection(watchCol)
        val job = launch {
            db.watch(watchCol).collect { println("  [${it.op}] id=${it.record?.id}") }
        }
        db.insert(watchCol, mapOf("event" to "first"))
        db.insert(watchCol, mapOf("event" to "second"))
        kotlinx.coroutines.delay(500)
        job.cancel()

        listOf(col, col + "_k", col + "_p", watchCol).forEach { db.dropCollection(it) }
        println("\nDone.")
    }
}
