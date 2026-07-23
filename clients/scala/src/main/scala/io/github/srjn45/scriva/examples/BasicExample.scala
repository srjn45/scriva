package io.github.srjn45.scriva.examples

import io.github.srjn45.scriva._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * End-to-end example mirroring the Java client's BasicExample: connect, manage
 * collections, CRUD, keyed CRUD + CAS (N1), keyset pagination + multi-field
 * ordering (N3), aggregations (N4), and a callback-based watch.
 *
 * Requires a running server (`make run` from the repo root). Run with:
 *   sbt "runMain io.github.srjn45.scriva.examples.BasicExample"
 */
object BasicExample {
  def main(args: Array[String]): Unit = {
    val host = sys.env.getOrElse("SCRIVA_HOST", "localhost")
    val port = sys.env.getOrElse("SCRIVA_PORT", "5433").toInt
    val apiKey = sys.env.getOrElse("SCRIVA_API_KEY", "dev-key")

    val db = ScrivaClient.connect(host, port, apiKey)
    def await[T](f: scala.concurrent.Future[T]): T = Await.result(f, 10.seconds)

    try {
      val col = "test_scala"
      println("=== Collection management ===")
      println("Created: " + await(db.createCollection(col)))
      await(db.ensureIndex(col, "name"))
      println("Indexes: " + await(db.listIndexes(col)))

      println("\n=== Insert ===")
      val id1 = await(db.insert(col, Map("name" -> "Alice", "age" -> 30, "role" -> "admin")))
      await(db.insertMany(col, Seq(
        Map("name" -> "Bob", "age" -> 25, "role" -> "user"),
        Map("name" -> "Carol", "age" -> 35, "role" -> "user"),
      )))
      println(s"Inserted id1=$id1")

      println("\n=== FindById ===")
      println(await(db.findById(col, id1)))

      println("\n=== Find (age > 25 AND role = user) ===")
      await(db.find(col, filter = Some(Filter.and(
        Filter.field("age", FilterOp.Gt, 25),
        Filter.field("role", FilterOp.Eq, "user"),
      )))).foreach(r => println("  " + r))

      println("\n=== Keyed CRUD + CAS (N1) ===")
      val kcol = col + "_k"
      await(db.createCollection(kcol))
      await(db.upsert(kcol, "user:1", Map("plan" -> "free")))
      val replaced = await(db.upsert(kcol, "user:1", Map("plan" -> "pro")))
      val cas = await(db.updateIfRev(kcol, "user:1", replaced.rev, Map("plan" -> "enterprise")))
      println(s"CAS swapped=${cas.swapped} -> ${cas.record}")

      println("\n=== Keyset pagination + multi-field order (N3) ===")
      val pcol = col + "_p"
      await(db.createCollection(pcol))
      await(db.insertMany(pcol, Seq(
        Map("team" -> "red", "score" -> 10),
        Map("team" -> "blue", "score" -> 20),
        Map("team" -> "red", "score" -> 30),
      )))
      val order = Seq(Order.asc("team"), Order.desc("score"))
      var token = ""
      do {
        val page = await(db.findPage(pcol, limit = 2, orderBy = order, pageToken = token))
        page.records.foreach(r => println(s"  ${r("team")} ${r("score")}"))
        token = page.nextPageToken
      } while (token.nonEmpty)

      println("\n=== Aggregations (N4) ===")
      println("count(all in _p) = " + await(db.count(pcol)))
      await(db.groupBy(pcol, "team", Seq("sum", "avg"), "score"))
        .foreach(g => println(s"  ${g.group} -> count=${g.count} sum=${g.sum}"))

      println("\n=== Watch ===")
      val wcol = col + "_w"
      await(db.createCollection(wcol))
      val handle = db.watch(wcol) { ev => println(s"  [${ev.op}] id=${ev.record.map(_.id).getOrElse(-1L)}") }
      await(db.insert(wcol, Map("event" -> "first")))
      await(db.insert(wcol, Map("event" -> "second")))
      Thread.sleep(500)
      handle.close()

      Seq(col, kcol, pcol, wcol).foreach(c => await(db.dropCollection(c)))
      println("\nDone.")
    } finally db.close()
  }
}
