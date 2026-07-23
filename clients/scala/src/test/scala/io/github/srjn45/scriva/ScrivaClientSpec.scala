package io.github.srjn45.scriva

import com.google.protobuf.struct.Struct
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, Server, Status}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scriva.v1.{scriva => pb}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * Hermetic tests: an in-process gRPC server backed by an in-memory fake exercises
 * client construction, message round-tripping and streaming — no external server,
 * no TCP ports, parallel-safe.
 */
class ScrivaClientSpec extends AnyFunSuite with BeforeAndAfterAll {

  private implicit val ec: ExecutionContext = ExecutionContext.global
  private var server: Server = _
  private var channel: ManagedChannel = _
  private var db: ScrivaClient = _
  private val fake = new TestService

  override def beforeAll(): Unit = {
    val name = InProcessServerBuilder.generateName()
    server = InProcessServerBuilder.forName(name).directExecutor()
      .addService(pb.ScrivaGrpc.bindService(fake, ec)).build().start()
    channel = InProcessChannelBuilder.forName(name).directExecutor().build()
    db = ScrivaClient.fromChannel(channel, "test-key")
  }

  override def afterAll(): Unit = {
    db.close()
    channel.shutdownNow()
    server.shutdownNow()
  }

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  test("struct round-trip preserves nested values") {
    val original: Map[String, Any] = Map(
      "name" -> "Alice",
      "age" -> 30.0,
      "active" -> true,
      "tags" -> List("a", "b"),
      "nested" -> Map("x" -> 1.0, "y" -> List(true, false)),
    )
    assert(Conversions.structToMap(Conversions.mapToStruct(original)) == original)
  }

  test("client construction + create/insert/findById round-trip") {
    assert(await(db.createCollection("users")) == "users")
    val id = await(db.insert("users", Map("name" -> "Alice", "age" -> 30)))
    assert(id > 0)
    val r = await(db.findById("users", id))
    assert(r("name") == "Alice")
    assert(r("age") == 30.0)
    assert(r.id == id)
    assert(r.rev == 1L)
  }

  test("find streams records and converts the filter to proto") {
    await(db.createCollection("people"))
    await(db.insert("people", Map("name" -> "Alice", "role" -> "admin")))
    await(db.insert("people", Map("name" -> "Bob", "role" -> "user")))

    val records = await(db.find("people",
      filter = Some(Filter.and(
        Filter.field("age", FilterOp.Gt, 18),
        Filter.field("role", FilterOp.Eq, "admin"),
      ))))

    assert(records.size == 2)
    val f = fake.lastFilter.get
    assert(f.kind.isAnd)
    assert(f.getAnd.filters.head.getField.field == "age")
    assert(f.getAnd.filters.head.getField.op == pb.FilterOp.GT)
  }

  test("watch delivers mapped events to the callback") {
    val got = mutable.ListBuffer.empty[WatchEvent]
    val done = Promise[Unit]()
    val handle = db.watch("people") { ev =>
      got += ev
      if (got.size == 2) done.trySuccess(())
    }
    await(done.future)
    handle.close()
    assert(got.map(_.op) == Seq(WatchOp.Inserted, WatchOp.Deleted))
    assert(got(1).record.isEmpty)
  }

  test("NOT_FOUND is translated to NotFoundException") {
    val ex = intercept[NotFoundException](await(db.findByKey("people", "ghost")))
    assert(ex.getMessage.contains("no such key"))
  }
}

/**
 * Minimal in-memory fake. Only the RPCs the tests touch carry real behaviour;
 * the rest fail with UNIMPLEMENTED so the trait is fully satisfied.
 */
class TestService extends pb.ScrivaGrpc.Scriva {
  private val ids = new AtomicLong(0)
  private val store = mutable.Map.empty[String, mutable.ListBuffer[pb.Record]]
  @volatile var lastFilter: Option[pb.Filter] = None

  override def createCollection(request: pb.CreateCollectionRequest): Future[pb.CreateCollectionResponse] = {
    store.getOrElseUpdate(request.name, mutable.ListBuffer.empty)
    Future.successful(pb.CreateCollectionResponse(name = request.name))
  }

  override def insert(request: pb.InsertRequest): Future[pb.InsertResponse] = {
    val id = ids.incrementAndGet()
    store.getOrElseUpdate(request.collection, mutable.ListBuffer.empty) +=
      pb.Record(id = id, rev = 1, data = request.data)
    Future.successful(pb.InsertResponse(id = id, rev = 1))
  }

  override def findById(request: pb.FindByIdRequest): Future[pb.FindResponse] =
    store.get(request.collection).flatMap(_.find(_.id == request.id)) match {
      case Some(r) => Future.successful(pb.FindResponse(record = Some(r)))
      case None    => Future.failed(Status.NOT_FOUND.withDescription("no such id").asRuntimeException())
    }

  override def find(request: pb.FindRequest, responseObserver: StreamObserver[pb.FindResponse]): Unit = {
    lastFilter = request.filter
    store.getOrElse(request.collection, mutable.ListBuffer.empty).foreach { r =>
      responseObserver.onNext(pb.FindResponse(record = Some(r)))
    }
    responseObserver.onCompleted()
  }

  override def findByKey(request: pb.FindByKeyRequest): Future[pb.FindResponse] =
    Future.failed(Status.NOT_FOUND.withDescription("no such key").asRuntimeException())

  override def watch(request: pb.WatchRequest, responseObserver: StreamObserver[pb.WatchEvent]): Unit = {
    responseObserver.onNext(pb.WatchEvent(
      op = pb.WatchOp.INSERTED, collection = request.collection,
      record = Some(pb.Record(id = 1, rev = 1))))
    responseObserver.onNext(pb.WatchEvent(op = pb.WatchOp.DELETED, collection = request.collection))
    responseObserver.onCompleted()
  }

  // -- Untested RPCs: satisfy the trait with UNIMPLEMENTED -----------------

  private def no[T]: Future[T] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
  private def noStream(o: StreamObserver[_]): Unit = o.onError(Status.UNIMPLEMENTED.asRuntimeException())

  override def dropCollection(request: pb.DropCollectionRequest): Future[pb.DropCollectionResponse] = no
  override def listCollections(request: pb.ListCollectionsRequest): Future[pb.ListCollectionsResponse] = no
  override def insertMany(request: pb.InsertManyRequest): Future[pb.InsertManyResponse] = no
  override def update(request: pb.UpdateRequest): Future[pb.UpdateResponse] = no
  override def delete(request: pb.DeleteRequest): Future[pb.DeleteResponse] = no
  override def upsert(request: pb.UpsertRequest): Future[pb.UpsertResponse] = no
  override def updateByKey(request: pb.UpdateByKeyRequest): Future[pb.UpdateResponse] = no
  override def deleteByKey(request: pb.DeleteByKeyRequest): Future[pb.DeleteResponse] = no
  override def updateIfRev(request: pb.UpdateIfRevRequest): Future[pb.UpdateIfRevResponse] = no
  override def ensureIndex(request: pb.EnsureIndexRequest): Future[pb.EnsureIndexResponse] = no
  override def dropIndex(request: pb.DropIndexRequest): Future[pb.DropIndexResponse] = no
  override def listIndexes(request: pb.ListIndexesRequest): Future[pb.ListIndexesResponse] = no
  override def beginTx(request: pb.BeginTxRequest): Future[pb.BeginTxResponse] = no
  override def commitTx(request: pb.CommitTxRequest): Future[pb.CommitTxResponse] = no
  override def rollbackTx(request: pb.RollbackTxRequest): Future[pb.RollbackTxResponse] = no
  override def aggregate(request: pb.AggregateRequest, responseObserver: StreamObserver[pb.AggregateResponse]): Unit = noStream(responseObserver)
  override def collectionStats(request: pb.CollectionStatsRequest): Future[pb.CollectionStatsResponse] = no
  override def compact(request: pb.CompactRequest): Future[pb.CompactResponse] = no
  override def snapshot(request: pb.SnapshotRequest, responseObserver: StreamObserver[pb.SnapshotChunk]): Unit = noStream(responseObserver)
  override def replicate(request: pb.ReplicateRequest, responseObserver: StreamObserver[pb.ReplicationRecord]): Unit = noStream(responseObserver)
  override def replicationStatus(request: pb.ReplicationStatusRequest): Future[pb.ReplicationStatusResponse] = no
  override def promote(request: pb.PromoteRequest): Future[pb.PromoteResponse] = no
}
