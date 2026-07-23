package io.github.srjn45.scriva

import com.google.protobuf.ByteString
import io.github.srjn45.scriva.Conversions._
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.stub.{MetadataUtils, StreamObserver}
import io.grpc.{Channel, ClientInterceptors, Context, ManagedChannel, ManagedChannelBuilder, Metadata, Status, StatusRuntimeException}
import scriva.v1.{scriva => pb}

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Idiomatic Scala client for ScrivaDB.
 *
 * Unary RPCs return a [[scala.concurrent.Future]]. The server-streaming `Find`
 * is offered both collected (`find`) and lazily (`findStream: Iterator`);
 * `Watch` uses a cancellable callback. Every call carries the `x-api-key`
 * metadata automatically.
 *
 * {{{
 *   val db = ScrivaClient.connect("localhost", 5433, "dev-key")
 *   for {
 *     _  <- db.createCollection("users")
 *     id <- db.insert("users", Map("name" -> "Alice", "age" -> 30))
 *     r  <- db.findById("users", id)
 *   } yield println(r("name"))
 * }}}
 */
class ScrivaClient private (channel: ManagedChannel, apiKey: String) extends AutoCloseable {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val intercepted: Channel = {
    val md = new Metadata()
    md.put(ScrivaClient.ApiKeyHeader, apiKey)
    ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md))
  }
  private val stub = pb.ScrivaGrpc.stub(intercepted)
  private val blockingStub = pb.ScrivaGrpc.blockingStub(intercepted)

  // -- Error translation ---------------------------------------------------

  private def translate(t: Throwable): Throwable = t match {
    case e: StatusRuntimeException =>
      val msg = Option(e.getStatus.getDescription).getOrElse(e.getMessage)
      e.getStatus.getCode match {
        case Status.Code.NOT_FOUND      => new NotFoundException(msg, e)
        case Status.Code.ALREADY_EXISTS => new AlreadyExistsException(msg, e)
        case _                          => e
      }
    case other => other
  }

  private def call[T](f: => Future[T]): Future[T] =
    f.transform {
      case s: Success[T] => s
      case Failure(e)    => Failure(translate(e))
    }

  private def blocking[T](body: => T): Future[T] = Future(body).transform {
    case s: Success[T] => s
    case Failure(e)    => Failure(translate(e))
  }

  // -- Collection management -----------------------------------------------

  def createCollection(name: String, defaultTtlSeconds: Long = 0): Future[String] =
    call(stub.createCollection(pb.CreateCollectionRequest(name = name, defaultTtlSeconds = defaultTtlSeconds)))
      .map(_.name)

  def dropCollection(name: String): Future[Boolean] =
    call(stub.dropCollection(pb.DropCollectionRequest(name = name))).map(_.ok)

  def listCollections(): Future[Seq[String]] =
    call(stub.listCollections(pb.ListCollectionsRequest())).map(_.names)

  // -- CRUD ----------------------------------------------------------------

  def insert(collection: String, data: Map[String, Any], ttlSeconds: Long = 0, key: String = ""): Future[Long] =
    call(stub.insert(pb.InsertRequest(
      collection = collection, data = Some(mapToStruct(data)), ttlSeconds = ttlSeconds, key = key,
    ))).map(_.id)

  /** Keyed insert under `key`; fails with [[AlreadyExistsException]] if taken. */
  def insertKeyed(collection: String, key: String, data: Map[String, Any]): Future[Long] =
    insert(collection, data, 0, key)

  def insertMany(collection: String, records: Seq[Map[String, Any]], ttlSeconds: Long = 0): Future[Seq[Long]] =
    call(stub.insertMany(pb.InsertManyRequest(
      collection = collection, records = records.map(mapToStruct), ttlSeconds = ttlSeconds,
    ))).map(_.ids)

  def findById(collection: String, id: Long, fields: Seq[String] = Seq.empty): Future[Record] =
    call(stub.findById(pb.FindByIdRequest(collection = collection, id = id, fields = fields)))
      .map(r => recordFromProto(r.getRecord))

  /** Collect the streamed records matching the query into a Future[Seq]. */
  def find(
      collection: String,
      filter: Option[Filter] = None,
      limit: Int = 0,
      offset: Int = 0,
      orderBy: Seq[Order] = Seq.empty,
      fields: Seq[String] = Seq.empty,
      pageToken: String = "",
  ): Future[Seq[Record]] =
    blocking {
      blockingStub.find(buildFindRequest(collection, filter, limit, offset, orderBy, fields, pageToken))
        .flatMap(_.record.map(recordFromProto)).toSeq
    }

  /** Lazily stream the matching records as a blocking Iterator. */
  def findStream(
      collection: String,
      filter: Option[Filter] = None,
      limit: Int = 0,
      offset: Int = 0,
      orderBy: Seq[Order] = Seq.empty,
      fields: Seq[String] = Seq.empty,
      pageToken: String = "",
  ): Iterator[Record] =
    blockingStub.find(buildFindRequest(collection, filter, limit, offset, orderBy, fields, pageToken))
      .flatMap(_.record.map(recordFromProto))

  /** Fetch one keyset page: records + a next-page cursor ("" when last) (N3). */
  def findPage(
      collection: String,
      filter: Option[Filter] = None,
      limit: Int = 0,
      offset: Int = 0,
      orderBy: Seq[Order] = Seq.empty,
      fields: Seq[String] = Seq.empty,
      pageToken: String = "",
  ): Future[Page] =
    blocking {
      val resps = blockingStub.find(buildFindRequest(collection, filter, limit, offset, orderBy, fields, pageToken)).toList
      val records = resps.flatMap(_.record.map(recordFromProto))
      val token = resps.reverseIterator.map(_.pageToken).find(_.nonEmpty).getOrElse("")
      Page(records, token)
    }

  private def buildFindRequest(
      collection: String, filter: Option[Filter], limit: Int, offset: Int,
      orderBy: Seq[Order], fields: Seq[String], pageToken: String,
  ): pb.FindRequest =
    pb.FindRequest(
      collection = collection,
      filter = filter.map(filterToProto),
      limit = limit,
      offset = offset,
      fields = fields,
      pageToken = pageToken,
      orderByFields = orderBy.map(o => pb.OrderBy(field = o.field, desc = o.desc)),
    )

  def update(collection: String, id: Long, data: Map[String, Any], ttlSeconds: Long = 0): Future[Long] =
    call(stub.update(pb.UpdateRequest(
      collection = collection, id = id, data = Some(mapToStruct(data)), ttlSeconds = ttlSeconds,
    ))).map(_.id)

  def delete(collection: String, id: Long): Future[Boolean] =
    call(stub.delete(pb.DeleteRequest(collection = collection, id = id))).map(_.ok)

  // -- Keyed CRUD, upsert & compare-and-swap (N1) --------------------------

  def upsert(collection: String, key: String, data: Map[String, Any]): Future[Record] =
    call(stub.upsert(pb.UpsertRequest(collection = collection, key = key, data = Some(mapToStruct(data)))))
      .map(r => recordFromProto(r.getRecord))

  def findByKey(collection: String, key: String, fields: Seq[String] = Seq.empty): Future[Record] =
    call(stub.findByKey(pb.FindByKeyRequest(collection = collection, key = key, fields = fields)))
      .map(r => recordFromProto(r.getRecord))

  def updateByKey(collection: String, key: String, data: Map[String, Any]): Future[UpdateResult] =
    call(stub.updateByKey(pb.UpdateByKeyRequest(collection = collection, key = key, data = Some(mapToStruct(data)))))
      .map(r => UpdateResult(r.id, r.key, r.rev, r.dateModified))

  def deleteByKey(collection: String, key: String): Future[Boolean] =
    call(stub.deleteByKey(pb.DeleteByKeyRequest(collection = collection, key = key))).map(_.ok)

  /** Compare-and-swap on `key`, conditional on `expectedRev`. A stale rev (or
    * missing key) is a clean no-op reported as `swapped = false`. */
  def updateIfRev(collection: String, key: String, expectedRev: Long, data: Map[String, Any]): Future[CasResult] =
    call(stub.updateIfRev(pb.UpdateIfRevRequest(
      collection = collection, key = key, expectedRev = expectedRev, data = Some(mapToStruct(data)),
    ))).map(r => CasResult(r.swapped, if (r.swapped) r.record.map(recordFromProto) else None))

  // -- Secondary indexes ---------------------------------------------------

  def ensureIndex(collection: String, field: String): Future[Unit] =
    call(stub.ensureIndex(pb.EnsureIndexRequest(collection = collection, field = field))).map(_ => ())

  def dropIndex(collection: String, field: String): Future[Boolean] =
    call(stub.dropIndex(pb.DropIndexRequest(collection = collection, field = field))).map(_.ok)

  def listIndexes(collection: String): Future[Seq[String]] =
    call(stub.listIndexes(pb.ListIndexesRequest(collection = collection))).map(_.fields)

  // -- Transactions --------------------------------------------------------

  def beginTx(collection: String): Future[String] =
    call(stub.beginTx(pb.BeginTxRequest(collection = collection))).map(_.txId)

  def commitTx(txId: String): Future[Boolean] =
    call(stub.commitTx(pb.CommitTxRequest(txId = txId))).map(_.ok)

  def rollbackTx(txId: String): Future[Boolean] =
    call(stub.rollbackTx(pb.RollbackTxRequest(txId = txId))).map(_.ok)

  // -- Watch (server-streaming change feed) --------------------------------

  /**
   * Subscribe to change events on `collection`, invoking `onEvent` for each.
   * Returns an [[AutoCloseable]] handle; `close()` cancels the subscription.
   */
  def watch(collection: String, filter: Option[Filter] = None)(onEvent: WatchEvent => Unit): AutoCloseable = {
    val req = pb.WatchRequest(collection = collection, filter = filter.map(filterToProto))
    val cancellable = Context.current().withCancellation()
    val observer = new StreamObserver[pb.WatchEvent] {
      def onNext(ev: pb.WatchEvent): Unit = onEvent(WatchEvent(
        op = watchOpFromProto(ev.op),
        collection = ev.collection,
        record = ev.record.map(recordFromProto),
        ts = ev.ts.map(isoOf),
      ))
      def onError(t: Throwable): Unit = ()
      def onCompleted(): Unit = ()
    }
    cancellable.run(() => stub.watch(req, observer))
    () => cancellable.cancel(null)
  }

  // -- Aggregations (N4) ---------------------------------------------------

  def aggregate(
      collection: String,
      aggregations: Seq[String] = Seq.empty,
      field: String = "",
      groupBy: String = "",
      filter: Option[Filter] = None,
  ): Future[Seq[AggResult]] =
    blocking {
      val req = pb.AggregateRequest(
        collection = collection,
        filter = filter.map(filterToProto),
        groupBy = groupBy,
        field = field,
        aggregations = aggregations.map(parseAgg),
      )
      blockingStub.aggregate(req).map { a =>
        AggResult(
          group = a.groupValue.map(valueToAny).flatMap(Option(_)),
          count = a.count,
          numeric = a.numeric,
          sum = a.sum, avg = a.avg, min = a.min, max = a.max,
        )
      }.toSeq
    }

  /** Count all live records, or those matching `filter`. */
  def count(collection: String, filter: Option[Filter] = None): Future[Long] =
    aggregate(collection, Seq.empty, "", "", filter).map(_.headOption.map(_.count).getOrElse(0L))

  /** Group live records by `field` and aggregate `metric` per group. */
  def groupBy(collection: String, field: String, aggregations: Seq[String], metric: String,
              filter: Option[Filter] = None): Future[Seq[AggResult]] =
    aggregate(collection, aggregations, metric, field, filter)

  private def parseAgg(op: String): pb.AggregateOp = op.toLowerCase match {
    case "count" => pb.AggregateOp.AGG_COUNT
    case "sum"   => pb.AggregateOp.AGG_SUM
    case "avg"   => pb.AggregateOp.AGG_AVG
    case "min"   => pb.AggregateOp.AGG_MIN
    case "max"   => pb.AggregateOp.AGG_MAX
    case other   => throw new IllegalArgumentException(
      s"unknown aggregation '$other'; expected one of [avg, count, max, min, sum]")
  }

  // -- Stats ---------------------------------------------------------------

  def stats(collection: String): Future[Stats] =
    call(stub.collectionStats(pb.CollectionStatsRequest(collection = collection)))
      .map(r => Stats(r.collection, r.recordCount, r.segmentCount, r.dirtyEntries, r.sizeBytes))

  // -- Maintenance ---------------------------------------------------------

  def compact(collection: String): Future[Boolean] =
    call(stub.compact(pb.CompactRequest(collection = collection))).map(_.ok)

  /** Lazily stream the gzip-tar snapshot archive chunk by chunk. */
  def snapshot(): Iterator[ByteString] =
    blockingStub.snapshot(pb.SnapshotRequest()).map(_.data)

  /** Stream a whole-database snapshot straight to `path`; returns bytes written. */
  def snapshotToFile(path: String): Future[Long] = blocking {
    val out = new BufferedOutputStream(new FileOutputStream(path))
    try {
      var total = 0L
      snapshot().foreach { data => data.writeTo(out); total += data.size() }
      total
    } finally out.close()
  }

  // -- Lifecycle -----------------------------------------------------------

  override def close(): Unit = channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
}

object ScrivaClient {
  private val ApiKeyHeader: Metadata.Key[String] =
    Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER)

  /** Connect over plaintext (no TLS). */
  def connect(host: String, port: Int, apiKey: String): ScrivaClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    new ScrivaClient(channel, apiKey)
  }

  /** Connect over TLS, verifying the server against `tlsCaCert`. */
  def connectTls(host: String, port: Int, apiKey: String, tlsCaCert: File): ScrivaClient = {
    val channel = NettyChannelBuilder.forAddress(host, port)
      .sslContext(GrpcSslContexts.forClient().trustManager(tlsCaCert).build())
      .build()
    new ScrivaClient(channel, apiKey)
  }

  /** Wrap an existing channel (used by tests with an in-process transport). */
  private[scriva] def fromChannel(channel: ManagedChannel, apiKey: String): ScrivaClient =
    new ScrivaClient(channel, apiKey)
}
