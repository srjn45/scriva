package io.github.srjn45.scriva

import com.google.protobuf.struct.{ListValue, NullValue, Struct, Value}
import com.google.protobuf.timestamp.Timestamp
import scriva.v1.{scriva => pb}

// ---------------------------------------------------------------------------
// Domain value types — idiomatic Scala mirrors of the proto messages.
// ---------------------------------------------------------------------------

/** A record returned from the engine. `key` is "" for keyless records; `rev`
  * is the monotonic per-record revision (starts at 1). */
final case class Record(
    id: Long,
    key: String,
    rev: Long,
    data: Map[String, Any],
    dateAdded: Option[String],
    dateModified: Option[String],
) {
  def hasKey: Boolean = key.nonEmpty
  def apply(field: String): Any = data(field)
  def get(field: String): Option[Any] = data.get(field)
}

/** One keyset page (N3): the records plus a next-page cursor ("" when last). */
final case class Page(records: Seq[Record], nextPageToken: String) {
  def hasNextPage: Boolean = nextPageToken.nonEmpty
}

/** Outcome of an `updateByKey` write. */
final case class UpdateResult(id: Long, key: String, rev: Long, dateModified: String)

/** Outcome of an `updateIfRev` compare-and-swap; `record` is set only when swapped. */
final case class CasResult(swapped: Boolean, record: Option[Record])

/** One group's aggregation result (N4). Numeric aggregates are meaningful only when `numeric`. */
final case class AggResult(
    group: Option[Any],
    count: Long,
    numeric: Boolean,
    sum: Double,
    avg: Double,
    min: Double,
    max: Double,
)

/** Per-collection statistics. */
final case class Stats(
    collection: String,
    recordCount: Long,
    segmentCount: Long,
    dirtyEntries: Long,
    sizeBytes: Long,
)

/** A single sort key for a multi-field order-by (N3). */
final case class Order(field: String, desc: Boolean = false)
object Order {
  def asc(field: String): Order = Order(field, desc = false)
  def desc(field: String): Order = Order(field, desc = true)
}

/** The kind of change reported by `watch`. */
sealed trait WatchOp
object WatchOp {
  case object Unspecified extends WatchOp
  case object Inserted extends WatchOp
  case object Updated extends WatchOp
  case object Deleted extends WatchOp
  case object Overflow extends WatchOp
}

/** A change-feed event. `record` is None for an OVERFLOW event. */
final case class WatchEvent(op: WatchOp, collection: String, record: Option[Record], ts: Option[String])

// ---------------------------------------------------------------------------
// Filters — a small sealed hierarchy mirroring the proto Filter message.
// ---------------------------------------------------------------------------

sealed trait FilterOp
object FilterOp {
  case object Eq extends FilterOp
  case object Neq extends FilterOp
  case object Gt extends FilterOp
  case object Gte extends FilterOp
  case object Lt extends FilterOp
  case object Lte extends FilterOp
  case object Contains extends FilterOp
  case object Regex extends FilterOp
}

sealed trait Filter
object Filter {
  final case class Field(field: String, op: FilterOp, value: String) extends Filter
  final case class And(filters: Seq[Filter]) extends Filter
  final case class Or(filters: Seq[Filter]) extends Filter

  /** A single field comparison; `value` is compared as the field's JSON value. */
  def field(field: String, op: FilterOp, value: Any): Field =
    Field(field, op, if (value == null) "" else value.toString)
  def and(filters: Filter*): And = And(filters)
  def or(filters: Filter*): Or = Or(filters)
}

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

class ScrivaException(message: String, cause: Throwable) extends RuntimeException(message, cause)
class NotFoundException(message: String, cause: Throwable) extends ScrivaException(message, cause)
class AlreadyExistsException(message: String, cause: Throwable) extends ScrivaException(message, cause)

// ---------------------------------------------------------------------------
// Proto <-> Scala conversions (exposed for round-trip testing).
// ---------------------------------------------------------------------------

private[scriva] object Conversions {

  def filterToProto(f: Filter): pb.Filter = f match {
    case Filter.And(fs) => pb.Filter(pb.Filter.Kind.And(pb.AndFilter(fs.map(filterToProto))))
    case Filter.Or(fs)  => pb.Filter(pb.Filter.Kind.Or(pb.OrFilter(fs.map(filterToProto))))
    case Filter.Field(field, op, value) =>
      pb.Filter(pb.Filter.Kind.Field(pb.FieldFilter(field = field, op = opToProto(op), value = value)))
  }

  private def opToProto(op: FilterOp): pb.FilterOp = op match {
    case FilterOp.Eq       => pb.FilterOp.EQ
    case FilterOp.Neq      => pb.FilterOp.NEQ
    case FilterOp.Gt       => pb.FilterOp.GT
    case FilterOp.Gte      => pb.FilterOp.GTE
    case FilterOp.Lt       => pb.FilterOp.LT
    case FilterOp.Lte      => pb.FilterOp.LTE
    case FilterOp.Contains => pb.FilterOp.CONTAINS
    case FilterOp.Regex    => pb.FilterOp.REGEX
  }

  def mapToStruct(m: Map[String, Any]): Struct =
    Struct(fields = m.map { case (k, v) => k -> anyToValue(v) })

  private def anyToValue(v: Any): Value = v match {
    case null            => Value(Value.Kind.NullValue(NullValue.NULL_VALUE))
    case b: Boolean      => Value(Value.Kind.BoolValue(b))
    case i: Int          => Value(Value.Kind.NumberValue(i.toDouble))
    case l: Long         => Value(Value.Kind.NumberValue(l.toDouble))
    case d: Double       => Value(Value.Kind.NumberValue(d))
    case f: Float        => Value(Value.Kind.NumberValue(f.toDouble))
    case s: String       => Value(Value.Kind.StringValue(s))
    case m: Map[_, _]    => Value(Value.Kind.StructValue(mapToStruct(m.asInstanceOf[Map[String, Any]])))
    case xs: Iterable[_] => Value(Value.Kind.ListValue(ListValue(xs.map(anyToValue).toSeq)))
    case other           => Value(Value.Kind.StringValue(other.toString))
  }

  def structToMap(s: Struct): Map[String, Any] =
    s.fields.map { case (k, v) => k -> valueToAny(v) }

  def valueToAny(v: Value): Any = v.kind match {
    case Value.Kind.NullValue(_)   => null
    case Value.Kind.BoolValue(b)   => b
    case Value.Kind.NumberValue(n) => n
    case Value.Kind.StringValue(s) => s
    case Value.Kind.StructValue(s) => structToMap(s)
    case Value.Kind.ListValue(l)   => l.values.map(valueToAny).toList
    case Value.Kind.Empty          => null
  }

  def recordFromProto(r: pb.Record): Record = Record(
    id = r.id,
    key = r.key,
    rev = r.rev,
    data = r.data.map(structToMap).getOrElse(Map.empty),
    dateAdded = r.dateAdded.map(isoOf),
    dateModified = r.dateModified.map(isoOf),
  )

  def isoOf(ts: Timestamp): String =
    java.time.Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong).toString

  def watchOpFromProto(op: pb.WatchOp): WatchOp = op match {
    case pb.WatchOp.INSERTED => WatchOp.Inserted
    case pb.WatchOp.UPDATED  => WatchOp.Updated
    case pb.WatchOp.DELETED  => WatchOp.Deleted
    case pb.WatchOp.OVERFLOW => WatchOp.Overflow
    case _                   => WatchOp.Unspecified
  }
}
