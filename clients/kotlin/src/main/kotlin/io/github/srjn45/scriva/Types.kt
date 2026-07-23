package io.github.srjn45.scriva

import com.google.protobuf.ListValue
import com.google.protobuf.NullValue
import com.google.protobuf.Struct
import com.google.protobuf.Value
import java.time.Instant
import scriva.v1.ScrivaOuterClass

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/**
 * A record returned from the engine.
 *
 * [id] is the server-assigned numeric id; [key] is the caller-supplied string
 * key (`""` for keyless records); [rev] is the monotonic per-record revision
 * (starts at 1, bumped on every write); [data] is the decoded document.
 */
data class Record(
    val id: Long,
    val key: String,
    val rev: Long,
    val data: Map<String, Any?>,
    val dateAdded: String?,
    val dateModified: String?,
) {
    /** True when this record carries a caller-supplied key. */
    val hasKey: Boolean get() = key.isNotEmpty()

    /** Shortcut for `data[field]`. */
    operator fun get(field: String): Any? = data[field]
}

/**
 * One keyset page from [ScrivaClient.findPage]: the records plus the next-page
 * cursor. An empty [nextPageToken] means the last page was reached.
 */
data class Page(val records: List<Record>, val nextPageToken: String) {
    /** True when a further page remains under the requested ordering. */
    val hasNextPage: Boolean get() = nextPageToken.isNotEmpty()
}

/**
 * The outcome of an [ScrivaClient.updateByKey] write: the affected record's id,
 * key, revision (after the write) and last-modified timestamp.
 */
data class UpdateResult(val id: Long, val key: String, val rev: Long, val dateModified: String)

/**
 * The outcome of an [ScrivaClient.updateIfRev] compare-and-swap. [record] is
 * populated only when [swapped] is `true`.
 */
data class CasResult(val swapped: Boolean, val record: Record?)

/**
 * One group's aggregation result (N4). [group] is the group-by value (`null`
 * for the whole-set group); the numeric aggregates are meaningful only when
 * [numeric] is `true`.
 */
data class AggResult(
    val group: Any?,
    val count: Long,
    val numeric: Boolean,
    val sum: Double,
    val avg: Double,
    val min: Double,
    val max: Double,
)

/** Per-collection statistics returned by [ScrivaClient.stats]. */
data class Stats(
    val collection: String,
    val recordCount: Long,
    val segmentCount: Long,
    val dirtyEntries: Long,
    val sizeBytes: Long,
)

/** A single sort key for a multi-field order-by (N3): a field name and a direction. */
data class Order(val field: String, val desc: Boolean = false) {
    companion object {
        fun asc(field: String) = Order(field, false)
        fun desc(field: String) = Order(field, true)
    }
}

/** The kind of change reported by [ScrivaClient.watch]. */
enum class WatchOp { UNSPECIFIED, INSERTED, UPDATED, DELETED, OVERFLOW }

/**
 * A change-feed event. [record] is null for an OVERFLOW event (the server
 * dropped events because this subscriber fell behind; resync).
 */
data class WatchEvent(
    val op: WatchOp,
    val collection: String,
    val record: Record?,
    val ts: String?,
)

// ---------------------------------------------------------------------------
// Filters — an idiomatic sealed hierarchy mirroring the proto Filter message.
// ---------------------------------------------------------------------------

/** Comparison operators for a [Filter.Field]. */
enum class FilterOp { EQ, NEQ, GT, GTE, LT, LTE, CONTAINS, REGEX }

/**
 * A composable query filter. Build one with [field], [and] or [or]:
 * ```
 * and(
 *     field("age", FilterOp.GTE, "18"),
 *     field("role", FilterOp.EQ, "admin"),
 * )
 * ```
 */
sealed interface Filter {
    data class Field(val field: String, val op: FilterOp, val value: String) : Filter
    data class And(val filters: List<Filter>) : Filter
    data class Or(val filters: List<Filter>) : Filter
}

/** A single field comparison. [value] is compared as the record field's JSON value. */
fun field(field: String, op: FilterOp, value: Any?): Filter.Field =
    Filter.Field(field, op, value?.toString() ?: "")

/** Match records satisfying every child filter. */
fun and(vararg filters: Filter): Filter.And = Filter.And(filters.toList())

/** Match records satisfying any child filter. */
fun or(vararg filters: Filter): Filter.Or = Filter.Or(filters.toList())

// ---------------------------------------------------------------------------
// Exceptions
// ---------------------------------------------------------------------------

/** Base class for ScrivaDB client errors surfaced from engine gRPC status codes. */
open class ScrivaException(message: String?, cause: Throwable? = null) : RuntimeException(message, cause)

/** Raised when a key/record does not exist (gRPC `NOT_FOUND`). */
class NotFoundException(message: String?, cause: Throwable? = null) : ScrivaException(message, cause)

/** Raised when a keyed insert collides with a live key (gRPC `ALREADY_EXISTS`). */
class AlreadyExistsException(message: String?, cause: Throwable? = null) : ScrivaException(message, cause)

// ---------------------------------------------------------------------------
// Proto <-> Kotlin conversion helpers (also used by the round-trip tests).
// ---------------------------------------------------------------------------

internal object Conversions {

    fun filterToProto(f: Filter): ScrivaOuterClass.Filter = when (f) {
        is Filter.And -> ScrivaOuterClass.Filter.newBuilder()
            .setAnd(
                ScrivaOuterClass.AndFilter.newBuilder()
                    .addAllFilters(f.filters.map { filterToProto(it) })
            ).build()
        is Filter.Or -> ScrivaOuterClass.Filter.newBuilder()
            .setOr(
                ScrivaOuterClass.OrFilter.newBuilder()
                    .addAllFilters(f.filters.map { filterToProto(it) })
            ).build()
        is Filter.Field -> ScrivaOuterClass.Filter.newBuilder()
            .setField(
                ScrivaOuterClass.FieldFilter.newBuilder()
                    .setField(f.field)
                    .setOp(opToProto(f.op))
                    .setValue(f.value)
            ).build()
    }

    private fun opToProto(op: FilterOp): ScrivaOuterClass.FilterOp = when (op) {
        FilterOp.EQ -> ScrivaOuterClass.FilterOp.EQ
        FilterOp.NEQ -> ScrivaOuterClass.FilterOp.NEQ
        FilterOp.GT -> ScrivaOuterClass.FilterOp.GT
        FilterOp.GTE -> ScrivaOuterClass.FilterOp.GTE
        FilterOp.LT -> ScrivaOuterClass.FilterOp.LT
        FilterOp.LTE -> ScrivaOuterClass.FilterOp.LTE
        FilterOp.CONTAINS -> ScrivaOuterClass.FilterOp.CONTAINS
        FilterOp.REGEX -> ScrivaOuterClass.FilterOp.REGEX
    }

    fun mapToStruct(map: Map<String, Any?>): Struct {
        val b = Struct.newBuilder()
        for ((k, v) in map) b.putFields(k, objectToValue(v))
        return b.build()
    }

    @Suppress("UNCHECKED_CAST")
    private fun objectToValue(obj: Any?): Value = when (obj) {
        null -> Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
        is Boolean -> Value.newBuilder().setBoolValue(obj).build()
        is Number -> Value.newBuilder().setNumberValue(obj.toDouble()).build()
        is String -> Value.newBuilder().setStringValue(obj).build()
        is Map<*, *> -> Value.newBuilder()
            .setStructValue(mapToStruct(obj as Map<String, Any?>)).build()
        is List<*> -> Value.newBuilder().setListValue(
            ListValue.newBuilder().addAllValues(obj.map { objectToValue(it) }).build()
        ).build()
        else -> Value.newBuilder().setStringValue(obj.toString()).build()
    }

    fun structToMap(struct: Struct): Map<String, Any?> =
        struct.fieldsMap.mapValues { valueToObject(it.value) }

    fun valueToObject(value: Value): Any? = when (value.kindCase) {
        Value.KindCase.NULL_VALUE -> null
        Value.KindCase.BOOL_VALUE -> value.boolValue
        Value.KindCase.NUMBER_VALUE -> value.numberValue
        Value.KindCase.STRING_VALUE -> value.stringValue
        Value.KindCase.STRUCT_VALUE -> structToMap(value.structValue)
        Value.KindCase.LIST_VALUE -> value.listValue.valuesList.map { valueToObject(it) }
        else -> null
    }

    fun recordFromProto(r: ScrivaOuterClass.Record): Record = Record(
        id = r.id,
        key = r.key,
        rev = r.rev,
        data = if (r.hasData()) structToMap(r.data) else emptyMap(),
        dateAdded = if (r.hasDateAdded()) isoOf(r.dateAdded) else null,
        dateModified = if (r.hasDateModified()) isoOf(r.dateModified) else null,
    )

    private fun isoOf(ts: com.google.protobuf.Timestamp): String =
        Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong()).toString()

    fun watchOpFromProto(op: ScrivaOuterClass.WatchOp): WatchOp = when (op) {
        ScrivaOuterClass.WatchOp.INSERTED -> WatchOp.INSERTED
        ScrivaOuterClass.WatchOp.UPDATED -> WatchOp.UPDATED
        ScrivaOuterClass.WatchOp.DELETED -> WatchOp.DELETED
        ScrivaOuterClass.WatchOp.OVERFLOW -> WatchOp.OVERFLOW
        else -> WatchOp.UNSPECIFIED
    }
}
