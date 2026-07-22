"""ScrivaDB Python client.

A thin, ergonomic wrapper over the gRPC API defined in ``proto/scriva.proto``.
Every RPC is exposed as a snake_case method; records are plain ``dict`` objects
and filters are plain ``dict`` structures (see :meth:`ScrivaDB.find`).

Example::

    from scriva import ScrivaDB

    db = ScrivaDB("localhost", 5433, "dev-key")
    db.create_collection("users")
    rid = db.insert("users", {"name": "Alice", "age": 30, "role": "admin"})
    record = db.find_by_id("users", rid)
    admins = db.find("users", {"field": "role", "op": "eq", "value": "admin"})
    db.update("users", rid, {"name": "Alice", "age": 31})
    db.delete("users", rid)
    db.drop_collection("users")
    db.close()
"""

from __future__ import annotations

import contextlib
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

import grpc
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

from .proto import scriva_pb2 as pb
from .proto import scriva_pb2_grpc as pb_grpc

__all__ = [
    "ScrivaDB",
    "ScrivaDBError",
    "NotFoundError",
    "AlreadyExistsError",
]

# Map the documented short op names to proto FilterOp enum values.
_OP_MAP: Dict[str, int] = {
    "eq": pb.EQ,
    "neq": pb.NEQ,
    "gt": pb.GT,
    "gte": pb.GTE,
    "lt": pb.LT,
    "lte": pb.LTE,
    "contains": pb.CONTAINS,
    "regex": pb.REGEX,
}

# Map the documented short aggregation names to proto AggregateOp enum values.
_AGG_MAP: Dict[str, int] = {
    "count": pb.AGG_COUNT,
    "sum": pb.AGG_SUM,
    "avg": pb.AGG_AVG,
    "min": pb.AGG_MIN,
    "max": pb.AGG_MAX,
}

# Map WatchOp enum values back to readable strings for event dicts.
_WATCH_OP_NAMES: Dict[int, str] = {
    pb.WATCH_OP_UNSPECIFIED: "UNSPECIFIED",
    pb.INSERTED: "INSERTED",
    pb.UPDATED: "UPDATED",
    pb.DELETED: "DELETED",
    pb.OVERFLOW: "OVERFLOW",
}

# An order_by spec: a single field name, a (field, desc) pair, or a
# {"field": ..., "desc": ...} mapping. `find` also accepts a list of these.
OrderBySpec = Union[str, Tuple[str, bool], Mapping[str, Any]]


# ---------------------------------------------------------------------------
# Exceptions — idiomatic mapping of the engine's gRPC status codes
# ---------------------------------------------------------------------------


class ScrivaDBError(Exception):
    """Base class for ScrivaDB client errors raised from engine status codes."""


class NotFoundError(ScrivaDBError):
    """A keyed lookup/update/delete referenced a key that no live record holds."""


class AlreadyExistsError(ScrivaDBError):
    """A keyed insert used a key already held by a live record."""


@contextlib.contextmanager
def _translate_errors() -> Iterator[None]:
    """Map gRPC ``NOT_FOUND`` / ``ALREADY_EXISTS`` onto typed exceptions.

    Other status codes propagate as the original :class:`grpc.RpcError`.
    """
    try:
        yield
    except grpc.RpcError as exc:
        code = exc.code()
        details = exc.details() if callable(getattr(exc, "details", None)) else str(exc)
        if code == grpc.StatusCode.NOT_FOUND:
            raise NotFoundError(details) from exc
        if code == grpc.StatusCode.ALREADY_EXISTS:
            raise AlreadyExistsError(details) from exc
        raise


def _dict_to_struct(data: Mapping[str, Any]) -> struct_pb2.Struct:
    """Convert a plain dict into a protobuf Struct."""
    s = struct_pb2.Struct()
    ParseDict(dict(data), s)
    return s


def _value_to_py(value: struct_pb2.Value) -> Any:
    """Convert a protobuf ``Value`` (google.protobuf.Value) into a Python value."""
    kind = value.WhichOneof("kind")
    if kind is None or kind == "null_value":
        return None
    if kind == "number_value":
        return value.number_value
    if kind == "string_value":
        return value.string_value
    if kind == "bool_value":
        return value.bool_value
    if kind == "struct_value":
        return MessageToDict(value.struct_value)
    if kind == "list_value":
        return [_value_to_py(v) for v in value.list_value.values]
    return None


def _record_to_dict(record: pb.Record) -> Dict[str, Any]:
    """Convert a proto Record into a plain dict.

    The shape mirrors the other SDKs: ``id`` is returned as a string because
    uint64 can exceed the safe integer range of some languages; ``data`` is the
    decoded document; timestamps are ISO-8601 strings when present. The
    caller-supplied ``key`` (when set) and the per-record ``rev`` (present on
    every live record, starting at 1) are surfaced too.
    """
    out: Dict[str, Any] = {
        "id": str(record.id),
        "data": MessageToDict(record.data) if record.HasField("data") else {},
    }
    if record.key:
        out["key"] = record.key
    if record.rev:
        out["rev"] = record.rev
    if record.HasField("date_added"):
        out["date_added"] = record.date_added.ToJsonString()
    if record.HasField("date_modified"):
        out["date_modified"] = record.date_modified.ToJsonString()
    return out


class ScrivaDB:
    """Synchronous gRPC client for ScrivaDB.

    :param host:        Server host, e.g. ``"localhost"``.
    :param port:        gRPC port (default server port is ``5433``).
    :param api_key:     API key — sent as ``x-api-key`` gRPC metadata on every call.
    :param tls_ca_cert: Optional PEM CA certificate bytes (or a path to a PEM
                        file). When provided, the channel verifies the server
                        certificate over TLS; otherwise a plaintext (insecure)
                        channel is used.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        api_key: str = "",
        tls_ca_cert: Optional[Any] = None,
    ) -> None:
        self._api_key = api_key
        # A `unix:` host is a full gRPC target (e.g. "unix:///tmp/scriva.sock");
        # don't append a port to it. Otherwise build a host:port TCP target.
        target = host if host.startswith("unix:") else f"{host}:{port}"

        if tls_ca_cert is not None:
            root_certs = self._load_ca(tls_ca_cert)
            creds = grpc.ssl_channel_credentials(root_certificates=root_certs)
            self._channel = grpc.secure_channel(target, creds)
        else:
            self._channel = grpc.insecure_channel(target)

        self._stub = pb_grpc.ScrivaStub(self._channel)

    @staticmethod
    def _load_ca(tls_ca_cert: Any) -> bytes:
        """Accept either raw PEM bytes/str or a path to a PEM file."""
        if isinstance(tls_ca_cert, bytes):
            return tls_ca_cert
        if isinstance(tls_ca_cert, str):
            # Heuristic: a PEM blob contains a header line; otherwise treat as path.
            if "BEGIN CERTIFICATE" in tls_ca_cert:
                return tls_ca_cert.encode("utf-8")
            with open(tls_ca_cert, "rb") as fh:
                return fh.read()
        raise TypeError("tls_ca_cert must be PEM bytes, a PEM string, or a file path")

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> "ScrivaDB":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying gRPC channel."""
        self._channel.close()

    # -- internal helpers ----------------------------------------------------

    def _metadata(self) -> List[tuple]:
        return [("x-api-key", self._api_key)]

    def _filter_to_proto(self, filter_dict: Optional[Mapping[str, Any]]) -> Optional[pb.Filter]:
        """Convert a plain-dict filter into a proto :class:`Filter`.

        Accepted shapes (see the README for full docs)::

            {"field": "age", "op": "gt", "value": "30"}
            {"and": [<filter>, <filter>, ...]}
            {"or":  [<filter>, <filter>, ...]}

        ``op`` is one of: ``eq neq gt gte lt lte contains regex``.
        ``value`` is coerced to a string (the server compares JSON-encoded values).
        """
        if filter_dict is None:
            return None

        # `and` / `or` are Python keywords, so the proto fields of the same name
        # must be passed through a kwargs dict rather than as direct kwargs.
        if "and" in filter_dict:
            children = [self._filter_to_proto(c) for c in filter_dict["and"]]
            return pb.Filter(**{"and": pb.AndFilter(filters=children)})
        if "or" in filter_dict:
            children = [self._filter_to_proto(c) for c in filter_dict["or"]]
            return pb.Filter(**{"or": pb.OrFilter(filters=children)})

        op_name = str(filter_dict["op"]).lower()
        if op_name not in _OP_MAP:
            raise ValueError(
                f"unknown filter op {filter_dict['op']!r}; "
                f"expected one of {sorted(_OP_MAP)}"
            )
        return pb.Filter(
            field=pb.FieldFilter(
                field=filter_dict["field"],
                op=_OP_MAP[op_name],
                value=str(filter_dict["value"]),
            )
        )

    @staticmethod
    def _order_by_to_proto(
        order_by: Union[str, Sequence[OrderBySpec], None],
    ) -> Optional[List[pb.OrderBy]]:
        """Convert a multi-field ``order_by`` argument to a list of ``OrderBy``.

        Returns ``None`` when ``order_by`` is empty or a plain string (the
        deprecated single-field path, handled by the caller). Otherwise accepts
        a list whose items are field-name strings, ``(field, desc)`` pairs, or
        ``{"field": ..., "desc": ...}`` mappings.
        """
        if not order_by or isinstance(order_by, str):
            return None
        specs: List[pb.OrderBy] = []
        for item in order_by:
            if isinstance(item, str):
                specs.append(pb.OrderBy(field=item, desc=False))
            elif isinstance(item, Mapping):
                desc = bool(item.get("desc", item.get("descending", False)))
                specs.append(pb.OrderBy(field=item["field"], desc=desc))
            elif isinstance(item, (tuple, list)):
                field = item[0]
                desc = bool(item[1]) if len(item) > 1 else False
                specs.append(pb.OrderBy(field=field, desc=desc))
            else:
                raise TypeError(
                    "order_by items must be a field name, a (field, desc) pair, "
                    "or a {'field': ..., 'desc': ...} mapping"
                )
        return specs

    # -- collection management ----------------------------------------------

    def create_collection(self, name: str, default_ttl_seconds: int = 0) -> str:
        """Create a new collection. Returns the collection name.

        ``default_ttl_seconds`` sets an optional per-collection default record
        TTL: when greater than 0, records inserted without their own TTL expire
        after this many seconds. It is persisted per-collection and overrides
        the server-wide default. 0 (the default) inherits the server-wide TTL.
        """
        resp = self._stub.CreateCollection(
            pb.CreateCollectionRequest(
                name=name, default_ttl_seconds=default_ttl_seconds
            ),
            metadata=self._metadata(),
        )
        return resp.name

    def drop_collection(self, name: str) -> bool:
        """Drop a collection and all its data. Returns ``True`` on success."""
        resp = self._stub.DropCollection(
            pb.DropCollectionRequest(name=name), metadata=self._metadata()
        )
        return resp.ok

    def list_collections(self) -> List[str]:
        """List all collection names."""
        resp = self._stub.ListCollections(
            pb.ListCollectionsRequest(), metadata=self._metadata()
        )
        return list(resp.names)

    # -- CRUD ----------------------------------------------------------------

    def insert(
        self,
        collection: str,
        data: Mapping[str, Any],
        ttl_seconds: int = 0,
        key: str = "",
    ) -> int:
        """Insert one record. Returns the assigned ID.

        ``ttl_seconds`` sets an optional per-record TTL: when greater than 0 the
        record expires this many seconds after insertion, overriding any
        collection default. 0 (the default) applies the collection's default TTL.

        ``key`` sets an optional caller-supplied string primary key (keyed
        create). When set, the record is inserted under this key and a key
        already held by a live record raises :class:`AlreadyExistsError`. A keyed
        insert does not participate in transactions or per-record TTL.
        """
        with _translate_errors():
            resp = self._stub.Insert(
                pb.InsertRequest(
                    collection=collection,
                    data=_dict_to_struct(data),
                    ttl_seconds=ttl_seconds,
                    key=key,
                ),
                metadata=self._metadata(),
            )
        return resp.id

    def insert_many(
        self,
        collection: str,
        records: Sequence[Mapping[str, Any]],
        ttl_seconds: int = 0,
    ) -> List[int]:
        """Insert multiple records. Returns the assigned IDs in insertion order.

        ``ttl_seconds`` applies the same per-record TTL to every record in the
        batch; see :meth:`insert` for the semantics.
        """
        resp = self._stub.InsertMany(
            pb.InsertManyRequest(
                collection=collection,
                records=[_dict_to_struct(r) for r in records],
                ttl_seconds=ttl_seconds,
            ),
            metadata=self._metadata(),
        )
        return list(resp.ids)

    def find_by_id(
        self,
        collection: str,
        id: int,
        fields: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Fetch a single record by ID. Returns a record dict.

        ``fields`` is an optional projection: when non-empty, only those
        top-level fields are returned in ``data`` (``id``, ``key`` and ``rev``
        are always included). Empty/``None`` returns the full record.
        """
        resp = self._stub.FindById(
            pb.FindByIdRequest(
                collection=collection, id=id, fields=list(fields) if fields else []
            ),
            metadata=self._metadata(),
        )
        return _record_to_dict(resp.record)

    def _build_find_request(
        self,
        collection: str,
        filter: Optional[Mapping[str, Any]],
        limit: int,
        offset: int,
        order_by: Union[str, Sequence[OrderBySpec]],
        descending: bool,
        fields: Optional[Sequence[str]],
        page_token: str,
    ) -> pb.FindRequest:
        req = pb.FindRequest(
            collection=collection,
            filter=self._filter_to_proto(filter),
            limit=limit,
            offset=offset,
            fields=list(fields) if fields else [],
            page_token=page_token,
        )
        order_by_fields = self._order_by_to_proto(order_by)
        if order_by_fields is not None:
            req.order_by_fields.extend(order_by_fields)
        elif isinstance(order_by, str):
            # Deprecated single-field path — honoured only when order_by_fields
            # is empty (server-side).
            req.order_by = order_by
            req.descending = descending
        return req

    def find(
        self,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        limit: int = 0,
        offset: int = 0,
        order_by: Union[str, Sequence[OrderBySpec]] = "",
        descending: bool = False,
        fields: Optional[Sequence[str]] = None,
        page_token: str = "",
    ) -> List[Dict[str, Any]]:
        """Query records.

        The ``Find`` RPC is server-streaming; this method collects the stream
        into a list for convenience. ``limit=0`` means no limit.

        ``order_by`` accepts either a single field name (the deprecated scalar
        sort, paired with ``descending``) or a list for a multi-field sort — each
        item a field name, a ``(field, desc)`` pair, or a
        ``{"field": ..., "desc": ...}`` mapping.

        ``fields`` projects each record's ``data`` down to those top-level fields
        (``id``/``key``/``rev`` are always returned). ``page_token`` requests a
        keyset page returned by a previous :meth:`find_page`; use
        :meth:`find_page` to also receive the next-page cursor.
        """
        records, _ = self.find_page(
            collection,
            filter=filter,
            limit=limit,
            offset=offset,
            order_by=order_by,
            descending=descending,
            fields=fields,
            page_token=page_token,
        )
        return records

    def find_page(
        self,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        limit: int = 0,
        offset: int = 0,
        order_by: Union[str, Sequence[OrderBySpec]] = "",
        descending: bool = False,
        fields: Optional[Sequence[str]] = None,
        page_token: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Query one keyset page, returning ``(records, next_page_token)``.

        Pass an ordering (``order_by``) and a ``limit``, then feed the returned
        ``next_page_token`` back as ``page_token`` on the next call to walk the
        collection page by page in O(page) time. An empty ``next_page_token``
        means the last page was reached. Keep the same filter, ordering and limit
        on every page.
        """
        req = self._build_find_request(
            collection, filter, limit, offset, order_by, descending, fields, page_token
        )
        records: List[Dict[str, Any]] = []
        next_token = ""
        for resp in self._stub.Find(req, metadata=self._metadata()):
            records.append(_record_to_dict(resp.record))
            if resp.page_token:
                next_token = resp.page_token
        return records, next_token

    def update(
        self,
        collection: str,
        id: int,
        data: Mapping[str, Any],
        ttl_seconds: int = 0,
    ) -> int:
        """Update a record by ID. Returns the updated ID.

        ``ttl_seconds`` greater than 0 resets the record's expiry to this many
        seconds from now, overriding the collection default. 0 (the default)
        leaves any existing deadline in place (a plain update is sticky and does
        not clear a previously set TTL).
        """
        resp = self._stub.Update(
            pb.UpdateRequest(
                collection=collection,
                id=id,
                data=_dict_to_struct(data),
                ttl_seconds=ttl_seconds,
            ),
            metadata=self._metadata(),
        )
        return resp.id

    def delete(self, collection: str, id: int) -> bool:
        """Delete a record by ID. Returns ``True`` if the record existed."""
        resp = self._stub.Delete(
            pb.DeleteRequest(collection=collection, id=id),
            metadata=self._metadata(),
        )
        return resp.ok

    # -- keyed CRUD, upsert & compare-and-swap (N1) --------------------------

    def upsert(
        self, collection: str, key: str, data: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Insert ``data`` under ``key``, or replace the existing keyed record.

        Atomic: if no live record carries ``key`` it is inserted; otherwise the
        existing record's data is replaced and its ``rev`` incremented. Returns
        the resulting record dict (including ``key`` and ``rev``).
        """
        resp = self._stub.Upsert(
            pb.UpsertRequest(
                collection=collection, key=key, data=_dict_to_struct(data)
            ),
            metadata=self._metadata(),
        )
        return _record_to_dict(resp.record)

    def find_by_key(
        self,
        collection: str,
        key: str,
        fields: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Fetch the record carrying ``key``. Raises :class:`NotFoundError` if none.

        ``fields`` projects ``data`` to those top-level fields (``id``/``key``/
        ``rev`` always returned).
        """
        with _translate_errors():
            resp = self._stub.FindByKey(
                pb.FindByKeyRequest(
                    collection=collection,
                    key=key,
                    fields=list(fields) if fields else [],
                ),
                metadata=self._metadata(),
            )
        return _record_to_dict(resp.record)

    def update_by_key(
        self, collection: str, key: str, data: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Overwrite the record carrying ``key``, preserving the key itself.

        Raises :class:`NotFoundError` if no live record carries ``key``. Returns
        a dict with ``id``, ``key``, ``rev`` (after the write) and
        ``date_modified``.
        """
        with _translate_errors():
            resp = self._stub.UpdateByKey(
                pb.UpdateByKeyRequest(
                    collection=collection, key=key, data=_dict_to_struct(data)
                ),
                metadata=self._metadata(),
            )
        out: Dict[str, Any] = {"id": str(resp.id)}
        if resp.key:
            out["key"] = resp.key
        if resp.rev:
            out["rev"] = resp.rev
        if resp.date_modified:
            out["date_modified"] = resp.date_modified
        return out

    def delete_by_key(self, collection: str, key: str) -> bool:
        """Delete the record carrying ``key``. Returns ``True`` on success.

        Raises :class:`NotFoundError` if no live record carries ``key``.
        """
        with _translate_errors():
            resp = self._stub.DeleteByKey(
                pb.DeleteByKeyRequest(collection=collection, key=key),
                metadata=self._metadata(),
            )
        return resp.ok

    def update_if_rev(
        self,
        collection: str,
        key: str,
        expected_rev: int,
        data: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Compare-and-swap update on ``key``, conditional on ``expected_rev``.

        The write is applied only if the record's current ``rev`` equals
        ``expected_rev``. A stale revision (or a missing key) is a clean no-op —
        never an error. Returns ``{"swapped": bool, "record": dict | None}``;
        ``record`` is the resulting record only when ``swapped`` is ``True``.
        """
        resp = self._stub.UpdateIfRev(
            pb.UpdateIfRevRequest(
                collection=collection,
                key=key,
                expected_rev=expected_rev,
                data=_dict_to_struct(data),
            ),
            metadata=self._metadata(),
        )
        out: Dict[str, Any] = {"swapped": resp.swapped, "record": None}
        if resp.swapped and resp.HasField("record"):
            out["record"] = _record_to_dict(resp.record)
        return out

    # -- secondary indexes ---------------------------------------------------

    def ensure_index(self, collection: str, field: str) -> None:
        """Create a secondary index on a field (no-op if it already exists)."""
        self._stub.EnsureIndex(
            pb.EnsureIndexRequest(collection=collection, field=field),
            metadata=self._metadata(),
        )

    def drop_index(self, collection: str, field: str) -> bool:
        """Drop a secondary index. Returns ``True`` if the index existed."""
        resp = self._stub.DropIndex(
            pb.DropIndexRequest(collection=collection, field=field),
            metadata=self._metadata(),
        )
        return resp.ok

    def list_indexes(self, collection: str) -> List[str]:
        """List all indexed field names for a collection."""
        resp = self._stub.ListIndexes(
            pb.ListIndexesRequest(collection=collection), metadata=self._metadata()
        )
        return list(resp.fields)

    # -- transactions --------------------------------------------------------

    def begin_tx(self, collection: str) -> str:
        """Begin a transaction on a collection. Returns the transaction ID."""
        resp = self._stub.BeginTx(
            pb.BeginTxRequest(collection=collection), metadata=self._metadata()
        )
        return resp.tx_id

    def commit_tx(self, tx_id: str) -> bool:
        """Commit a transaction. Returns ``True`` on success."""
        resp = self._stub.CommitTx(
            pb.CommitTxRequest(tx_id=tx_id), metadata=self._metadata()
        )
        return resp.ok

    def rollback_tx(self, tx_id: str) -> bool:
        """Roll back a transaction. Returns ``True`` on success."""
        resp = self._stub.RollbackTx(
            pb.RollbackTxRequest(tx_id=tx_id), metadata=self._metadata()
        )
        return resp.ok

    # -- watch ---------------------------------------------------------------

    def watch(
        self, collection: str, filter: Optional[Mapping[str, Any]] = None
    ) -> Iterator[Dict[str, Any]]:
        """Subscribe to change events on a collection.

        Returns an iterator of event dicts, each shaped like::

            {"op": "INSERTED", "collection": "users",
             "record": {...}, "ts": "2026-06-29T..."}

        ``op`` is one of ``INSERTED`` / ``UPDATED`` / ``DELETED`` / ``OVERFLOW``
        (``OVERFLOW`` signals dropped events — the subscriber fell behind and
        should resync; no record is set). Stop watching by breaking out of the
        loop or calling :meth:`close`.
        """
        req = pb.WatchRequest(
            collection=collection, filter=self._filter_to_proto(filter)
        )
        for event in self._stub.Watch(req, metadata=self._metadata()):
            out: Dict[str, Any] = {
                "op": _WATCH_OP_NAMES.get(event.op, "UNSPECIFIED"),
                "collection": event.collection,
                "record": _record_to_dict(event.record),
            }
            if event.HasField("ts"):
                out["ts"] = event.ts.ToJsonString()
            yield out

    # -- aggregations (N4) ---------------------------------------------------

    def aggregate(
        self,
        collection: str,
        aggregations: Optional[Sequence[str]] = None,
        field: str = "",
        group_by: str = "",
        filter: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Compute count + numeric aggregations over the filtered live records.

        The ``Aggregate`` RPC is server-streaming — one message per group; this
        collects them into a list. Each result dict carries ``group`` (the
        group-by value, ``None`` for the whole-set group), ``count``, and — when
        the group had at least one numeric ``field`` value — ``sum``/``avg``/
        ``min``/``max`` plus ``numeric: True``.

        :param aggregations: which numeric aggregations to compute — any of
            ``"count"``, ``"sum"``, ``"avg"``, ``"min"``, ``"max"``. ``count`` is
            always returned; ``sum``/``avg``/``min``/``max`` require ``field``.
        :param field:    numeric field for ``sum``/``avg``/``min``/``max``.
        :param group_by: optional group-by field — one result per distinct value.
        :param filter:   the same plain-dict filter as :meth:`find`.
        """
        agg_ops: List[int] = []
        for name in aggregations or []:
            key = str(name).lower()
            if key not in _AGG_MAP:
                raise ValueError(
                    f"unknown aggregation {name!r}; expected one of {sorted(_AGG_MAP)}"
                )
            agg_ops.append(_AGG_MAP[key])

        req = pb.AggregateRequest(
            collection=collection,
            filter=self._filter_to_proto(filter),
            group_by=group_by,
            field=field,
            aggregations=agg_ops,
        )
        out: List[Dict[str, Any]] = []
        for resp in self._stub.Aggregate(req, metadata=self._metadata()):
            group: Dict[str, Any] = {
                "group": _value_to_py(resp.group_value),
                "count": resp.count,
                "numeric": resp.numeric,
            }
            if resp.numeric:
                group["sum"] = resp.sum
                group["avg"] = resp.avg
                group["min"] = resp.min
                group["max"] = resp.max
            out.append(group)
        return out

    def count(
        self, collection: str, filter: Optional[Mapping[str, Any]] = None
    ) -> int:
        """Count the live records matching ``filter`` (or all records)."""
        groups = self.aggregate(collection, filter=filter)
        return groups[0]["count"] if groups else 0

    def group_by(
        self,
        collection: str,
        field: str,
        aggregations: Optional[Sequence[str]] = None,
        metric: str = "",
        filter: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Group live records by ``field`` and aggregate each group.

        Convenience wrapper over :meth:`aggregate`. ``field`` is the group-by
        field; ``metric`` is the numeric field for ``sum``/``avg``/``min``/
        ``max`` (pass those in ``aggregations``). Returns one dict per distinct
        group value; see :meth:`aggregate` for the dict shape.
        """
        return self.aggregate(
            collection,
            aggregations=aggregations,
            field=metric,
            group_by=field,
            filter=filter,
        )

    # -- stats ---------------------------------------------------------------

    def stats(self, collection: str) -> Dict[str, Any]:
        """Return collection statistics as a dict."""
        resp = self._stub.CollectionStats(
            pb.CollectionStatsRequest(collection=collection),
            metadata=self._metadata(),
        )
        return {
            "collection": resp.collection,
            "record_count": resp.record_count,
            "segment_count": resp.segment_count,
            "dirty_entries": resp.dirty_entries,
            "size_bytes": resp.size_bytes,
        }

    # -- maintenance ---------------------------------------------------------

    def compact(self, collection: str) -> bool:
        """Force a synchronous compaction pass on a collection.

        Merges and deduplicates sealed segments and reclaims space from deleted
        or expired records, blocking until the pass completes. Returns ``True``
        on success.
        """
        resp = self._stub.Compact(
            pb.CompactRequest(collection=collection), metadata=self._metadata()
        )
        return resp.ok

    # -- backup --------------------------------------------------------------

    def snapshot(self) -> Iterator[bytes]:
        """Stream a consistent gzip backup of the entire database.

        The ``Snapshot`` RPC is server-streaming; this yields the raw gzip
        archive bytes chunk by chunk so large databases never have to be held in
        memory. Concatenate the chunks (or use :meth:`snapshot_to_file`) to
        reconstruct the archive, then restore it with ``tar xzf`` into a data
        directory.
        """
        for chunk in self._stub.Snapshot(
            pb.SnapshotRequest(), metadata=self._metadata()
        ):
            yield chunk.data

    def snapshot_to_file(self, path: str) -> int:
        """Stream a gzip backup straight to a file. Returns the bytes written.

        Convenience wrapper over :meth:`snapshot` that writes each chunk to
        ``path`` as it arrives. Restore with ``tar xzf <path>``.
        """
        written = 0
        with open(path, "wb") as fh:
            for chunk in self.snapshot():
                written += fh.write(chunk)
        return written
