"""FileDB v2 Python client.

A thin, ergonomic wrapper over the gRPC API defined in ``proto/filedb.proto``.
Every RPC is exposed as a snake_case method; records are plain ``dict`` objects
and filters are plain ``dict`` structures (see :meth:`FileDB.find`).

Example::

    from filedbv2 import FileDB

    db = FileDB("localhost", 5433, "dev-key")
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

from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence

import grpc
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

from .proto import filedb_pb2 as pb
from .proto import filedb_pb2_grpc as pb_grpc

__all__ = ["FileDB"]

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

# Map WatchOp enum values back to readable strings for event dicts.
_WATCH_OP_NAMES: Dict[int, str] = {
    pb.WATCH_OP_UNSPECIFIED: "UNSPECIFIED",
    pb.INSERTED: "INSERTED",
    pb.UPDATED: "UPDATED",
    pb.DELETED: "DELETED",
}


def _dict_to_struct(data: Mapping[str, Any]) -> struct_pb2.Struct:
    """Convert a plain dict into a protobuf Struct."""
    s = struct_pb2.Struct()
    ParseDict(dict(data), s)
    return s


def _record_to_dict(record: pb.Record) -> Dict[str, Any]:
    """Convert a proto Record into a plain dict.

    The shape mirrors the other SDKs: ``id`` is returned as a string because
    uint64 can exceed the safe integer range of some languages; ``data`` is the
    decoded document; timestamps are ISO-8601 strings when present.
    """
    out: Dict[str, Any] = {
        "id": str(record.id),
        "data": MessageToDict(record.data) if record.HasField("data") else {},
    }
    if record.HasField("date_added"):
        out["date_added"] = record.date_added.ToJsonString()
    if record.HasField("date_modified"):
        out["date_modified"] = record.date_modified.ToJsonString()
    return out


class FileDB:
    """Synchronous gRPC client for FileDB v2.

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
        # A `unix:` host is a full gRPC target (e.g. "unix:///tmp/filedb.sock");
        # don't append a port to it. Otherwise build a host:port TCP target.
        target = host if host.startswith("unix:") else f"{host}:{port}"

        if tls_ca_cert is not None:
            root_certs = self._load_ca(tls_ca_cert)
            creds = grpc.ssl_channel_credentials(root_certificates=root_certs)
            self._channel = grpc.secure_channel(target, creds)
        else:
            self._channel = grpc.insecure_channel(target)

        self._stub = pb_grpc.FileDBStub(self._channel)

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

    def __enter__(self) -> "FileDB":
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
        self, collection: str, data: Mapping[str, Any], ttl_seconds: int = 0
    ) -> int:
        """Insert one record. Returns the assigned ID.

        ``ttl_seconds`` sets an optional per-record TTL: when greater than 0 the
        record expires this many seconds after insertion, overriding any
        collection default. 0 (the default) applies the collection's default TTL.
        """
        resp = self._stub.Insert(
            pb.InsertRequest(
                collection=collection,
                data=_dict_to_struct(data),
                ttl_seconds=ttl_seconds,
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

    def find_by_id(self, collection: str, id: int) -> Dict[str, Any]:
        """Fetch a single record by ID. Returns a record dict."""
        resp = self._stub.FindById(
            pb.FindByIdRequest(collection=collection, id=id),
            metadata=self._metadata(),
        )
        return _record_to_dict(resp.record)

    def find(
        self,
        collection: str,
        filter: Optional[Mapping[str, Any]] = None,
        limit: int = 0,
        offset: int = 0,
        order_by: str = "",
        descending: bool = False,
    ) -> List[Dict[str, Any]]:
        """Query records.

        The ``Find`` RPC is server-streaming; this method collects the stream
        into a list for convenience. ``limit=0`` means no limit.
        """
        req = pb.FindRequest(
            collection=collection,
            filter=self._filter_to_proto(filter),
            limit=limit,
            offset=offset,
            order_by=order_by,
            descending=descending,
        )
        return [
            _record_to_dict(resp.record)
            for resp in self._stub.Find(req, metadata=self._metadata())
        ]

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

        ``op`` is one of ``INSERTED`` / ``UPDATED`` / ``DELETED``. Stop watching
        by breaking out of the loop or calling :meth:`close`.
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
