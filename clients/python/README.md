# FileDB v2 — Python Client

Synchronous Python gRPC client for [FileDB v2](../../README.md).

**PyPI package:** `filedbv2`

---

## Requirements

- Python 3.9+
- A running FileDB v2 server (`make run` from the repo root)

---

## Install

```bash
pip install filedbv2
```

This pulls in `grpcio`, `protobuf`, and `googleapis-common-protos`.

---

## Install from source

```bash
cd clients/python
pip install .
```

To regenerate the gRPC stubs from `proto/filedb.proto`:

```bash
pip install ".[codegen]"   # installs grpcio-tools
./generate.sh              # writes src/filedbv2/proto/filedb_pb2*.py
```

---

## Quick start

```python
from filedbv2 import FileDB

db = FileDB("localhost", 5433, "dev-key")

db.create_collection("users")

rid = db.insert("users", {"name": "Alice", "age": 30, "role": "admin"})

record = db.find_by_id("users", rid)
print(record)  # {'id': '1', 'data': {'name': 'Alice', 'age': 30, ...}, 'date_added': '...'}

admins = db.find("users", {"field": "role", "op": "eq", "value": "admin"}, order_by="name")

db.update("users", rid, {"name": "Alice", "age": 31, "role": "superadmin"})
db.delete("users", rid)
db.drop_collection("users")

db.close()
```

`FileDB` is also a context manager:

```python
with FileDB("localhost", 5433, "dev-key") as db:
    db.create_collection("users")
    ...
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

## API reference

### Constructor

```python
# Plaintext (no TLS)
FileDB(host="localhost", port=5433, api_key="dev-key")

# TLS — verify the server against a CA certificate
FileDB(host, port, api_key, tls_ca_cert="/path/to/ca.crt")  # path or PEM bytes/str
```

### Collection management

```python
name: str        = db.create_collection("col")
ok: bool         = db.drop_collection("col")
names: list[str] = db.list_collections()

# Optional per-collection default TTL — records without their own TTL expire
# after this many seconds. Persisted; overrides the server-wide default.
db.create_collection("sessions", default_ttl_seconds=3600)
```

### CRUD

```python
# Insert one record — returns the assigned integer ID
rid: int = db.insert("col", {"field": "value"})

# Insert many — returns IDs in insertion order
ids: list[int] = db.insert_many("col", [{"name": "Alice"}, {"name": "Bob"}])

# ttl_seconds > 0 expires the record(s) that long after insertion, overriding
# the collection default (works on insert, insert_many, and update):
rid = db.insert("col", {"field": "value"}, ttl_seconds=60)

# Find by ID — returns a record dict
record: dict = db.find_by_id("col", rid)

# Query — returns a list of record dicts (the Find RPC streams; results are collected)
records: list[dict] = db.find(
    "col",
    filter={"field": "age", "op": "gte", "value": "18"},
    limit=0,            # 0 = no limit
    offset=0,
    order_by="age",
    descending=False,
)

# Update — returns the updated ID
rid = db.update("col", rid, {"field": "new value"})

# ttl_seconds > 0 resets the expiry to that long from now; 0 (the default)
# leaves any existing deadline untouched (a plain update is sticky).
rid = db.update("col", rid, {"field": "new value"}, ttl_seconds=120)

# Delete — returns True if the record existed
deleted: bool = db.delete("col", rid)
```

Record dict shape:

```python
{
    "id": "1",                       # uint64 returned as a string
    "data": {...},                   # the document
    "date_added": "2026-06-29T...",  # ISO-8601, present when set
    "date_modified": "2026-06-29T...",
}
```

### Secondary indexes

```python
db.ensure_index("col", "field")
ok: bool         = db.drop_index("col", "field")
fields: list[str] = db.list_indexes("col")
```

Once an index exists, `find` with a single `eq` filter on that field uses the
index automatically — no query hint needed.

### Transactions

```python
tx_id: str = db.begin_tx("col")
ok: bool   = db.commit_tx(tx_id)
ok: bool   = db.rollback_tx(tx_id)
```

### Watch (streaming change feed)

`watch` returns an iterator of event dicts:

```python
for event in db.watch("col"):
    print(event["op"], event["record"]["id"], event["record"]["data"])
    # event["op"] is "INSERTED" | "UPDATED" | "DELETED"
```

With an optional filter — only matching events are delivered:

```python
for event in db.watch("col", {"field": "role", "op": "eq", "value": "admin"}):
    ...
```

Break out of the loop to stop watching.

### Stats

```python
s = db.stats("col")
# {"collection": "col", "record_count": 3, "segment_count": 1,
#  "dirty_entries": 0, "size_bytes": 512}
```

### Maintenance

```python
# Force a synchronous compaction pass — merges/deduplicates sealed segments and
# reclaims space from deleted or expired records. Returns True on success.
ok: bool = db.compact("col")
```

### Backup

```python
# Stream a consistent gzip snapshot of the whole database straight to a file.
# Returns the number of bytes written; restore with `tar xzf backup.tar.gz`.
n: int = db.snapshot_to_file("backup.tar.gz")

# Or consume the raw gzip byte chunks yourself (Snapshot is server-streaming):
with open("backup.tar.gz", "wb") as fh:
    for chunk in db.snapshot():
        fh.write(chunk)
```

### Lifecycle

```python
db.close()  # closes the gRPC channel
```

---

## Filter syntax

Filters are plain dicts.

### Field filter

```python
{"field": "age",   "op": "gt",       "value": "30"}
{"field": "name",  "op": "contains", "value": "alice"}
{"field": "email", "op": "regex",    "value": r".*@gmail\.com"}
```

`value` is coerced to a string; the server compares JSON-encoded values.

### AND composite

```python
{"and": [
    {"field": "age",  "op": "gte", "value": "18"},
    {"field": "city", "op": "eq",  "value": "Berlin"},
]}
```

### OR composite

```python
{"or": [
    {"field": "role", "op": "eq", "value": "admin"},
    {"field": "role", "op": "eq", "value": "superadmin"},
]}
```

Composites nest arbitrarily.

### Supported `op` values

| `op`       | Meaning                     |
|------------|-----------------------------|
| `eq`       | equal                       |
| `neq`      | not equal                   |
| `gt`       | greater than                |
| `gte`      | greater than or equal       |
| `lt`       | less than                   |
| `lte`      | less than or equal          |
| `contains` | string contains (substring) |
| `regex`    | regular expression match    |

---

## TLS

```python
# From a PEM file path
db = FileDB("myserver.example.com", 5433, "api-key", tls_ca_cert="/path/to/ca.crt")

# From PEM bytes
with open("/path/to/ca.crt", "rb") as fh:
    db = FileDB("myserver.example.com", 5433, "api-key", tls_ca_cert=fh.read())
```

When no CA cert is supplied the client connects over plaintext (insecure channel).

---

## Unix socket

Python can connect over the Unix domain socket for local connections by passing a
`unix:` target as the host (with port `0`):

```python
db = FileDB("unix:///tmp/filedb.sock", 0, "dev-key")
```

For the common case, TCP (`localhost:5433`) is sufficient.

---

## Running the examples

Start the server first (from the repo root):

```bash
make run
```

Then, in `clients/python`:

```bash
pip install .
python examples/test_basic.py
python examples/test_watch.py
```
