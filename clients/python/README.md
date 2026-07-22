# ScrivaDB — Python Client

Synchronous Python gRPC client for [ScrivaDB](../../README.md).

**PyPI package:** `scriva`

---

## Requirements

- Python 3.9+
- A running ScrivaDB server (`make run` from the repo root)

---

## Install

```bash
pip install scriva
```

This pulls in `grpcio`, `protobuf`, and `googleapis-common-protos`.

---

## Install from source

```bash
cd clients/python
pip install .
```

To regenerate the gRPC stubs from `proto/scriva.proto`:

```bash
pip install ".[codegen]"   # installs grpcio-tools
./generate.sh              # writes src/scriva/proto/scriva_pb2*.py
```

---

## Quick start

```python
from scriva import ScrivaDB

db = ScrivaDB("localhost", 5433, "dev-key")

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

`ScrivaDB` is also a context manager:

```python
with ScrivaDB("localhost", 5433, "dev-key") as db:
    db.create_collection("users")
    ...
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

## API reference

### Constructor

```python
# Plaintext (no TLS)
ScrivaDB(host="localhost", port=5433, api_key="dev-key")

# TLS — verify the server against a CA certificate
ScrivaDB(host, port, api_key, tls_ca_cert="/path/to/ca.crt")  # path or PEM bytes/str
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
    order_by="age",     # scalar (deprecated) OR a multi-field list — see below
    descending=False,
    fields=None,        # projection: only these top-level fields (id/key/rev always returned)
    page_token="",      # keyset cursor from a previous find_page (see Pagination)
)

# Insert one record under a caller-supplied string key (keyed create) — a key
# already held by a live record raises AlreadyExistsError.
rid = db.insert("col", {"field": "value"}, key="user:alice")

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
    "key": "user:alice",             # caller-supplied string key, present when set
    "rev": 1,                        # per-record revision, bumped on every write
    "date_added": "2026-06-29T...",  # ISO-8601, present when set
    "date_modified": "2026-06-29T...",
}
```

### Field projection

Pass `fields=[...]` to `find`, `find_page`, `find_by_id` or `find_by_key` to
return only those top-level fields in `data`. `id`, `key` and `rev` are always
included; an unknown field is silently omitted; an empty/omitted list returns the
full record.

```python
db.find("col", fields=["name", "email"])
db.find_by_id("col", rid, fields=["name"])
```

### Sorting & keyset pagination

`order_by` accepts either a single field name (the deprecated scalar sort, paired
with `descending=`) or a list for a stable multi-field sort — each item a field
name, a `(field, desc)` pair, or a `{"field": ..., "desc": ...}` mapping:

```python
db.find("col", order_by=[("role", False), ("age", True)])   # role asc, age desc
db.find("col", order_by=["name"])                           # single field, asc
```

`find_page` returns `(records, next_page_token)` for O(page) keyset pagination.
Feed the token back as `page_token=` and keep the same filter, ordering and
limit on every page; an empty token means the last page was reached:

```python
page, token = db.find_page("col", limit=100, order_by="age")
while token:
    page, token = db.find_page("col", limit=100, order_by="age", page_token=token)
```

### Keyed CRUD, upsert & compare-and-swap

Records can carry a caller-supplied string **key** and a monotonic **rev**
(revision), enabling natural keys, upsert and optimistic-concurrency updates.

```python
# Upsert — insert under key, or replace the existing keyed record (bumps rev).
rec: dict = db.upsert("col", "user:alice", {"name": "Alice", "score": 10})

# Fetch / update / delete by key. A missing key raises NotFoundError.
rec: dict  = db.find_by_key("col", "user:alice", fields=None)
info: dict = db.update_by_key("col", "user:alice", {"name": "Alice", "score": 20})
ok: bool   = db.delete_by_key("col", "user:alice")

# Keyed create via insert(..., key=...) — a duplicate key raises AlreadyExistsError.
rid = db.insert("col", {"name": "Bob"}, key="user:bob")

# Compare-and-swap: applies only if the record's current rev == expected_rev.
# A stale rev (or missing key) is a clean no-op — swapped=False, never an error.
res = db.update_if_rev("col", "user:alice", expected_rev=2, data={"score": 30})
if res["swapped"]:
    print("new rev:", res["record"]["rev"])
```

`update_by_key` returns `{"id", "key", "rev", "date_modified"}`; `update_if_rev`
returns `{"swapped": bool, "record": dict | None}` (record set only on success).

### Aggregations

`Aggregate` computes a count plus optional numeric aggregations (`sum`/`avg`/
`min`/`max`), honouring the same filter as `find`, optionally grouped by a field.

```python
# Count matching records.
n: int = db.count("col")
n = db.count("col", {"field": "role", "op": "eq", "value": "admin"})

# Group by a field, aggregating a numeric field per group.
groups = db.group_by(
    "col", "role",
    aggregations=["sum", "avg", "min", "max"],
    metric="age",
)
# [{"group": "admin", "count": 2, "numeric": True,
#   "sum": 61.0, "avg": 30.5, "min": 30.0, "max": 35.0}, ...]

# Full form — mirrors the proto (group_by="" aggregates the whole set).
groups = db.aggregate(
    "col",
    aggregations=["sum", "avg"],
    field="age",          # numeric field for sum/avg/min/max
    group_by="role",      # optional group-by field
    filter={"field": "age", "op": "gte", "value": "18"},
)
```

Each group dict carries `group` (the group value, `None` for the whole-set group
or records missing the field), `count`, and `numeric` — `sum`/`avg`/`min`/`max`
are present only when `numeric` is `True`.

### Errors

Keyed operations map the engine's gRPC status codes onto typed exceptions
(exported from `scriva`):

```python
from scriva import ScrivaDB, NotFoundError, AlreadyExistsError

try:
    db.insert("col", {"name": "Bob"}, key="user:bob")   # duplicate key
except AlreadyExistsError:
    ...

try:
    db.find_by_key("col", "missing")                    # or update_by_key/delete_by_key
except NotFoundError:
    ...
```

Both derive from `ScrivaDBError`. Other gRPC failures propagate as `grpc.RpcError`.

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
    # event["op"] is "INSERTED" | "UPDATED" | "DELETED" | "OVERFLOW"
    # OVERFLOW signals dropped events (the subscriber fell behind); resync.
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
db = ScrivaDB("myserver.example.com", 5433, "api-key", tls_ca_cert="/path/to/ca.crt")

# From PEM bytes
with open("/path/to/ca.crt", "rb") as fh:
    db = ScrivaDB("myserver.example.com", 5433, "api-key", tls_ca_cert=fh.read())
```

When no CA cert is supplied the client connects over plaintext (insecure channel).

---

## Unix socket

Python can connect over the Unix domain socket for local connections by passing a
`unix:` target as the host (with port `0`):

```python
db = ScrivaDB("unix:///tmp/scriva.sock", 0, "dev-key")
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
