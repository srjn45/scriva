# FileDBv2 Ruby Client

Idiomatic Ruby gRPC client for [FileDB v2](https://github.com/srjn45/filedbv2).

**Ruby 3.1+ · gem `filedbv2`**

---

## Install

```bash
gem install filedbv2
```

Or add to your `Gemfile`:

```ruby
gem "filedbv2", "~> 0.7"
```

---

## Quick start

```ruby
require "filedbv2"

db = FileDBv2::Client.new(host: "localhost", port: 5433, api_key: "dev-key")

db.create_collection("users")

id = db.insert("users", { name: "Alice", age: 30, role: "admin" })

record = db.find_by_id("users", id)
# => { "id" => 1, "data" => { "name" => "Alice", "age" => 30, ... }, "date_added" => "..." }

admins = db.find("users", filter: { field: "role", op: "eq", value: "admin" })

db.update("users", id, { name: "Alice", age: 31 })
db.delete("users", id)
db.drop_collection("users")
db.close
```

Use `.open` for automatic close on block exit:

```ruby
FileDBv2::Client.open(host: "localhost", port: 5433, api_key: "dev-key") do |db|
  db.create_collection("orders")
  # ...
end
```

---

## Connection options

| Option | Type | Default | Description |
|---|---|---|---|
| `host` | String | `"localhost"` | Server hostname |
| `port` | Integer | `5433` | gRPC port |
| `api_key` | String | *(required)* | Sent as `x-api-key` metadata on every call |
| `tls_ca_cert` | String / nil | `nil` | PEM CA cert string or path to PEM file; enables TLS |

### TLS

```ruby
# Path to CA cert file
db = FileDBv2::Client.new(
  host: "myserver.example.com",
  port: 5433,
  api_key: "my-key",
  tls_ca_cert: "/path/to/ca.crt"
)

# Or inline PEM string
pem = File.read("/path/to/ca.crt")
db = FileDBv2::Client.new(host: "...", port: 5433, api_key: "...", tls_ca_cert: pem)
```

---

## API reference

### Collection management

```ruby
db.create_collection("users")   # => "users"
db.drop_collection("users")     # => true
db.list_collections             # => ["users", "orders"]

# Give the collection a default per-record TTL (seconds). Records inserted
# without an explicit ttl then expire this many seconds after being written.
db.create_collection("sessions", default_ttl_seconds: 3600)
```

### CRUD

```ruby
# Insert one record — returns integer ID
id = db.insert("users", { name: "Alice", age: 30 })

# Insert many — returns array of IDs
ids = db.insert_many("users", [
  { name: "Bob",   age: 25 },
  { name: "Carol", age: 28 },
])

# Find by ID — returns Hash
record = db.find_by_id("users", id)
# { "id" => 1, "data" => { "name" => "Alice", ... }, "date_added" => "2026-..." }

# Find (server-streaming, collected into Array by default)
all = db.find("users")
filtered = db.find("users",
  filter:     { field: "age", op: "gte", value: "25" },
  limit:      10,
  offset:     0,
  order_by:   "name",
  descending: false
)

# Streaming find via block — no buffering
db.find("users") { |record| puts record["data"]["name"] }

# Update
db.update("users", id, { name: "Alice", age: 31 })  # => id

# Delete
db.delete("users", id)  # => true
```

Records carry `"key"` and `"rev"` when the server set them (see
[Keyed CRUD](#keyed-crud-upsert--compare-and-swap)); `"rev"` is the monotonic
per-record revision, starting at 1 and bumped on every write.

### Keyed CRUD, upsert & compare-and-swap

Every record has an optional caller-supplied string **key** and a monotonic
**rev**. These let you address records by your own identifier and do
optimistic-concurrency updates.

```ruby
# upsert: insert under a key, or atomically replace an existing keyed record
# (bumping its rev). Returns the resulting record Hash.
rec = db.upsert("users", "user:alice", { name: "Alice", age: 30 })
# => { "id" => 1, "data" => {...}, "key" => "user:alice", "rev" => 1, ... }

# Keyed create via insert(key:) — a key already held by a live record raises
# FileDBv2::AlreadyExistsError.
db.insert("users", { name: "Bob" }, key: "user:bob")

# Fetch / overwrite / delete by key. A missing key raises FileDBv2::NotFoundError.
db.find_by_key("users", "user:alice")                        # => record Hash
db.update_by_key("users", "user:alice", { name: "Alice", age: 31 })
# => { "id" => 1, "key" => "user:alice", "rev" => 2, "date_modified" => "..." }
db.delete_by_key("users", "user:bob")                        # => true

# Compare-and-swap: the write applies only if the record's current rev matches
# expected_rev. A stale rev (or a missing key) is a clean no-op — never an error.
res = db.update_if_rev("users", "user:alice", 2, { name: "Alice", age: 32 })
# => { "swapped" => true,  "record" => { ... "rev" => 3 } }   when rev matched
# => { "swapped" => false, "record" => nil }                  when rev was stale
```

Typed errors (`FileDBv2::NotFoundError`, `FileDBv2::AlreadyExistsError`) both
subclass `FileDBv2::Error`; other gRPC failures propagate as `GRPC::BadStatus`.

### Field projection

`find`, `find_by_id` and `find_by_key` accept a `fields:` list. When non-empty,
only those top-level fields are returned in each record's `data`; `id`, `key`
and `rev` are always included.

```ruby
db.find_by_id("users", id, fields: ["name", "email"])
db.find_by_key("users", "user:alice", fields: ["name"])
db.find("users", filter: { field: "role", op: "eq", value: "admin" }, fields: ["name"])
```

### Keyset pagination & multi-field sort

`order_by:` also accepts an **Array** for a multi-field, per-field-directional
sort — each item a field name, a `[field, desc]` pair, or a `{ field:, desc: }`
Hash. `find_page` returns `[records, next_page_token]`; feed the token back as
`page_token:` to walk the collection page by page in O(page) time. An empty
token means the last page was reached — keep the same filter, ordering and limit
on every page.

```ruby
ordering = [{ field: "dept", desc: false }, ["age", true]]  # dept ↑, then age ↓

page1, token = db.find_page("users", order_by: ordering, limit: 50)
page2, token = db.find_page("users", order_by: ordering, limit: 50, page_token: token) unless token.empty?
```

### Aggregations

Compute count and numeric aggregations (`sum`/`avg`/`min`/`max`) entirely in the
engine — the collection is never materialised on the client. All three honour
the same filter as `find`.

```ruby
# Count matching (or all) live records
db.count("orders")                                                  # => 128
db.count("orders", filter: { field: "status", op: "eq", value: "paid" })

# Group by a field, aggregating a numeric metric per group
db.group_by("orders", "region", aggregations: ["sum", "avg", "min", "max"], metric: "total")
# => [ { "group" => "us", "count" => 40, "numeric" => true,
#        "sum" => 9000.0, "avg" => 225.0, "min" => 5.0, "max" => 999.0 }, ... ]

# Or the general form: optional group_by, optional numeric field
db.aggregate("orders", aggregations: ["sum"], field: "total")
# => [ { "group" => nil, "count" => 128, "numeric" => true, "sum" => 30000.0, ... } ]
```

Each result Hash carries `"group"` (the group-by value, `nil` for the whole-set
group), `"count"`, `"numeric"`, and — when the group had at least one numeric
`field` value — `"sum"`, `"avg"`, `"min"`, `"max"`.

#### Per-record TTL

`insert`, `insert_many`, and `update` each take a `ttl_seconds:` keyword:

```ruby
# Expire this record 60 seconds from now, regardless of the collection default.
db.insert("sessions", { token: "abc" }, ttl_seconds: 60)

# Same TTL applied to every record in the batch.
db.insert_many("sessions", [{ token: "a" }, { token: "b" }], ttl_seconds: 60)

# On update, ttl_seconds > 0 resets the expiry; ttl_seconds: 0 (the default) is
# sticky and leaves the existing deadline untouched.
db.update("sessions", id, { token: "abc", seen: true }, ttl_seconds: 120)
```

`ttl_seconds: 0` (the default) inherits the collection's default TTL on insert;
a value greater than 0 overrides it. Negative values are rejected by the server.

### Filter syntax

```ruby
# Field filter
{ field: "age",  op: "gt",       value: "18" }
{ field: "name", op: "contains", value: "ali" }
{ field: "email",op: "regex",    value: ".*@gmail\\.com" }

# AND composite
{ and: [
    { field: "role", op: "eq",  value: "admin" },
    { field: "age",  op: "gte", value: "25"    },
] }

# OR composite
{ or: [
    { field: "status", op: "eq", value: "active"    },
    { field: "role",   op: "eq", value: "superuser" },
] }
```

Supported `op` values: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `regex`

### Secondary indexes

```ruby
db.ensure_index("users", "email")
db.list_indexes("users")          # => ["email"]
db.drop_index("users", "email")   # => true
```

### Transactions

```ruby
tx = db.begin_tx("orders")
# ... perform inserts / updates in transaction context ...
db.commit_tx(tx)    # => true
# or
db.rollback_tx(tx)  # => true
```

### Watch (server-streaming change feed)

```ruby
# Enumerator — iterate manually
enum = db.watch("users")
enum.each do |event|
  puts "#{event[:op]}: #{event[:record]["data"].inspect}"
  break if done?
end

# Block form — cleaner for long-running subscriptions
db.watch("users", filter: { field: "role", op: "eq", value: "admin" }) do |event|
  puts event.inspect
end
```

Each event is a Hash:

```ruby
{
  op:         "INSERTED",          # "INSERTED" | "UPDATED" | "DELETED" | "OVERFLOW"
  collection: "users",
  record:     { "id" => 1, "data" => { ... }, "date_added" => "..." },
  ts:         "2026-06-29T12:00:00.000000000Z",
}
```

### Stats

```ruby
s = db.stats("users")
# {
#   collection:    "users",
#   record_count:  42,
#   segment_count: 3,
#   dirty_entries: 0,
#   size_bytes:    8192,
# }
```

### Maintenance

```ruby
# Force a synchronous compaction of a collection — merges dirty segments and
# reclaims space from deleted/overwritten records. Returns true on success.
db.compact("users")

# Stream a consistent gzip-compressed tar snapshot of the whole database
# straight to a file. Returns the number of bytes written; restore with
# `tar xzf backup.tar.gz`.
bytes = db.snapshot_to_file("backup.tar.gz")

# Or consume the raw archive chunks yourself (Snapshot is server-streaming):
db.snapshot { |chunk| out.write(chunk) }   # chunk is a binary String
```

---

## Regenerate proto stubs

The stubs under `lib/filedbv2/proto/` are pre-generated and committed. To
regenerate after a proto change:

```bash
gem install grpc-tools
cd clients/ruby
./generate.sh
```

---

## Run the examples

Start the server (`make run` from repo root), then:

```bash
cd clients/ruby
bundle install
bundle exec ruby examples/test_basic.rb
bundle exec ruby examples/test_watch.rb
```

---

## Publish to RubyGems

```bash
gem build filedbv2.gemspec
gem push filedbv2-0.7.0.gem
```
