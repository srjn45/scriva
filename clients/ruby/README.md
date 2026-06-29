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
gem "filedbv2", "~> 0.1"
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
  op:         "INSERTED",          # "INSERTED" | "UPDATED" | "DELETED"
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
gem push filedbv2-0.1.0.gem
```
