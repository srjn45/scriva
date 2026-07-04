#!/usr/bin/env ruby
# examples/test_basic.rb — end-to-end smoke test for the FileDBv2 Ruby client.
#
# Start the server first:
#   make run   (from the repo root)
#
# Then run:
#   bundle exec ruby examples/test_basic.rb

$LOAD_PATH.unshift File.join(__dir__, "..", "lib")
require "filedbv2"

HOST    = ENV.fetch("FILEDB_HOST",    "localhost")
PORT    = ENV.fetch("FILEDB_PORT",    "5433").to_i
API_KEY = ENV.fetch("FILEDB_API_KEY", "dev-key")
COLL    = "test_ruby"

db = FileDBv2::Client.new(host: HOST, port: PORT, api_key: API_KEY)

# ── setup ──────────────────────────────────────────────────────────────────────
db.drop_collection(COLL) rescue nil   # clean slate if a previous run left debris
name = db.create_collection(COLL)
puts "Created collection: #{name}"

# ── list collections ───────────────────────────────────────────────────────────
cols = db.list_collections
puts "Collections: #{cols.inspect}"

# ── insert ─────────────────────────────────────────────────────────────────────
id1 = db.insert(COLL, { name: "Alice",   age: 30, role: "admin" })
id2 = db.insert(COLL, { name: "Bob",     age: 25, role: "user"  })
id3 = db.insert(COLL, { name: "Charlie", age: 35, role: "admin" })
puts "Inserted IDs: #{id1}, #{id2}, #{id3}"

# ── insert_many ────────────────────────────────────────────────────────────────
batch_ids = db.insert_many(COLL, [
  { name: "Dave",  age: 28, role: "user" },
  { name: "Eve",   age: 22, role: "user" },
])
puts "Batch insert IDs: #{batch_ids.inspect}"

# ── find_by_id ─────────────────────────────────────────────────────────────────
record = db.find_by_id(COLL, id1)
puts "find_by_id(#{id1}): #{record.inspect}"

# ── find with field filter ─────────────────────────────────────────────────────
admins = db.find(COLL, filter: { field: "role", op: "eq", value: "admin" })
puts "Admins (#{admins.size}): #{admins.map { |r| r["data"]["name"] }.inspect}"

# ── find with AND composite filter ────────────────────────────────────────────
results = db.find(COLL, filter: {
  and: [
    { field: "role", op: "eq",  value: "user" },
    { field: "age",  op: "gte", value: "25"   },
  ]
})
puts "Users aged >= 25 (#{results.size}): #{results.map { |r| r["data"]["name"] }.inspect}"

# ── find with order + limit ────────────────────────────────────────────────────
top2 = db.find(COLL, order_by: "name", limit: 2)
puts "First 2 by name: #{top2.map { |r| r["data"]["name"] }.inspect}"

# ── streaming find via block ───────────────────────────────────────────────────
print "Streaming all records: "
db.find(COLL) { |r| print "#{r["data"]["name"]} " }
puts

# ── secondary index ────────────────────────────────────────────────────────────
db.ensure_index(COLL, "role")
puts "Indexes: #{db.list_indexes(COLL).inspect}"
db.drop_index(COLL, "role")
puts "Indexes after drop: #{db.list_indexes(COLL).inspect}"

# ── update ─────────────────────────────────────────────────────────────────────
db.update(COLL, id1, { name: "Alice", age: 31, role: "admin" })
updated = db.find_by_id(COLL, id1)
puts "After update: #{updated["data"].inspect}"

# ── delete ─────────────────────────────────────────────────────────────────────
ok = db.delete(COLL, id2)
puts "Deleted id #{id2}: #{ok}"

# ── transactions ───────────────────────────────────────────────────────────────
tx = db.begin_tx(COLL)
puts "Started tx: #{tx}"
db.commit_tx(tx)
puts "Committed tx"

tx2 = db.begin_tx(COLL)
db.rollback_tx(tx2)
puts "Rolled back tx2"

# ── stats ──────────────────────────────────────────────────────────────────────
s = db.stats(COLL)
puts "Stats: collection=#{s[:collection]} records=#{s[:record_count]} " \
     "segments=#{s[:segment_count]} size=#{s[:size_bytes]}B"

# ── compaction ─────────────────────────────────────────────────────────────────
puts "Compact: #{db.compact(COLL)}"

# ── per-record TTL ─────────────────────────────────────────────────────────────
ttl_id = db.insert(COLL, { name: "Ephemeral", role: "temp" }, ttl_seconds: 3600)
puts "Inserted #{ttl_id} with a 3600s TTL"
# ttl_seconds: 0 (default) is sticky — it keeps the existing deadline
db.update(COLL, ttl_id, { name: "Ephemeral", role: "temp", touched: true })
puts "Updated the TTL record (deadline preserved)"

# ── keyed CRUD, upsert & compare-and-swap (N1) ─────────────────────────────────
KEYED = "test_ruby_keyed"
db.drop_collection(KEYED) rescue nil
db.create_collection(KEYED)

# upsert inserts under a caller-supplied key, or replaces + bumps rev if it exists
r = db.upsert(KEYED, "user:alice", { name: "Alice", age: 30 })
puts "upsert (insert): key=#{r["key"]} rev=#{r["rev"]}"
r = db.upsert(KEYED, "user:alice", { name: "Alice", age: 31 })
puts "upsert (replace): rev=#{r["rev"]}"

# keyed insert — a duplicate key raises AlreadyExistsError
db.insert(KEYED, { name: "Bob" }, key: "user:bob")
begin
  db.insert(KEYED, { name: "Bob again" }, key: "user:bob")
rescue FileDBv2::AlreadyExistsError => e
  puts "keyed insert dup rejected: #{e.class}"
end

# fetch / update / delete by key; a missing key raises NotFoundError
fetched = db.find_by_key(KEYED, "user:alice")
puts "find_by_key(user:alice): #{fetched["data"].inspect} rev=#{fetched["rev"]}"
upd = db.update_by_key(KEYED, "user:alice", { name: "Alice", age: 32 })
puts "update_by_key: id=#{upd["id"]} rev=#{upd["rev"]}"
begin
  db.find_by_key(KEYED, "user:ghost")
rescue FileDBv2::NotFoundError => e
  puts "find_by_key(missing) raised: #{e.class}"
end

# compare-and-swap: the write applies only if rev matches (stale = clean no-op)
current = db.find_by_key(KEYED, "user:alice")
stale = db.update_if_rev(KEYED, "user:alice", 1, { name: "Nope", age: 0 })
puts "update_if_rev(stale rev): swapped=#{stale["swapped"]}"
ok = db.update_if_rev(KEYED, "user:alice", current["rev"], { name: "Alice", age: 33 })
puts "update_if_rev(current rev): swapped=#{ok["swapped"]} rev=#{ok["record"]["rev"]}"
puts "delete_by_key(user:bob): #{db.delete_by_key(KEYED, "user:bob")}"

# ── field projection (N2) ──────────────────────────────────────────────────────
slim = db.find_by_key(KEYED, "user:alice", fields: ["name"])
puts "projected find_by_key: #{slim["data"].inspect}"   # only requested top-level fields

# ── keyset pagination + multi-field order_by (N3) ──────────────────────────────
PAGED = "test_ruby_paged"
db.drop_collection(PAGED) rescue nil
db.create_collection(PAGED)
db.insert_many(PAGED, [
  { name: "Carol", age: 25, dept: "eng"   },
  { name: "Dave",  age: 35, dept: "eng"   },
  { name: "Eve",   age: 28, dept: "sales" },
  { name: "Frank", age: 45, dept: "sales" },
])

# multi-field sort: dept ascending, then age descending within each dept
ordering = [{ field: "dept", desc: false }, ["age", true]]
page, token = db.find_page(PAGED, order_by: ordering, limit: 2)
puts "page 1 (#{page.size}): #{page.map { |r| r["data"]["name"] }.inspect} next_token?=#{!token.empty?}"
page2, _ = db.find_page(PAGED, order_by: ordering, limit: 2, page_token: token)
puts "page 2 (#{page2.size}): #{page2.map { |r| r["data"]["name"] }.inspect}"

# ── aggregations: count / group_by / numeric (N4) ──────────────────────────────
puts "count(all): #{db.count(PAGED)}"
puts "count(dept=eng): #{db.count(PAGED, filter: { field: "dept", op: "eq", value: "eng" })}"
db.group_by(PAGED, "dept", aggregations: ["sum", "avg", "min", "max"], metric: "age").each do |g|
  puts "  group #{g["group"].inspect}: count=#{g["count"]} sum=#{g["sum"]} avg=#{g["avg"]} min=#{g["min"]} max=#{g["max"]}"
end

db.drop_collection(KEYED)
db.drop_collection(PAGED)

# ── snapshot (whole-database backup) ───────────────────────────────────────────
require "tmpdir"
backup = File.join(Dir.tmpdir, "filedb_ruby_snapshot.tar.gz")
bytes  = db.snapshot_to_file(backup)
puts "Snapshot: wrote #{bytes} bytes to #{backup}"
File.delete(backup) if File.exist?(backup)

# ── teardown ───────────────────────────────────────────────────────────────────
db.drop_collection(COLL)
puts "Dropped collection #{COLL}"

db.close
puts "Done."
