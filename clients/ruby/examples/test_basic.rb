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
