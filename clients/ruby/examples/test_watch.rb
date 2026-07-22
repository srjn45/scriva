#!/usr/bin/env ruby
# examples/test_watch.rb — Watch streaming demo for the Scriva Ruby client.
#
# Start the server first:
#   make run   (from the repo root)
#
# Then run:
#   bundle exec ruby examples/test_watch.rb

$LOAD_PATH.unshift File.join(__dir__, "..", "lib")
require "scriva"
require "thread"

HOST    = ENV.fetch("SCRIVA_HOST",    "localhost")
PORT    = ENV.fetch("SCRIVA_PORT",    "5433").to_i
API_KEY = ENV.fetch("SCRIVA_API_KEY", "dev-key")
COLL    = "watch_demo_ruby"

# Two independent clients: one to watch, one to insert.
watcher_db = Scriva::Client.new(host: HOST, port: PORT, api_key: API_KEY)
writer_db  = Scriva::Client.new(host: HOST, port: PORT, api_key: API_KEY)

writer_db.drop_collection(COLL) rescue nil
writer_db.create_collection(COLL)
puts "Collection #{COLL} ready"

events_received = []
watch_ready     = Mutex.new
watch_cond      = ConditionVariable.new
started         = false

# Watch thread — enumerator-based streaming
watch_thread = Thread.new do
  watch_ready.synchronize do
    started = true
    watch_cond.signal
  end

  watcher_db.watch(COLL) do |event|
    events_received << event
    puts "[WATCH] op=#{event[:op]} name=#{event.dig(:record, "data", "name")}"
    break if events_received.size >= 3
  end
end

# Wait until the watch stream is initialised before writing
watch_ready.synchronize { watch_cond.wait(watch_ready) until started }
sleep 0.2   # give the server time to register the subscription

# Insert 3 records — each should trigger a INSERTED event
puts "Inserting 3 records..."
writer_db.insert(COLL, { name: "Alice", role: "admin" })
writer_db.insert(COLL, { name: "Bob",   role: "user"  })
writer_db.insert(COLL, { name: "Carol", role: "user"  })

watch_thread.join(5)

puts "\nReceived #{events_received.size} watch event(s)."
events_received.each { |e| puts "  #{e[:op]} — #{e.dig(:record, "data").inspect}" }

writer_db.drop_collection(COLL)
watcher_db.close
writer_db.close
puts "Done."
