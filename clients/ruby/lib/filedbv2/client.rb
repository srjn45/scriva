require "grpc"
require "google/protobuf/well_known_types"
require "filedbv2/proto/filedb_pb"
require "filedbv2/proto/filedb_services_pb"

module FileDBv2
  # Synchronous gRPC client for FileDB v2.
  #
  # Example:
  #   db = FileDBv2::Client.new(host: "localhost", port: 5433, api_key: "dev-key")
  #   db.create_collection("users")
  #   id = db.insert("users", { name: "Alice", age: 30 })
  #   record = db.find_by_id("users", id)
  #   db.update("users", id, { name: "Alice", age: 31 })
  #   db.delete("users", id)
  #   db.drop_collection("users")
  #   db.close
  class Client
    OP_MAP = {
      "eq"       => :EQ,
      "neq"      => :NEQ,
      "gt"       => :GT,
      "gte"      => :GTE,
      "lt"       => :LT,
      "lte"      => :LTE,
      "contains" => :CONTAINS,
      "regex"    => :REGEX,
    }.freeze

    WATCH_OP_NAMES = {
      0 => "UNSPECIFIED",
      1 => "INSERTED",
      2 => "UPDATED",
      3 => "DELETED",
      4 => "OVERFLOW",
    }.freeze

    # @param host        [String]  server hostname
    # @param port        [Integer] gRPC port (default 5433)
    # @param api_key     [String]  sent as x-api-key metadata on every call
    # @param tls_ca_cert [String, nil] PEM CA certificate string or path to PEM file;
    #                    when nil the channel uses plaintext (insecure) transport
    def initialize(host: "localhost", port: 5433, api_key:, tls_ca_cert: nil)
      @api_key = api_key
      target   = "#{host}:#{port}"

      if tls_ca_cert
        pem = load_pem(tls_ca_cert)
        creds = GRPC::Core::ChannelCredentials.new(pem)
      else
        creds = :this_channel_is_insecure
      end

      @stub = Filedb::V1::FileDB::Stub.new(target, creds)
    end

    # -- lifecycle -----------------------------------------------------------

    def close
      # GRPC::ClientStub does not expose an explicit close method in all
      # versions; the channel is GC-collected. This method exists for API
      # symmetry with other SDKs and for use as a context manager via #tap.
    end

    # Yields self then closes — use like:
    #   FileDBv2::Client.open(host: ...) { |db| db.insert(...) }
    def self.open(**kwargs, &block)
      client = new(**kwargs)
      if block
        begin
          block.call(client)
        ensure
          client.close
        end
      else
        client
      end
    end

    # -- collection management -----------------------------------------------

    # Create a collection. When +default_ttl_seconds+ > 0, records inserted
    # without an explicit ttl expire that many seconds after being written;
    # 0 inherits the server-wide default.
    def create_collection(name, default_ttl_seconds: 0)
      req  = Filedb::V1::CreateCollectionRequest.new(
        name:                name,
        default_ttl_seconds: default_ttl_seconds,
      )
      resp = @stub.create_collection(req, metadata: metadata)
      resp.name
    end

    def drop_collection(name)
      req  = Filedb::V1::DropCollectionRequest.new(name: name)
      resp = @stub.drop_collection(req, metadata: metadata)
      resp.ok
    end

    def list_collections
      req  = Filedb::V1::ListCollectionsRequest.new
      resp = @stub.list_collections(req, metadata: metadata)
      resp.names.to_a
    end

    # -- CRUD ----------------------------------------------------------------

    # Insert one record. Returns the assigned integer ID.
    #
    # +ttl_seconds+ > 0 expires the record that long after insertion, overriding
    # any collection default; 0 applies the collection default (if any).
    def insert(collection, data, ttl_seconds: 0)
      req  = Filedb::V1::InsertRequest.new(
        collection:  collection,
        data:        hash_to_struct(data),
        ttl_seconds: ttl_seconds,
      )
      resp = @stub.insert(req, metadata: metadata)
      resp.id
    end

    # Insert multiple records. Returns an array of assigned IDs.
    # +ttl_seconds+ is applied uniformly to every record in the batch.
    def insert_many(collection, records, ttl_seconds: 0)
      req  = Filedb::V1::InsertManyRequest.new(
        collection:  collection,
        records:     records.map { |r| hash_to_struct(r) },
        ttl_seconds: ttl_seconds,
      )
      resp = @stub.insert_many(req, metadata: metadata)
      resp.ids.to_a
    end

    # Fetch a single record by ID. Returns a Hash.
    def find_by_id(collection, id)
      req  = Filedb::V1::FindByIdRequest.new(collection: collection, id: id)
      resp = @stub.find_by_id(req, metadata: metadata)
      record_to_hash(resp.record)
    end

    # Query records. Returns an Array of Hashes.
    # The Find RPC is server-streaming; this collects the stream for convenience.
    # Pass a block to stream results one by one without buffering.
    #
    # @param filter     [Hash, nil] filter hash (see filter_to_proto)
    # @param limit      [Integer]   0 = no limit
    # @param offset     [Integer]
    # @param order_by   [String]    field name to sort by
    # @param descending [Boolean]
    def find(collection, filter: nil, limit: 0, offset: 0, order_by: "", descending: false)
      req = Filedb::V1::FindRequest.new(
        collection: collection,
        filter:     filter_to_proto(filter),
        limit:      limit,
        offset:     offset,
        order_by:   order_by,
        descending: descending,
      )
      stream = @stub.find(req, metadata: metadata)
      if block_given?
        stream.each { |resp| yield record_to_hash(resp.record) }
        nil
      else
        stream.map { |resp| record_to_hash(resp.record) }
      end
    end

    # Update a record. +ttl_seconds+ > 0 resets the record's expiry deadline to
    # that long from now; 0 (the default) is sticky and leaves any existing
    # deadline untouched.
    def update(collection, id, data, ttl_seconds: 0)
      req  = Filedb::V1::UpdateRequest.new(
        collection:  collection,
        id:          id,
        data:        hash_to_struct(data),
        ttl_seconds: ttl_seconds,
      )
      resp = @stub.update(req, metadata: metadata)
      resp.id
    end

    def delete(collection, id)
      req  = Filedb::V1::DeleteRequest.new(collection: collection, id: id)
      resp = @stub.delete(req, metadata: metadata)
      resp.ok
    end

    # -- secondary indexes ---------------------------------------------------

    def ensure_index(collection, field)
      req = Filedb::V1::EnsureIndexRequest.new(collection: collection, field: field)
      @stub.ensure_index(req, metadata: metadata)
      nil
    end

    def drop_index(collection, field)
      req  = Filedb::V1::DropIndexRequest.new(collection: collection, field: field)
      resp = @stub.drop_index(req, metadata: metadata)
      resp.ok
    end

    def list_indexes(collection)
      req  = Filedb::V1::ListIndexesRequest.new(collection: collection)
      resp = @stub.list_indexes(req, metadata: metadata)
      resp.fields.to_a
    end

    # -- transactions --------------------------------------------------------

    def begin_tx(collection)
      req  = Filedb::V1::BeginTxRequest.new(collection: collection)
      resp = @stub.begin_tx(req, metadata: metadata)
      resp.tx_id
    end

    def commit_tx(tx_id)
      req  = Filedb::V1::CommitTxRequest.new(tx_id: tx_id)
      resp = @stub.commit_tx(req, metadata: metadata)
      resp.ok
    end

    def rollback_tx(tx_id)
      req  = Filedb::V1::RollbackTxRequest.new(tx_id: tx_id)
      resp = @stub.rollback_tx(req, metadata: metadata)
      resp.ok
    end

    # -- watch ---------------------------------------------------------------

    # Subscribe to change events on a collection.
    #
    # Without a block, returns an Enumerator that yields event Hashes:
    #   { op: "INSERTED", collection: "users", record: {...}, ts: "2026-..." }
    #
    # With a block, iterates until the server closes the stream or the block
    # raises StopIteration / the caller breaks.
    #
    # @param filter [Hash, nil] optional filter
    def watch(collection, filter: nil)
      req    = Filedb::V1::WatchRequest.new(
        collection: collection,
        filter:     filter_to_proto(filter),
      )
      stream = @stub.watch(req, metadata: metadata)
      enum   = Enumerator.new do |yielder|
        stream.each do |event|
          out = {
            op:         WATCH_OP_NAMES[event.op] || "UNSPECIFIED",
            collection: event.collection,
            record:     record_to_hash(event.record),
          }
          out[:ts] = event.ts.to_time.utc.iso8601(9) if event.has_field?(:ts)
          yielder << out
        end
      end
      if block_given?
        enum.each { |ev| yield ev }
        nil
      else
        enum
      end
    end

    # -- stats ---------------------------------------------------------------

    def stats(collection)
      req  = Filedb::V1::CollectionStatsRequest.new(collection: collection)
      resp = @stub.collection_stats(req, metadata: metadata)
      {
        collection:    resp.collection,
        record_count:  resp.record_count,
        segment_count: resp.segment_count,
        dirty_entries: resp.dirty_entries,
        size_bytes:    resp.size_bytes,
      }
    end

    # -- maintenance ---------------------------------------------------------

    # Force a synchronous compaction of a collection — merges dirty segments and
    # reclaims space from deleted/overwritten records. Returns only once it is
    # done. Returns true on success.
    def compact(collection)
      req  = Filedb::V1::CompactRequest.new(collection: collection)
      resp = @stub.compact(req, metadata: metadata)
      resp.ok
    end

    # Stream a consistent, gzip-compressed tar snapshot of the whole database.
    #
    # Without a block, returns an Enumerator yielding binary String chunks.
    # With a block, yields each chunk in turn. Concatenate the chunks in order
    # to reconstruct the archive; restore with `tar xzf backup.tar.gz`.
    def snapshot(&block)
      req    = Filedb::V1::SnapshotRequest.new
      stream = @stub.snapshot(req, metadata: metadata)
      enum   = Enumerator.new { |y| stream.each { |chunk| y << chunk.data } }
      if block
        enum.each(&block)
        nil
      else
        enum
      end
    end

    # Stream a whole-database snapshot straight to a file (binary). Returns the
    # number of bytes written.
    def snapshot_to_file(path)
      total = 0
      File.open(path, "wb") do |f|
        snapshot { |chunk| total += f.write(chunk) }
      end
      total
    end

    private

    def metadata
      { "x-api-key" => @api_key }
    end

    def load_pem(tls_ca_cert)
      return tls_ca_cert if tls_ca_cert.include?("BEGIN CERTIFICATE")
      File.read(tls_ca_cert)
    end

    # Convert a plain Ruby Hash into a google.protobuf.Struct message.
    def hash_to_struct(hash)
      Google::Protobuf::Struct.from_hash(stringify_keys(hash))
    end

    def stringify_keys(hash)
      hash.transform_keys(&:to_s)
    end

    # Convert a proto Record into a plain Ruby Hash.
    def record_to_hash(record)
      return {} if record.nil?
      out = {
        "id"   => record.id,
        "data" => record.data ? record.data.to_h : {},
      }
      out["date_added"]    = record.date_added.to_time.utc.iso8601(9)    if record.has_field?(:date_added)
      out["date_modified"] = record.date_modified.to_time.utc.iso8601(9) if record.has_field?(:date_modified)
      out
    end

    # Convert a plain Hash filter into a proto Filter message.
    #
    # Accepted shapes:
    #   { field: "age", op: "gt", value: "30" }
    #   { and: [ <filter>, ... ] }
    #   { or:  [ <filter>, ... ] }
    def filter_to_proto(hash)
      return nil if hash.nil?

      h = hash.transform_keys(&:to_s)

      if h.key?("and")
        children = h["and"].map { |c| filter_to_proto(c) }
        return Filedb::V1::Filter.new(and: Filedb::V1::AndFilter.new(filters: children))
      end

      if h.key?("or")
        children = h["or"].map { |c| filter_to_proto(c) }
        return Filedb::V1::Filter.new(or: Filedb::V1::OrFilter.new(filters: children))
      end

      op_name = h["op"].to_s.downcase
      op_val  = OP_MAP[op_name] or raise ArgumentError,
        "unknown filter op #{h["op"].inspect}; expected one of #{OP_MAP.keys.sort.join(", ")}"

      Filedb::V1::Filter.new(
        field: Filedb::V1::FieldFilter.new(
          field: h["field"].to_s,
          op:    Filedb::V1::FilterOp.const_get(op_val),
          value: h["value"].to_s,
        )
      )
    end
  end
end
