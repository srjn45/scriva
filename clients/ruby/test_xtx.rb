require "minitest/autorun"
require "scriva"

class XTxWireTest < Minitest::Test
  def test_begin_sends_idempotency_and_timeouts
    stub = Object.new
    stub.define_singleton_method(:begin_x_tx) do |request, metadata:|
      @request = request
      Scriva::V1::BeginXTxResponse.new(xtx_id: "x", collections: %w[a b], idempotency_key: "key")
    end
    client = Scriva::Client.allocate
    client.instance_variable_set(:@stub, stub)
    client.instance_variable_set(:@api_key, "")
    result = client.begin_xtx(%w[b a], idempotency_key: "key", idle_timeout_ms: 12, max_lifetime_ms: 34)
    assert_equal "x", result[:xtx_id]
    assert_equal 12, stub.instance_variable_get(:@request).idle_timeout_ms
    assert_equal 34, stub.instance_variable_get(:@request).max_lifetime_ms
  end

  def test_unknown_commit_decodes_errorinfo_then_status_resolution
    varint = ->(n) { out = +""; loop { b=n & 127; n >>= 7; out << (n.zero? ? b : b | 128); break if n.zero? }; out }
    bytes = ->(n, s) { varint.call((n << 3) | 2) + varint.call(s.bytesize) + s }
    str = ->(n, s) { bytes.call(n, s) }
    entry = str.call(1, 'status_ref') + str.call(2, 'idem-9')
    info = str.call(1, 'XTX_OUTCOME_UNKNOWN') + str.call(2, 'scriva.xtx') + bytes.call(3, entry)
    any = str.call(1, 'type.googleapis.com/google.rpc.ErrorInfo') + bytes.call(2, info)
    wire = varint.call(8) + varint.call(2) + bytes.call(3, any)
    stub = Object.new; stub.instance_variable_set(:@commits, 0)
    stub.define_singleton_method(:commit_x_tx) { |*| @commits += 1; raise GRPC::Unknown.new('unknown', { 'grpc-status-details-bin' => wire }) }
    stub.define_singleton_method(:x_tx_status) { |req, metadata:| Scriva::V1::XTxStatusResponse.new(status: :XTX_STATUS_COMMITTED, tx_id: 'durable') }
    client = Scriva::Client.allocate; client.instance_variable_set(:@stub, stub); client.instance_variable_set(:@api_key, '')
    error = assert_raises(Scriva::XTxError) { client.commit_xtx('x') }
    assert_equal 'XTX_OUTCOME_UNKNOWN', error.reason
    assert_equal 'idem-9', error.error_metadata['status_ref']
    assert_equal 'durable', client.xtx_status(error.error_metadata['status_ref'])[:tx_id]
    assert_equal 1, stub.instance_variable_get(:@commits)
  end

  def test_all_operations_map_wire_fields_and_results
    stub = Object.new
    stub.define_singleton_method(:begin_x_tx) { |req, metadata:| Scriva::V1::BeginXTxResponse.new(xtx_id: 'x', collections: req.collections.to_a, idempotency_key: req.idempotency_key) }
    stub.define_singleton_method(:stage_x_tx) { |req, metadata:| @stages ||= []; @stages << req; Scriva::V1::StageXTxResponse.new(id: req.id.zero? ? 17 : req.id) }
    stub.define_singleton_method(:get_x_tx) { |req, metadata:| Scriva::V1::GetXTxResponse.new(record: Scriva::V1::Record.new(id: req.id, rev: 0)) }
    stub.define_singleton_method(:commit_x_tx) { |req, metadata:| Scriva::V1::CommitXTxResponse.new(tx_id: 'durable', replayed: true) }
    stub.define_singleton_method(:rollback_x_tx) { |req, metadata:| Scriva::V1::RollbackXTxResponse.new(ok: true) }
    stub.define_singleton_method(:x_tx_status) { |req, metadata:| @status_ref = req.ref; Scriva::V1::XTxStatusResponse.new(status: :XTX_STATUS_COMMITTED, tx_id: 'durable') }
    client = Scriva::Client.allocate; client.instance_variable_set(:@stub, stub); client.instance_variable_set(:@api_key, '')
    x = client.begin_xtx(%w[a b], idempotency_key: 'idem')
    id = client.stage_xtx(x[:xtx_id], collection: 'a', op: :insert, data: { value: 1 })
    client.stage_xtx(x[:xtx_id], collection: 'a', op: :update, id: id, expected_rev: 2, data: { value: 2 })
    client.stage_xtx(x[:xtx_id], collection: 'b', op: :delete, id: 9, expected_rev: 3)
    assert_equal 0, client.get_xtx(x[:xtx_id], 'a', id)['rev']
    assert client.commit_xtx(x[:xtx_id])[:replayed]
    assert client.rollback_xtx(x[:xtx_id])
    assert_equal 'durable', client.xtx_status('idem')[:tx_id]
    requests = stub.instance_variable_get(:@stages)
    assert_equal :XTX_OP_INSERT, requests[0].op
    assert_equal 2, requests[1].expected_rev
    refute requests[2].has_data?
    assert_equal 'idem', stub.instance_variable_get(:@status_ref)
  end
end
