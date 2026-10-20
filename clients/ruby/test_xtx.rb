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
end
