import unittest

from scriva.client import ScrivaDB


class Stub:
    def BeginXTx(self, request, metadata):
        self.request = request
        from scriva.proto import scriva_pb2 as pb
        return pb.BeginXTxResponse(xtx_id="x", collections=["a", "b"], idempotency_key="key")


class XTxWireTest(unittest.TestCase):
    def test_begin_preserves_idempotency_and_timeout_wire_fields(self):
        db = ScrivaDB.__new__(ScrivaDB)
        db._stub, db._api_key = Stub(), ""
        result = db.begin_xtx(["b", "a"], idempotency_key="key", idle_timeout_ms=12, max_lifetime_ms=34)
        self.assertEqual(result["xtx_id"], "x")
        self.assertEqual(db._stub.request.idempotency_key, "key")
        self.assertEqual(db._stub.request.idle_timeout_ms, 12)
        self.assertEqual(db._stub.request.max_lifetime_ms, 34)

