import unittest
import grpc
from google.protobuf.any_pb2 import Any
from google.rpc import error_details_pb2, status_pb2

from scriva.client import ScrivaDB, XTxError


class Stub:
    def BeginXTx(self, request, metadata):
        self.request = request
        from scriva.proto import scriva_pb2 as pb
        return pb.BeginXTxResponse(xtx_id="x", collections=["a", "b"], idempotency_key="key")

    def StageXTx(self, request, metadata):
        self.stage = request
        from scriva.proto import scriva_pb2 as pb
        return pb.StageXTxResponse(id=7)
    def GetXTx(self, request, metadata):
        self.get = request
        from scriva.proto import scriva_pb2 as pb
        return pb.GetXTxResponse(record=pb.Record(id=7, rev=0))
    def CommitXTx(self, request, metadata):
        self.commit = request
        from scriva.proto import scriva_pb2 as pb
        return pb.CommitXTxResponse(tx_id="durable", replayed=True, ops=[pb.XTxOpResult(collection="a", id=7, rev=3, op=pb.XTX_OP_INSERT)])
    def RollbackXTx(self, request, metadata):
        self.rollback = request
        from scriva.proto import scriva_pb2 as pb
        return pb.RollbackXTxResponse(ok=True)
    def XTxStatus(self, request, metadata):
        self.status = request
        from scriva.proto import scriva_pb2 as pb
        return pb.XTxStatusResponse(status=pb.XTX_STATUS_COMMITTED, tx_id="durable")


class XTxWireTest(unittest.TestCase):
    def test_begin_preserves_idempotency_and_timeout_wire_fields(self):
        db = ScrivaDB.__new__(ScrivaDB)
        db._stub, db._api_key = Stub(), ""
        result = db.begin_xtx(["b", "a"], idempotency_key="key", idle_timeout_ms=12, max_lifetime_ms=34)
        self.assertEqual(result["xtx_id"], "x")
        self.assertEqual(db._stub.request.idempotency_key, "key")
        self.assertEqual(db._stub.request.idle_timeout_ms, 12)
        self.assertEqual(db._stub.request.max_lifetime_ms, 34)

    def test_stage_get_commit_rollback_and_status_wire_fields(self):
        db = ScrivaDB.__new__(ScrivaDB); db._stub, db._api_key = Stub(), ""
        self.assertEqual(db.stage_xtx("x", {"collection": "a", "op": "insert", "data": {"n": 1}}), "7")
        self.assertEqual(db._stub.stage.op, 1)
        self.assertEqual(db.get_xtx("x", "a", 7)["rev"], "0")
        self.assertEqual(db.commit_xtx("x", lock_timeout_ms=9)["tx_id"], "durable")
        self.assertEqual(db._stub.commit.lock_timeout_ms, 9)
        self.assertTrue(db.rollback_xtx("x"))
        self.assertEqual(db.xtx_status("key")["tx_id"], "durable")
        self.assertEqual(db._stub.status.ref, "key")

    def test_unknown_commit_decodes_errorinfo_then_uses_status_ref_once(self):
        info = error_details_pb2.ErrorInfo(reason="XTX_OUTCOME_UNKNOWN", domain="scriva.xtx",
            metadata={"status_ref": "idem-9", "retry_safe": "false"})
        detail = Any(); detail.Pack(info)
        wire = status_pb2.Status(code=2, message="unknown", details=[detail]).SerializeToString()
        class Unknown(grpc.RpcError):
            def code(self): return grpc.StatusCode.UNKNOWN
            def details(self): return "unknown"
            def trailing_metadata(self): return (("grpc-status-details-bin", wire),)
        class UnknownStub(Stub):
            commits = 0
            def CommitXTx(self, request, metadata):
                self.commits += 1
                raise Unknown()
        db = ScrivaDB.__new__(ScrivaDB); db._stub, db._api_key = UnknownStub(), ""
        with self.assertRaises(XTxError) as caught: db.commit_xtx("x")
        self.assertEqual(caught.exception.reason, "XTX_OUTCOME_UNKNOWN")
        ref = caught.exception.error_metadata["status_ref"]
        self.assertEqual(ref, "idem-9")
        self.assertEqual(db.xtx_status(ref)["tx_id"], "durable")
        self.assertEqual(db._stub.commits, 1)

