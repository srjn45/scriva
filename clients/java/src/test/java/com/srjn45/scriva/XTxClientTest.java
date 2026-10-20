package com.srjn45.scriva;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.protobuf.StatusProto;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;
import scriva.v1.ScrivaGrpc;
import scriva.v1.ScrivaOuterClass;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Hermetic XTx wire coverage; a commit is deliberately issued exactly once. */
class XTxClientTest {
    @Test void allXTxMethodsMapFieldsAndDoNotBlindRetry() throws Exception {
        String name = InProcessServerBuilder.generateName();
        Fake fake = new Fake();
        Server server = InProcessServerBuilder.forName(name).directExecutor().addService(fake).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try (ScrivaDBClient db = new ScrivaDBClient(channel, "key")) {
            var tx = db.beginXTx(List.of("ledger", "accounts"), new ScrivaDBClient.XTxBeginOptions("idem", 20, 40));
            assertEquals(List.of("accounts", "ledger"), tx.collections);
            assertEquals(8, db.stageXTx(tx.xtxId, new ScrivaDBClient.XTxOperation("accounts", "update", 8, Map.of("balance", 9), 3)));
            assertEquals(3, db.getXTx(tx.xtxId, "accounts", 8).rev());
            assertEquals("decision-1", db.commitXTx(tx.xtxId, new ScrivaDBClient.XTxCommitOptions(50)).txId);
            assertTrue(db.rollbackXTx(tx.xtxId));
            assertEquals("decision-1", db.xTxStatus("idem").txId);
            assertEquals(1, fake.commitCalls, "unknown outcomes must be resolved with XTxStatus, never a blind retry");
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test void unknownOutcomeDetailRetainsStatusReferenceForRecovery() {
        var info = com.google.rpc.ErrorInfo.newBuilder().setDomain("scriva.xtx").setReason("XTX_OUTCOME_UNKNOWN")
                .putMetadata("status_ref", "idem").putMetadata("retry_safe", "false").build();
        var error = StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder().setCode(io.grpc.Status.Code.UNKNOWN.value()).addDetails(com.google.protobuf.Any.pack(info)).build());
        var detail = ScrivaDBClient.xTxErrorDetail(error);
        assertEquals("XTX_OUTCOME_UNKNOWN", detail.reason);
        assertEquals("idem", detail.metadata.get("status_ref"));
        assertEquals("false", detail.metadata.get("retry_safe"));
    }

    static final class Fake extends ScrivaGrpc.ScrivaImplBase {
        int commitCalls;
        @Override public void beginXTx(ScrivaOuterClass.BeginXTxRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.BeginXTxResponse> o) { o.onNext(ScrivaOuterClass.BeginXTxResponse.newBuilder().setXtxId("xtx-1").addAllCollections(List.of("accounts", "ledger")).setIdempotencyKey(r.getIdempotencyKey()).build()); o.onCompleted(); }
        @Override public void stageXTx(ScrivaOuterClass.StageXTxRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.StageXTxResponse> o) { o.onNext(ScrivaOuterClass.StageXTxResponse.newBuilder().setId(r.getId()).build()); o.onCompleted(); }
        @Override public void getXTx(ScrivaOuterClass.GetXTxRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.GetXTxResponse> o) { o.onNext(ScrivaOuterClass.GetXTxResponse.newBuilder().setRecord(ScrivaOuterClass.Record.newBuilder().setId(r.getId()).setRev(3)).build()); o.onCompleted(); }
        @Override public void commitXTx(ScrivaOuterClass.CommitXTxRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.CommitXTxResponse> o) { commitCalls++; o.onNext(ScrivaOuterClass.CommitXTxResponse.newBuilder().setTxId("decision-1").addOps(ScrivaOuterClass.XTxOpResult.newBuilder().setCollection("accounts").setId(8).setRev(4).setOp(ScrivaOuterClass.XTxOpKind.XTX_OP_UPDATE)).build()); o.onCompleted(); }
        @Override public void rollbackXTx(ScrivaOuterClass.RollbackXTxRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.RollbackXTxResponse> o) { o.onNext(ScrivaOuterClass.RollbackXTxResponse.newBuilder().setOk(true).build()); o.onCompleted(); }
        @Override public void xTxStatus(ScrivaOuterClass.XTxStatusRequest r, io.grpc.stub.StreamObserver<ScrivaOuterClass.XTxStatusResponse> o) { o.onNext(ScrivaOuterClass.XTxStatusResponse.newBuilder().setStatus(ScrivaOuterClass.XTxState.XTX_STATUS_COMMITTED).setTxId("decision-1").build()); o.onCompleted(); }
    }
}
