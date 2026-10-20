using Google.Protobuf;
using Grpc.Core;
using Scriva.Client;
using Xunit;
using System.Reflection;
using Scriva.V1;

public class XTxErrorTests
{
    static byte[] Field(int n, byte[] body)
    {
        using var ms = new MemoryStream(); using var output = new CodedOutputStream(ms, true);
        output.WriteTag(n, WireFormat.WireType.LengthDelimited); output.WriteBytes(ByteString.CopyFrom(body)); output.Flush(); return ms.ToArray();
    }
    static byte[] Text(int n, string value) => Field(n, System.Text.Encoding.UTF8.GetBytes(value));
    static byte[] ErrorInfo()
    {
        var map = Text(1, "status_ref").Concat(Text(2, "idem-9")).ToArray();
        var info = Text(1, "XTX_OUTCOME_UNKNOWN").Concat(Text(2, "scriva.xtx")).Concat(Field(3, map)).ToArray();
        var any = Text(1, "type.googleapis.com/google.rpc.ErrorInfo").Concat(Field(2, info)).ToArray();
        using var ms = new MemoryStream(); using var output = new CodedOutputStream(ms, true);
        output.WriteTag(1, WireFormat.WireType.Varint); output.WriteInt32((int)StatusCode.Unknown);
        output.WriteTag(3, WireFormat.WireType.LengthDelimited); output.WriteBytes(ByteString.CopyFrom(any)); output.Flush(); return ms.ToArray();
    }

    [Fact]
    public void UnknownCommitErrorExposesStatusReferenceAndOriginalStatus()
    {
        var trailers = new Metadata { { "grpc-status-details-bin", ErrorInfo() } };
        var rpc = new RpcException(new Status(StatusCode.Unknown, "ambiguous"), trailers);
        var error = XTxException.From(rpc);
        Assert.Same(rpc, error.InnerException);
        Assert.Equal(StatusCode.Unknown, error.StatusCode);
        Assert.Equal("XTX_OUTCOME_UNKNOWN", error.Reason);
        Assert.Equal("idem-9", error.ErrorMetadata["status_ref"]);
    }

    [Fact]
    public async Task XTxCallWrapsUnknownOnceThenCallerCanResolveStatusRef()
    {
        var trailers = new Metadata { { "grpc-status-details-bin", ErrorInfo() } };
        var rpc = new RpcException(new Status(StatusCode.Unknown, "ambiguous"), trailers);
        int commits = 0, statusCalls = 0;
        var call = new AsyncUnaryCall<BeginXTxResponse>(Task.FromException<BeginXTxResponse>(rpc), Task.FromResult(new Metadata()), () => rpc.Status, () => trailers, () => { });
        var helper = typeof(ScrivaDB).GetMethod("XTxCall", BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(typeof(BeginXTxResponse));
        Func<AsyncUnaryCall<BeginXTxResponse>> invokeCommit = () => { commits++; return call; };
        var task = (Task)helper.Invoke(null, new object[] { invokeCommit })!;
        var error = await Assert.ThrowsAsync<XTxException>(async () => await (Task<BeginXTxResponse>)task);
        Assert.Equal(1, commits);
        statusCalls++;
        Assert.Equal("idem-9", error.ErrorMetadata["status_ref"]);
        Assert.Equal(1, statusCalls);
    }
}
