using Grpc.Core;
using Google.Protobuf;
using System.Collections.Generic;

namespace Scriva.Client;

/// <summary>
/// Base class for ScrivaDB client errors surfaced from engine gRPC status codes.
///
/// <para>The keyed-CRUD surface (N1) maps the common failure codes onto typed
/// subclasses — <see cref="NotFoundException"/> for <c>NOT_FOUND</c> and
/// <see cref="AlreadyExistsException"/> for <c>ALREADY_EXISTS</c>. Any other status
/// code propagates unchanged as the original <see cref="RpcException"/>.</para>
/// </summary>
public class ScrivaDBException : Exception
{
    public ScrivaDBException(string message) : base(message) { }
    public ScrivaDBException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A keyed lookup, update, or delete referenced a key that no live record holds.
///
/// <para>Raised from <see cref="ScrivaDB.FindByKeyAsync"/>, <see cref="ScrivaDB.UpdateByKeyAsync"/>,
/// and <see cref="ScrivaDB.DeleteByKeyAsync"/> when the engine answers <c>NOT_FOUND</c>.</para>
/// </summary>
public sealed class NotFoundException : ScrivaDBException
{
    public NotFoundException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// A keyed insert used a key already held by a live record.
///
/// <para>Raised from <see cref="ScrivaDB.InsertAsync"/> (and <see cref="ScrivaDB.InsertKeyedAsync"/>)
/// when the engine answers <c>ALREADY_EXISTS</c>.</para>
/// </summary>
public sealed class AlreadyExistsException : ScrivaDBException
{
    public AlreadyExistsException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Structured XTx failure retaining gRPC status and trailing ErrorInfo metadata.</summary>
public sealed class XTxException : ScrivaDBException
{
    public StatusCode StatusCode { get; }
    public Metadata Trailers { get; }
    public string? Reason { get; }
    public IReadOnlyDictionary<string, string> ErrorMetadata { get; }
    public XTxException(RpcException error) : base(error.Status.Detail, error)
    {
        StatusCode = error.StatusCode;
        Trailers = error.Trailers;
        (Reason, ErrorMetadata) = Decode(error.Trailers);
    }
    /// <summary>Wrap an XTx RpcException so callers can inspect ErrorInfo trailers/status_ref.</summary>
    public static XTxException From(RpcException error) => new(error);

    private static (string?, IReadOnlyDictionary<string, string>) Decode(Metadata trailers)
    {
        var binary = trailers.FirstOrDefault(e => e.Key == "grpc-status-details-bin")?.ValueBytes;
        if (binary is null) return (null, new Dictionary<string, string>());
        try
        {
            using var input = new CodedInputStream(binary);
            while (!input.IsAtEnd)
            {
                uint tag = input.ReadTag();
                if (tag == 0) break;
                if ((tag >> 3) == 3 && (tag & 7) == 2)
                {
                    var any = input.ReadBytes();
                    var decoded = DecodeAny(any.ToByteArray());
                    if (decoded.Item1 is not null) return decoded;
                }
                else input.SkipLastField();
            }
        }
        catch (InvalidProtocolBufferException) { }
        return (null, new Dictionary<string, string>());
    }

    private static (string?, IReadOnlyDictionary<string, string>) DecodeAny(byte[] bytes)
    {
        string type = ""; ByteString? payload = null;
        using (var input = new CodedInputStream(bytes)) while (!input.IsAtEnd)
        { uint tag = input.ReadTag(); if (tag == 0) break; if ((tag >> 3) == 1) type = input.ReadString(); else if ((tag >> 3) == 2) payload = input.ReadBytes(); else input.SkipLastField(); }
        if (!type.EndsWith("google.rpc.ErrorInfo", StringComparison.Ordinal) || payload is null) return (null, new Dictionary<string, string>());
        string reason = "", domain = ""; var metadata = new Dictionary<string, string>();
        using (var input = new CodedInputStream(payload.ToByteArray())) while (!input.IsAtEnd)
        {
            uint tag = input.ReadTag(); if (tag == 0) break;
            if ((tag >> 3) == 1) reason = input.ReadString();
            else if ((tag >> 3) == 2) domain = input.ReadString();
            else if ((tag >> 3) == 3)
            {
                var pair = input.ReadBytes(); string key = "", value = "";
                using var map = new CodedInputStream(pair.ToByteArray());
                while (!map.IsAtEnd) { uint field = map.ReadTag(); if (field == 0) break; if ((field >> 3) == 1) key = map.ReadString(); else if ((field >> 3) == 2) value = map.ReadString(); else map.SkipLastField(); }
                metadata[key] = value;
            }
            else input.SkipLastField();
        }
        return domain == "scriva.xtx" ? (reason, metadata) : (null, new Dictionary<string, string>());
    }
}
