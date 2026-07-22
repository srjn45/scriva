using Grpc.Core;

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
