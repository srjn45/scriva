using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Filedb.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace FileDBv2.Client;

/// <summary>
/// Async C# / .NET 8 client for FileDB v2.
///
/// <para>Wraps every RPC in the <c>filedb.proto</c> service with idiomatic async methods.
/// Streaming RPCs (<c>Find</c>, <c>Watch</c>) are exposed as <see cref="IAsyncEnumerable{T}"/>.</para>
///
/// <example>
/// <code>
/// await using var db = new FileDB("localhost", 5433, "dev-key");
/// await db.CreateCollectionAsync("users");
/// ulong id = await db.InsertAsync("users", new() { ["name"] = "Alice", ["age"] = 30 });
/// var record = await db.FindByIdAsync("users", id);
/// await foreach (var r in db.FindAsync("users", filter: new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" }))
///     Console.WriteLine(r["name"]);
/// </code>
/// </example>
/// </summary>
public sealed class FileDB : IAsyncDisposable, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly Filedb.V1.FileDB.FileDBClient _stub;
    private readonly Metadata _headers;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// <summary>Connect to a FileDB server without TLS (plaintext HTTP/2).</summary>
    public FileDB(string host, int port, string apiKey)
        : this(BuildPlaintextChannel(host, port), apiKey) { }

    /// <summary>
    /// Connect to a FileDB server with TLS, verifying the server certificate against
    /// a PEM-encoded CA certificate file.
    /// </summary>
    public FileDB(string host, int port, string apiKey, string tlsCaCertPath)
        : this(BuildTlsChannel(host, port, tlsCaCertPath), apiKey) { }

    private FileDB(GrpcChannel channel, string apiKey)
    {
        _channel = channel;
        _stub    = new Filedb.V1.FileDB.FileDBClient(channel);
        _headers = new Metadata { { "x-api-key", apiKey } };
    }

    private static GrpcChannel BuildPlaintextChannel(string host, int port)
    {
        // .NET requires this switch for unencrypted HTTP/2 (gRPC over plaintext).
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        return GrpcChannel.ForAddress($"http://{host}:{port}");
    }

    private static GrpcChannel BuildTlsChannel(string host, int port, string caCertPath)
    {
        var caCert = X509Certificate2.CreateFromPemFile(caCertPath);
        var handler = new SocketsHttpHandler
        {
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = (_, cert, chain, _) =>
                {
                    if (cert is null || chain is null) return false;
                    chain.ChainPolicy.ExtraStore.Add(caCert);
                    chain.ChainPolicy.VerificationFlags =
                        X509VerificationFlags.AllowUnknownCertificateAuthority;
                    return chain.Build(new X509Certificate2(cert));
                },
            },
        };
        return GrpcChannel.ForAddress($"https://{host}:{port}",
            new GrpcChannelOptions { HttpHandler = handler });
    }

    // -----------------------------------------------------------------------
    // Collection management
    // -----------------------------------------------------------------------

    public async Task<string> CreateCollectionAsync(string name, CancellationToken ct = default)
    {
        var resp = await _stub.CreateCollectionAsync(
            new CreateCollectionRequest { Name = name }, _headers, cancellationToken: ct);
        return resp.Name;
    }

    public async Task<bool> DropCollectionAsync(string name, CancellationToken ct = default)
    {
        var resp = await _stub.DropCollectionAsync(
            new DropCollectionRequest { Name = name }, _headers, cancellationToken: ct);
        return resp.Ok;
    }

    public async Task<IReadOnlyList<string>> ListCollectionsAsync(CancellationToken ct = default)
    {
        var resp = await _stub.ListCollectionsAsync(
            new ListCollectionsRequest(), _headers, cancellationToken: ct);
        return resp.Names;
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// <summary>Insert one record and return its assigned id.</summary>
    public async Task<ulong> InsertAsync(
        string collection,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
    {
        var resp = await _stub.InsertAsync(new InsertRequest
        {
            Collection = collection,
            Data       = DictToStruct(data),
        }, _headers, cancellationToken: ct);
        return resp.Id;
    }

    /// <summary>Insert multiple records and return their assigned ids in insertion order.</summary>
    public async Task<IReadOnlyList<ulong>> InsertManyAsync(
        string collection,
        IEnumerable<Dictionary<string, object?>> records,
        CancellationToken ct = default)
    {
        var req = new InsertManyRequest { Collection = collection };
        foreach (var r in records) req.Records.Add(DictToStruct(r));
        var resp = await _stub.InsertManyAsync(req, _headers, cancellationToken: ct);
        return resp.Ids;
    }

    /// <summary>Fetch a single record by its id.</summary>
    public async Task<Dictionary<string, object?>> FindByIdAsync(
        string collection,
        ulong id,
        CancellationToken ct = default)
    {
        var resp = await _stub.FindByIdAsync(new FindByIdRequest
        {
            Collection = collection,
            Id         = id,
        }, _headers, cancellationToken: ct);
        return StructToDict(resp.Record.Data);
    }

    /// <summary>
    /// Stream records matching <paramref name="filter"/> from the server.
    /// Results are yielded one-by-one as they arrive (server-streaming RPC).
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="filter">
    /// Optional filter as a plain dictionary — see <see cref="FilterToProto"/> for the accepted shape.
    /// </param>
    /// <param name="limit">Maximum results to return (0 = no limit).</param>
    /// <param name="offset">Number of results to skip.</param>
    /// <param name="orderBy">Field name to sort by, or empty string for unordered.</param>
    /// <param name="descending">Sort descending when <c>true</c>.</param>
    public async IAsyncEnumerable<Dictionary<string, object?>> FindAsync(
        string collection,
        Dictionary<string, object?>? filter    = null,
        uint   limit                           = 0,
        uint   offset                          = 0,
        string orderBy                         = "",
        bool   descending                      = false,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = new FindRequest
        {
            Collection = collection,
            Limit      = limit,
            Offset     = offset,
            OrderBy    = orderBy,
            Descending = descending,
        };
        if (filter is not null) req.Filter = FilterToProto(filter);

        using var call = _stub.Find(req, _headers, cancellationToken: ct);
        await foreach (var resp in call.ResponseStream.ReadAllAsync(ct))
            yield return StructToDict(resp.Record.Data);
    }

    /// <summary>Convenience overload — returns all records with no filter or ordering.</summary>
    public IAsyncEnumerable<Dictionary<string, object?>> FindAsync(string collection, CancellationToken ct = default)
        => FindAsync(collection, filter: null, ct: ct);

    /// <summary>Collect <see cref="FindAsync"/> into a list.</summary>
    public async Task<List<Dictionary<string, object?>>> FindAllAsync(
        string collection,
        Dictionary<string, object?>? filter = null,
        uint   limit                        = 0,
        uint   offset                       = 0,
        string orderBy                      = "",
        bool   descending                   = false,
        CancellationToken ct                = default)
    {
        var results = new List<Dictionary<string, object?>>();
        await foreach (var r in FindAsync(collection, filter, limit, offset, orderBy, descending, ct))
            results.Add(r);
        return results;
    }

    /// <summary>Update an existing record and return its id.</summary>
    public async Task<ulong> UpdateAsync(
        string collection,
        ulong  id,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
    {
        var resp = await _stub.UpdateAsync(new UpdateRequest
        {
            Collection = collection,
            Id         = id,
            Data       = DictToStruct(data),
        }, _headers, cancellationToken: ct);
        return resp.Id;
    }

    /// <summary>Delete a record by id. Returns <c>true</c> if the record existed.</summary>
    public async Task<bool> DeleteAsync(
        string collection,
        ulong  id,
        CancellationToken ct = default)
    {
        var resp = await _stub.DeleteAsync(new DeleteRequest
        {
            Collection = collection,
            Id         = id,
        }, _headers, cancellationToken: ct);
        return resp.Ok;
    }

    // -----------------------------------------------------------------------
    // Secondary indexes
    // -----------------------------------------------------------------------

    public async Task EnsureIndexAsync(string collection, string field, CancellationToken ct = default)
        => await _stub.EnsureIndexAsync(
            new EnsureIndexRequest { Collection = collection, Field = field },
            _headers, cancellationToken: ct);

    public async Task<bool> DropIndexAsync(string collection, string field, CancellationToken ct = default)
    {
        var resp = await _stub.DropIndexAsync(
            new DropIndexRequest { Collection = collection, Field = field },
            _headers, cancellationToken: ct);
        return resp.Ok;
    }

    public async Task<IReadOnlyList<string>> ListIndexesAsync(string collection, CancellationToken ct = default)
    {
        var resp = await _stub.ListIndexesAsync(
            new ListIndexesRequest { Collection = collection },
            _headers, cancellationToken: ct);
        return resp.Fields;
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    /// <summary>Begin a transaction on <paramref name="collection"/> and return the transaction id.</summary>
    public async Task<string> BeginTxAsync(string collection, CancellationToken ct = default)
    {
        var resp = await _stub.BeginTxAsync(
            new BeginTxRequest { Collection = collection },
            _headers, cancellationToken: ct);
        return resp.TxId;
    }

    public async Task<bool> CommitTxAsync(string txId, CancellationToken ct = default)
    {
        var resp = await _stub.CommitTxAsync(
            new CommitTxRequest { TxId = txId },
            _headers, cancellationToken: ct);
        return resp.Ok;
    }

    public async Task<bool> RollbackTxAsync(string txId, CancellationToken ct = default)
    {
        var resp = await _stub.RollbackTxAsync(
            new RollbackTxRequest { TxId = txId },
            _headers, cancellationToken: ct);
        return resp.Ok;
    }

    // -----------------------------------------------------------------------
    // Watch (server-streaming change feed)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Subscribe to the change feed for <paramref name="collection"/>.
    /// Events are yielded as they arrive. Cancel <paramref name="ct"/> to stop the stream.
    /// </summary>
    public async IAsyncEnumerable<WatchEventResult> WatchAsync(
        string collection,
        Dictionary<string, object?>? filter = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = new WatchRequest { Collection = collection };
        if (filter is not null) req.Filter = FilterToProto(filter);

        using var call = _stub.Watch(req, _headers, cancellationToken: ct);
        await foreach (var evt in call.ResponseStream.ReadAllAsync(ct))
        {
            yield return new WatchEventResult
            {
                Op         = evt.Op.ToString(),
                Collection = evt.Collection,
                RecordId   = evt.Record.Id,
                Record     = StructToDict(evt.Record.Data),
                Timestamp  = evt.Ts?.ToDateTimeOffset() ?? DateTimeOffset.UtcNow,
            };
        }
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    public async Task<CollectionStats> StatsAsync(string collection, CancellationToken ct = default)
    {
        var resp = await _stub.CollectionStatsAsync(
            new CollectionStatsRequest { Collection = collection },
            _headers, cancellationToken: ct);
        return new CollectionStats
        {
            Collection   = resp.Collection,
            RecordCount  = resp.RecordCount,
            SegmentCount = resp.SegmentCount,
            DirtyEntries = resp.DirtyEntries,
            SizeBytes    = resp.SizeBytes,
        };
    }

    // -----------------------------------------------------------------------
    // Filter builder
    // -----------------------------------------------------------------------

    /// <summary>
    /// Convert a plain dictionary to a proto <c>Filter</c> message.
    ///
    /// <para>Field filter:</para>
    /// <code>
    /// new() { ["field"] = "age", ["op"] = "gt", ["value"] = "30" }
    /// </code>
    ///
    /// <para>AND composite:</para>
    /// <code>
    /// new() { ["and"] = new List&lt;Dictionary&lt;string, object?&gt;&gt; {
    ///     new() { ["field"] = "age",  ["op"] = "gte", ["value"] = "18" },
    ///     new() { ["field"] = "role", ["op"] = "eq",  ["value"] = "admin" },
    /// }}
    /// </code>
    ///
    /// <para>OR composite: same but key is <c>"or"</c>.</para>
    /// <para>Supported <c>op</c> values: <c>eq neq gt gte lt lte contains regex</c></para>
    /// </summary>
    public static Filter FilterToProto(Dictionary<string, object?> filter)
    {
        if (filter.TryGetValue("and", out var andVal))
        {
            var and = new AndFilter();
            foreach (var child in CastFilterList(andVal))
                and.Filters.Add(FilterToProto(child));
            return new Filter { And = and };
        }
        if (filter.TryGetValue("or", out var orVal))
        {
            var or = new OrFilter();
            foreach (var child in CastFilterList(orVal))
                or.Filters.Add(FilterToProto(child));
            return new Filter { Or = or };
        }
        return new Filter
        {
            Field = new FieldFilter
            {
                Field_ = filter["field"]?.ToString() ?? "",
                Op     = ParseOp(filter["op"]?.ToString() ?? ""),
                Value  = filter["value"]?.ToString() ?? "",
            },
        };
    }

    private static IEnumerable<Dictionary<string, object?>> CastFilterList(object? val)
    {
        if (val is IEnumerable<Dictionary<string, object?>> typed) return typed;
        if (val is IEnumerable<object?> untyped)
            return untyped.OfType<Dictionary<string, object?>>();
        throw new ArgumentException("Filter 'and'/'or' value must be a list of filter dictionaries.");
    }

    private static FilterOp ParseOp(string op) => op.ToLowerInvariant() switch
    {
        "eq"       => FilterOp.Eq,
        "neq"      => FilterOp.Neq,
        "gt"       => FilterOp.Gt,
        "gte"      => FilterOp.Gte,
        "lt"       => FilterOp.Lt,
        "lte"      => FilterOp.Lte,
        "contains" => FilterOp.Contains,
        "regex"    => FilterOp.Regex,
        _          => FilterOp.FilterOpUnspecified,
    };

    // -----------------------------------------------------------------------
    // Struct ↔ Dictionary helpers
    // -----------------------------------------------------------------------

    private static Struct DictToStruct(Dictionary<string, object?> dict)
    {
        var s = new Struct();
        foreach (var (k, v) in dict)
            s.Fields[k] = ObjectToValue(v);
        return s;
    }

    private static Value ObjectToValue(object? obj) => obj switch
    {
        null                           => Value.ForNull(),
        bool   b                       => Value.ForBool(b),
        sbyte  n                       => Value.ForNumber(n),
        byte   n                       => Value.ForNumber(n),
        short  n                       => Value.ForNumber(n),
        ushort n                       => Value.ForNumber(n),
        int    n                       => Value.ForNumber(n),
        uint   n                       => Value.ForNumber(n),
        long   n                       => Value.ForNumber(n),
        ulong  n                       => Value.ForNumber(n),
        float  n                       => Value.ForNumber(n),
        double n                       => Value.ForNumber(n),
        decimal n                      => Value.ForNumber((double)n),
        string s                       => Value.ForString(s),
        Dictionary<string, object?> d  => Value.ForStruct(DictToStruct(d)),
        IEnumerable<object?> list      => Value.ForList(list.Select(ObjectToValue).ToArray()),
        _                              => Value.ForString(obj.ToString() ?? string.Empty),
    };

    private static Dictionary<string, object?> StructToDict(Struct s) =>
        s.Fields.ToDictionary(kvp => kvp.Key, kvp => ValueToObject(kvp.Value));

    private static object? ValueToObject(Value v) => v.KindCase switch
    {
        Value.KindOneofCase.NullValue   => null,
        Value.KindOneofCase.BoolValue   => v.BoolValue,
        Value.KindOneofCase.NumberValue => v.NumberValue,
        Value.KindOneofCase.StringValue => v.StringValue,
        Value.KindOneofCase.StructValue => StructToDict(v.StructValue),
        Value.KindOneofCase.ListValue   => v.ListValue.Values.Select(ValueToObject).ToList<object?>(),
        _                               => null,
    };

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public void Dispose() => _channel.Dispose();

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }
}

// -----------------------------------------------------------------------
// Result types
// -----------------------------------------------------------------------

/// <summary>A change-feed event returned by <see cref="FileDB.WatchAsync"/>.</summary>
public sealed class WatchEventResult
{
    /// <summary>One of: <c>Inserted</c>, <c>Updated</c>, <c>Deleted</c>.</summary>
    public required string Op { get; init; }

    public required string Collection { get; init; }

    public required ulong RecordId { get; init; }

    /// <summary>Record data at the time of the event.</summary>
    public required Dictionary<string, object?> Record { get; init; }

    public required DateTimeOffset Timestamp { get; init; }

    public override string ToString() =>
        $"[{Timestamp:O}] {Op} {Collection} id={RecordId}";
}

/// <summary>Collection statistics returned by <see cref="FileDB.StatsAsync"/>.</summary>
public sealed class CollectionStats
{
    public required string Collection   { get; init; }
    public required ulong  RecordCount  { get; init; }
    public required ulong  SegmentCount { get; init; }
    public required ulong  DirtyEntries { get; init; }
    public required ulong  SizeBytes    { get; init; }

    public override string ToString() =>
        $"{Collection}: records={RecordCount} segments={SegmentCount} dirty={DirtyEntries} size={SizeBytes}B";
}
