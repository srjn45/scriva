using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Scriva.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace Scriva.Client;

/// <summary>
/// Async C# / .NET 8 client for ScrivaDB.
///
/// <para>Wraps every RPC in the <c>scriva.proto</c> service with idiomatic async methods.
/// Streaming RPCs (<c>Find</c>, <c>Watch</c>, <c>Snapshot</c>) are exposed as
/// <see cref="IAsyncEnumerable{T}"/>. Records are returned as <see cref="Record"/> value
/// objects carrying the server id, the optional caller-supplied string key, and the
/// monotonic per-record revision.</para>
///
/// <example>
/// <code>
/// await using var db = new ScrivaDB("localhost", 5433, "dev-key");
/// await db.CreateCollectionAsync("users");
/// ulong id = await db.InsertAsync("users", new() { ["name"] = "Alice", ["age"] = 30 });
/// var record = await db.FindByIdAsync("users", id);
/// Console.WriteLine(record["name"]);
/// </code>
/// </example>
/// </summary>
public sealed class ScrivaDB : IAsyncDisposable, IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly Scriva.V1.Scriva.ScrivaClient _stub;
    private readonly Metadata _headers;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// <summary>Connect to a ScrivaDB server without TLS (plaintext HTTP/2).</summary>
    public ScrivaDB(string host, int port, string apiKey)
        : this(BuildPlaintextChannel(host, port), apiKey) { }

    /// <summary>
    /// Connect to a ScrivaDB server with TLS, verifying the server certificate against
    /// a PEM-encoded CA certificate file.
    /// </summary>
    public ScrivaDB(string host, int port, string apiKey, string tlsCaCertPath)
        : this(BuildTlsChannel(host, port, tlsCaCertPath), apiKey) { }

    private ScrivaDB(GrpcChannel channel, string apiKey)
    {
        _channel = channel;
        _stub    = new Scriva.V1.Scriva.ScrivaClient(channel);
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
    // Error translation — map engine gRPC status codes to typed exceptions
    // -----------------------------------------------------------------------

    /// <summary>
    /// Translate a gRPC <see cref="RpcException"/> into a typed ScrivaDB exception:
    /// <c>NOT_FOUND</c> becomes <see cref="NotFoundException"/> and <c>ALREADY_EXISTS</c>
    /// becomes <see cref="AlreadyExistsException"/>. Any other status code returns
    /// <c>null</c> so the original exception propagates unchanged.
    /// </summary>
    private static ScrivaDBException? Translate(RpcException e) => e.StatusCode switch
    {
        StatusCode.NotFound      => new NotFoundException(e.Status.Detail, e),
        StatusCode.AlreadyExists => new AlreadyExistsException(e.Status.Detail, e),
        _                        => null,
    };

    private static async Task<T> GuardAsync<T>(Func<Task<T>> op)
    {
        try
        {
            return await op().ConfigureAwait(false);
        }
        catch (RpcException e)
        {
            var typed = Translate(e);
            if (typed is not null) throw typed;
            throw;
        }
    }

    // -----------------------------------------------------------------------
    // Collection management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a collection. When <paramref name="defaultTtlSeconds"/> is greater than 0,
    /// records inserted without an explicit TTL expire that many seconds after being
    /// written; the value is persisted per-collection and overrides the server-wide
    /// default. 0 (the default) inherits the server-wide default.
    /// </summary>
    public async Task<string> CreateCollectionAsync(
        string name, long defaultTtlSeconds = 0, CancellationToken ct = default)
    {
        var resp = await _stub.CreateCollectionAsync(
            new CreateCollectionRequest { Name = name, DefaultTtlSeconds = defaultTtlSeconds },
            _headers, cancellationToken: ct);
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

    /// <summary>
    /// Insert one record and return its assigned id.
    ///
    /// <para>When <paramref name="key"/> is non-empty the record is inserted under that
    /// caller-supplied string primary key (keyed create); a key already held by a live
    /// record raises <see cref="AlreadyExistsException"/>. A keyed insert does not
    /// participate in transactions or per-record TTL.</para>
    ///
    /// <para>When <paramref name="ttlSeconds"/> is greater than 0 the record expires that
    /// many seconds after insertion, overriding any collection default (keyless inserts
    /// only); 0 (the default) applies the collection's default TTL, if any.</para>
    /// </summary>
    public Task<ulong> InsertAsync(
        string collection,
        Dictionary<string, object?> data,
        long ttlSeconds = 0,
        string key = "",
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var resp = await _stub.InsertAsync(new InsertRequest
            {
                Collection = collection,
                Data       = DictToStruct(data),
                TtlSeconds = ttlSeconds,
                Key        = key ?? "",
            }, _headers, cancellationToken: ct);
            return resp.Id;
        });

    /// <summary>
    /// Insert one record under a caller-supplied string primary key (keyed create).
    /// Raises <see cref="AlreadyExistsException"/> if <paramref name="key"/> is already
    /// held by a live record. Returns the assigned id.
    /// </summary>
    public Task<ulong> InsertKeyedAsync(
        string collection,
        string key,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
        => InsertAsync(collection, data, ttlSeconds: 0, key: key, ct: ct);

    /// <summary>
    /// Insert multiple records and return their assigned ids in insertion order.
    /// <paramref name="ttlSeconds"/> is applied to every record in the batch, with the
    /// same semantics as <see cref="InsertAsync"/>.
    /// </summary>
    public async Task<IReadOnlyList<ulong>> InsertManyAsync(
        string collection,
        IEnumerable<Dictionary<string, object?>> records,
        long ttlSeconds = 0,
        CancellationToken ct = default)
    {
        var req = new InsertManyRequest { Collection = collection, TtlSeconds = ttlSeconds };
        foreach (var r in records) req.Records.Add(DictToStruct(r));
        var resp = await _stub.InsertManyAsync(req, _headers, cancellationToken: ct);
        return resp.Ids;
    }

    /// <summary>
    /// Fetch a single record by its id, optionally projecting its data (N2).
    ///
    /// <para>When <paramref name="fields"/> is non-empty only those top-level fields are
    /// returned in the record's data (<c>id</c>, <c>key</c> and <c>rev</c> are always
    /// included). Pass <c>null</c>/empty for the full record.</para>
    /// </summary>
    public Task<Record> FindByIdAsync(
        string collection,
        ulong id,
        IEnumerable<string>? fields = null,
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var req = new FindByIdRequest { Collection = collection, Id = id };
            if (fields is not null) req.Fields.AddRange(fields);
            var resp = await _stub.FindByIdAsync(req, _headers, cancellationToken: ct);
            return RecordFromProto(resp.Record);
        });

    /// <summary>
    /// Stream records matching <paramref name="filter"/> from the server (N2/N3).
    /// Results are yielded one-by-one as they arrive (server-streaming RPC).
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="filter">Optional filter as a plain dictionary — see <see cref="FilterToProto"/>.</param>
    /// <param name="limit">Maximum results to return (0 = no limit).</param>
    /// <param name="offset">Number of results to skip (use 0 with <paramref name="pageToken"/>).</param>
    /// <param name="orderBy">Multi-field sort keys (N3), applied in order; null/empty for unordered.</param>
    /// <param name="fields">Top-level data fields to project (N2), or null/empty for the full record.</param>
    /// <param name="pageToken">Keyset cursor from a prior page (N3), or "" for the first page.</param>
    public async IAsyncEnumerable<Record> FindAsync(
        string collection,
        Dictionary<string, object?>? filter          = null,
        uint   limit                                 = 0,
        uint   offset                                = 0,
        IEnumerable<Order>? orderBy                  = null,
        IEnumerable<string>? fields                  = null,
        string pageToken                             = "",
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = BuildFindRequest(collection, filter, limit, offset,
            orderBy, legacyOrderBy: "", descending: false, fields, pageToken);

        using var call = _stub.Find(req, _headers, cancellationToken: ct);
        var stream = call.ResponseStream;
        while (true)
        {
            bool moved;
            try
            {
                moved = await stream.MoveNext(ct).ConfigureAwait(false);
            }
            catch (RpcException e)
            {
                var typed = Translate(e);
                if (typed is not null) throw typed;
                throw;
            }
            if (!moved) yield break;
            yield return RecordFromProto(stream.Current.Record);
        }
    }

    /// <summary>Convenience overload — returns all records with no filter or ordering.</summary>
    public IAsyncEnumerable<Record> FindAsync(string collection, CancellationToken ct = default)
        => FindAsync(collection, filter: null, ct: ct);

    /// <summary>
    /// Legacy single-field sort (deprecated). Superseded by the multi-field
    /// <see cref="FindAsync(string, Dictionary{string, object?}, uint, uint, IEnumerable{Order}, IEnumerable{string}, string, CancellationToken)"/>
    /// overload. Streams records sorted on one field.
    /// </summary>
    public async IAsyncEnumerable<Record> FindAsync(
        string collection,
        Dictionary<string, object?>? filter,
        uint   limit,
        uint   offset,
        string orderBy,
        bool   descending,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = BuildFindRequest(collection, filter, limit, offset,
            orderBy: null, legacyOrderBy: orderBy, descending: descending, fields: null, pageToken: "");

        using var call = _stub.Find(req, _headers, cancellationToken: ct);
        await foreach (var resp in call.ResponseStream.ReadAllAsync(ct))
            yield return RecordFromProto(resp.Record);
    }

    /// <summary>Collect <see cref="FindAsync"/> into a list (N2/N3 rich overload).</summary>
    public async Task<List<Record>> FindAllAsync(
        string collection,
        Dictionary<string, object?>? filter = null,
        uint   limit                        = 0,
        uint   offset                       = 0,
        IEnumerable<Order>? orderBy         = null,
        IEnumerable<string>? fields         = null,
        string pageToken                    = "",
        CancellationToken ct                = default)
    {
        var results = new List<Record>();
        await foreach (var r in FindAsync(collection, filter, limit, offset, orderBy, fields, pageToken, ct))
            results.Add(r);
        return results;
    }

    /// <summary>
    /// Fetch one keyset page, returning the records plus a next-page cursor (N3).
    ///
    /// <para>Pass an ordering and a <paramref name="limit"/>, then feed the returned
    /// <see cref="Page.NextPageToken"/> back as <paramref name="pageToken"/> on the next
    /// call to walk the collection page by page in O(page) time. An empty next-page token
    /// means the last page was reached. Keep the same filter, ordering and limit on every
    /// page.</para>
    /// </summary>
    public async Task<Page> FindPageAsync(
        string collection,
        Dictionary<string, object?>? filter = null,
        uint   limit                        = 0,
        uint   offset                       = 0,
        IEnumerable<Order>? orderBy         = null,
        IEnumerable<string>? fields         = null,
        string pageToken                    = "",
        CancellationToken ct                = default)
    {
        var req = BuildFindRequest(collection, filter, limit, offset,
            orderBy, legacyOrderBy: "", descending: false, fields, pageToken);

        var records   = new List<Record>();
        var nextToken = "";
        try
        {
            using var call = _stub.Find(req, _headers, cancellationToken: ct);
            await foreach (var resp in call.ResponseStream.ReadAllAsync(ct))
            {
                records.Add(RecordFromProto(resp.Record));
                if (!string.IsNullOrEmpty(resp.PageToken)) nextToken = resp.PageToken;
            }
        }
        catch (RpcException e)
        {
            var typed = Translate(e);
            if (typed is not null) throw typed;
            throw;
        }
        return new Page(records, nextToken);
    }

    private static FindRequest BuildFindRequest(
        string collection,
        Dictionary<string, object?>? filter,
        uint limit,
        uint offset,
        IEnumerable<Order>? orderBy,
        string legacyOrderBy,
        bool descending,
        IEnumerable<string>? fields,
        string pageToken)
    {
        var req = new FindRequest
        {
            Collection = collection,
            Limit      = limit,
            Offset     = offset,
            PageToken  = pageToken ?? "",
        };
        if (filter is not null) req.Filter = FilterToProto(filter);
        if (fields is not null) req.Fields.AddRange(fields);

        var orderList = orderBy?.ToList();
        if (orderList is { Count: > 0 })
        {
            foreach (var o in orderList)
                req.OrderByFields.Add(new Scriva.V1.OrderBy { Field = o.Field, Desc = o.Desc });
        }
        else if (!string.IsNullOrEmpty(legacyOrderBy))
        {
            // Deprecated single-field path — honoured only when order_by_fields is empty.
#pragma warning disable CS0612
            req.OrderBy    = legacyOrderBy;
            req.Descending = descending;
#pragma warning restore CS0612
        }
        return req;
    }

    /// <summary>
    /// Update an existing record and return its id. When <paramref name="ttlSeconds"/>
    /// is greater than 0, the record's deadline is reset to that many seconds from now,
    /// overriding the collection default; 0 (the default) is sticky — it re-applies the
    /// collection default TTL and leaves an existing deadline untouched.
    /// </summary>
    public async Task<ulong> UpdateAsync(
        string collection,
        ulong  id,
        Dictionary<string, object?> data,
        long ttlSeconds = 0,
        CancellationToken ct = default)
    {
        var resp = await _stub.UpdateAsync(new UpdateRequest
        {
            Collection = collection,
            Id         = id,
            Data       = DictToStruct(data),
            TtlSeconds = ttlSeconds,
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
    // Keyed CRUD, upsert & compare-and-swap (N1)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Insert <paramref name="data"/> under <paramref name="key"/>, or replace the
    /// existing keyed record — atomically. If no live record carries the key it is
    /// inserted; otherwise the existing record's data is replaced and its <c>rev</c>
    /// incremented. Returns the resulting record (including its key and rev).
    /// </summary>
    public Task<Record> UpsertAsync(
        string collection,
        string key,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var resp = await _stub.UpsertAsync(new UpsertRequest
            {
                Collection = collection,
                Key        = key,
                Data       = DictToStruct(data),
            }, _headers, cancellationToken: ct);
            return RecordFromProto(resp.Record);
        });

    /// <summary>
    /// Fetch the record carrying <paramref name="key"/>, optionally projecting its data (N2).
    /// Raises <see cref="NotFoundException"/> if no live record carries the key.
    /// </summary>
    public Task<Record> FindByKeyAsync(
        string collection,
        string key,
        IEnumerable<string>? fields = null,
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var req = new FindByKeyRequest { Collection = collection, Key = key };
            if (fields is not null) req.Fields.AddRange(fields);
            var resp = await _stub.FindByKeyAsync(req, _headers, cancellationToken: ct);
            return RecordFromProto(resp.Record);
        });

    /// <summary>
    /// Overwrite the record carrying <paramref name="key"/>, preserving the key itself.
    /// Raises <see cref="NotFoundException"/> if no live record carries the key. Returns
    /// the write's outcome — id, key, rev (after the write) and the modified timestamp.
    /// </summary>
    public Task<UpdateResult> UpdateByKeyAsync(
        string collection,
        string key,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var resp = await _stub.UpdateByKeyAsync(new UpdateByKeyRequest
            {
                Collection = collection,
                Key        = key,
                Data       = DictToStruct(data),
            }, _headers, cancellationToken: ct);
            return new UpdateResult
            {
                Id           = resp.Id,
                Key          = resp.Key,
                Rev          = resp.Rev,
                DateModified = resp.DateModified,
            };
        });

    /// <summary>
    /// Delete the record carrying <paramref name="key"/>. Returns <c>true</c> on success.
    /// Raises <see cref="NotFoundException"/> if no live record carries the key.
    /// </summary>
    public Task<bool> DeleteByKeyAsync(
        string collection,
        string key,
        CancellationToken ct = default)
        => GuardAsync(async () =>
        {
            var resp = await _stub.DeleteByKeyAsync(new DeleteByKeyRequest
            {
                Collection = collection,
                Key        = key,
            }, _headers, cancellationToken: ct);
            return resp.Ok;
        });

    /// <summary>
    /// Compare-and-swap update on <paramref name="key"/>, conditional on
    /// <paramref name="expectedRev"/>. The write is applied only if the record's current
    /// <c>rev</c> equals <paramref name="expectedRev"/>. A stale revision (or a missing
    /// key) is a clean no-op — never an error — reported as <see cref="CasResult.Swapped"/>
    /// <c>false</c>. The result's <see cref="CasResult.Record"/> is populated only when
    /// the swap applied.
    /// </summary>
    public async Task<CasResult> UpdateIfRevAsync(
        string collection,
        string key,
        ulong  expectedRev,
        Dictionary<string, object?> data,
        CancellationToken ct = default)
    {
        var resp = await _stub.UpdateIfRevAsync(new UpdateIfRevRequest
        {
            Collection  = collection,
            Key         = key,
            ExpectedRev = expectedRev,
            Data        = DictToStruct(data),
        }, _headers, cancellationToken: ct);
        var record = resp is { Swapped: true, Record: not null }
            ? RecordFromProto(resp.Record)
            : null;
        return new CasResult(resp.Swapped, record);
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
    // Aggregations (N4)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Compute count and numeric aggregations over the filtered live records (N4).
    ///
    /// <para>The <c>Aggregate</c> RPC is server-streaming — one message per group; this
    /// collects them into a list. Each <see cref="AggResult"/> carries <c>Group</c> (the
    /// group-by value, <c>null</c> for the whole-set group), <c>Count</c>, and — when the
    /// group held at least one numeric <paramref name="field"/> value — sum/avg/min/max
    /// with <see cref="AggResult.Numeric"/> <c>true</c>.</para>
    /// </summary>
    /// <param name="collection">Collection name.</param>
    /// <param name="aggregations">
    /// Which numeric aggregations to compute: any of <c>count sum avg min max</c>.
    /// <c>count</c> is always returned; the rest require <paramref name="field"/>.
    /// Null/empty = count only.
    /// </param>
    /// <param name="field">Numeric field for <c>sum/avg/min/max</c>, or "".</param>
    /// <param name="groupBy">Optional group-by field — one result per distinct value, or "".</param>
    /// <param name="filter">The same plain-map filter as <see cref="FindAsync"/>, or null.</param>
    public async Task<List<AggResult>> AggregateAsync(
        string collection,
        IEnumerable<string>? aggregations   = null,
        string field                        = "",
        string groupBy                      = "",
        Dictionary<string, object?>? filter = null,
        CancellationToken ct                = default)
    {
        var req = new AggregateRequest
        {
            Collection = collection,
            Field      = field ?? "",
            GroupBy    = groupBy ?? "",
        };
        if (filter is not null) req.Filter = FilterToProto(filter);
        if (aggregations is not null)
            foreach (var a in aggregations) req.Aggregations.Add(ParseAgg(a));

        var results = new List<AggResult>();
        using var call = _stub.Aggregate(req, _headers, cancellationToken: ct);
        await foreach (var r in call.ResponseStream.ReadAllAsync(ct))
        {
            results.Add(new AggResult
            {
                Group   = ValueToObject(r.GroupValue),
                Count   = r.Count,
                Numeric = r.Numeric,
                Sum     = r.Sum,
                Avg     = r.Avg,
                Min     = r.Min,
                Max     = r.Max,
            });
        }
        return results;
    }

    /// <summary>Count the live records matching <paramref name="filter"/> (or all records when null).</summary>
    public async Task<ulong> CountAsync(
        string collection,
        Dictionary<string, object?>? filter = null,
        CancellationToken ct = default)
    {
        var groups = await AggregateAsync(collection, aggregations: null, field: "", groupBy: "", filter, ct);
        return groups.Count == 0 ? 0UL : groups[0].Count;
    }

    /// <summary>
    /// Group live records by <paramref name="field"/> and aggregate each group (N4).
    ///
    /// <para>Convenience wrapper over <see cref="AggregateAsync"/>. <paramref name="field"/>
    /// is the group-by field; <paramref name="metric"/> is the numeric field for
    /// <c>sum/avg/min/max</c> (pass those names in <paramref name="aggregations"/>).
    /// Returns one <see cref="AggResult"/> per distinct group value.</para>
    /// </summary>
    public Task<List<AggResult>> GroupByAsync(
        string collection,
        string field,
        IEnumerable<string>? aggregations   = null,
        string metric                       = "",
        Dictionary<string, object?>? filter = null,
        CancellationToken ct                = default)
        => AggregateAsync(collection, aggregations, metric, field, filter, ct);

    private static AggregateOp ParseAgg(string op) => op.ToLowerInvariant() switch
    {
        "count" => AggregateOp.AggCount,
        "sum"   => AggregateOp.AggSum,
        "avg"   => AggregateOp.AggAvg,
        "min"   => AggregateOp.AggMin,
        "max"   => AggregateOp.AggMax,
        _       => throw new ArgumentException(
            $"unknown aggregation '{op}'; expected one of [avg, count, max, min, sum]"),
    };

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
    // Maintenance
    // -----------------------------------------------------------------------

    /// <summary>
    /// Run a forced, synchronous compaction pass on a collection — merges dirty
    /// segments and reclaims space from deleted or overwritten records. Returns only
    /// after the pass completes; <c>true</c> on success.
    /// </summary>
    public async Task<bool> CompactAsync(string collection, CancellationToken ct = default)
    {
        var resp = await _stub.CompactAsync(
            new CompactRequest { Collection = collection }, _headers, cancellationToken: ct);
        return resp.Ok;
    }

    /// <summary>
    /// Stream a consistent, gzip-compressed tar snapshot of the whole database.
    /// Chunks are yielded as they arrive (server-streaming RPC); concatenate them to
    /// reconstruct the <c>.tar.gz</c>. See <see cref="SnapshotToFileAsync"/> for a
    /// convenience wrapper that writes straight to disk.
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> SnapshotAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var call = _stub.Snapshot(new SnapshotRequest(), _headers, cancellationToken: ct);
        await foreach (var chunk in call.ResponseStream.ReadAllAsync(ct))
            yield return chunk.Data.Memory;
    }

    /// <summary>
    /// Stream a database snapshot straight to a file at <paramref name="path"/>,
    /// returning the total number of bytes written. Restore with <c>tar xzf</c>.
    /// </summary>
    public async Task<long> SnapshotToFileAsync(string path, CancellationToken ct = default)
    {
        await using var fs = File.Create(path);
        long total = 0;
        await foreach (var chunk in SnapshotAsync(ct))
        {
            await fs.WriteAsync(chunk, ct);
            total += chunk.Length;
        }
        return total;
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
                Field = filter["field"]?.ToString() ?? "",
                Op    = ParseOp(filter["op"]?.ToString() ?? ""),
                Value = filter["value"]?.ToString() ?? "",
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
        _          => FilterOp.Unspecified,
    };

    // -----------------------------------------------------------------------
    // Record / Struct ↔ Dictionary helpers
    // -----------------------------------------------------------------------

    private static Record RecordFromProto(Scriva.V1.Record r) => new()
    {
        Id           = r.Id,
        Key          = r.Key,
        Rev          = r.Rev,
        Data         = r.Data is not null ? StructToDict(r.Data) : new Dictionary<string, object?>(),
        DateAdded    = r.DateAdded?.ToDateTimeOffset(),
        DateModified = r.DateModified?.ToDateTimeOffset(),
    };

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
// Value types
// -----------------------------------------------------------------------

/// <summary>
/// A record returned from the engine.
///
/// <para><see cref="Id"/> is the server-assigned numeric id; <see cref="Key"/> is the
/// caller-supplied string key (<c>""</c> for keyless records); <see cref="Rev"/> is the
/// monotonic per-record revision (starts at 1, bumped on every write); <see cref="Data"/>
/// is the decoded document. The indexer is a shortcut to a top-level <see cref="Data"/>
/// field.</para>
/// </summary>
public sealed class Record
{
    public required ulong Id { get; init; }

    /// <summary>Caller-supplied string key, or <c>""</c> for a keyless record.</summary>
    public required string Key { get; init; }

    /// <summary>Monotonic per-record revision (starts at 1, bumped on every write).</summary>
    public required ulong Rev { get; init; }

    public required Dictionary<string, object?> Data { get; init; }

    /// <summary>Creation timestamp, or <c>null</c> when unset.</summary>
    public DateTimeOffset? DateAdded { get; init; }

    /// <summary>Last-modified timestamp, or <c>null</c> when unset.</summary>
    public DateTimeOffset? DateModified { get; init; }

    /// <summary>True when this record carries a caller-supplied key.</summary>
    public bool HasKey => !string.IsNullOrEmpty(Key);

    /// <summary>Shortcut for <c>Data[field]</c>; returns <c>null</c> if the field is absent.</summary>
    public object? this[string field] => Data.TryGetValue(field, out var v) ? v : null;

    public override string ToString() =>
        HasKey ? $"Record{{id={Id}, key={Key}, rev={Rev}, data={{{Data.Count} fields}}}}"
               : $"Record{{id={Id}, rev={Rev}, data={{{Data.Count} fields}}}}";
}

/// <summary>
/// A single sort key for a multi-field order-by (N3): a field name and a direction.
/// Use <see cref="Asc"/> / <see cref="Desc"/> to build one.
/// </summary>
public sealed record Order(string Field, bool Desc)
{
    public static Order Ascending(string field)  => new(field, false);
    public static Order Descending(string field) => new(field, true);
}

/// <summary>
/// One keyset page from <see cref="ScrivaDB.FindPageAsync"/>: the records plus the next-page
/// cursor. An empty <see cref="NextPageToken"/> means the last page was reached.
/// </summary>
public sealed class Page
{
    public Page(IReadOnlyList<Record> records, string nextPageToken)
    {
        Records = records;
        NextPageToken = nextPageToken;
    }

    public IReadOnlyList<Record> Records { get; }
    public string NextPageToken { get; }

    /// <summary>True when a further page remains under the requested ordering.</summary>
    public bool HasNextPage => !string.IsNullOrEmpty(NextPageToken);
}

/// <summary>
/// The outcome of an <see cref="ScrivaDB.UpdateByKeyAsync"/> write: the affected record's id,
/// key, revision (after the write) and last-modified timestamp.
/// </summary>
public sealed class UpdateResult
{
    public required ulong  Id           { get; init; }
    public required string Key          { get; init; }
    public required ulong  Rev          { get; init; }

    /// <summary>Last-modified timestamp string as returned by the engine.</summary>
    public required string DateModified { get; init; }

    public override string ToString() =>
        $"UpdateResult{{id={Id}, key={Key}, rev={Rev}, dateModified={DateModified}}}";
}

/// <summary>
/// The outcome of an <see cref="ScrivaDB.UpdateIfRevAsync"/> compare-and-swap.
/// <see cref="Record"/> is populated only when <see cref="Swapped"/> is <c>true</c>.
/// </summary>
public sealed class CasResult
{
    public CasResult(bool swapped, Record? record)
    {
        Swapped = swapped;
        Record = record;
    }

    public bool Swapped { get; }

    /// <summary>The resulting record when <see cref="Swapped"/> is true; <c>null</c> otherwise.</summary>
    public Record? Record { get; }

    public override string ToString() => $"CasResult{{swapped={Swapped}, record={Record}}}";
}

/// <summary>
/// One group's aggregation result (N4). <see cref="Group"/> is the group-by value
/// (<c>null</c> for the whole-set group); the numeric aggregates are meaningful only when
/// <see cref="Numeric"/> is <c>true</c>.
/// </summary>
public sealed class AggResult
{
    /// <summary>The group-by field's value (number/string/bool), or <c>null</c> for the whole set.</summary>
    public required object? Group { get; init; }

    public required ulong Count { get; init; }

    /// <summary>True when at least one record in the group carried a numeric aggregate field.</summary>
    public required bool Numeric { get; init; }

    public required double Sum { get; init; }
    public required double Avg { get; init; }
    public required double Min { get; init; }
    public required double Max { get; init; }

    public override string ToString() =>
        Numeric
            ? $"AggResult{{group={Group}, count={Count}, sum={Sum}, avg={Avg}, min={Min}, max={Max}}}"
            : $"AggResult{{group={Group}, count={Count}}}";
}

/// <summary>A change-feed event returned by <see cref="ScrivaDB.WatchAsync"/>.</summary>
public sealed class WatchEventResult
{
    /// <summary>
    /// One of: <c>Inserted</c>, <c>Updated</c>, <c>Deleted</c>, or <c>Overflow</c>.
    /// <c>Overflow</c> means the server dropped events because this subscriber fell
    /// behind — the client missed writes and should resync; no record accompanies it.
    /// </summary>
    public required string Op { get; init; }

    public required string Collection { get; init; }

    public required ulong RecordId { get; init; }

    /// <summary>Record data at the time of the event.</summary>
    public required Dictionary<string, object?> Record { get; init; }

    public required DateTimeOffset Timestamp { get; init; }

    public override string ToString() =>
        $"[{Timestamp:O}] {Op} {Collection} id={RecordId}";
}

/// <summary>Collection statistics returned by <see cref="ScrivaDB.StatsAsync"/>.</summary>
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
