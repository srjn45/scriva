// ScrivaDB — C# SDK end-to-end example
//
// Run against a live server:
//   make run           (from repo root; starts server on :5433 with api-key dev-key)
//   dotnet run         (from this directory)
//
// Override with environment variables:
//   SCRIVA_HOST, SCRIVA_PORT, SCRIVA_API_KEY

using Scriva.Client;

string host   = Environment.GetEnvironmentVariable("SCRIVA_HOST")    ?? "localhost";
int    port   = int.Parse(Environment.GetEnvironmentVariable("SCRIVA_PORT")    ?? "5433");
string apiKey = Environment.GetEnvironmentVariable("SCRIVA_API_KEY") ?? "dev-key";

await using var db = new ScrivaDB(host, port, apiKey);

await RunBasicExampleAsync(db);
await RunKeyedExampleAsync(db);
await RunAggregateExampleAsync(db);
await RunWatchExampleAsync(db);

// ---------------------------------------------------------------------------

static async Task RunBasicExampleAsync(ScrivaDB db)
{
    const string col = "test_csharp";

    Console.WriteLine("=== Collection management ===");
    string created = await db.CreateCollectionAsync(col);
    Console.WriteLine($"Created: {created}");
    var collections = await db.ListCollectionsAsync();
    Console.WriteLine($"Collections: [{string.Join(", ", collections)}]");

    // Secondary index
    await db.EnsureIndexAsync(col, "name");
    var indexes = await db.ListIndexesAsync(col);
    Console.WriteLine($"Indexes on '{col}': [{string.Join(", ", indexes)}]");

    // ---- Insert ----
    Console.WriteLine("\n=== Insert ===");
    ulong id1 = await db.InsertAsync(col, new() { ["name"] = "Alice", ["age"] = 30, ["role"] = "admin" });
    ulong id2 = await db.InsertAsync(col, new() { ["name"] = "Bob",   ["age"] = 25, ["role"] = "user" });
    ulong id3 = await db.InsertAsync(col, new() { ["name"] = "Carol", ["age"] = 35, ["role"] = "user" });
    Console.WriteLine($"Inserted ids: {id1}, {id2}, {id3}");

    // ---- InsertMany ----
    var manyIds = await db.InsertManyAsync(col, new[]
    {
        new Dictionary<string, object?> { ["name"] = "Dave", ["age"] = 28, ["role"] = "user" },
        new Dictionary<string, object?> { ["name"] = "Eve",  ["age"] = 22, ["role"] = "admin" },
    });
    Console.WriteLine($"InsertMany ids: [{string.Join(", ", manyIds)}]");

    // ---- FindById ----
    Console.WriteLine("\n=== FindById ===");
    var record = await db.FindByIdAsync(col, id1);
    Console.WriteLine($"FindById({id1}): {FormatRecord(record)}");

    // ---- FindById with field projection (N2) ----
    var projected = await db.FindByIdAsync(col, id1, fields: new[] { "name" });
    Console.WriteLine($"FindById({id1}) fields=[name]: {FormatRecord(projected)}");

    // ---- Find (streaming) with field filter ----
    Console.WriteLine("\n=== Find (role = user) ===");
    await foreach (var r in db.FindAsync(col,
        filter:  new() { ["field"] = "role", ["op"] = "eq", ["value"] = "user" },
        orderBy: new[] { Order.Ascending("name") }))
    {
        Console.WriteLine($"  {FormatRecord(r)}");
    }

    // ---- Find with AND filter ----
    Console.WriteLine("\n=== Find (age > 25 AND role = user) ===");
    var filtered = await db.FindAllAsync(col,
        filter: new()
        {
            ["and"] = new List<Dictionary<string, object?>>
            {
                new() { ["field"] = "age",  ["op"] = "gt", ["value"] = "25" },
                new() { ["field"] = "role", ["op"] = "eq", ["value"] = "user" },
            },
        });
    foreach (var r in filtered) Console.WriteLine($"  {FormatRecord(r)}");

    // ---- Multi-field ordering + keyset pagination (N3) ----
    Console.WriteLine("\n=== Keyset pagination (order by role, age; 2 per page) ===");
    var order = new[] { Order.Ascending("role"), Order.Descending("age") };
    string token = "";
    int pageNo = 1;
    do
    {
        Page page = await db.FindPageAsync(col, limit: 2, orderBy: order, pageToken: token);
        Console.WriteLine($"  page {pageNo++}:");
        foreach (var r in page.Records) Console.WriteLine($"    {FormatRecord(r)}");
        token = page.NextPageToken;
    } while (!string.IsNullOrEmpty(token));

    // ---- Update ----
    Console.WriteLine("\n=== Update ===");
    ulong updatedId = await db.UpdateAsync(col, id2,
        new() { ["name"] = "Bob", ["age"] = 26, ["role"] = "moderator" });
    Console.WriteLine($"Updated id: {updatedId}");
    var after = await db.FindByIdAsync(col, id2);
    Console.WriteLine($"After update: {FormatRecord(after)}");

    // ---- Delete ----
    Console.WriteLine("\n=== Delete ===");
    bool deleted = await db.DeleteAsync(col, id3);
    Console.WriteLine($"Deleted id {id3}: {deleted}");

    // ---- Transactions ----
    Console.WriteLine("\n=== Transactions ===");
    string txId = await db.BeginTxAsync(col);
    Console.WriteLine($"BeginTx: {txId}");
    bool committed = await db.CommitTxAsync(txId);
    Console.WriteLine($"CommitTx: {committed}");

    // ---- Stats ----
    Console.WriteLine("\n=== Stats ===");
    var stats = await db.StatsAsync(col);
    Console.WriteLine(stats);

    // ---- Compaction ----
    Console.WriteLine("\n=== Compact ===");
    bool compacted = await db.CompactAsync(col);
    Console.WriteLine($"Compacted: {compacted}");

    // ---- Per-record TTL ----
    Console.WriteLine("\n=== Per-record TTL ===");
    ulong ttlId = await db.InsertAsync(col,
        new() { ["name"] = "Ephemeral", ["role"] = "temp" }, ttlSeconds: 3600);
    Console.WriteLine($"Inserted {ttlId} with a 3600s TTL");
    // ttlSeconds 0 (the default) is sticky — it keeps the existing deadline.
    await db.UpdateAsync(col, ttlId, new() { ["name"] = "Ephemeral", ["role"] = "temp", ["touched"] = true });
    Console.WriteLine("Updated the TTL record (deadline preserved)");

    // ---- Snapshot (whole-database backup) ----
    Console.WriteLine("\n=== Snapshot ===");
    string backup = Path.Combine(Path.GetTempPath(), "scriva_csharp_snapshot.tar.gz");
    long bytes = await db.SnapshotToFileAsync(backup);
    Console.WriteLine($"Snapshot: wrote {bytes} bytes to {backup}");
    File.Delete(backup);

    // ---- Drop indexes + collection ----
    Console.WriteLine("\n=== Cleanup ===");
    bool indexDropped = await db.DropIndexAsync(col, "name");
    Console.WriteLine($"DropIndex(name): {indexDropped}");
    bool dropped = await db.DropCollectionAsync(col);
    Console.WriteLine($"DropCollection: {dropped}");
    var remaining = await db.ListCollectionsAsync();
    Console.WriteLine($"Collections after drop: [{string.Join(", ", remaining)}]");
}

// Keyed CRUD, upsert & compare-and-swap (N1).
static async Task RunKeyedExampleAsync(ScrivaDB db)
{
    const string col = "keyed_csharp";
    await db.CreateCollectionAsync(col);

    Console.WriteLine("\n=== Keyed CRUD / Upsert / CAS (N1) ===");

    // Keyed insert — the caller supplies the primary key.
    ulong id = await db.InsertKeyedAsync(col, "user:alice",
        new() { ["name"] = "Alice", ["age"] = 30 });
    Console.WriteLine($"InsertKeyed(user:alice) -> id {id}");

    // A duplicate keyed insert raises AlreadyExistsException.
    try
    {
        await db.InsertKeyedAsync(col, "user:alice", new() { ["name"] = "Impostor" });
    }
    catch (AlreadyExistsException)
    {
        Console.WriteLine("Duplicate keyed insert correctly rejected (AlreadyExists)");
    }

    // FindByKey — with optional projection (N2).
    var byKey = await db.FindByKeyAsync(col, "user:alice");
    Console.WriteLine($"FindByKey(user:alice): {FormatRecord(byKey)} rev={byKey.Rev}");

    // A missing key raises NotFoundException.
    try
    {
        await db.FindByKeyAsync(col, "user:ghost");
    }
    catch (NotFoundException)
    {
        Console.WriteLine("FindByKey(user:ghost) correctly raised NotFound");
    }

    // Upsert — replaces the existing record's data, bumping rev.
    var upserted = await db.UpsertAsync(col, "user:alice",
        new() { ["name"] = "Alice", ["age"] = 31 });
    Console.WriteLine($"Upsert(user:alice) -> rev {upserted.Rev}");

    // Compare-and-swap: succeeds only when the expected rev matches.
    var stale = await db.UpdateIfRevAsync(col, "user:alice", expectedRev: 1,
        new() { ["name"] = "Alice", ["age"] = 99 });
    Console.WriteLine($"UpdateIfRev(expected=1, stale): swapped={stale.Swapped}");

    var fresh = await db.UpdateIfRevAsync(col, "user:alice", expectedRev: upserted.Rev,
        new() { ["name"] = "Alice", ["age"] = 32 });
    Console.WriteLine($"UpdateIfRev(expected={upserted.Rev}): swapped={fresh.Swapped}, rev={fresh.Record?.Rev}");

    // UpdateByKey overwrites the keyed record, preserving the key.
    var updated = await db.UpdateByKeyAsync(col, "user:alice",
        new() { ["name"] = "Alice A.", ["age"] = 33 });
    Console.WriteLine($"UpdateByKey(user:alice) -> id={updated.Id}, rev={updated.Rev}");

    // DeleteByKey removes it.
    bool del = await db.DeleteByKeyAsync(col, "user:alice");
    Console.WriteLine($"DeleteByKey(user:alice): {del}");

    await db.DropCollectionAsync(col);
}

// Aggregations: Count, Aggregate, GroupBy (N4).
static async Task RunAggregateExampleAsync(ScrivaDB db)
{
    const string col = "agg_csharp";
    await db.CreateCollectionAsync(col);

    Console.WriteLine("\n=== Aggregations (N4) ===");

    await db.InsertManyAsync(col, new[]
    {
        new Dictionary<string, object?> { ["dept"] = "eng",   ["salary"] = 100 },
        new Dictionary<string, object?> { ["dept"] = "eng",   ["salary"] = 120 },
        new Dictionary<string, object?> { ["dept"] = "sales", ["salary"] = 80  },
        new Dictionary<string, object?> { ["dept"] = "sales", ["salary"] = 90  },
        new Dictionary<string, object?> { ["dept"] = "sales", ["salary"] = 70  },
    });

    // Count — whole collection and filtered.
    ulong total = await db.CountAsync(col);
    Console.WriteLine($"Count(all): {total}");
    ulong sales = await db.CountAsync(col,
        new() { ["field"] = "dept", ["op"] = "eq", ["value"] = "sales" });
    Console.WriteLine($"Count(dept=sales): {sales}");

    // Aggregate the whole set (single group).
    var overall = await db.AggregateAsync(col,
        aggregations: new[] { "sum", "avg", "min", "max" }, field: "salary");
    Console.WriteLine($"Aggregate(salary): {overall[0]}");

    // GroupBy dept with per-group numeric aggregates.
    Console.WriteLine("GroupBy(dept):");
    var groups = await db.GroupByAsync(col, field: "dept",
        aggregations: new[] { "count", "sum", "avg" }, metric: "salary");
    foreach (var g in groups) Console.WriteLine($"  {g}");

    await db.DropCollectionAsync(col);
}

static async Task RunWatchExampleAsync(ScrivaDB db)
{
    const string col = "watch_csharp";
    await db.CreateCollectionAsync(col);

    Console.WriteLine("\n=== Watch ===");

    // Use a CancellationTokenSource to stop the watch after receiving events.
    using var cts = new CancellationTokenSource();

    // Start watching in the background.
    var watchTask = Task.Run(async () =>
    {
        try
        {
            await foreach (var evt in db.WatchAsync(col, ct: cts.Token))
            {
                Console.WriteLine($"  {evt}");
            }
        }
        // Cancelling the token ends the stream; gRPC surfaces that either as an
        // OperationCanceledException or as an RpcException with StatusCode.Cancelled.
        catch (OperationCanceledException) { /* normal exit */ }
        catch (Grpc.Core.RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Cancelled) { /* normal exit */ }
    });

    // Give the stream time to connect, then insert a few records.
    await Task.Delay(200);
    await db.InsertAsync(col, new() { ["event"] = "first" });
    await db.InsertAsync(col, new() { ["event"] = "second" });
    await db.InsertAsync(col, new() { ["event"] = "third" });

    // Wait briefly for events to arrive, then cancel the stream.
    await Task.Delay(500);
    await cts.CancelAsync();
    await watchTask;

    await db.DropCollectionAsync(col);
}

static string FormatRecord(Record r) =>
    "{" + string.Join(", ", r.Data.Select(kvp => $"{kvp.Key}: {kvp.Value}")) + "}";
