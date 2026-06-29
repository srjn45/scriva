// FileDB v2 — C# SDK end-to-end example
//
// Run against a live server:
//   make run           (from repo root; starts server on :5433 with api-key dev-key)
//   dotnet run         (from this directory)
//
// Override with environment variables:
//   FILEDB_HOST, FILEDB_PORT, FILEDB_API_KEY

using FileDBv2.Client;

string host   = Environment.GetEnvironmentVariable("FILEDB_HOST")    ?? "localhost";
int    port   = int.Parse(Environment.GetEnvironmentVariable("FILEDB_PORT")    ?? "5433");
string apiKey = Environment.GetEnvironmentVariable("FILEDB_API_KEY") ?? "dev-key";

await using var db = new FileDB(host, port, apiKey);

await RunBasicExampleAsync(db);
await RunWatchExampleAsync(db);

// ---------------------------------------------------------------------------

static async Task RunBasicExampleAsync(FileDB db)
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

    // ---- Find (streaming) with field filter ----
    Console.WriteLine("\n=== Find (role = user) ===");
    await foreach (var r in db.FindAsync(col,
        filter:    new() { ["field"] = "role", ["op"] = "eq", ["value"] = "user" },
        orderBy:   "name"))
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

    // ---- Drop indexes + collection ----
    Console.WriteLine("\n=== Cleanup ===");
    bool indexDropped = await db.DropIndexAsync(col, "name");
    Console.WriteLine($"DropIndex(name): {indexDropped}");
    bool dropped = await db.DropCollectionAsync(col);
    Console.WriteLine($"DropCollection: {dropped}");
    var remaining = await db.ListCollectionsAsync();
    Console.WriteLine($"Collections after drop: [{string.Join(", ", remaining)}]");
}

static async Task RunWatchExampleAsync(FileDB db)
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
        catch (OperationCanceledException) { /* normal exit */ }
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

static string FormatRecord(Dictionary<string, object?> r) =>
    "{" + string.Join(", ", r.Select(kvp => $"{kvp.Key}: {kvp.Value}")) + "}";
