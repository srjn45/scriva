# FileDB v2 — C# / .NET SDK

.NET 8 gRPC client for [FileDB v2](../../README.md).

**NuGet package:** `FileDBv2.Client` (version `0.1.0`)

---

## Requirements

- .NET 8 SDK or later
- A running FileDB v2 server (`make run` from the repo root, or Docker)

---

## Build

```bash
cd clients/csharp
dotnet build
```

Proto stubs are generated automatically by `Grpc.Tools` during `dotnet build`. No manual `protoc` invocation is needed.

---

## Install

> **Note:** Replace `0.1.0` with the published version once released to NuGet.

### Package reference (`.csproj`)

```xml
<PackageReference Include="FileDBv2.Client" Version="0.1.0" />
```

### .NET CLI

```bash
dotnet add package FileDBv2.Client --version 0.1.0
```

---

## Quick start

```csharp
using FileDBv2.Client;

await using var db = new FileDB("localhost", 5433, "dev-key");

// Collection management
await db.CreateCollectionAsync("users");

// Insert a record — returns the assigned ulong id
ulong id = await db.InsertAsync("users", new()
{
    ["name"] = "Alice",
    ["age"]  = 30,
    ["role"] = "admin",
});

// Fetch by id
var record = await db.FindByIdAsync("users", id);
Console.WriteLine(record["name"]); // Alice

// Streaming find — use `await foreach`
await foreach (var r in db.FindAsync("users",
    filter:  new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" },
    orderBy: "name"))
{
    Console.WriteLine(r["name"]);
}

// Or collect all results at once
var admins = await db.FindAllAsync("users",
    filter: new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" });

// Update
await db.UpdateAsync("users", id, new() { ["name"] = "Alice", ["age"] = 31 });

// Delete
await db.DeleteAsync("users", id);

// Stats
CollectionStats stats = await db.StatsAsync("users");
Console.WriteLine(stats); // users: records=0 segments=1 dirty=1 size=...B

// Drop
await db.DropCollectionAsync("users");
```

`FileDB` implements `IAsyncDisposable` (`await using`) and `IDisposable` (`using`).

---

## API reference

### Constructor

```csharp
// Plaintext (no TLS)
var db = new FileDB(string host, int port, string apiKey);

// TLS — verifies server against a PEM-encoded CA certificate
var db = new FileDB(string host, int port, string apiKey, string tlsCaCertPath);
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

### Collection management

```csharp
string             name  = await db.CreateCollectionAsync("col");
bool               ok    = await db.DropCollectionAsync("col");
IReadOnlyList<string> ns = await db.ListCollectionsAsync();

// Give the collection a default per-record TTL (seconds). Records inserted
// without an explicit TTL then expire this long after being written.
await db.CreateCollectionAsync("sessions", defaultTtlSeconds: 3600);
```

---

### CRUD

```csharp
// Insert one record — returns assigned id
ulong id = await db.InsertAsync("col", new() { ["field"] = "value" });

// Insert multiple records — returns ids in insertion order
IReadOnlyList<ulong> ids = await db.InsertManyAsync("col", new[]
{
    new Dictionary<string, object?> { ["name"] = "Alice" },
    new Dictionary<string, object?> { ["name"] = "Bob" },
});

// Find by id
Dictionary<string, object?> record = await db.FindByIdAsync("col", id);

// Streaming find (IAsyncEnumerable)
await foreach (var r in db.FindAsync("col",
    filter:     filter,     // Dictionary<string,object?>? — null = no filter
    limit:      0,          // uint — 0 = no limit
    offset:     0,          // uint
    orderBy:    "name",     // string — "" = no ordering
    descending: false))
{
    Console.WriteLine(r["name"]);
}

// Collect all results
List<Dictionary<string, object?>> all = await db.FindAllAsync("col", filter: filter);

// Convenience — all records, no filter
await foreach (var r in db.FindAsync("col")) { ... }

// Update — returns updated id
ulong updatedId = await db.UpdateAsync("col", id, new() { ["name"] = "new value" });

// Delete — returns true if record existed
bool deleted = await db.DeleteAsync("col", id);
```

#### Per-record TTL

`InsertAsync`, `InsertManyAsync`, and `UpdateAsync` each take an optional
`ttlSeconds` (seconds):

```csharp
// Expire this record 60 seconds from now, regardless of the collection default.
await db.InsertAsync("sessions", new() { ["token"] = "abc" }, ttlSeconds: 60);

// Same TTL applied to every record in the batch.
await db.InsertManyAsync("sessions", new[]
{
    new Dictionary<string, object?> { ["token"] = "a" },
    new Dictionary<string, object?> { ["token"] = "b" },
}, ttlSeconds: 60);

// On update, ttlSeconds > 0 resets the deadline; ttlSeconds 0 (the default) is
// sticky and leaves the existing deadline untouched.
await db.UpdateAsync("sessions", id, new() { ["token"] = "abc", ["seen"] = true }, ttlSeconds: 120);
```

`ttlSeconds` of `0` (the default) inherits the collection's default TTL on
insert; a value greater than 0 overrides it. Negative values are rejected by
the server.

---

### Secondary indexes

```csharp
await db.EnsureIndexAsync("col", "fieldName");
bool               ok     = await db.DropIndexAsync("col", "fieldName");
IReadOnlyList<string> flds = await db.ListIndexesAsync("col");
```

---

### Transactions

```csharp
string txId = await db.BeginTxAsync("col");
bool committed  = await db.CommitTxAsync(txId);
bool rolledBack = await db.RollbackTxAsync(txId);
```

---

### Watch (streaming change feed)

```csharp
using var cts = new CancellationTokenSource();

await foreach (var evt in db.WatchAsync("col", ct: cts.Token))
{
    Console.WriteLine($"{evt.Op} id={evt.RecordId} data={evt.Record["name"]}");
    // evt.Op          — "Inserted" | "Updated" | "Deleted" | "Overflow"
    //                   ("Overflow" = server dropped events; resync needed)
    // evt.Collection  — collection name
    // evt.RecordId    — ulong record id
    // evt.Record      — Dictionary<string, object?> record data
    // evt.Timestamp   — DateTimeOffset
}

// Cancel to stop the stream
cts.Cancel();
```

With an optional filter (only matching events are delivered):

```csharp
await foreach (var evt in db.WatchAsync("col",
    filter: new() { ["field"] = "role", ["op"] = "eq", ["value"] = "admin" },
    ct: cts.Token))
{ ... }
```

---

### Stats

```csharp
CollectionStats stats = await db.StatsAsync("col");
// stats.Collection    string
// stats.RecordCount   ulong
// stats.SegmentCount  ulong
// stats.DirtyEntries  ulong
// stats.SizeBytes     ulong
```

---

### Maintenance

```csharp
// Force a synchronous compaction of a collection — merges dirty segments and
// reclaims space from deleted/overwritten records. Returns true on success.
await db.CompactAsync("users");

// Stream a consistent gzip-compressed tar snapshot of the whole database
// straight to a file. Returns the number of bytes written; restore with
// `tar xzf backup.tar.gz`.
long bytes = await db.SnapshotToFileAsync("backup.tar.gz");

// Or consume the raw archive chunks yourself (Snapshot is server-streaming):
await foreach (ReadOnlyMemory<byte> chunk in db.SnapshotAsync())
{
    // await outStream.WriteAsync(chunk);
}
```

---

## Filter syntax

Filters are `Dictionary<string, object?>` values that mirror the proto `Filter` message.

### Field filter

```csharp
new() { ["field"] = "age",  ["op"] = "gt",       ["value"] = "30" }
new() { ["field"] = "name", ["op"] = "contains",  ["value"] = "alice" }
new() { ["field"] = "bio",  ["op"] = "regex",     ["value"] = "engineer|developer" }
```

### AND composite

```csharp
new()
{
    ["and"] = new List<Dictionary<string, object?>>
    {
        new() { ["field"] = "age",  ["op"] = "gte", ["value"] = "18" },
        new() { ["field"] = "role", ["op"] = "eq",  ["value"] = "admin" },
    },
}
```

### OR composite

```csharp
new()
{
    ["or"] = new List<Dictionary<string, object?>>
    {
        new() { ["field"] = "status", ["op"] = "eq", ["value"] = "active" },
        new() { ["field"] = "role",   ["op"] = "eq", ["value"] = "admin" },
    },
}
```

### Supported `op` values

| op | Meaning |
|---|---|
| `eq` | equal |
| `neq` | not equal |
| `gt` | greater than |
| `gte` | greater than or equal |
| `lt` | less than |
| `lte` | less than or equal |
| `contains` | string contains (substring) |
| `regex` | regular expression match |

---

## TLS

```csharp
var db = new FileDB("myserver.example.com", 5433, "my-api-key", "/path/to/ca.crt");
```

Pass the path to a PEM-encoded CA certificate. The client verifies the server certificate against this CA. Without a CA cert path the client connects over plaintext (no TLS).

---

## Running the example

```bash
# 1. Start the server (from repo root)
make run

# 2. In another terminal
cd clients/csharp
dotnet run --project FileDBv2.Example
```

Override connection settings with environment variables:

```bash
FILEDB_HOST=myserver.example.com FILEDB_PORT=5433 FILEDB_API_KEY=my-key \
  dotnet run --project FileDBv2.Example
```

---

## NuGet publish

```bash
cd clients/csharp
dotnet pack FileDBv2.Client/FileDBv2.Client.csproj -c Release -o dist/
dotnet nuget push dist/FileDBv2.Client.0.1.0.nupkg --api-key $NUGET_API_KEY \
  --source https://api.nuget.org/v3/index.json
```
