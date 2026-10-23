---
title: Client SDKs
description: Idiomatic, hand-written client libraries for ten languages — plus a generated OpenAPI spec for everything else.
---

Idiomatic, **hand-written** client libraries are available for ten languages.
Each wraps the same gRPC API, takes the same connection config (`host`, `port`,
`api_key`, optional TLS CA cert), and wraps the core collection, record, index,
single-collection transaction (`BeginTx`/`CommitTx`/`RollbackTx`), `Aggregate`,
`Compact`, `Snapshot` and streaming `Find`/`Watch` calls, plus the cross-collection
transaction methods (`BeginXTx`, `StageXTx`, `GetXTx`, `CommitXTx`, `RollbackXTx`,
`XTxStatus`). They do **not** wrap `Replicate`, `ReplicationStatus` and `Promote`; for
those, use the generated gRPC stubs (bundled in each SDK, built from
`proto/scriva.proto`) or REST where a route exists. `Snapshot` and `Replicate` are
gRPC-only (no REST route).

**Unknown commit outcomes.** If `CommitXTx` fails with `XTX_OUTCOME_UNKNOWN` (gRPC `UNKNOWN`)
or `XTX_IN_PROGRESS` (`UNAVAILABLE`), never retry the commit blindly. Call `XTxStatus` with the
error's `status_ref` (or the idempotency key you chose at begin): `COMMITTED` means success,
`ABORTED`/`EXPIRED` permits a new transaction, and `PENDING` means poll with backoff. Choose and
keep an explicit idempotency key if a commit must be recoverable across a server restart. Inspect
the `scriva.xtx` `ErrorInfo` reason, never the message text. Contract:
[SDK transaction contract](https://github.com/srjn45/scriva/blob/main/docs/sdk-cross-collection-transaction-contract.md).

**Verification status.** The table below is what was actually run locally; "unverified" SDKs have the
code and (where noted) tests but could not be executed by the release verifier, so treat their
cross-collection transaction support as unverified until CI or a maintainer runs them.

| SDK | XTx methods | Local verification (2026-10-20) |
|---|---|---|
| Python | yes | **Unverified** — `pytest`/`grpcio` not installed on the verifier; `tests/test_xtx.py` not run |
| JavaScript / TypeScript | yes | Verified — `tests/xtx.test.ts` passes, `tsc --noEmit` clean |
| PHP | yes | **Unverified** — no PHP runtime available; `tests/test_xtx.php` not run |
| Ruby | yes | Verified — `test_xtx.rb` passes (3 runs, 16 assertions) |
| Rust | yes | **Unverified** — build needs `protoc` (not installed); no XTx tests exist yet |
| C# / .NET | yes | **Unverified** — no .NET SDK available; only `XTxErrorTests.cs` exists, not run |
| Java | yes | Verified — `XTxClientTest` passes (`ExampleTest` needs a live server and was not run) |
| Kotlin | yes | Verified — `./gradlew test` passes (8 tests) |
| Scala | yes | Verified — `sbt test` passes (7 tests) |
| Clojure | yes | Verified — `lein test` passes (7 tests, 30 assertions) |

**Versions.** The SDKs are versioned independently of the server (currently `1.3.0`, which adds the
additive XTx methods; existing `BeginTx`/`CommitTx`/`RollbackTx` are unchanged). The XTx methods
need a server that supports `BeginXTx` (v1.6.0+).

| Language | Install | Reference |
|---|---|---|
| Python | `pip install scriva` | [clients/python](https://github.com/srjn45/scriva/tree/main/clients/python) |
| JavaScript / TypeScript | `npm install scriva` | [clients/js](https://github.com/srjn45/scriva/tree/main/clients/js) |
| PHP | `composer require srjn45/scriva` | [clients/php](https://github.com/srjn45/scriva/tree/main/clients/php) |
| Java | `io.github.srjn45:scriva-client` (Maven Central) | [clients/java](https://github.com/srjn45/scriva/tree/main/clients/java) |
| Kotlin | `io.github.srjn45:scriva-client-kotlin` (Gradle / Maven Central) | [clients/kotlin](https://github.com/srjn45/scriva/tree/main/clients/kotlin) |
| Scala | `"io.github.srjn45" %% "scriva-client-scala" % "1.3.0"` (sbt) | [clients/scala](https://github.com/srjn45/scriva/tree/main/clients/scala) |
| Clojure | `io.github.srjn45/scriva-client-clojure {:mvn/version "1.3.0"}` (deps.edn / Clojars) | [clients/clojure](https://github.com/srjn45/scriva/tree/main/clients/clojure) |
| Ruby | `gem install scriva` | [clients/ruby](https://github.com/srjn45/scriva/tree/main/clients/ruby) |
| Rust | `cargo add scriva` | [clients/rust](https://github.com/srjn45/scriva/tree/main/clients/rust) |
| C# / .NET | `dotnet add package Scriva.Client` | [clients/csharp](https://github.com/srjn45/scriva/tree/main/clients/csharp/Scriva.Client) |

> **PHP — not on Packagist.** The `srjn45/scriva` package has not been submitted to
> Packagist. To install it, add a VCS repository entry to your `composer.json` and
> then run `composer install`:
>
> ```json
> {
>     "repositories": [{"type": "vcs", "url": "https://github.com/srjn45/scriva"}],
>     "require": {"srjn45/scriva": "dev-main"}
> }
> ```

## Example (Python)

```python
from scriva import Client

db = Client(host="localhost", port=5433, api_key="dev-key")
db.insert("users", {"name": "alice", "age": 30})

for rec in db.find("users", {"field": "age", "op": "gt", "value": 18}):
    print(rec["data"])
```

## Example (JavaScript / TypeScript)

`find` is a server-streaming RPC — iterate it with `for await`.

```typescript
import { ScrivaDB } from 'scriva';

const db = new ScrivaDB('localhost', 5433, 'dev-key');

const id = await db.insert('users', { name: 'Alice', age: 30 });
console.log(await db.findById('users', id));

for await (const rec of db.find('users', {
  filter: { field: 'age', op: 'gt', value: 18 },
})) {
  console.log(rec.data);
}

db.close();
```

## Example (Java)

```java
import com.srjn45.scriva.ScrivaDBClient;
import java.util.List;
import java.util.Map;

try (ScrivaDBClient db = new ScrivaDBClient("localhost", 5433, "dev-key")) {
    long id = db.insert("users", Map.of("name", "Alice", "age", 30));
    System.out.println(db.findById("users", id).get("name")); // Alice

    List<ScrivaDBClient.Record> adults = db.find("users",
            Map.of("field", "age", "op", "gt", "value", "18"),
            0, 0, null, false);
    adults.forEach(r -> System.out.println(r.get("name")));
}
```

## Example (Kotlin)

Kotlin calls are `suspend` functions; streaming RPCs return a `Flow`.

```kotlin
import io.github.srjn45.scriva.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

runBlocking {
    ScrivaClient.connect("localhost", 5433, "dev-key").use { db ->
        val id = db.insert("users", mapOf("name" to "Alice", "age" to 30))
        println(db.findById("users", id)["name"])  // Alice

        // Streaming find returns a Flow<Record>
        val adults: List<Record> = db.find(
            "users",
            filter = field("age", FilterOp.GT, 18),
        ).toList()
        adults.forEach { println(it["name"]) }
    }
}
```

## Example (Scala)

Unary RPCs return `Future`; reads return an immutable `Record` case class.

```scala
import io.github.srjn45.scriva._
import scala.concurrent.ExecutionContext.Implicits.global

val db = ScrivaClient.connect("localhost", 5433, "dev-key")

for {
  id     <- db.insert("users", Map("name" -> "Alice", "age" -> 30))
  r      <- db.findById("users", id)
  adults <- db.find("users", filter = Some(Filter.field("age", FilterOp.Gt, 18)))
} yield {
  println(r("name"))    // Alice
  adults.foreach(r => println(r("name")))
}

db.close()
```

## Example (Clojure)

Functions take and return plain Clojure maps; `find-records` returns a lazy seq.

```clojure
(require '[scriva.client :as scriva])

(let [db (scriva/connect {:host "localhost" :port 5433 :api-key "dev-key"})]
  (try
    (let [id (scriva/insert db "users" {"name" "Alice" "age" 30})]
      (println (get-in (scriva/find-by-id db "users" id) [:data "name"])))  ; Alice
    (doseq [r (scriva/find-records db "users"
                                   :filter {:field "age" :op :gt :value 18})]
      (println r))
    (finally (scriva/close db))))
```

## Example (Ruby)

```ruby
require "scriva"

db = Scriva::Client.new(host: "localhost", port: 5433, api_key: "dev-key")

id = db.insert("users", { name: "Alice", age: 30 })
puts db.find_by_id("users", id).dig("data", "name")  # Alice

db.find("users", filter: { field: "age", op: "gt", value: 18 }).each do |rec|
  puts rec["data"]["name"]
end

db.close
```

## Example (Rust)

Async client built on Tokio; `find` collects results, `find_stream` streams them.

```rust
use scriva::{ScrivaDB, FilterInput, FilterOp, FindOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut db = ScrivaDB::connect("localhost", 5433, "dev-key").await?;

    let id = db.insert("users", json!({"name": "Alice", "age": 30})).await?;
    println!("{}", db.find_by_id("users", id).await?.data);

    let adults = db.find("users", FindOptions {
        filter: Some(FilterInput::field("age", FilterOp::Gt, "18")),
        ..Default::default()
    }).await?;
    for rec in &adults { println!("{}", rec.data); }

    Ok(())
}
```

## Example (C# / .NET)

`FindAsync` is server-streaming — iterate with `await foreach`.

```csharp
using Scriva.Client;

await using var db = new ScrivaDB("localhost", 5433, "dev-key");

ulong id = await db.InsertAsync("users", new() { ["name"] = "Alice", ["age"] = 30 });
var record = await db.FindByIdAsync("users", id);
Console.WriteLine(record["name"]);  // Alice

await foreach (var r in db.FindAsync("users",
    filter: new() { ["field"] = "age", ["op"] = "gt", ["value"] = "18" }))
{
    Console.WriteLine(r["name"]);
}
```

## Example (PHP)

```php
<?php
require 'vendor/autoload.php';

use ScrivaDB\ScrivaDB;

$db = new ScrivaDB('localhost', 5433, 'dev-key');

$id = $db->insert('users', ['name' => 'Alice', 'age' => 30]);
$record = $db->findById('users', $id);
echo $record['data']['name'] . "\n";  // Alice

$adults = $db->find('users', ['field' => 'age', 'op' => 'gt', 'value' => '18']);
foreach ($adults as $r) {
    echo $r['data']['name'] . "\n";
}
```

> See the [PHP install note above](#) — this package is not on Packagist and requires
> a Composer VCS entry pointing at the GitHub repository.

## Generate your own

Prefer to generate a client, or working in a language without a hand-written
SDK? The checked-in **OpenAPI spec** (`docs/openapi/scriva.swagger.json`) is
generated from the proto and describes the 29 REST-annotated operations. The five
gRPC-only RPCs (`BeginTx`, `CommitTx`, `RollbackTx`, `Snapshot`, `Replicate`) are not
in it; generate gRPC stubs from `proto/scriva.proto` to reach them. Feed the spec to
[openapi-generator](https://openapi-generator.tech/) for any language.

See the [API reference](/scriva/reference/api/) for details on the gRPC and
REST surfaces.

## Web admin UI

There's also a browser-based collection and record manager under
[`clients/web/`](https://github.com/srjn45/scriva/tree/main/clients/web)
(React + Vite), which talks to the REST gateway.
