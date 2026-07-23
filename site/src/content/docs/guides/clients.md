---
title: Client SDKs
description: Idiomatic, hand-written client libraries for ten languages — plus a generated OpenAPI spec for everything else.
---

Idiomatic, **hand-written** client libraries are available for ten languages.
Each wraps the same gRPC API, takes the same connection config (`host`, `port`,
`api_key`, optional TLS CA cert), and exposes every RPC — including the streaming
`Find` and `Watch` calls.

| Language | Install | Reference |
|---|---|---|
| Python | `pip install scriva` | [clients/python](https://github.com/srjn45/scriva/tree/main/clients/python) |
| JavaScript / TypeScript | `npm install scriva` | [clients/js](https://github.com/srjn45/scriva/tree/main/clients/js) |
| PHP | `composer require srjn45/scriva` | [clients/php](https://github.com/srjn45/scriva/tree/main/clients/php) |
| Java | `io.github.srjn45:scriva-client` (Maven Central) | [clients/java](https://github.com/srjn45/scriva/tree/main/clients/java) |
| Kotlin | `io.github.srjn45:scriva-client-kotlin` (Gradle / Maven Central) | [clients/kotlin](https://github.com/srjn45/scriva/tree/main/clients/kotlin) |
| Scala | `"io.github.srjn45" %% "scriva-client-scala" % "1.2.1"` (sbt) | [clients/scala](https://github.com/srjn45/scriva/tree/main/clients/scala) |
| Clojure | `io.github.srjn45/scriva-client-clojure {:mvn/version "1.2.1"}` (deps.edn / Clojars) | [clients/clojure](https://github.com/srjn45/scriva/tree/main/clients/clojure) |
| Ruby | `gem install scriva` | [clients/ruby](https://github.com/srjn45/scriva/tree/main/clients/ruby) |
| Rust | `cargo add scriva` | [clients/rust](https://github.com/srjn45/scriva/tree/main/clients/rust) |
| C# / .NET | `dotnet add package Scriva.Client` | [clients/csharp](https://github.com/srjn45/scriva/tree/main/clients/csharp) |

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
generated from the proto and covers every RPC. Feed it to
[openapi-generator](https://openapi-generator.tech/) for any language.

See the [API reference](/scriva/reference/api/) for details on the gRPC and
REST surfaces.

## Web admin UI

There's also a browser-based collection and record manager under
[`clients/web/`](https://github.com/srjn45/scriva/tree/main/clients/web)
(React + Vite), which talks to the REST gateway.
