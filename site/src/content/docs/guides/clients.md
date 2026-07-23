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

## Example (Kotlin)

Kotlin calls are `suspend` functions; streaming RPCs return a `Flow`.

```kotlin
import io.github.srjn45.scriva.*
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

runBlocking {
    ScrivaClient.connect("localhost", 5433, "dev-key").use { db ->
        val id = db.insert("users", mapOf("name" to "Alice", "age" to 30))
        val record = db.findById("users", id)
        println(record["name"])           // Alice

        // Streaming find returns a Flow<Record>
        val admins: List<Record> = db.find(
            "users",
            filter = field("role", FilterOp.EQ, "admin"),
        ).toList()
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
  admins <- db.find("users", filter = Some(Filter.field("role", FilterOp.Eq, "admin")))
} yield {
  println(r("name"))    // Alice
  println(admins)
}

db.close()
```

## Example (Clojure)

Functions take and return plain Clojure maps; `find-records` returns a lazy seq.

```clojure
(require '[scriva.client :as scriva])

(let [db (scriva/connect {:host "localhost" :port 5433 :api-key "dev-key"})]
  (try
    (scriva/create-collection db "users")
    (let [id (scriva/insert db "users" {"name" "Alice" "age" 30})]
      (let [record (scriva/find-by-id db "users" id)]
        (println (get-in record [:data "name"])))  ; Alice
      (doseq [r (scriva/find-records db "users"
                                     :filter {:field "role" :op :eq :value "admin"})]
        (println r)))
    (finally (scriva/close db))))
```

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
