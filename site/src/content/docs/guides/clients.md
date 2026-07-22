---
title: Client SDKs
description: Idiomatic, hand-written client libraries for seven languages — plus a generated OpenAPI spec for everything else.
---

Idiomatic, **hand-written** client libraries are available for seven languages.
Each wraps the same gRPC API, takes the same connection config (`host`, `port`,
`api_key`, optional TLS CA cert), and exposes every RPC — including the streaming
`Find` and `Watch` calls.

| Language | Install | Reference |
|---|---|---|
| Python | `pip install scriva` | [clients/python](https://github.com/srjn45/scriva/tree/main/clients/python) |
| JavaScript / TypeScript | `npm install scriva` | [clients/js](https://github.com/srjn45/scriva/tree/main/clients/js) |
| PHP | `composer require srjn45/scriva` | [clients/php](https://github.com/srjn45/scriva/tree/main/clients/php) |
| Java | `com.srjn45:scriva-client` (Maven Central) | [clients/java](https://github.com/srjn45/scriva/tree/main/clients/java) |
| Ruby | `gem install scriva` | [clients/ruby](https://github.com/srjn45/scriva/tree/main/clients/ruby) |
| Rust | `cargo add scriva` | [clients/rust](https://github.com/srjn45/scriva/tree/main/clients/rust) |
| C# / .NET | `dotnet add package ScrivaDB.Client` | [clients/csharp](https://github.com/srjn45/scriva/tree/main/clients/csharp) |

## Example (Python)

```python
from scriva import Client

db = Client(host="localhost", port=5433, api_key="dev-key")
db.insert("users", {"name": "alice", "age": 30})

for rec in db.find("users", {"field": "age", "op": "gt", "value": 18}):
    print(rec["data"])
```

## Generate your own

Prefer to generate a client, or working in a language without a hand-written
SDK? The checked-in **OpenAPI spec** (`docs/openapi/filedb.swagger.json`) is
generated from the proto and covers every RPC. Feed it to
[openapi-generator](https://openapi-generator.tech/) for any language.

See the [API reference](/scriva/reference/api/) for details on the gRPC and
REST surfaces.

## Web admin UI

There's also a browser-based collection and record manager under
[`clients/web/`](https://github.com/srjn45/scriva/tree/main/clients/web)
(React + Vite), which talks to the REST gateway.
