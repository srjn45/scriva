# ScrivaDB — Language Clients Implementation Plan

This document is the single source of truth for tracking language client progress.
Each client is a thin wrapper over the gRPC API defined in `proto/scriva.proto`.

**How to use this file:**
- Work through one language at a time, top to bottom.
- Check off each box as it is done and commit after each logical step.
- You can stop at any checkbox and resume from that exact point later.

---

## Languages Covered

| # | Language | Package Registry | Directory | Status |
|---|---|---|---|---|
| 1 | Python | PyPI (`pip install scriva`) | `clients/python/` | ✅ Done |
| 2 | TypeScript / JavaScript | npm (`npm install scriva`) | `clients/js/` | ✅ Done |
| 3 | PHP | Packagist (`composer require srjn45/scriva`) | `clients/php/` | ✅ Done |
| 4 | Java | Maven Central (`com.srjn45:scriva-client`) | `clients/java/` | ✅ Done |
| 5 | Ruby | RubyGems (`gem install scriva`) | `clients/ruby/` | ✅ Done |
| 6 | Rust | crates.io (`scriva`) | `clients/rust/` | ✅ Done |
| 7 | C# / .NET | NuGet (`Scriva.Client`) | `clients/csharp/` | ✅ Done |

**Wire-API parity:** all seven clients are current with the **v0.7.0** server
surface. Beyond the base operations checklisted below, each client also
implements the N1–N4 additions: keyed CRUD / `upsert` / `find-by-key` /
`update-by-key` / `delete-by-key` / `update-if-rev` (CAS) with `key`/`rev` on
records and typed not-found / already-exists errors (N1); `fields` read
projection (N2); multi-field `order_by` + keyset `page_token` pagination (N3);
and the streaming `aggregate` / `count` / `group_by` RPC (N4). PRs #56 (python),
#57 (php), #58 (js), #59 (rust), #60 (java), #61 (csharp), #62 (ruby).

---

## Conventions (all languages)

- Every client exposes **exactly the same set of operations** as the proto service.
- Connection config always accepts: `host`, `port`, `api_key`, optional `tls_ca_cert`.
- Streaming RPCs (`Find`, `Watch`) return an iterable/async iterable/callback — idiomatic per language.
- Each client ships an `examples/` sub-directory with a runnable end-to-end test script.
- Each client has its own `README.md` explaining install + usage.
- Main `ROADMAP.md` is updated when a client is fully done.
- Main `docs/getting-started.md` gets a new "SDK" section when the first client lands.

---

## Commit strategy

One commit per logical unit:
```
feat(clients/python): scaffold package + proto stubs
feat(clients/python): implement ScrivaDB client class
feat(clients/python): add example test program
docs(clients/python): add README and getting-started entry
```

---

## 1 — Python

**Directory:** `clients/python/`
**Transport:** `grpcio` + `grpcio-tools` (or buf.build/grpc/python plugin)
**Wrapper:** pure Python class `ScrivaDB`
**Target Python:** 3.9+

### 1.1 Scaffold
- [x] Add `clients/python/` directory with `pyproject.toml` (flit or hatchling build backend)
  - package name: `scriva`
  - version: `0.1.0`
  - dependencies: `grpcio>=1.60`, `grpcio-tools>=1.60`, `protobuf>=4.25`
- [x] Add `clients/python/src/scriva/__init__.py` (re-exports `ScrivaDB`)
- [x] Add `clients/python/src/scriva/proto/` (generated stub destination)
- [x] Add `clients/python/generate.sh` — runs `python -m grpc_tools.protoc` to regenerate stubs from `../../proto/`
- [x] Commit: `feat(clients/python): scaffold package structure`

### 1.2 Proto stubs
- [x] Run `generate.sh` — produces `scriva_pb2.py` + `scriva_pb2_grpc.py` in `proto/`
- [x] Verify import works: `from scriva.proto import scriva_pb2`
- [x] Commit: `feat(clients/python): add generated proto stubs`

### 1.3 Client class (`client.py`)
- [x] `ScrivaDB.__init__(host, port, api_key, tls_ca_cert=None)` — builds channel + stub
- [x] Collection management
  - [x] `create_collection(name) -> str`
  - [x] `drop_collection(name) -> bool`
  - [x] `list_collections() -> list[str]`
- [x] CRUD
  - [x] `insert(collection, data: dict) -> int`
  - [x] `insert_many(collection, records: list[dict]) -> list[int]`
  - [x] `find_by_id(collection, id: int) -> dict`
  - [x] `find(collection, filter=None, limit=0, offset=0, order_by="", descending=False) -> list[dict]`
  - [x] `update(collection, id: int, data: dict) -> int`
  - [x] `delete(collection, id: int) -> bool`
- [x] Indexes
  - [x] `ensure_index(collection, field) -> None`
  - [x] `drop_index(collection, field) -> bool`
  - [x] `list_indexes(collection) -> list[str]`
- [x] Transactions
  - [x] `begin_tx(collection) -> str`
  - [x] `commit_tx(tx_id) -> bool`
  - [x] `rollback_tx(tx_id) -> bool`
- [x] Watch
  - [x] `watch(collection, filter=None) -> Iterator[WatchEvent]`
- [x] Stats
  - [x] `stats(collection) -> dict`
- [x] Helper: `_filter_to_proto(filter_dict)` — converts `{"field":"name","op":"eq","value":"alice"}` / `{"and":[...]}` to proto `Filter`
- [x] Commit: `feat(clients/python): implement ScrivaDB client class`

### 1.4 Example / test program
- [x] `clients/python/examples/test_basic.py`
  - Connect to `localhost:5433` with `dev-key`
  - Create collection `test_py`
  - Insert 3 records
  - `find_by_id`
  - `find` with filter
  - `update` a record
  - `delete` a record
  - `stats`
  - Drop collection
  - Print all results
- [x] `clients/python/examples/test_watch.py` — subscribes to `Watch`, inserts in background thread, prints events
- [x] Commit: `feat(clients/python): add example programs`

### 1.5 Documentation
- [x] `clients/python/README.md`
  - Install, connect, full usage snippet for every method
  - Filter syntax reference
  - TLS example
- [x] Add Python SDK section to `docs/getting-started.md`
- [x] Mark `clients/python/` row in this PLAN.md as ✅
- [x] Mark `ROADMAP.md` Python client as done
- [x] Commit: `docs(clients/python): README and getting-started entry`

---

## 2 — TypeScript / JavaScript

**Directory:** `clients/js/`
**Transport:** `@grpc/grpc-js` + `@grpc/proto-loader` (dynamic) or `grpc-tools` codegen
**Wrapper:** TypeScript class `ScrivaDB`, ships with `.d.ts` + CommonJS + ESM
**Target:** Node.js 18+, TypeScript 5+

### 2.1 Scaffold
- [x] `clients/js/package.json`
  - name: `scriva`, version `0.1.0`
  - deps: `@grpc/grpc-js`, `@grpc/proto-loader`
  - devDeps: `typescript`, `ts-node`, `@types/node`
  - scripts: `build`, `generate`, `clean`
- [x] `clients/js/tsconfig.json` — target ES2020, declaration: true, outDir: `dist/`
- [x] `clients/js/proto/` directory with `scriva.proto` copy + `google/api/` stubs for dynamic loading
- [x] `clients/js/generate.sh` — script to run `grpc_tools_node_protoc` for static codegen (optional)
- [x] Commit: `feat(clients/js): scaffold package structure`

### 2.2 Proto stubs
- [x] Proto loaded dynamically via `@grpc/proto-loader` at runtime — no pre-generated files needed
- [x] `clients/js/proto/scriva.proto` + `google/api/{annotations,http}.proto` stubs bundled in package
- [x] Commit: `feat(clients/js): add proto files for dynamic loading`

### 2.3 Client class (`src/client.ts`)
- [x] `new ScrivaDB(host, port, apiKey, tlsCaCert?)` — builds `grpc.Client` with `x-api-key` metadata on every call
- [x] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [x] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `AsyncGenerator<DBRecord>`), `findAll`, `update`, `delete`
- [x] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [x] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [x] Watch: `watch(collection, filter?) -> AsyncGenerator<WatchEvent>`
- [x] Stats: `stats(collection)`
- [x] Helper: `filterToProto(filter: FilterInput)` — converts plain JS object to proto Filter
- [x] TypeScript types: `DBRecord`, `WatchEvent`, `FilterInput`, `FindOptions`, `StatsResult` exported from `index.ts`
- [x] Commit: `feat(clients/js): implement ScrivaDB client class`

### 2.4 Build
- [x] `npm run build` produces `dist/` with `.js` + `.d.ts` files (CJS + declaration maps)
- [x] `require('scriva')` and `import { ScrivaDB } from 'scriva'` both work
- [x] Commit: `feat(clients/js): build pipeline (CJS + types)`

### 2.5 Example / test program
- [x] `clients/js/examples/test_basic.ts`
  - Same flow as Python example (create, insert, find, update, delete, stats, drop)
- [x] `clients/js/examples/test_watch.ts` — async watch with background inserts
- [x] Commit: `feat(clients/js): add example programs`

### 2.6 Documentation
- [x] `clients/js/README.md` — install, connect, usage, filter syntax, TLS
- [x] Add JS/TS SDK section to `docs/getting-started.md`
- [x] Mark row in PLAN.md + ROADMAP.md
- [x] Commit: `docs(clients/js): README and getting-started entry`

---

## 3 — PHP

**Directory:** `clients/php/`
**Transport:** `grpc/grpc` + `google/protobuf` Composer packages
**Wrapper:** PHP class `ScrivaDB` (PHP 8.1+)
**Code gen:** `protoc` + `grpc_php_plugin`

### 3.1 Scaffold
- [x] `clients/php/composer.json`
  - name: `srjn45/scriva`, version `0.1.0`
  - require: `grpc/grpc: ^1.56`, `google/protobuf: ^3.25`
  - autoload PSR-4: `Scriva\\` → `src/`
- [x] `clients/php/src/Proto/` — generated stub destination
- [x] `clients/php/generate.sh` — runs `protoc` with `--php_out` + `--grpc_out`
- [x] Commit: `feat(clients/php): scaffold package + proto stubs + ScrivaDB client + examples`

### 3.2 Proto stubs
- [x] Run `generate.sh` — produces `ScrivaDB/V1/` namespace PHP files
- [x] `composer install` + verify autoload resolves stubs
- [x] Commit: included in scaffold commit above

### 3.3 Client class (`src/ScrivaDB.php`)
- [x] `new ScrivaDB(string $host, int $port, string $apiKey, ?string $tlsCaCert = null)`
- [x] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [x] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `array`), `update`, `delete`
- [x] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [x] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [x] Watch: `watch(string $collection, ?array $filter = null): \Generator`
- [x] Stats: `stats(string $collection): array`
- [x] Helper: `filterToProto(array $filter): \Scriva\V1\Filter`
- [x] Commit: included in scaffold commit above

### 3.4 Example / test program
- [x] `clients/php/examples/test_basic.php` — same end-to-end flow
- [x] `clients/php/examples/test_watch.php`
- [x] Commit: included in scaffold commit above

### 3.5 Documentation
- [x] `clients/php/README.md`
- [x] Add PHP SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/php): README and getting-started entry`

---

## 4 — Java

**Directory:** `clients/java/`
**Transport:** `io.grpc:grpc-netty-shaded` + `com.google.protobuf:protobuf-java`
**Wrapper:** Java class `ScrivaDBClient` (Java 11+)
**Build:** Gradle 8 with `com.google.protobuf` plugin for auto codegen

### 4.1 Scaffold
- [x] `clients/java/build.gradle.kts`
  - Apply `java-library`, `com.google.protobuf` plugins
  - Deps: `grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`, `protobuf-java`, `javax.annotation-api`
  - `protobuf { generateProtoTasks { ... } }` — points to `../../proto/`
- [x] `clients/java/settings.gradle.kts` — rootProject name = `scriva-client`
- [x] `clients/java/src/main/proto/` — symlink or copy of `scriva.proto`
- [x] Commit: `feat(clients/java): scaffold Gradle project`

### 4.2 Proto stubs
- [x] `./gradlew generateProto` — produces Java stubs in `build/generated/source/proto/`
- [x] Verify compilation passes: `./gradlew compileJava`
- [x] Commit: `feat(clients/java): add generated proto stubs`

### 4.3 Client class (`src/main/java/com/srjn45/scriva/ScrivaDBClient.java`)
- [x] Constructor: `ScrivaDBClient(String host, int port, String apiKey)` + `ScrivaDBClient(String host, int port, String apiKey, File tlsCaCert)`
- [x] Intercept all calls to attach `x-api-key` metadata
- [x] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [x] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `List<Map<String,Object>>`), `update`, `delete`
- [x] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [x] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [x] Watch: `watch(collection, filter, StreamObserver<WatchEvent>)`
- [x] Stats: `stats(collection)`
- [x] Helper: `filterToProto(Map<String,Object> filter)` — recursive filter builder
- [x] `close()` — shutdown channel
- [x] Commit: `feat(clients/java): implement ScrivaDBClient class`

### 4.4 Example / test program
- [x] `clients/java/src/test/java/com/srjn45/scriva/ExampleTest.java` — same end-to-end flow as other clients (run with `./gradlew test`)
- [x] `clients/java/src/main/java/com/srjn45/scriva/examples/BasicExample.java` — standalone runnable main
- [x] Commit: `feat(clients/java): add example programs`

### 4.5 Documentation
- [x] `clients/java/README.md`
- [x] Add Java SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/java): README and getting-started entry`

---

## 5 — Ruby

**Directory:** `clients/ruby/`
**Transport:** `grpc` gem + `google-protobuf` gem
**Wrapper:** Ruby class `Scriva::Client` (Ruby 3.1+)
**Code gen:** `grpc_tools_ruby_protoc` or `buf` ruby plugin

### 5.1 Scaffold
- [x] `clients/ruby/scriva.gemspec`
  - spec.name = `scriva`, spec.version = `0.1.0`
  - runtime deps: `grpc ~> 1.60`, `google-protobuf ~> 3.25`
- [x] `clients/ruby/Gemfile` — source + gemspec
- [x] `clients/ruby/lib/scriva/proto/` — generated stub destination
- [x] `clients/ruby/generate.sh` — runs `grpc_tools_ruby_protoc`
- [x] Commit: `feat(clients/ruby): scaffold gem structure`

### 5.2 Proto stubs
- [x] Run `generate.sh` — produces `scriva_pb.rb` + `scriva_services_pb.rb`
- [x] `bundle install` + verify `require 'scriva/proto/scriva_pb'` works
- [x] Commit: `feat(clients/ruby): add generated proto stubs`

### 5.3 Client class (`lib/scriva/client.rb`)
- [x] `Scriva::Client.new(host:, port:, api_key:, tls_ca_cert: nil)`
- [x] Attaches `x-api-key` via gRPC metadata on every call
- [x] Collection management: `create_collection`, `drop_collection`, `list_collections`
- [x] CRUD: `insert`, `insert_many`, `find_by_id`, `find` (returns `Array<Hash>`), `update`, `delete`
- [x] Indexes: `ensure_index`, `drop_index`, `list_indexes`
- [x] Transactions: `begin_tx`, `commit_tx`, `rollback_tx`
- [x] Watch: `watch(collection, filter: nil) -> Enumerator` (also accepts block)
- [x] Stats: `stats(collection)`
- [x] Helper: `filter_to_proto(hash)` — recursive filter builder
- [x] Commit: `feat(clients/ruby): implement Scriva::Client class`

### 5.4 Example / test program
- [x] `clients/ruby/examples/test_basic.rb`
- [x] `clients/ruby/examples/test_watch.rb`
- [x] Commit: `feat(clients/ruby): add example programs`

### 5.5 Documentation
- [x] `clients/ruby/README.md`
- [x] Add Ruby SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/ruby): README and getting-started entry`

---

## 6 — Rust

**Directory:** `clients/rust/`
**Transport:** `tonic` (gRPC) + `prost` (protobuf)
**Wrapper:** Rust struct `ScrivaDB` with async methods (Tokio runtime)
**Code gen:** `tonic-build` in `build.rs`

### 6.1 Scaffold
- [x] `clients/rust/Cargo.toml`
  - name = `scriva`, version = `0.1.0`, edition = `2021`
  - deps: `tonic`, `prost`, `tokio { features = ["full"] }`, `serde_json`
  - build-deps: `tonic-build`
- [x] `clients/rust/build.rs` — calls `tonic_build::compile_protos("../../proto/scriva.proto")`
- [x] `clients/rust/proto/` — copy (or symlink) of `scriva.proto` for build.rs path resolution
- [x] Commit: `feat(clients/rust): scaffold Cargo project`

### 6.2 Proto stubs
- [x] `cargo build` — verifies `tonic-build` generates Rust code into `OUT_DIR`
- [x] `include_proto!("scriva.v1")` in `src/lib.rs` — verify it compiles
- [x] Commit: `feat(clients/rust): add generated proto stubs`

### 6.3 Client struct (`src/client.rs`)
- [x] `ScrivaDB::connect(host: &str, port: u16, api_key: &str) -> Result<Self>`
- [x] `ScrivaDB::connect_tls(host, port, api_key, ca_cert_pem) -> Result<Self>` — uses `tonic::transport::ClientTlsConfig`
- [x] All calls attach `x-api-key` via `tonic::metadata::MetadataValue` on each request
- [x] Collection management: `create_collection`, `drop_collection`, `list_collections`
- [x] CRUD: `insert`, `insert_many`, `find_by_id`, `find` (returns `Vec<Record>`), `update`, `delete`
- [x] Indexes: `ensure_index`, `drop_index`, `list_indexes`
- [x] Transactions: `begin_tx`, `commit_tx`, `rollback_tx`
- [x] Watch: `watch(collection, filter) -> impl Stream<Item=WatchEvent>` (tonic streaming)
- [x] Stats: `stats(collection)`
- [x] Helper: `json_to_struct(serde_json::Value) -> prost_types::Struct`
- [x] Commit: `feat(clients/rust): implement ScrivaDB client struct`

### 6.4 Example / test program
- [x] `clients/rust/examples/test_basic.rs` — `cargo run --example test_basic`
- [x] `clients/rust/examples/test_watch.rs`
- [x] Commit: `feat(clients/rust): add example programs`

### 6.5 Documentation
- [x] `clients/rust/README.md`
- [x] Add Rust SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/rust): README and getting-started entry`

---

## 7 — C# / .NET

**Directory:** `clients/csharp/`
**Transport:** `Grpc.Net.Client` + `Google.Protobuf`
**Wrapper:** C# class `ScrivaDB` (netstandard2.1 / .NET 6+)
**Code gen:** `Grpc.Tools` MSBuild integration (auto codegen on build)

### 7.1 Scaffold
- [x] `clients/csharp/Scriva.Client/Scriva.Client.csproj`
  - TargetFramework: `net8.0`
  - PackageReferences: `Grpc.Net.Client`, `Grpc.Tools`, `Google.Protobuf`
  - `<Protobuf>` item group pointing to `proto/scriva.proto` with `GrpcServices="Client"`
- [x] `clients/csharp/Scriva.Client.sln`
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.2 Proto stubs
- [x] `dotnet build` — Grpc.Tools auto-generates `Scriva.cs` + `ScrivaGrpc.cs`
- [x] `proto/scriva.proto` copy + `proto/google/api/` stubs bundled under `clients/csharp/`
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.3 Client class (`ScrivaDB.cs`)
- [x] `new ScrivaDB(string host, int port, string apiKey, string? tlsCaCertPath = null)`
- [x] All calls attach `x-api-key` via `Grpc.Core.Metadata`
- [x] Collection management: `CreateCollectionAsync`, `DropCollectionAsync`, `ListCollectionsAsync`
- [x] CRUD: `InsertAsync`, `InsertManyAsync`, `FindByIdAsync`, `FindAsync` (returns `IAsyncEnumerable<Dictionary<string,object?>>`), `FindAllAsync`, `UpdateAsync`, `DeleteAsync`
- [x] Indexes: `EnsureIndexAsync`, `DropIndexAsync`, `ListIndexesAsync`
- [x] Transactions: `BeginTxAsync`, `CommitTxAsync`, `RollbackTxAsync`
- [x] Watch: `WatchAsync(collection, filter?) -> IAsyncEnumerable<WatchEventResult>`
- [x] Stats: `StatsAsync(collection) -> CollectionStats`
- [x] Helper: `FilterToProto(Dictionary<string,object?> filter)` — field / AND / OR
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.4 Example / test program
- [x] `clients/csharp/Scriva.Example/Scriva.Example.csproj`
- [x] `clients/csharp/Scriva.Example/Program.cs` — same end-to-end flow (`dotnet run`)
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.5 Documentation
- [x] `clients/csharp/README.md`
- [x] Add C# SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/csharp): README and getting-started entry`

---

## Final Steps (after all 7 clients)

- [x] Update ROADMAP.md table for all 7 clients — mark as done
- [x] Update `docs/getting-started.md` — add "SDK Quick Reference" table linking all 7 READMEs
- [x] Update root `README.md` — add client SDKs section
- [x] Commit: `docs: mark all language clients complete in ROADMAP and README`

---

## Notes

### Filter input format (all clients)

All clients accept filters as a plain data structure (dict/hash/map/object):

```python
# Field filter
{"field": "age", "op": "gt", "value": "30"}

# AND composite
{"and": [
    {"field": "age", "op": "gt", "value": "18"},
    {"field": "name", "op": "contains", "value": "alice"}
]}

# OR composite
{"or": [
    {"field": "status", "op": "eq", "value": "active"},
    {"field": "role",   "op": "eq", "value": "admin"}
]}
```

`op` values: `eq` `neq` `gt` `gte` `lt` `lte` `contains` `regex`

### Unix socket support

- Python and Node.js can connect over Unix domain socket (`/tmp/scriva.sock`).
- Java, Rust, C#, PHP, Ruby use TCP only (localhost:5433 default).

### Auth header

All clients must send `x-api-key: <value>` as gRPC metadata on every call.

### TLS

When a CA cert is provided, the client verifies the server certificate.
When not provided, the client uses plaintext (insecure) transport.
