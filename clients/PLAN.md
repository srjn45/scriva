# FileDB v2 — Language Clients Implementation Plan

This document is the single source of truth for tracking language client progress.
Each client is a thin wrapper over the gRPC API defined in `proto/filedb.proto`.

**How to use this file:**
- Work through one language at a time, top to bottom.
- Check off each box as it is done and commit after each logical step.
- You can stop at any checkbox and resume from that exact point later.

---

## Languages Covered

| # | Language | Package Registry | Directory | Status |
|---|---|---|---|---|
| 1 | Python | PyPI (`pip install filedbv2`) | `clients/python/` | ✅ Done |
| 2 | TypeScript / JavaScript | npm (`npm install filedbv2`) | `clients/js/` | ✅ Done |
| 3 | PHP | Packagist (`composer require srjn45/filedbv2`) | `clients/php/` | ⬜ Not started |
| 4 | Java | Maven Central (`com.srjn45:filedbv2-client`) | `clients/java/` | ✅ Done |
| 5 | Ruby | RubyGems (`gem install filedbv2`) | `clients/ruby/` | ⬜ Not started |
| 6 | Rust | crates.io (`filedbv2`) | `clients/rust/` | ⬜ Not started |
| 7 | C# / .NET | NuGet (`FileDBv2.Client`) | `clients/csharp/` | ✅ Done |

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
feat(clients/python): implement FileDB client class
feat(clients/python): add example test program
docs(clients/python): add README and getting-started entry
```

---

## 1 — Python

**Directory:** `clients/python/`
**Transport:** `grpcio` + `grpcio-tools` (or buf.build/grpc/python plugin)
**Wrapper:** pure Python class `FileDB`
**Target Python:** 3.9+

### 1.1 Scaffold
- [x] Add `clients/python/` directory with `pyproject.toml` (flit or hatchling build backend)
  - package name: `filedbv2`
  - version: `0.1.0`
  - dependencies: `grpcio>=1.60`, `grpcio-tools>=1.60`, `protobuf>=4.25`
- [x] Add `clients/python/src/filedbv2/__init__.py` (re-exports `FileDB`)
- [x] Add `clients/python/src/filedbv2/proto/` (generated stub destination)
- [x] Add `clients/python/generate.sh` — runs `python -m grpc_tools.protoc` to regenerate stubs from `../../proto/`
- [x] Commit: `feat(clients/python): scaffold package structure`

### 1.2 Proto stubs
- [x] Run `generate.sh` — produces `filedb_pb2.py` + `filedb_pb2_grpc.py` in `proto/`
- [x] Verify import works: `from filedbv2.proto import filedb_pb2`
- [x] Commit: `feat(clients/python): add generated proto stubs`

### 1.3 Client class (`client.py`)
- [x] `FileDB.__init__(host, port, api_key, tls_ca_cert=None)` — builds channel + stub
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
- [x] Commit: `feat(clients/python): implement FileDB client class`

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
**Wrapper:** TypeScript class `FileDB`, ships with `.d.ts` + CommonJS + ESM
**Target:** Node.js 18+, TypeScript 5+

### 2.1 Scaffold
- [x] `clients/js/package.json`
  - name: `filedbv2`, version `0.1.0`
  - deps: `@grpc/grpc-js`, `@grpc/proto-loader`
  - devDeps: `typescript`, `ts-node`, `@types/node`
  - scripts: `build`, `generate`, `clean`
- [x] `clients/js/tsconfig.json` — target ES2020, declaration: true, outDir: `dist/`
- [x] `clients/js/proto/` directory with `filedb.proto` copy + `google/api/` stubs for dynamic loading
- [x] `clients/js/generate.sh` — script to run `grpc_tools_node_protoc` for static codegen (optional)
- [x] Commit: `feat(clients/js): scaffold package structure`

### 2.2 Proto stubs
- [x] Proto loaded dynamically via `@grpc/proto-loader` at runtime — no pre-generated files needed
- [x] `clients/js/proto/filedb.proto` + `google/api/{annotations,http}.proto` stubs bundled in package
- [x] Commit: `feat(clients/js): add proto files for dynamic loading`

### 2.3 Client class (`src/client.ts`)
- [x] `new FileDB(host, port, apiKey, tlsCaCert?)` — builds `grpc.Client` with `x-api-key` metadata on every call
- [x] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [x] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `AsyncGenerator<DBRecord>`), `findAll`, `update`, `delete`
- [x] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [x] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [x] Watch: `watch(collection, filter?) -> AsyncGenerator<WatchEvent>`
- [x] Stats: `stats(collection)`
- [x] Helper: `filterToProto(filter: FilterInput)` — converts plain JS object to proto Filter
- [x] TypeScript types: `DBRecord`, `WatchEvent`, `FilterInput`, `FindOptions`, `StatsResult` exported from `index.ts`
- [x] Commit: `feat(clients/js): implement FileDB client class`

### 2.4 Build
- [x] `npm run build` produces `dist/` with `.js` + `.d.ts` files (CJS + declaration maps)
- [x] `require('filedbv2')` and `import { FileDB } from 'filedbv2'` both work
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
**Wrapper:** PHP class `FileDB` (PHP 8.1+)
**Code gen:** `protoc` + `grpc_php_plugin`

### 3.1 Scaffold
- [ ] `clients/php/composer.json`
  - name: `srjn45/filedbv2`, version `0.1.0`
  - require: `grpc/grpc: ^1.56`, `google/protobuf: ^3.25`
  - autoload PSR-4: `FileDBv2\\` → `src/`
- [ ] `clients/php/src/Proto/` — generated stub destination
- [ ] `clients/php/generate.sh` — runs `protoc` with `--php_out` + `--grpc_out`
- [ ] Commit: `feat(clients/php): scaffold package structure`

### 3.2 Proto stubs
- [ ] Run `generate.sh` — produces `FileDB/V1/` namespace PHP files
- [ ] `composer install` + verify autoload resolves stubs
- [ ] Commit: `feat(clients/php): add generated proto stubs`

### 3.3 Client class (`src/FileDB.php`)
- [ ] `new FileDB(string $host, int $port, string $apiKey, ?string $tlsCaCert = null)`
- [ ] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [ ] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `array`), `update`, `delete`
- [ ] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [ ] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [ ] Watch: `watch(string $collection, ?array $filter = null): \Generator`
- [ ] Stats: `stats(string $collection): array`
- [ ] Helper: `filterToProto(array $filter): \FileDB\V1\Filter`
- [ ] Commit: `feat(clients/php): implement FileDB client class`

### 3.4 Example / test program
- [ ] `clients/php/examples/test_basic.php` — same end-to-end flow
- [ ] `clients/php/examples/test_watch.php`
- [ ] Commit: `feat(clients/php): add example programs`

### 3.5 Documentation
- [ ] `clients/php/README.md`
- [ ] Add PHP SDK section to `docs/getting-started.md`
- [ ] Mark rows + ROADMAP.md
- [ ] Commit: `docs(clients/php): README and getting-started entry`

---

## 4 — Java

**Directory:** `clients/java/`
**Transport:** `io.grpc:grpc-netty-shaded` + `com.google.protobuf:protobuf-java`
**Wrapper:** Java class `FileDBClient` (Java 11+)
**Build:** Gradle 8 with `com.google.protobuf` plugin for auto codegen

### 4.1 Scaffold
- [x] `clients/java/build.gradle.kts`
  - Apply `java-library`, `com.google.protobuf` plugins
  - Deps: `grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`, `protobuf-java`, `javax.annotation-api`
  - `protobuf { generateProtoTasks { ... } }` — points to `../../proto/`
- [x] `clients/java/settings.gradle.kts` — rootProject name = `filedbv2-client`
- [x] `clients/java/src/main/proto/` — symlink or copy of `filedb.proto`
- [x] Commit: `feat(clients/java): scaffold Gradle project`

### 4.2 Proto stubs
- [x] `./gradlew generateProto` — produces Java stubs in `build/generated/source/proto/`
- [x] Verify compilation passes: `./gradlew compileJava`
- [x] Commit: `feat(clients/java): add generated proto stubs`

### 4.3 Client class (`src/main/java/com/srjn45/filedbv2/FileDBClient.java`)
- [x] Constructor: `FileDBClient(String host, int port, String apiKey)` + `FileDBClient(String host, int port, String apiKey, File tlsCaCert)`
- [x] Intercept all calls to attach `x-api-key` metadata
- [x] Collection management: `createCollection`, `dropCollection`, `listCollections`
- [x] CRUD: `insert`, `insertMany`, `findById`, `find` (returns `List<Map<String,Object>>`), `update`, `delete`
- [x] Indexes: `ensureIndex`, `dropIndex`, `listIndexes`
- [x] Transactions: `beginTx`, `commitTx`, `rollbackTx`
- [x] Watch: `watch(collection, filter, StreamObserver<WatchEvent>)`
- [x] Stats: `stats(collection)`
- [x] Helper: `filterToProto(Map<String,Object> filter)` — recursive filter builder
- [x] `close()` — shutdown channel
- [x] Commit: `feat(clients/java): implement FileDBClient class`

### 4.4 Example / test program
- [x] `clients/java/src/test/java/com/srjn45/filedbv2/ExampleTest.java` — same end-to-end flow as other clients (run with `./gradlew test`)
- [x] `clients/java/src/main/java/com/srjn45/filedbv2/examples/BasicExample.java` — standalone runnable main
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
**Wrapper:** Ruby class `FileDBv2::Client` (Ruby 3.1+)
**Code gen:** `grpc_tools_ruby_protoc` or `buf` ruby plugin

### 5.1 Scaffold
- [ ] `clients/ruby/filedbv2.gemspec`
  - spec.name = `filedbv2`, spec.version = `0.1.0`
  - runtime deps: `grpc ~> 1.60`, `google-protobuf ~> 3.25`
- [ ] `clients/ruby/Gemfile` — source + gemspec
- [ ] `clients/ruby/lib/filedbv2/proto/` — generated stub destination
- [ ] `clients/ruby/generate.sh` — runs `grpc_tools_ruby_protoc`
- [ ] Commit: `feat(clients/ruby): scaffold gem structure`

### 5.2 Proto stubs
- [ ] Run `generate.sh` — produces `filedb_pb.rb` + `filedb_services_pb.rb`
- [ ] `bundle install` + verify `require 'filedbv2/proto/filedb_pb'` works
- [ ] Commit: `feat(clients/ruby): add generated proto stubs`

### 5.3 Client class (`lib/filedbv2/client.rb`)
- [ ] `FileDBv2::Client.new(host:, port:, api_key:, tls_ca_cert: nil)`
- [ ] Attaches `x-api-key` via `GRPC::Core::CallCredentials`
- [ ] Collection management: `create_collection`, `drop_collection`, `list_collections`
- [ ] CRUD: `insert`, `insert_many`, `find_by_id`, `find` (returns `Array<Hash>`), `update`, `delete`
- [ ] Indexes: `ensure_index`, `drop_index`, `list_indexes`
- [ ] Transactions: `begin_tx`, `commit_tx`, `rollback_tx`
- [ ] Watch: `watch(collection, filter: nil) -> Enumerator`
- [ ] Stats: `stats(collection)`
- [ ] Helper: `filter_to_proto(hash)` — recursive filter builder
- [ ] Commit: `feat(clients/ruby): implement FileDBv2::Client class`

### 5.4 Example / test program
- [ ] `clients/ruby/examples/test_basic.rb`
- [ ] `clients/ruby/examples/test_watch.rb`
- [ ] Commit: `feat(clients/ruby): add example programs`

### 5.5 Documentation
- [ ] `clients/ruby/README.md`
- [ ] Add Ruby SDK section to `docs/getting-started.md`
- [ ] Mark rows + ROADMAP.md
- [ ] Commit: `docs(clients/ruby): README and getting-started entry`

---

## 6 — Rust

**Directory:** `clients/rust/`
**Transport:** `tonic` (gRPC) + `prost` (protobuf)
**Wrapper:** Rust struct `FileDB` with async methods (Tokio runtime)
**Code gen:** `tonic-build` in `build.rs`

### 6.1 Scaffold
- [ ] `clients/rust/Cargo.toml`
  - name = `filedbv2`, version = `0.1.0`, edition = `2021`
  - deps: `tonic`, `prost`, `tokio { features = ["full"] }`, `serde_json`
  - build-deps: `tonic-build`
- [ ] `clients/rust/build.rs` — calls `tonic_build::compile_protos("../../proto/filedb.proto")`
- [ ] `clients/rust/proto/` — copy (or symlink) of `filedb.proto` for build.rs path resolution
- [ ] Commit: `feat(clients/rust): scaffold Cargo project`

### 6.2 Proto stubs
- [ ] `cargo build` — verifies `tonic-build` generates Rust code into `OUT_DIR`
- [ ] `include_proto!("filedb.v1")` in `src/lib.rs` — verify it compiles
- [ ] Commit: `feat(clients/rust): add generated proto stubs`

### 6.3 Client struct (`src/client.rs`)
- [ ] `FileDB::connect(host: &str, port: u16, api_key: &str) -> Result<Self>`
- [ ] `FileDB::connect_tls(host, port, api_key, ca_cert_pem) -> Result<Self>` — uses `tonic::transport::ClientTlsConfig`
- [ ] All calls attach `x-api-key` via `tonic::metadata::MetadataValue` on each request
- [ ] Collection management: `create_collection`, `drop_collection`, `list_collections`
- [ ] CRUD: `insert`, `insert_many`, `find_by_id`, `find` (returns `Vec<Record>`), `update`, `delete`
- [ ] Indexes: `ensure_index`, `drop_index`, `list_indexes`
- [ ] Transactions: `begin_tx`, `commit_tx`, `rollback_tx`
- [ ] Watch: `watch(collection, filter) -> impl Stream<Item=WatchEvent>` (tonic streaming)
- [ ] Stats: `stats(collection)`
- [ ] Helper: `json_to_struct(serde_json::Value) -> prost_types::Struct`
- [ ] Commit: `feat(clients/rust): implement FileDB client struct`

### 6.4 Example / test program
- [ ] `clients/rust/examples/test_basic.rs` — `cargo run --example test_basic`
- [ ] `clients/rust/examples/test_watch.rs`
- [ ] Commit: `feat(clients/rust): add example programs`

### 6.5 Documentation
- [ ] `clients/rust/README.md`
- [ ] Add Rust SDK section to `docs/getting-started.md`
- [ ] Mark rows + ROADMAP.md
- [ ] Commit: `docs(clients/rust): README and getting-started entry`

---

## 7 — C# / .NET

**Directory:** `clients/csharp/`
**Transport:** `Grpc.Net.Client` + `Google.Protobuf`
**Wrapper:** C# class `FileDB` (netstandard2.1 / .NET 6+)
**Code gen:** `Grpc.Tools` MSBuild integration (auto codegen on build)

### 7.1 Scaffold
- [x] `clients/csharp/FileDBv2.Client/FileDBv2.Client.csproj`
  - TargetFramework: `net8.0`
  - PackageReferences: `Grpc.Net.Client`, `Grpc.Tools`, `Google.Protobuf`
  - `<Protobuf>` item group pointing to `proto/filedb.proto` with `GrpcServices="Client"`
- [x] `clients/csharp/FileDBv2.Client.sln`
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.2 Proto stubs
- [x] `dotnet build` — Grpc.Tools auto-generates `Filedb.cs` + `FiledbGrpc.cs`
- [x] `proto/filedb.proto` copy + `proto/google/api/` stubs bundled under `clients/csharp/`
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.3 Client class (`FileDB.cs`)
- [x] `new FileDB(string host, int port, string apiKey, string? tlsCaCertPath = null)`
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
- [x] `clients/csharp/FileDBv2.Example/FileDBv2.Example.csproj`
- [x] `clients/csharp/FileDBv2.Example/Program.cs` — same end-to-end flow (`dotnet run`)
- [x] Commit: `feat(clients/csharp): scaffold .NET project`

### 7.5 Documentation
- [x] `clients/csharp/README.md`
- [x] Add C# SDK section to `docs/getting-started.md`
- [x] Mark rows + ROADMAP.md
- [x] Commit: `docs(clients/csharp): README and getting-started entry`

---

## Final Steps (after all 7 clients)

- [ ] Update ROADMAP.md table for all 7 clients — mark as done
- [ ] Update `docs/getting-started.md` — add "SDK Quick Reference" table linking all 7 READMEs
- [ ] Update root `README.md` — add client SDKs section
- [ ] Commit: `docs: mark all language clients complete in ROADMAP and README`

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

- Python and Node.js can connect over Unix domain socket (`/tmp/filedb.sock`).
- Java, Rust, C#, PHP, Ruby use TCP only (localhost:5433 default).

### Auth header

All clients must send `x-api-key: <value>` as gRPC metadata on every call.

### TLS

When a CA cert is provided, the client verifies the server certificate.
When not provided, the client uses plaintext (insecure) transport.
