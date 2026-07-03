# CLAUDE.md — FileDB v2 Developer Guide

This file is read by Claude Code at the start of every session. It documents how to build, test, run, and distribute FileDB v2, and records the conventions to follow when implementing new features.

---

## Project layout

```
cmd/
  filedb/           # server binary (cobra: "filedb serve")
  filedb-cli/       # CLI client binary (cobra subcommands + REPL)
engine/             # public: storage engine — segments, index, compactor, secondary indexes, db
store/              # public: NDJSON entry encoding/decoding (store.Entry)
query/              # public: filter types and evaluation (query.Filter)
internal/
  auth/             # gRPC interceptors, API key validation
  metrics/          # Prometheus instrumentation
  pb/proto/         # generated gRPC stubs (do not edit by hand)
server/
  config.go         # Config struct, defaults, YAML loader
  grpc.go           # FileDBServer — proto ↔ engine mapping
  rest.go           # grpc-gateway REST bridge
proto/
  filedb.proto      # single source of truth for the API — edit here first
docs/
  getting-started.md
  architecture.md
```

---

## Build

```bash
make build        # compiles bin/filedb and bin/filedb-cli
make clean        # removes bin/, coverage.out, dist/
```

Binaries are built with `-trimpath -ldflags "-s -w"` (stripped, reproducible).

Requirements: Go 1.22+

---

## Test

```bash
make test         # go test ./... -race -count=1 -coverprofile=coverage.out
```

- The race detector is always on — never skip it.
- All tests must pass before opening a PR.
- Integration tests (`server/grpc_integration_test.go`) spin up a real in-process gRPC server — no mocking of the engine layer.

Run a specific package:

```bash
go test ./engine/... -race -v
go test ./server/... -race -v -run TestInsert
```

---

## Lint

```bash
make lint         # golangci-lint run ./...
make vet          # go vet ./...
```

Install golangci-lint: https://golangci-lint.run/usage/install/

---

## Run locally

```bash
make run          # builds + starts: bin/filedb serve --data ./data --api-key dev-key
make cli          # builds + starts: bin/filedb-cli --api-key dev-key (REPL)
```

Default ports:

| Service | Address |
|---|---|
| gRPC (TCP) | `:5433` |
| REST gateway | `:8080` |
| Unix socket | `/tmp/filedb.sock` |
| Prometheus metrics | `:9090/metrics` |

---

## Regenerate gRPC stubs

```bash
make proto        # runs: buf generate
```

Requirements: [buf](https://buf.build/docs/installation) CLI.

Always edit `proto/filedb.proto` first, then regenerate. Never edit files under `internal/pb/proto/` by hand.

---

## Distribute

### Snapshot build (local, no publish)

```bash
make release      # goreleaser release --snapshot --clean
```

Produces cross-compiled archives in `dist/` for: linux/darwin/windows × amd64/arm64.

Requirements: [goreleaser](https://goreleaser.com/install/)

### Publish a release

```bash
git tag v0.x.y
git push origin v0.x.y
```

The `.github/workflows/release.yml` CI job triggers on `v*` tags and:
1. Runs `goreleaser release`
2. Publishes tarballs to GitHub Releases
3. Pushes the Docker image to `ghcr.io/srjn45/filedbv2`

### Docker image (manual)

```bash
docker build -t ghcr.io/srjn45/filedbv2:dev .
docker run -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e FILEDB_API_KEY=dev-key \
  ghcr.io/srjn45/filedbv2:dev serve --data /data
```

---

## Conventions

### API changes
1. Edit `proto/filedb.proto` — add the RPC and message types.
2. Run `make proto` to regenerate stubs.
3. Implement the handler in `server/grpc.go`.
4. Add the engine method to `engine/collection.go` (or `db.go`).
5. Add a CLI command in `cmd/filedb-cli/commands.go` and register it in `rootCmd()`.
6. Write tests: engine unit tests + `server/grpc_integration_test.go`.

### Documentation
- Every new feature must be documented before the PR is merged.
- Update `docs/getting-started.md` (usage) and `docs/architecture.md` (how it works internally).
- Update `README.md` if the feature changes the key properties list.
- Mark the ROADMAP item as done in `ROADMAP.md`.

### Testing rules
- Engine tests live in `engine/`.
- Server-level tests live in `server/grpc_integration_test.go` and use an in-process gRPC server.
- Never mock the engine in integration tests — the whole point is testing real disk I/O.
- Use `t.TempDir()` for data directories; tests must be hermetic and parallel-safe.

### Adding a new server flag
1. Add the field to `server.Config` with a `yaml:"..."` tag.
2. Add it to `fileConfig` (the YAML intermediate struct) in `server/config.go`.
3. Add it to `DefaultConfig()`.
4. Wire it in `LoadConfigFile()` (decode → convert → return).
5. Add the cobra flag in `serveCmd()` in `cmd/filedb/main.go`.
6. Handle the CLI override in the `cmd.Flags().Visit(...)` block.

### Metrics
- All new long-running operations should emit a counter and a histogram.
- Add instruments to `internal/metrics/metrics.go`.
- Inject via hooks in `engine.CollectionConfig` or via an interceptor — never call `metrics.*` directly from the engine layer.

### Commit style
- Use conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`
- Scope in parentheses when useful: `feat(engine):`, `docs(getting-started):`
