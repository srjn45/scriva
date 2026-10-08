# CLAUDE.md — ScrivaDB Developer Guide

This file is read by Claude Code at the start of every session. It documents how to build, test, run, and distribute ScrivaDB, and records the conventions to follow when implementing new features.

---

## Project layout

```
cmd/
  scriva/           # server binary (cobra: "scriva serve")
  scriva-cli/       # CLI client binary (cobra subcommands + REPL)
engine/             # public: storage engine — segments, index, compactor, secondary indexes, db
scriva.go           # public: embedded façade (root package) — scriva.Open + per-collection options, embedded durability defaults
store/              # public: NDJSON entry encoding/decoding (store.Entry)
query/              # public: filter types and evaluation (query.Filter)
internal/
  auth/             # gRPC interceptors, API key validation
  metrics/          # Prometheus instrumentation
  pb/proto/         # generated gRPC stubs (do not edit by hand)
server/
  config.go         # Config struct, defaults, YAML loader
  grpc.go           # ScrivaServer — proto ↔ engine mapping
  rest.go           # grpc-gateway REST bridge
proto/
  scriva.proto      # single source of truth for the API — edit here first
docs/
  getting-started.md
  architecture.md
```

---

## Build

```bash
make build        # compiles bin/scriva and bin/scriva-cli
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

### Randomized model test and soak

`engine/model_harness_test.go` (`TestModel`) applies a seeded random op sequence to a
collection and an in-memory model: insert/update/delete/get/scan/scan-index, clean
reopen, crash-reopen (with optional torn tail), torn writes (injected ENOSPC), compaction
interleaved with tiny-segment rotation, `CommitTx` with injected write failure, crash-image
recovery (open a copy of the live dir, check Get/scan/`IndexLookup`), periodic online
`Verify`, and `Repair` as a no-op on a copied healthy DB. CI runs a fixed set of seeds
(`modelDefaultSeeds`, 200 steps). Failures print `replay: SCRIVA_MODEL_SEED=<n>`.

| Env var | Meaning |
|---|---|
| `SCRIVA_MODEL_SEED` | comma-separated seeds to run (replay) |
| `SCRIVA_MODEL_STEPS` | ops per seed (default 200) |
| `SCRIVA_MODEL_OPS` | comma-separated op names to run (default: all) |
| `SCRIVA_MODEL_SEED_COUNT` / `SCRIVA_MODEL_SEED_BASE` | soak: N consecutive seeds from BASE (default 1000) |
| `SCRIVA_MODEL_RACE_COMPACTION=1` | let background compaction race the next op (exposes the known scan-vs-compaction bug) |

```bash
make test-soak                                   # 100 seeds x 500 steps, race detector
make test-soak SOAK_SEEDS=300 SOAK_STEPS=1000    # longer; also SOAK_BASE, SOAK_TIMEOUT
```

### Crash, fault and integrity tests

All of these run under `make test` (race detector on) and are hermetic (`t.TempDir()`):

| Test | What it covers |
|---|---|
| `TestCrashMatrix_*` (`engine/crash_matrix_test.go`) | single-writer crash at every step boundary, short/torn writes, rotation, compaction swap, index persist, shutdown, using the fault-injecting FS in `faultfs_test.go` |
| `TestKill9_*` (`engine/multiprocess_kill_test.go`, non-Windows) | a re-exec'd child is SIGKILLed mid-write/rotate/compact; recovered state must equal the acknowledged ops. `SCRIVA_KILL_SEED=<n>` replays one seed, `SCRIVA_KILL_SOAK=1` raises iterations |
| `TestIntegrityFixtures`, `TestIntegrityFixturesUpToDate` | synthetic incident directories in `engine/testdata/integrity/`; never hand-edit — regenerate with `go generate ./engine` and commit the result |
| `verify_test.go`, `repair_test.go`, `salvage_test.go`, `sidx_recovery_test.go` | `Verify` / `Repair` / index recovery |

```bash
go test ./engine -race -count=1 -run 'TestCrashMatrix|TestKill9'
SCRIVA_KILL_SOAK=1 go test ./engine -race -count=1 -run TestKill9
go generate ./engine                                     # rewrite integrity fixtures
go test ./engine -run xxx -bench 'Open|Persist' -benchtime=2x   # recovery cost (SCRIVA_BENCH_N=300000 for the documented table)
```

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
make run          # builds + starts: bin/scriva serve --data ./data --api-key dev-key
make cli          # builds + starts: bin/scriva-cli --api-key dev-key (REPL)
```

Default ports:

| Service | Address |
|---|---|
| gRPC (TCP) | `:5433` |
| REST gateway | `:8080` |
| Unix socket | `/tmp/scriva.sock` |
| Prometheus metrics | `:9090/metrics` |

---

## Regenerate gRPC stubs

```bash
make proto        # runs: buf generate
```

Requirements: [buf](https://buf.build/docs/installation) CLI.

Always edit `proto/scriva.proto` first, then regenerate. Never edit files under `internal/pb/proto/` by hand.

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
3. Pushes the Docker image to `ghcr.io/srjn45/scriva`

### Docker image (manual)

```bash
docker build -t ghcr.io/srjn45/scriva:dev .
docker run -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e SCRIVA_API_KEY=dev-key \
  ghcr.io/srjn45/scriva:dev serve --data /data
```

---

## Conventions

### API changes
1. Edit `proto/scriva.proto` — add the RPC and message types.
2. Run `make proto` to regenerate stubs.
3. Implement the handler in `server/grpc.go`.
4. Add the engine method to `engine/collection.go` (or `db.go`).
5. Add a CLI command in `cmd/scriva-cli/commands.go` and register it in `rootCmd()`.
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
5. Add the cobra flag in `serveCmd()` in `cmd/scriva/main.go`.
6. Handle the CLI override in the `cmd.Flags().Visit(...)` block.

### Metrics
- All new long-running operations should emit a counter and a histogram.
- Add instruments to `internal/metrics/metrics.go`.
- Inject via hooks in `engine.CollectionConfig` or via an interceptor — never call `metrics.*` directly from the engine layer.

### Commit style
- Use conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`
- Scope in parentheses when useful: `feat(engine):`, `docs(getting-started):`
