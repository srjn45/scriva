# Contributing to FileDB v2

Thanks for your interest in improving FileDB v2! This guide covers everything
you need to build, test, and submit a change. For the full developer reference
— project layout, distribution, and per-feature conventions — see
[CLAUDE.md](CLAUDE.md).

Requirements: **Go 1.22+**.

---

## Build

```bash
make build        # compiles bin/filedb and bin/filedb-cli
make clean        # removes bin/, coverage.out, dist/
```

## Test

```bash
make test         # go test ./... -race -count=1 -coverprofile=coverage.out
```

The race detector is **always on** — never skip it. All tests must pass before
you open a PR.

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

| Service | Address |
|---|---|
| gRPC (TCP) | `:5433` |
| REST gateway | `:8080` |
| Unix socket | `/tmp/filedb.sock` |
| Prometheus metrics | `:9090/metrics` |

You can also run the server in a container — see the
[Run with Docker Compose](README.md#run-with-docker-compose) quick start.

---

## Branch and commit conventions

- Branch off `main`; never commit directly to `main`.
- Name branches by intent, e.g. `feat/secondary-index`, `fix/compaction-race`,
  `docs/getting-started`, `chore/project-polish`.
- Use [Conventional Commits](https://www.conventionalcommits.org/):
  `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`. Add a scope in
  parentheses when useful, e.g. `feat(engine):`, `docs(getting-started):`.

---

## Pull request expectations

Before opening a PR, make sure:

- [ ] `make build` succeeds.
- [ ] `make test` passes **with the race detector** (it is on by default).
- [ ] `make lint` and `make vet` are clean.
- [ ] Documentation is updated for any user-facing change — `docs/getting-started.md`
      (usage), `docs/architecture.md` (internals), and `README.md` if the key
      properties change. Mark the relevant item done in `ROADMAP.md`.
- [ ] New API changes follow the workflow in [CLAUDE.md](CLAUDE.md): edit
      `proto/filedb.proto` first, run `make proto`, then implement the handler,
      engine method, CLI command, and tests.
- [ ] Integration tests live in `server/grpc_integration_test.go` and use the
      real in-process gRPC server — never mock the engine.

Keep PRs focused and small where possible. Thanks for contributing!
