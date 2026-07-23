---
title: Contributing
description: How to build, test, regenerate stubs, and submit changes to ScrivaDB.
---

Contributions are welcome. ScrivaDB is written in **Go (1.22+)** and the whole
project builds from a single repo.

## Build & test

```bash
make build        # compiles bin/scriva and bin/scriva-cli
make test         # go test ./... -race -count=1
make lint         # golangci-lint run ./...
make vet          # go vet ./...
```

The **race detector is always on** — don't skip it. All tests must pass before
opening a PR. Integration tests spin up a **real in-process gRPC server** — the
engine layer is never mocked, because the whole point is testing real disk I/O.

## Changing the API

The proto is the source of truth. To add or change an RPC:

1. Edit [`proto/scriva.proto`](https://github.com/srjn45/scriva/blob/main/proto/scriva.proto).
2. Run `make proto` (requires the [buf](https://buf.build/docs/installation) CLI)
   to regenerate stubs. **Never** hand-edit files under `internal/pb/proto/`.
3. Implement the server handler, then the engine method.
4. Add a CLI command and register it.
5. Write tests — engine unit tests **and** an integration test.

## Documentation

Every new feature must be documented before its PR is merged — update the
getting-started guide, the architecture doc, the `README`, and mark the roadmap
item done.

## Commit style

Conventional commits, scoped where useful:

```
feat(engine): add keyset pagination cursor
fix(server): map duplicate key to AlreadyExists
docs(getting-started): document --slow-query-ms
```

## Getting help

Read the full [`CONTRIBUTING.md`](https://github.com/srjn45/scriva/blob/main/CONTRIBUTING.md)
and the [`CLAUDE.md`](https://github.com/srjn45/scriva/blob/main/CLAUDE.md)
developer guide in the repo, and open an
[issue](https://github.com/srjn45/scriva/issues) or discussion for anything
unclear.
