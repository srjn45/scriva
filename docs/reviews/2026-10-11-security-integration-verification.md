# Security integration verification

**Scope:** integration of the D1/D2 collection-name and containment changes
(PR #175) with the D3/D4 RPC authorization changes (PR #174), on
`autopilot/01-security-path-containment-and-authorization`.

## Result

- Unsafe names are rejected at the engine boundary and map to
  `InvalidArgument` through gRPC, REST, and the CLI path.  The negative
  corpus covers create, lookup, drop, XTx participant resolution, and REST
  and gRPC requests without modifying an outside-tree sentinel.
- The descriptor-backed authorization table classifies every Scriva and health
  RPC.  Its integration matrix verifies gRPC/REST parity, all-or-nothing
  multi-collection authorization, scoped create/drop behavior, list filtering
  after reload, whole-database-operation denial, no existence oracle, and
  transaction-handle ownership.
- `go build ./...`, `go vet ./...`, and `golangci-lint run ./...` passed.
  `go test ./... -race -count=1` passed, as did focused race runs for the
  engine and the security integration tests in `server` and `internal/auth`.

## Deliberate follow-up

PR #175 records that the engine still uses pathname operations after opening a
collection.  Its `Lstat`/`SameFile` checks reject the tested swap before drop,
but they are not an `openat`/descriptor-based guarantee against every later
post-open symlink swap.  That residual filesystem race is outside this
integration change and must not be represented as a complete D2 TOCTOU fix.
