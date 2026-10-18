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

## Post-open containment follow-up

PR #177 closes the post-open collection-directory swap gap on Linux by keeping
the opened collection directory descriptor alive and routing collection I/O
through that anchored descriptor path.  The regression coverage swaps a live
collection directory for an outside symlink, then writes and snapshots while
asserting the outside tree is unchanged.  Crash-recovery manifests store
collection-relative paths so restart recovery does not persist process-local
descriptor paths.
