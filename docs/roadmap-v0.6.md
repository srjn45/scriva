# FileDB — v0.6+ Roadmap & Implementation Plan

The post-v0.1.0 plan ([`roadmap-v0.2.md`](roadmap-v0.2.md), milestones v0.2.0–
v0.5.0) has fully shipped: durability hardening, query-at-scale, TTL /
snapshot / on-demand compaction, scoped rotatable API keys, an embeddable Go
engine, and full API parity across all seven client SDKs. FileDB is now a
correct, embeddable, feature-complete **single node**.

This document plans the next arc: **making FileDB operable, observable, and
survivable as a real service — and closing the gap between what the embedded
engine can do and what the network API exposes — on the road to a frozen
`v1.0.0`.**

It is derived from a codebase review of 2026-07-03. Each item lists the
*problem* (with `file:line` evidence where it exists today), the *approach*,
*proto/API* impact, the *files* touched, *tests*, and an *acceptance bar*.
Items are sized **S** (≤½ day), **M** (1–2 days), **L** (3+ days).

> **Workflow:** one PR per task (small related fixes may batch). CI green before
> merge; every feature updates `docs/`, `README.md`, `CHANGELOG.md`, and ticks
> its box here. Large items (R1, N2, Q6) are candidate warden agents in isolated
> worktrees.

---

## Milestone summary

| Milestone | Theme | Items | Risk | Headline |
|---|---|---|---|---|
| **v0.6.0** | Operability & observability | O1–O5 | Low | Run it as a service — health, structured logs, tracing, backpressure |
| **v0.7.0** | Network/engine API parity + query breadth | N1–N4 | Medium | The wire API is as capable as the embedded one; return only what you ask for |
| **v0.8.0** | Replication & high availability | R1–R3 | High | Survive a node loss; scale reads to followers |
| **v0.9.0** | Security & tenancy depth → 1.0 readiness | S1–S4 | Medium | mTLS, audit, per-collection ACLs, quotas; then freeze for v1.0.0 |

The critical, differentiating arc is **v0.8.0 (replication)** — everything before
it de-risks operating a fleet; everything after it hardens the multi-tenant
security story before the API is frozen at 1.0.

---

## v0.6.0 — Operability & observability

Today the server is a black box to operators. It logs with the stdlib `log`
package from exactly one place (`cmd/filedb/main.go:7`), exposes **no
health/readiness probe** and **no gRPC health service**, has **no tracing**, and
applies **no backpressure** — a single client can open unbounded concurrent
streams. These are the table-stakes for running FileDB behind a load balancer or
in Kubernetes.

### O1 — structured, leveled logging — **S/M**
- **Problem:** the only logging is stdlib `log` in `cmd/filedb/main.go`; the
  engine and gRPC layers log nothing. There is no request log, no level control,
  no machine-parseable output.
- **Approach:** adopt `log/slog` (stdlib, zero new deps). A configured
  `*slog.Logger` (JSON or text handler, level from config) is threaded through
  `server` and injected into the engine via the existing hook pattern in
  `engine.CollectionConfig` (never import a logger into the engine directly —
  same rule as metrics). Add a unary/stream interceptor that logs each RPC
  (method, principal, duration, status) at debug/info.
- **API/flags:** `--log-level` and `--log-format` (Config + YAML + flag, per the
  CLAUDE.md flag checklist).
- **Files:** `server/logging.go` (new), `server/grpc.go`, `server/config.go`,
  `cmd/filedb/main.go`, `internal/metrics/`-style hook wiring.
- **Tests:** interceptor emits one structured record per call with the right
  fields; level filtering works; engine hook fires without importing slog into
  the engine package (keep `make deps-check` green).
- **Acceptance:** every RPC produces one structured log line at info; the engine
  package still has no logging dependency.

### O2 — health & readiness (gRPC health + REST probes) — **S**
- **Problem:** no `grpc.health.v1.Health` service and no HTTP `/healthz`
  `/readyz`. Orchestrators and LBs cannot probe liveness/readiness.
- **Approach:** register the standard `google.golang.org/grpc/health` service and
  mark `SERVING` once listeners are up. Add `GET /healthz` (process alive) and
  `GET /readyz` (DB open, data dir writable) to the REST mux. Flip to
  `NOT_SERVING` during graceful shutdown so in-flight drains cleanly.
- **API:** standard gRPC health proto (no change to `filedb.proto`); two REST
  routes registered directly on the gateway mux.
- **Files:** `server/health.go` (new), `server/rest.go`, `cmd/filedb/main.go`.
- **Tests:** integration — health returns SERVING when up, NOT_SERVING during
  shutdown; `/readyz` fails when the data dir is unwritable.
- **Acceptance:** a k8s-style readiness probe correctly gates traffic on startup
  and shutdown.

### O3 — request backpressure & limits — **M**
- **Problem:** no cap on concurrent streams or in-flight requests
  (`server/grpc.go`); a greedy or buggy client can exhaust goroutines/FDs. No
  per-key rate limiting despite scoped keys existing.
- **Approach:** (a) set `grpc.MaxConcurrentStreams` and a server-wide in-flight
  semaphore interceptor returning `RESOURCE_EXHAUSTED` when saturated; (b)
  optional token-bucket rate limit keyed by API-key principal (reuse the auth
  interceptor's resolved principal). Both off by default, opt-in via flags.
- **API/flags:** `--max-concurrent-streams`, `--max-inflight`, `--rate-limit`
  (per-key rps).
- **Files:** `server/limits.go` (new), `server/grpc.go`, `internal/auth/`
  (expose principal to the limiter), `server/config.go`, `cmd/filedb/main.go`.
- **Tests:** saturate the semaphore → excess calls get `RESOURCE_EXHAUSTED`;
  rate limiter throttles one key without affecting another.
- **Acceptance:** under a flood, the server sheds load with a typed error instead
  of unbounded resource growth.

### O4 — OpenTelemetry tracing (opt-in) — **M**
- **Problem:** no distributed tracing; a slow `Find` can't be attributed across
  the gateway → gRPC → engine hops.
- **Approach:** add an OTel trace interceptor (OTLP exporter, off unless
  `--otlp-endpoint` is set) that spans each RPC; add engine-level span hooks
  (via the config hook pattern) around scan/compaction so slow segments are
  visible. Keep the engine package dependency-free — the engine emits timing
  through the existing hook, the server owns the OTel SDK.
- **API/flags:** `--otlp-endpoint`, `--otlp-sample-ratio`.
- **Files:** `server/tracing.go` (new), `server/grpc.go`, `cmd/filedb/main.go`,
  engine hook call sites.
- **Tests:** interceptor creates a span per RPC with method/status attributes
  (in-memory exporter); disabled path adds no measurable overhead.
- **Acceptance:** with an OTLP collector configured, a `Find` produces a trace
  spanning gateway → gRPC → engine scan; disabled by default.

### O5 — slow-query log & richer stats — **S**
- **Problem:** `CollectionStats` reports records/segments/dirty/size but nothing
  about query cost; there's no way to find pathological scans.
- **Approach:** log (at warn) any `Find`/`Scan` exceeding a configurable
  threshold with filter shape, rows scanned vs returned, and whether an index
  was used (the planner in `engine/scan.go` already knows). Surface
  `rows_scanned` on the metrics histogram.
- **API/flags:** `--slow-query-ms`.
- **Files:** `engine/scan.go` (return scan stats), `server/grpc.go`,
  `internal/metrics/metrics.go`.
- **Tests:** a full-scan query over the threshold logs once with index-used=false;
  an indexed lookup does not.
- **Acceptance:** an operator can identify unindexed hot queries from logs/metrics.

---

## v0.7.0 — Network/engine API parity + query breadth

The embedded engine is materially more capable than the wire API. Keyed CRUD
(`InsertWithKey`/`FindByKey`/…), `Upsert`, compare-and-swap (`UpdateIfRev` /
`UpdateIfMatch`), per-record `Rev`, and `Count`/`Exists` all exist in the engine
(shipped in v0.2.0, see `CHANGELOG.md`) but have **no gRPC/REST surface**. Network
clients also can't project fields, aggregate, sort by more than one field, or
paginate without O(offset) cost. This milestone closes those gaps — mostly
*exposing* proven engine capability, which keeps risk moderate.

### N1 — surface keyed CRUD, Upsert, CAS & Rev over the wire — **M**
- **Problem:** the richest engine operations are unreachable from any client.
  Network users are stuck with server-assigned `uint64` ids and last-writer-wins
  updates — no optimistic concurrency, no natural keys, no upsert.
- **Approach:** add RPCs `Upsert`, `FindByKey`, `UpdateByKey`, `DeleteByKey`,
  `UpdateIfRev`, and include `key`/`rev` on Insert/Find/Update responses. Map
  straight onto the existing engine methods; return the typed engine errors
  (`ErrDuplicateKey`, `ErrKeyNotFound`) as gRPC status codes
  (`AlreadyExists`/`NotFound`).
- **Proto/API:** new messages + RPCs in `proto/filedb.proto`; add `string key`
  and `uint64 rev` to record-bearing responses. Regenerate stubs.
- **Files:** `proto/filedb.proto`, `server/grpc.go`, `cmd/filedb-cli/`, docs; and
  a follow-on parity pass across the 7 SDKs (one PR each, as with the TTL sweep).
- **Tests:** `grpc_integration_test.go` — upsert insert-then-replace; CAS with a
  stale rev fails cleanly; duplicate key → `AlreadyExists`.
- **Acceptance:** a network client can do a natural-key upsert and an
  optimistic-concurrency update without dropping to the embedded engine.

### N2 — field projection on Find/FindById — **M**
- **Problem:** every read returns the full record; wide documents waste
  bandwidth and CPU when the caller wants two fields.
- **Approach:** add a repeated `fields` projection to `FindRequest` /
  `FindByIdRequest`; the engine filters the decoded `data` map to the requested
  keys before it hits the wire (id/key/rev always included). Empty = full record
  (backward compatible).
- **Proto/API:** `repeated string fields` on the read requests.
- **Files:** `proto/filedb.proto`, `engine/scan.go`, `server/grpc.go`, CLI
  `--fields`, docs, SDK follow-on.
- **Tests:** projection returns only requested fields + id/key/rev; empty =
  full; unknown field is silently absent.
- **Acceptance:** a projected query transmits only the requested fields.

### N3 — keyset (cursor) pagination + multi-field order_by — **M**
- **Problem:** pagination is offset-only (`FindRequest.offset`,
  `proto/filedb.proto:242`) — O(offset) to skip — and `order_by` is a single
  field (`engine/scan.go:28`). Deep pagination and tie-broken ordering are
  expensive/ambiguous.
- **Approach:** add an opaque `page_token` (encodes the last (sort-key, id) seen)
  so the engine can seek past already-returned rows instead of counting; support
  a repeated/typed `order_by` with per-field direction, reusing the typed
  `query.Compare`. Offset stays for compatibility.
- **Proto/API:** `string page_token` on request/response; `repeated OrderBy
  {field, desc}` (deprecate the scalar `order_by`/`order_dir` with a migration
  note — a legitimate minor-bump break per the semver policy).
- **Files:** `proto/filedb.proto`, `engine/scan.go`, `server/grpc.go`, CLI, docs,
  SDK follow-on.
- **Tests:** cursor paginate a large collection with no dupes/gaps under
  concurrent inserts; multi-field sort tie-breaks deterministically.
- **Acceptance:** paging to offset N is O(page), not O(N); stable multi-key sort.

### N4 — aggregations (count / group-by / numeric) — **M/L**
- **Problem:** `Count` exists in the engine but not on the wire, and there is no
  group-by or numeric aggregation — clients pull whole collections to count.
- **Approach:** add an `Aggregate` RPC: `count`, and `group_by(field)` yielding
  per-group `count`/`sum`/`avg`/`min`/`max` over a numeric field, honoring the
  same `Filter`. Streams groups. Uses indexes for grouped-eq where available.
- **Proto/API:** new `Aggregate` RPC + request/response messages.
- **Files:** `proto/filedb.proto`, `engine/aggregate.go` (new), `server/grpc.go`,
  CLI, docs, SDK follow-on.
- **Tests:** count matches `Find` length; group-by sums equal a manual reduction;
  filter is honored.
- **Acceptance:** `count`/`group-by` return correct results without materializing
  the collection client-side.

---

## v0.8.0 — Replication & high availability

The flagship arc. FileDB is single-node: a lost disk or crashed process is an
outage and a potential data loss beyond the last snapshot. The append-only
segment design is *already a write-ahead log*, which makes leader→follower log
shipping the natural HA primitive.

### R1 — leader→follower segment/log replication — **L**
- **Problem:** no redundancy. Durability protects against torn writes, not
  against losing the machine.
- **Approach:** a follower connects to the leader and tails a new server-streaming
  `Replicate` RPC that ships committed segment entries (post-fsync) with a
  monotonic global sequence number; the follower applies them through the normal
  write path (index + secondary indexes maintained identically) and tracks its
  applied LSN. Bootstrap a fresh follower from a `Snapshot` (already exists) then
  catch up via the stream. Async replication first (bounded lag), with an
  acknowledged-LSN channel to enable R2.
- **Proto/API:** new `Replicate` streaming RPC; a `ReplicationStatus` RPC
  (leader LSN, per-follower applied LSN, lag).
- **Files:** `engine/replication.go` (new), `engine/collection.go` (emit
  committed entries with LSN — reuse the Watch fan-out plumbing),
  `proto/filedb.proto`, `server/grpc.go`, `cmd/filedb/main.go` (`--replicate-from`
  follower mode), docs.
- **Tests:** a follower started against a live leader converges to identical
  query results; kill+resume the follower resumes from its LSN without gaps;
  snapshot-bootstrap + tail equals a full replay.
- **Acceptance:** a follower stays consistent with the leader under continuous
  writes and recovers from disconnect without data loss or duplication.

### R2 — read replicas & follower reads — **M**
- **Problem:** all reads hit the single node.
- **Approach:** a follower serves read-only RPCs (Find/FindById/Count/Aggregate)
  from its applied state and rejects writes with a typed `FailedPrecondition`
  ("read-only replica; write to the leader"), optionally reporting its LSN lag so
  clients can bound staleness.
- **Proto/API:** reuse existing read RPCs; writes on a follower return a typed
  error; expose lag via `ReplicationStatus`.
- **Files:** `server/grpc.go` (role-aware routing), `cmd/filedb/main.go`, docs,
  optional SDK helper for "read from any replica".
- **Tests:** a follower answers reads consistent with its LSN; a write to a
  follower is refused with the documented code.
- **Acceptance:** reads scale horizontally across followers with a documented,
  observable staleness bound.

### R3 — manual failover & role management — **M**
- **Problem:** promoting a follower after a leader loss is undefined.
- **Approach:** an admin `Promote` RPC (and CLI) that flips a caught-up follower
  to leader (stops replicating, accepts writes) with a guard that refuses to
  promote a lagging replica beyond a threshold. Document the operator runbook;
  leave automatic leader election (consensus) explicitly out of scope for now.
- **Proto/API:** `Promote` admin RPC (scoped to an admin key from A1/S3).
- **Files:** `engine/replication.go`, `server/grpc.go`, `cmd/filedb-cli/`, docs
  (`docs/operations.md`).
- **Tests:** promote a caught-up follower → it accepts writes; promoting a
  lagging follower is refused unless forced.
- **Acceptance:** a documented, tested manual-failover path exists; promotion
  guards against silent divergence.

---

## v0.9.0 — Security & tenancy depth → v1.0 readiness

The last hardening before freezing the API. Scoped rotatable keys shipped in
v0.5.0 (A1); this milestone rounds out transport auth, auditability,
finer-grained authorization, and resource fairness, then freezes the surface.

### S1 — mutual TLS (client-cert auth) — **S/M**
- **Problem:** TLS is server-auth only; there's no cryptographic client identity.
- **Approach:** optional mTLS — verify client certs against a CA and map the
  cert subject/SAN to a principal (compose with, or as an alternative to, API
  keys). Off by default.
- **Files:** `internal/auth/`, `server/grpc.go`, `cmd/filedb/main.go`, docs.
- **Tests:** a valid client cert authenticates; an untrusted cert is rejected.
- **Acceptance:** clients can authenticate by certificate; server-only TLS still
  works unchanged.

### S2 — audit log — **S**
- **Problem:** no durable record of who did what (create/drop/compact/promote,
  auth failures).
- **Approach:** an append-only audit sink (its own NDJSON stream) recording
  principal, RPC, target, and outcome for mutating and admin RPCs; reuse the O1
  structured-logging plumbing with a dedicated audit logger + `--audit-log` path.
- **Files:** `server/audit.go` (new), `server/grpc.go`, config/flags, docs.
- **Tests:** a mutating RPC and an auth failure each produce exactly one audit
  record with the expected fields.
- **Acceptance:** all mutating/admin actions and auth failures are auditable.

### S3 — per-collection ACLs on scoped keys — **M**
- **Problem:** scopes are global read/read-write (A1); a key cannot be restricted
  to specific collections.
- **Approach:** extend the key schema with an optional collection allow/deny
  list; the auth interceptor enforces it per RPC using the collection argument.
  Absent list = all collections (backward compatible).
- **Files:** `internal/auth/`, `server/config.go`, docs.
- **Tests:** a collection-scoped key is allowed on its collection and denied
  (`PermissionDenied`) elsewhere; reload picks up ACL changes.
- **Acceptance:** a key can be confined to a named set of collections.

### S4 — per-tenant quotas & limits — **M**
- **Problem:** no guard on a single key/collection consuming unbounded disk or
  record count.
- **Approach:** optional per-key/per-collection limits (max records, max bytes)
  enforced on the write path with a typed `ResourceExhausted`; surface usage via
  stats/metrics.
- **Files:** `engine/collection.go` (write-path check via a config hook),
  `server/config.go`, `internal/metrics/`, docs.
- **Tests:** writes past a quota are refused; usage metric tracks consumption.
- **Acceptance:** a tenant cannot exceed its configured storage/record budget.

### → v1.0.0 — freeze
- Audit the gRPC/REST surface and the embedding API; settle any last renames.
- Promise "no breaking changes without a major bump" (per `CHANGELOG.md` policy).
- Final docs pass; publish `v1.0.0`.

---

## Sequencing & delegation

```
v0.6.0  PR-A: O1 (slog) + O2 (health)      (do first — everything else logs/probes)
        PR-B: O3 (backpressure)
        PR-C: O4 (tracing)  ,  PR-D: O5 (slow-query log)
v0.7.0  Agent-1: N1 (keyed/CAS/upsert wire) + 7-SDK parity follow-on
        PR-E:    N2 (projection)
        PR-F:    N3 (cursor pagination + multi-sort)
        Agent-2: N4 (aggregations)
v0.8.0  Agent-3: R1 (replication — flagship)
        PR-G:    R2 (read replicas)  ,  PR-H: R3 (failover)
v0.9.0  PR-I: S1 (mTLS) , PR-J: S2 (audit) , PR-K: S3 (ACLs) , PR-L: S4 (quotas)
        then v1.0.0 freeze
```

Each large item (N1, N4, R1) is a candidate warden development agent in its own
worktree; the rest ship as direct PRs. Every merged item ticks its box below and
updates `ROADMAP.md`, `docs/`, `README.md`, and `CHANGELOG.md` per CLAUDE.md.

---

## Checklist

**v0.6.0 — Operability & observability**
- [x] O1 — structured, leveled logging (`slog`)
- [x] O2 — health & readiness (gRPC health + REST probes)
- [x] O3 — request backpressure & limits
- [x] O4 — OpenTelemetry tracing (opt-in)
- [x] O5 — slow-query log & richer stats

**v0.7.0 — Network/engine API parity + query breadth**
- [x] N1 — keyed CRUD, Upsert, CAS & Rev over the wire
- [x] N2 — field projection on Find/FindById
- [x] N3 — keyset (cursor) pagination + multi-field order_by
- [x] N4 — aggregations (count / group-by / numeric)

**v0.8.0 — Replication & HA**
- [x] R1 — leader→follower segment/log replication
- [x] R2 — read replicas & follower reads
- [x] R3 — manual failover & role management

**v0.9.0 — Security & tenancy depth**
- [x] S1 — mutual TLS (client-cert auth)
- [x] S2 — audit log
- [x] S3 — per-collection ACLs on scoped keys
- [x] S4 — per-tenant quotas & limits
- [ ] v1.0.0 — surface freeze
