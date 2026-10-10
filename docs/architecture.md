# ScrivaDB — Architecture

## Overview

ScrivaDB is a lightweight, append-only, file-based document database written in Go. It exposes a gRPC API (with a REST gateway) and stores data as human-readable NDJSON files on disk.

---

## Storage Model

### One directory per collection

```
data/
└── users/
    ├── seg_000001.ndjson   # sealed (immutable)
    ├── seg_000002.ndjson   # sealed
    ├── seg_000003.ndjson   # active (append target)
    ├── index.json          # persisted id → {segment, offset} map
    ├── meta.json           # id counter, created_at
    └── sidx_email.json     # secondary index for "email" field (if created)
```

### Segment files (NDJSON)

Each line is one operation entry:

```json
{"id":1,"op":"insert","ts":"2026-03-29T10:00:00Z","data":{"userName":"admin"},"crc":2872375771}
{"id":1,"op":"update","ts":"2026-03-29T11:00:00Z","data":{"userName":"admin2"},"crc":1483902337}
{"id":2,"op":"delete","ts":"2026-03-29T12:00:00Z","crc":1032541209}
```

- `op` is one of `insert`, `update`, `delete`
- For `delete`, `data` is omitted (tombstone entry)
- The **latest entry for each id wins**

A segment is **sealed** (made immutable) when its file size exceeds `SegmentMaxSize` (default 4 MiB). After sealing a new active segment is created.

#### Record-size ceiling

All three read paths (`ReadAt`, `ScanAll`, `ScanFrom`) scan segment lines with a shared 16 MiB buffer, so a single record — one NDJSON line including its trailing newline — cannot exceed **16 MiB**. `Append` enforces this bound at write time: a record whose encoded form is larger is rejected with `engine.ErrRecordTooLarge` (surfaced as `INVALID_ARGUMENT` over gRPC) rather than being written to disk where no read path could scan it back. In practice the gRPC surface is bounded well below this by the transport's 4 MiB max-message size; the ceiling matters most for the embedded façade and internal re-append paths (compaction, replication).

#### Per-entry checksums

Every entry carries a `crc` field: a **CRC32C (Castagnoli)** checksum computed over the entry's `id`, `op`, and canonical `data` (the timestamp and the `crc` field itself are excluded, so the value is stable across encode/decode). It is written on `Encode` and verified on `Decode`.

This guards against silent bit-rot in sealed segments: without it, a flipped byte that still parses as JSON would return wrong data with no error. A checksum mismatch instead surfaces as a typed `store.ErrCorruptEntry`, which propagates out of `ScanAll`/`ReadAt` with the segment path and offset.

The field is **backward-compatible**: an entry with no `crc` key (a line written before checksums existed) is decoded without verification. Because compaction rewrites entries through `Encode`, legacy lines gain a checksum the next time their segment is compacted.

---

## Write Path

```
client request
    │
    ▼
Collection.Insert / Update / Delete
    │
    ├── acquire write lock (sync.RWMutex)
    ├── append NDJSON entry to active segment (sequential write)
    ├── update in-memory primary index
    ├── update in-memory secondary indexes (all indexed fields)
    ├── release write lock
    │
    ├── if segment size ≥ limit → rotate (seal + new active)
    └── emit WatchEvent to subscribers
```

Writes are always sequential appends — the fastest possible disk operation.

---

## Quotas

A collection can carry an optional **write-path budget** — a maximum live-record
count (`MaxRecords`) and/or a maximum on-disk size in bytes (`MaxBytes`), either
`0` for unlimited. Enforcement lives **in the engine**, on the write path,
because that is the only layer that (a) holds the collection write lock and (b)
sees live usage without a round-trip: `MaxRecords` compares against the in-memory
primary index length, and `MaxBytes` against the summed segment size — the same
figures `Stats()` exposes. Doing it at the server would race concurrent writers
and duplicate the size bookkeeping the engine already maintains.

The check runs under the write lock, **before** the durable append, so a refused
write appends nothing and mutates no index. A breach returns the typed
`engine.ErrResourceExhausted`, which the server maps to gRPC `ResourceExhausted`
(HTTP `429`). The engine stays dependency-free: the rejection counter is fed by a
server-supplied observer hook (`WithQuotaObserver`), never a metrics import in
the engine — the same pattern as the compaction/scan hooks.

Quotas **gate the creation of new records only.** The record-adding paths
(`Insert`, `InsertMany`, `InsertWithKey`, an inserting `Upsert`, and transaction
inserts) are checked; an in-place `Update`, a compare-and-swap, an `Upsert` that
replaces, and a `Delete` are not — blocking them would trap a tenant that needs
to edit or delete to get back under budget. Batch paths (`InsertMany`,
`CommitTx`) evaluate the whole batch against the budget under a single lock hold,
so a batch that would breach is rejected atomically with nothing written; this is
what makes `InsertMany` a genuinely atomic engine operation rather than a
server-side loop. When a byte budget is set, the per-entry encoded size is
measured for the check; an unlimited collection skips that work entirely, so the
hot path pays nothing.

Per-collection budgets are supplied two ways that converge on the same
`CollectionConfig` fields: the embedded façade sets `MaxRecords`/`MaxBytes`
directly (`scriva.WithMaxRecords`/`WithMaxBytes`), while the server passes a
DB-wide `Quotas` name→budget map that `OpenCollection` overlays onto each
collection by name. Per-**key** quotas are out of scope — the engine has no key
identity on the write path.

---

## Read Path

```
FindById:
    acquire read lock → primary index lookup → seek to offset → read one line → decode

Find (scan) without index:
    stream segments in insertion order → skip stale/deleted versions via the
    primary index → apply filter → order/paginate → stream results

Find (scan) with secondary index (single eq or range filter on indexed field):
    secondary index lookup → fetch candidate ids via primary index → filter → stream
```

The in-memory primary index makes `FindById` an O(1) index lookup + one disk seek. A secondary index on a field reduces `Find` with a single equality filter from O(n) to O(1), and a single range filter (`gt`/`gte`/`lt`/`lte`) from O(n) to O(matches) via the index's ordered key view.

### Streaming, push-down `Find`

`Find` is served by the engine's `ScanStream`, which emits matches to the gRPC
stream *as it reads* rather than buffering the whole result set. It never builds
an in-memory `map[id]entry` of the collection: instead it streams each segment
sequentially and treats an entry as live only when the primary index still
points at exactly its `(segment, offset)`, skipping superseded versions and
tombstones. This keeps the deduplication cost off the heap.

`limit`, `offset`, `order_by_fields`, and the `page_token` cursor are pushed
**into** the engine so their cost is paid before materialization:

| Query shape | Rows examined | Memory held |
|---|---|---|
| unordered, `limit > 0` | stops after `offset+limit` matches | O(`offset+limit`) |
| ordered, `limit > 0` | all candidates (needs full comparison) | O(`offset+limit`) — bounded **top-K** buffer |
| ordered, no limit | all candidates | O(matches) — an inherent full sort |

So `Find … limit 10` over a million-row collection reads and holds ~10 rows, not
a million. A `gt`/`lt` predicate on an indexed field is likewise served from the
index's ordered key view (see [Secondary Indexes](#secondary-indexes)); ordering
by a non-indexed field still examines every candidate.

**Ordering guarantees.** `order_by_fields` is a list of `{field, desc}` sort keys
(`ScanOptions.Sort` in the engine) applied lexicographically — the first field is
dominant, and each field carries its own direction. Every comparison uses
`query.Compare` — the *same* type-aware comparison the `gt`/`gte`/`lt`/`lte`
filter operators use, so a sort and a filter never disagree about how two values
relate. Numbers order numerically (`2` before `10`, not the lexical `"10"` before
`"2"`) and strings order lexically; mixed types degrade to a deterministic string
comparison. The record **`id` is always the implicit final tiebreaker** (ascending),
so the ordering is *total*: results are deterministic, a bounded top-K agrees with a
full sort, and — crucially — a keyset cursor is unambiguous. The deprecated scalar
`order_by`/`descending` is promoted to a single-element `Sort` when
`order_by_fields` is empty, so the two share one code path. Without any ordering,
results are returned in insertion (id) order.

**Keyset (cursor) pagination.** `offset` skips rows by *counting* past them —
O(offset). A `page_token` instead lets the engine *seek* past the rows already
returned — O(page) regardless of depth. The mechanics:

- The token is an opaque, URL-safe **base64 of compact JSON** — `{"k":[…sort-key
  values…],"i":<id>}` — encoding the `(sort-key tuple, id)` of the last row emitted
  on the previous page. The codec (`encodeCursor`/`decodeCursor` in `engine/scan.go`)
  is defined entirely in the engine with **no grpc/proto dependency**, so the
  embeddable engine keeps its zero transport imports; JSON round-trips numbers as
  `float64`, which `query.Compare` treats identically to the numeric types decoded
  from a segment.
- On a paginated scan the engine rebuilds a synthetic boundary `ScanResult` from the
  token and keeps only rows that sort **strictly after** it under the very same
  `sortLess` used for ordering. Because that order is total (id tiebreak), "strictly
  after" excludes exactly the boundary row and nothing else — so no row is skipped or
  re-emitted, even across concurrent inserts. Concatenated pages therefore cover every
  matching row **exactly once, no duplicates and no gaps**.
- After emitting a page, the engine sets `ScanStats.NextPageToken` **only when the
  limit truncated the result** (there were more matching rows than `offset+limit`),
  encoding the last emitted row. The server rides this token on the **final streamed
  `FindResponse`** (buffering one record so the last message can carry it) — no extra
  record-less message that would break an older client. An empty token means the last
  page was reached. A malformed token, or one whose key count disagrees with the
  ordering, surfaces as `engine.ErrInvalidPageToken` → gRPC `InvalidArgument`.

A cursor requires an ordering (`order_by_fields` or the deprecated scalar); pass the
same ordering, filter, and limit on every page, with `offset = 0`.

**Cancellation.** `ScanStream` threads the request `context` and checks it
between segments and records, so a client that cancels a long `Find` (or
disconnects) stops server-side work promptly instead of scanning to completion.

**Field projection.** `FindRequest.fields` (and `FindByIdRequest` /
`FindByKeyRequest`) carry an optional projection: a list of top-level field
names to return. It is passed to the engine as `ScanOptions.Fields`, and
`ScanStream` applies it via the exported `engine.ProjectData` helper **after**
filtering and ordering and just before each record is yielded to the server —
so it narrows only what crosses the wire, never what the filter or `order_by`
see (an `order_by` field need not be projected). The point-lookup handlers
(`FindById` / `FindByKey`) call the same helper on the fetched record. The rules
live in one place, `engine.ProjectData`:

- An **empty** projection returns the record's data unchanged (full record —
  backward compatible), never copying the map.
- A non-empty projection builds a fresh map holding only the requested keys that
  exist; an unknown key is silently skipped, and the input map is never mutated.
- The reserved `_key` field is always retained so a record's caller-supplied
  string `key` survives projection. `id` and `rev` live outside the data map, so
  they are inherently unaffected — `id`, `key`, and `rev` are always returned.

### Slow-query log & scan stats

`ScanStream` returns a plain `engine.ScanStats` value alongside its error,
describing the cost of the scan it just ran:

| Field | Meaning |
|---|---|
| `RowsScanned` | live records examined against the filter |
| `RowsReturned` | records emitted to the caller |
| `IndexUsed` | whether a secondary index produced the candidate set |

The engine already decides index-vs-scan in `forEachMatch` — an eq or range
predicate on an indexed field walks the index's candidate ids (`IndexUsed =
true`, `RowsScanned` counts only those candidates), and everything else streams
the segments (`IndexUsed = false`, `RowsScanned` counts every live record the
filter is tested against). This change simply *surfaces* that decision as data;
`ScanStats` carries no `slog`, `grpc`, or `prometheus` types, so the embeddable
engine gains **no** dependency (`make deps-check` enforces it — the same
discipline as `OnCompaction` for metrics and the request logger).

The **server layer** turns the stats into two operator signals in
`GRPCServer.Find` (`server/grpc.go`), both injected at construction as optional
hooks so the default and embedded paths add nothing:

1. **Slow-query log.** When `--slow-query-ms > 0` and a `Find` reaches that
   wall-clock duration, one record is logged at `WARN` on the shared `slog`
   logger: the `collection`, the `filter` **shape** (fields and operators only,
   never the compared values — rendered by `filterShape`), `rows_scanned`,
   `rows_returned`, `index_used`, and `duration`. Logging the shape rather than
   the values makes the line safe to aggregate and keeps record data out of the
   logs.
2. **Rows-scanned metric.** A `scriva_scan_rows_scanned` Prometheus histogram
   (labelled by `collection`) records `RowsScanned` for every `Find`, via a
   `WithScanObserver` hook that calls `metrics.ObserveScan`. As with compaction,
   the engine never references the metrics package — the server owns the
   instrument and feeds it through the hook.

Together an operator can spot an unindexed hot query two ways: a `WARN` line
showing `index_used=false` with `rows_scanned ≫ rows_returned`, or a rising
`scriva_scan_rows_scanned` histogram for a collection. The fix — an index on the
filtered field — flips `index_used` to `true` and collapses `rows_scanned`.

---

## Encryption at rest

Encryption at rest is transparent and lives entirely at the **collection
boundary** of the embedded engine. The core invariant is that ciphertext lives
only inside the stored `data` map: everything downstream — segment files,
compaction, the primary and secondary indexes, the replication feed, and the
`store` package — stays **key-oblivious**, because it only ever sees already-sealed
bytes. The `crypto/` package is the sole holder of key material.

### Envelope and cipher

Each encrypted value is an AEAD envelope
`<marker>:v1:<key-id>:<base64url(nonce‖ciphertext+tag)>`, where `<marker>` is a
reserved leading-NUL sentinel. The cipher is XChaCha20-Poly1305 (192-bit random
nonce, so per-write nonce collision is not a concern); the collection name, field
name, and record key are bound in as additional authenticated data, so a blob
cannot be silently relocated to another field or record. A user value that begins
with the reserved marker is rejected on write (`ErrReservedPrefix`), keeping the
marker an infallible discriminator: reads are **policy-independent**, driven only
by whether a value carries the marker, so a half-migrated collection still decodes
correctly.

### Two modes

- **Field-level** (`EncryptFields`) — a deny-list of top-level fields is sealed in
  place; every other field stays plaintext and queryable.
- **Record-level** (`EncryptRecord`) — the whole record is sealed into a single
  reserved `secure_data` blob, keeping only an allow-list of index fields (and the
  reserved `_key`) plaintext.

The reconstruction (**read**) mode is fixed when encryption is first enabled and
never changes for the life of the collection, so records written under any earlier
write policy still decode. The **write** policy is mutable (an empty policy
disables sealing while reads still reconstruct residual ciphertext). The encryptor
is immutable and swapped atomically on a policy change, so lock-free readers always
see a consistent snapshot.

### Where it hooks in

`sealEntryData` runs on every write path (Insert/InsertMany/Update/Upsert/CommitTx/
CAS) before the entry is appended, so nothing is written if sealing fails.
Decryption is lazy and **fail-closed** at the read/yield boundary (`Get`,
`ScanStream`): a wrong or missing key surfaces as a typed error
(`ErrKeyUnavailable` / `ErrDecryptFailed` / `ErrWrongEncryptionKey`) rather than
garbage. Because encrypted fields are opaque on disk, indexing, filtering, sorting,
or aggregating on them is rejected at the `forEachMatch` choke point with
`ErrFieldEncrypted`.

### Keys, wrong-key detection, and meta.json

Keys come from a `crypto.KeyProvider` — the built-in `Keyring` (behind
`WithEncryptionKey` / `WithPassphrase`) or a caller-supplied provider
(`WithKeyProvider`). The `encryption` block of `meta.json` records the write
policy, read mode, policy epoch, the passphrase KDF parameters + salt (non-secret),
a `key_check` (an AEAD-sealed known constant), and the current key id. On `Open`,
the `key_check` is verified under the supplied key so a wrong key/passphrase fails
fast. None of this block is secret — it reveals nothing without the key.

### Migration: epochs, functional vs security completion

Every policy change (enable/disable, field add/remove, key rotation) bumps a
monotonic **epoch**, stamped onto each written entry and mirrored onto its index
entry. Existing records migrate two ways: **lazily**, as an update rewrites them
under the current policy, or in **bulk** via a re-encrypting compaction pass
(`MigrateNow` seals the active segment, then forces a compaction that decrypts each
entry below the current epoch and re-encrypts it under the current key — all before
the manifest is written, so a retired-key decrypt failure aborts the pass with
nothing mutated). Two notions of "done":

- **Functional completion** — every *live* record is at the current epoch, so reads
  behave exactly per the current policy. It is judged from an in-memory index walk
  (`EncryptionStatus`), with no segment reads.
- **Security completion** — no stale old-form bytes remain at rest (superseded rows
  can still hold them until reclaimed). It requires a completed compaction pass, and
  is the bar for retiring an old key or indexing a de-encrypted field.

Because segments already hold ciphertext, **backups and replication carry encrypted
data for free** (see [Backup / snapshot](#backup--snapshot)); the key must be backed
up separately, or the backup is unrecoverable. The full design rationale is in
[encryption-at-rest.md](encryption-at-rest.md).

---

## In-Memory Primary Index

```
map[uint64]IndexEntry{
    SegmentPath string
    Offset      int64
}
```

- Updated on every write (same write lock scope)
- Persisted to `index.json` with a SHA-256 checksum on every close, after every compaction, when a segment rotation *requests* one, and by a background goroutine every `--index-persist-interval` (default 30s; negative disables the timer). Each persist fsyncs the active segment first so coverage never claims bytes a crash could lose, and secondary indexes (`sidx_*.json`) are written in the same pass. Rotation-requested persists are debounced: they coalesce and run no more often than `CollectionConfig.IndexPersistMinInterval` (default 10s) nor more than once per 4x the previous pass's duration, so a write burst never re-encodes the whole index per 4 MiB segment; Close and compaction always persist. Sealed-segment coverage is memoized and cheap (below), so the work done under the read lock is bounded by the active segment size. All writers of these files serialize on one mutex, and `Close()` stops and waits for the background persister before writing the final index.
- Format v2 is self-describing (`"version": 2`): segment paths are stored relative to the collection directory (so a data dir can be moved without a rebuild), and a `coverage` list records, per segment, the byte count covered and a fingerprint of those bytes: every segment, sealed or active, records the full SHA-256 of its covered prefix, and open re-reads and re-hashes every covered prefix before trusting the primary or any secondary index (one read per segment, shared by all indexes of the collection; hashes of sealed segments are memoized so a steady-state persist does not re-read them). A bounded fingerprint is deliberately not accepted: a same-size edit of an early record in a sealed segment would keep its id, offset and size, pass the identity spot-check, and leave a secondary index silently stale. Index files from earlier builds that carry only the 64 KiB `tail` fingerprint prove nothing about the prefix; they force one rebuild and are rewritten with full checksums. A length or checksum mismatch forces a rebuild; the identity spot-check still runs as an extra guard. Cost: clean open and verify are O(covered bytes) in hashing. The checksum covers version, entries and coverage and is computed over the exact bytes written, so `Load` verifies it without re-marshalling the index. Legacy v1 files (absolute paths, no coverage) still load; their paths are re-rooted at the collection directory.
- Loaded on startup and **validated against the segments, never trusted** (see below); rebuilt from segment scans if the checksum fails
- Rebuilt after compaction (offsets change)
- Compaction is deterministic (resolved records are written in id order), reuses the replaced segments' names in order (aborting before the swap if the output would need more segments than the input), discards stale `.compact_*` temps before each pass, fsyncs the renames before unlinking old segments, and rebuilds + snapshots the secondary indexes under the same write lock as the swap so no concurrent write can be lost. A full scan holds a shared layout lease while walking its segment snapshot; a compaction probes that lease before creating its manifest and defers with a bounded backoff when scans are active, so it never queues a writer that blocks later scans. Primary and secondary indexes are then persisted with v2 coverage of the new layout before the swap manifest is retired. A swap that fails after the first rename blocks further passes until reopen, where `recoverCompaction` rolls it forward.

### Load-time validation and tail replay

After an unclean stop the persisted `index.json` is checksum-valid but stale (it was written by the last clean close or compaction). On open the engine reconciles it with the segments using its v2 `coverage`, ordering segments by **numeric** id (not lexical name):

| Condition | Action |
|---|---|
| every covered segment exists, its first `size` bytes hash to the recorded SHA-256, and only the newest covered segment grew | replay only `[covered, EOF)` of that segment onto the loaded index |
| unlisted segments newer than all covered ones (rotation after the last persist) | replay them in full |
| v1 file (no coverage), corrupt/truncated file, missing or shorter covered segment, hash mismatch, a non-newest covered segment grew, an unlisted segment older than the covered range | full rebuild from all segments |
| a spot check fails: up to 64 random plus the 16 newest entries must each point at a line boundary whose decoded `id` matches | full rebuild |

Secondary indexes (`sidx_<field>.json`) follow the same protocol independently: each carries its own v2 coverage and checksum, is replayed from the covered tail when only the newest segment grew, and is rebuilt from the segments when its file is missing, corrupt, v1, or its coverage disagrees (`Collection.IndexRecoveryStats()` counts `SecondaryReplays` / `SecondaryRebuilds`). A primary rebuild always forces a secondary rebuild. Persisted index paths are relative to the collection directory, so a data directory may be moved or restored elsewhere without triggering a rebuild.

Replay and `Rebuild` share one routine (`applyEntries`), so insert/update (rev bump, last-writer-wins) and delete (entry removed — deletes are not resurrected) behave identically. A torn last line in the active segment is trimmed by `recoverPartialLine` before replay. The recovered index is persisted and the secondary indexes are rebuilt from the segments whenever the primary changed. Cost: validating coverage hashes the covered bytes (sequential read), far cheaper than a full decode-and-rebuild; replay cost is bounded by the unpersisted tail.

Recovery is observable: `Collection.IndexRecoveryStats()` exposes replay/rebuild/spot-check-failure counters and replayed bytes, and `CollectionConfig.OnIndexRecovery(collection, kind, bytes, dur)` fires with kind `replay`, `rebuild` or `spotcheck_fail` (not at all on a clean reopen).

### Identity verification & integrity protection

When retrieving a record via an indexed offset (`Get`/`getStored`, `GetByKey`, or CAS), the engine verifies that the decoded entry's `id` strictly matches the requested/indexed `id` after reading the segment. If the offset is corrupted or points to a different entry, the engine returns a typed `*IntegrityError` (which wraps `engine.ErrIndexCorrupt` and details `ID`, `FoundID`, `SegmentPath`, and `Offset`). This ensures valid JSON at the wrong physical location never returns another record as a false match.

During scans (`ScanStream` and `streamLive`), if an entry in a segment is encountered whose physical offset disagrees with the primary index, the engine does not blindly skip it as stale: it verifies whether the index's target location points to a legitimate newer version of that record. If the target entry does not exist, fails to decode, or has a mismatched ID, the engine surfaces the integrity error immediately instead of silently shortening scan results.

At the network layer (`server/grpc.go`), `ErrIndexCorrupt` is mapped to `codes.DataLoss` rather than `NotFound` across `FindById`, `FindByKey`, `Find`, and `Aggregate`.

---

### Integrity verification (`Verify`)

`engine/verify.go` is a read-only detector. It reports what is wrong and where; it never repairs, trims, resolves or rewrites anything (repair, the CLI surface and the open-time policy are separate layers built on its report).

| Entry point | Runs against | Notes |
|---|---|---|
| `(*DB).Verify(ctx, VerifyOptions)` | every open collection (or `VerifyOptions.Collections`) | online |
| `(*Collection).Verify(ctx, VerifyOptions)` | one open collection | online |
| `engine.VerifyDir(ctx, dataDir, VerifyOptions)` | a directory that is **not** opened | offline; also validates the persisted `index.json` / `sidx_*.json` / `meta.json` exactly as they sit on disk, without running open-time recovery |

All three return an `*IntegrityReport`: database-level `Findings` (lock state) plus one `CollectionReport` per collection (`Stats`, `Findings`, and `Truncated` counts once `MaxFindingsPerCode`, default 1000, is hit). Helpers: `AllFindings()`, `MaxSeverity()`, `Clean()` (nothing above `info`), `Has(code)`, `Codes()`. Each `Finding` carries a `Severity`, a stable machine-readable `Code`, a `Location` (`Segment` base name, `Offset`, `ID`, `Field`) and a human `Message`.

**Online snapshot.** An online run holds `compactMu` for its whole duration (compaction is the only thing that replaces sealed files) and, under `c.mu.RLock`, copies the primary index, every secondary index's buckets, the id counter and each segment's size. Every writer mutates segment + indexes inside one `c.mu.Lock` section, so that copy is a consistent cut. The lock is then released and the segments are scanned only up to the snapshotted sizes — appends extend files past them and are never seen, so writers are not blocked and a concurrent append can never look like a torn line. `ensureIndex` registers a new secondary index under `c.mu` too, so a snapshot never observes an index that is registered but not yet built.

**Ground truth.** In `full` mode every segment is scanned with the tolerant salvage scanner (`scanSegmentTolerantLimit`) and folded, segment by segment, into a per-id replay that applies exactly the `Index.Rebuild` semantics (last line wins, revision = max(replay count, revision on the line), delete resets). Memory is proportional to the number of ids and entries, not to the data size. The primary and secondary indexes are then compared against that truth. History that a single in-order writer cannot produce is reported as a `conflict` and never resolved.

**Interrupted compaction.** If `compact.manifest` is present, an offline run reads the layout that open will roll forward to (each outstanding temp stands in for its final segment, listed removals are ignored) so superseded history sitting next to its compacted form is not misread as conflicting writers. The files themselves are not touched.

**Modes.** `quick` validates persisted-index coverage and fingerprints the same way open does, spot-checks a deterministic sample of index entries (the 16 newest ids plus up to 64 evenly spaced), checks the file set, the id counter and parses only the newest segment. `full` (the default) additionally parses every segment and runs the truth comparison.

Severities, in increasing order: `info` (expected or self-healing at open), `repairable-index` (a derived structure disagrees with the segments; rebuilding from segments fixes it without data loss), `data-corruption` (segment bytes are damaged), `conflict` (ambiguous history; must not be auto-resolved).

| Code | Severity | Meaning |
|---|---|---|
| `segment-torn-tail` | info / data-corruption | partial last line of the newest segment (info: open trims it); anywhere else it is corruption |
| `segment-bad-region` | data-corruption | a complete line that is not a valid record |
| `segment-glued-line` | data-corruption | a partial record glued in front of a valid one on the same line |
| `segment-unreadable` | data-corruption | a segment (or the collection directory) cannot be read |
| `orphan-segment-file` | info | a `seg_*` file the naming scheme does not reach, a `seg_*.ndjson` with no number (open still replays it), or — online — a segment on disk the open handle does not hold |
| `leftover-temp-file` | info | `.compact_*` not named by a manifest, or a `*.tmp` (offline only) |
| `compaction-manifest-pending` | repairable-index | an interrupted swap; open rolls it forward and rebuilds |
| `compaction-manifest-corrupt` | data-corruption | manifest unparseable; open refuses to guess |
| `lock-held` | info | another holder has the directory `LOCK` (offline run) |
| `lock-held-by-this-process` | info | always present on `DB.Verify` |
| `lock-probe-failed` | info | lock state could not be determined |
| `meta-missing` | info | open recomputes the id counter |
| `meta-unreadable` | repairable-index | |
| `idcounter-behind` | repairable-index | counter below the highest id present (after the active-segment reconciliation open performs) |
| `record-expired-live` | info | past its TTL, not yet reaped |
| `duplicate-record-identical` | info | the same write appears twice with identical content and revision |
| `conflict-duplicate-id` | conflict | a live id is inserted again with different content or revision |
| `conflict-id-reuse-after-delete` | conflict | an id is inserted again after its delete |
| `conflict-write-after-delete` | conflict | an id is updated after its delete |
| `conflict-revision-regression` | conflict | an update's revision goes backwards, or two different updates share one revision |
| `index-missing` / `index-unreadable` | repairable-index | `index.json` absent with data present / fails checksum or parse |
| `index-coverage-unknown` | repairable-index | v1 file without coverage |
| `index-coverage-mismatch` | repairable-index | covered bytes shrank, changed, or a non-newest covered segment grew |
| `index-coverage-segment-missing` | repairable-index | covers a segment that is not on disk |
| `index-stale-tail` | repairable-index | bytes appended after the last persist (unclean stop); open replays them |
| `index-segment-unlisted` | repairable-index | a segment older than covered ones is absent from coverage |
| `index-spotcheck-failed` | repairable-index | quick mode: a sampled entry does not point at a record with its id |
| `index-missing-record` | repairable-index | a live record is absent from the index |
| `index-stale-record` | repairable-index | entry points at an older version, or its rev / expiry / epoch differ |
| `index-wrong-record` | repairable-index | the offset holds a record with a different id |
| `index-dangling-offset` | repairable-index | the offset is not the start of any record |
| `index-dangling-entry` | repairable-index | no segment holds any record for the id |
| `index-resurrected-delete` | repairable-index | the id's latest record is a delete |
| `sidx-unreadable`, `sidx-coverage-unknown`, `sidx-coverage-mismatch`, `sidx-coverage-segment-missing`, `sidx-stale-tail`, `sidx-segment-unlisted` | repairable-index | as the `index-*` equivalents, per field (`Location.Field`) |
| `sidx-missing-entry` / `sidx-extra-entry` / `sidx-wrong-bucket` | repairable-index | secondary index disagrees with the record's value |
| `sidx-unique-violation` | conflict | several live records share a value under a unique index |

The engine emits no metrics from `Verify`; callers that want them instrument around the call.

**Guarantees.** `Verify` never writes, trims or locks anything and returns the same findings for the same bytes. Online it is a consistent cut that does not block writers; offline (`VerifyDir`, `scriva verify`) it reflects the directory as it sits on disk, and a directory held open by a live process is reported (`lock-held`) because in-flight writes may show up as findings. A `Clean()` report from a `full` run means every segment line parses, the history is unambiguous and the primary and secondary indexes equal what a rebuild would produce; `quick` gives no such guarantee for bytes before the newest segment's tail beyond the sampled entries.

### Offline repair (`engine.Repair`, `scriva repair`)

`engine/repair.go` repairs what `Verify` reports as `repairable-index`, and nothing else unless asked. The segments are the source of truth and are never edited, except for the three cases below.

1. **Lock, then plan.** `Repair` takes the directory lock first (a directory open elsewhere fails with `ErrDatabaseLocked`, CLI exit 3), verifies in `full` mode, and builds a per-collection plan. `--dry-run` stops here (it likewise refuses a held lock with exit 3).
2. **Backup before the first change.** A byte-for-byte copy (`repair-backup-<UTC time>`, next to the data directory unless `--backup-dir`, which must be outside it) is written and every file re-hashed (SHA-256). A backup that does not verify aborts the run with nothing changed. A `REPAIR_JOURNAL.json` in the data directory pins the backup and the multi-step plan.
3. **Rebuild derived state.** The primary index, secondary indexes and `meta.json` (id counter) are rebuilt atomically (temp → fsync → rename) from a tolerant scan of the segments, with v2 coverage that open accepts without further work. Tombstones stay in the segments and so stay deleted. An interrupted compaction swap is rolled forward exactly as open would.
4. **Segment-level changes** are limited to: trimming an unacknowledged torn tail on the newest segment (as open does); renaming unnumbered `seg_*.ndjson` files to the next free number when their ids overlap no other segment; and, only with `--salvage`, moving the valid records of damaged segments into one new segment (originals are quarantined, never deleted: each is renamed into `<collection>/quarantine/<run>/`, a subdirectory that open, verify and the rebuild do not descend into, after a `MANIFEST.json` with size and SHA-256 is written first; every move is an atomic same-filesystem rename, so an interrupted run leaves each original in exactly one place and a rerun, driven by the journal, finishes the moves and re-checks hashes. The verified backup holds a second copy). The backup directory must resolve outside the data directory with symlinks followed, including through not-yet-existing path components.
5. **Conflicts are never resolved.** Duplicate ids, id reuse after delete, revision regressions and unique violations are listed in the report; `--on-conflict abort` refuses before the backup is taken. Salvage is refused for a collection with conflicts, and such a collection is left untouched (`ErrRepairIncomplete`, exit 2); the other collections are still repaired.
6. **Restartable and idempotent.** Re-running after a crash resumes from the journal (`Resumed: true`); re-running on a repaired directory does nothing.

Repair has no open-time hook: the server never repairs by itself. See the [runbook](runbook-index-recovery.md) for the operator sequence.

## Secondary Indexes

Secondary indexes are per-field inverted indexes stored in memory and on disk:

```
map[string]map[string][]uint64
// field → value → []id
```

### Lifecycle

- Created with `EnsureIndex(field)` — idempotent, builds from existing data on first call
- Dropped with `DropIndex(field)` — removes from memory and deletes `sidx_<field>.json`
- Listed with `ListIndexes()`
- Maintained automatically on every Insert / Update / Delete (same write lock scope)
- Persisted to `sidx_<field>.json` with a SHA-256 checksum
- Reloaded on startup; rebuilt from segments if the checksum fails
- **v2 format with coverage**: like `index.json`, a cleanly persisted `sidx_<field>.json` records the segment bytes it describes (segment name, size, SHA-256 of those bytes) inside its checksum. On load the same rules as the primary index apply: the newest covered segment may have grown and newer unlisted segments are replayed onto the buckets (tail replay, last writer wins, unique flag preserved); any other growth, a missing/shorter segment, a hash mismatch, a corrupt file, or a **v1 file (no coverage)** forces a full rebuild, after which the file is rewritten as v2. `IndexRecoveryStats` exposes `SecondaryReplays`/`SecondaryRebuilds`.
- Rebuilt transparently after each compaction run

### Range queries (ordered key view)

Alongside the hash buckets, each index keeps its **distinct keys in a sorted
slice** together with the field's value *kind* (numeric, string, or mixed),
maintained incrementally as records are inserted, updated, and deleted:

- A range predicate (`gt`/`gte`/`lt`/`lte`) binary-searches the sorted view for
  the matching key window and unions those buckets' ids — reading O(log k + matches)
  instead of scanning the whole collection.
- Ordering is the type-aware `query.Compare` (numbers numerically, strings
  lexically), so `age > 9` matches `10` rather than falling for the lexical
  `"10" < "9"`.
- The candidate ids are then re-validated against the primary index and the
  filter, so results are **identical** to a full scan.
- The sorted view is only kept while the field is homogeneous. If a field mixes
  numbers and strings its range ordering is undefined, so the index reports it
  cannot serve the range and the query falls back to a full scan (eq lookups
  still work). A range whose bound type differs from the indexed values falls
  back the same way.

The kind is persisted in `sidx_<field>.json` (outside the checksum, so old files
stay valid) and the sorted view is rebuilt from the buckets on load and after
compaction.

### Query acceleration

`Scan` uses the secondary index when the filter is a single `eq` **or a single
range** (`gt`/`gte`/`lt`/`lte`) on an indexed field. All other filter shapes
(composite filters, `contains`/`regex`, non-indexed fields) fall back to a full
segment scan.

### Unique indexes

`EnsureUniqueIndex(field)` creates a secondary index that additionally enforces
a uniqueness constraint: any insert or update that would map the indexed value
to a *different* live record is rejected with the typed `ErrDuplicateKey`
(wrapped with field/value context). The check is performed under the same write
lock as the index mutation, so it is atomic — a rejected write appends nothing
to the segment and mutates no index. `CommitTx` pre-validates every staged op
(against committed data and against other ops in the same batch) before applying
any of them.

The `unique` flag is persisted in `sidx_<field>.json` and restored on reload —
including when the file is stale and the buckets are rebuilt from segments.
Uniqueness is enforced on new writes going forward only; historical duplicates
already present in the data (resolved last-write-wins during a rebuild) are
tolerated, not rejected retroactively.

### Caller-supplied string keys

The engine assigns every record a monotonic `uint64` id, but callers often have
their own string identifier (a session id, a name, a context key). Rather than
generalise the primary index to strings — which would touch every offset and
tombstone path — string keys are layered on top of the unique-index machinery:

- A reserved data field, `_key` (`engine.KeyField`), holds the caller's key.
  Plain `Insert`/`Update` **reject** data that sets `_key` directly with the
  typed `ErrReservedField`; it is settable only through the keyed API.
- `InsertWithKey(key, data)` stamps `data["_key"] = key`, lazily ensures a
  **unique** secondary index on `_key` exists (created on the first keyed write
  to a collection), and inserts. A key already held by a live record is rejected
  with `ErrDuplicateKey`.
- `FindByKey`, `UpdateByKey`, and `DeleteByKey` resolve the key to its `uint64`
  id via `IndexLookup("_key", key)` — an O(1) index hit — and then reuse the
  existing id-based path. A key with no live record yields `ErrKeyNotFound`.
  `UpdateByKey` re-stamps `_key`, so a record's key is fixed for its lifetime.

Because `_key` is an ordinary field inside `data`, keyed records need no special
handling anywhere else: they survive segment rotation, compaction, index
rebuild, and reopen exactly like any other record, and their key is visible in
`WatchEvent.Data` for free. The `uint64` id, primary index, `WatchEvent`, and
`CommitTx` are all unchanged.

### Revisions and compare-and-swap

Each record carries an explicit monotonic revision, `rev`: `1` on insert, `+1`
on every update. It is stored on both the segment entry (`store.Entry.Rev`,
`json:"rev,omitempty"`) and the in-memory `IndexEntry`, so the current revision
is readable without a segment read. `rev` is a real field, deliberately **not**
derived from the timestamp (`Ts` is not collision-proof).

Backward compatibility is by construction: `rev` is omitted when zero, so a
segment line or `index.json` written before revisions existed decodes as rev 0
and still verifies its checksum (the CRC folds in `rev` only when non-zero).
Durability of the value across the engine's existing paths:

- **Update** reads the current `IndexEntry.Rev` under the write lock and writes
  `rev+1`; **insert** writes `rev 1`.
- **Rebuild** recomputes revisions by replay order — counting the surviving
  writes per id — but never below a revision already recorded in an entry, so a
  compacted record keeps its true rev instead of resetting to 1.
- **Compaction** preserves the latest entry's `rev` (the collapsed line carries
  it), and the post-compaction rebuild honours it via the rule above.

Two conditional-update primitives build on the revision, both executed under a
single `c.mu.Lock` critical section so the read-check-write is atomic against
every other writer — the direct, lock-free-to-the-caller CAS the embedded
consumer needs:

- `UpdateIfRev(key, expectedRev, data)` applies only if the record's current
  revision equals `expectedRev` (optimistic concurrency).
- `UpdateIfMatch(key, pred, data)` applies only if `pred(currentData)` holds
  (value-based CAS).

Both return `(applied bool, err error)`. A stale revision, a false predicate, or
a missing key is a clean `(false, nil)` no-op — never an error. On success the
revision bumps and a normal update `WatchEvent` is emitted; the string key is
preserved. Reads expose the revision through a `Record{ID, Key, Rev, Ts, Data}`
struct returned by `Get`/`GetByKey`, and through `ScanResult.Rev`.

### Key-based upsert

`Upsert(key, data)` is a create-or-replace on a string key, for the many
call sites that would otherwise do a get-then-branch (e.g. archiving a record to
its final state). It runs the whole decision in one `c.mu.Lock` critical
section, so concurrent upserts on the same key serialise cleanly with no lost
updates:

- The `_key` index is looked up under the write lock. If a live record carries
  the key, the upsert appends an **update** (`rev+1`, preserving the id); if not,
  it appends an **insert** (`rev 1`, a freshly assigned id) — the same revision
  convention `InsertWithKey`/`UpdateByKey` follow.
- The key is stamped into `_key` either way, so supplying `_key` inside `data` is
  rejected with `ErrReservedField`, and unique indexes on other fields are still
  enforced before the append.
- It returns the resulting `Record{ID, Key, Rev, Ts, Data}` and emits the
  matching `WatchEvent` — `OpInsert` for a create, `OpUpdate` for a replace.

Because a replace is an ordinary update entry, the stale versions collapse on
compaction to a single live line, exactly as with `UpdateByKey`.

### Count and existence checks

Dashboards and list views ask "how many?" and "does this key exist?" far more
often than they ask for the rows themselves. `Count` and `Exists` answer those
without materialising the collection:

- `Count(filter)` picks the cheapest path the filter allows:
  - a `nil` or match-all filter returns the primary index length in **O(1)** — no
    segment is read, because the index already tracks exactly the live records;
  - a single `eq` filter on an indexed field returns the size of that value's id
    set from the secondary index (**O(matches)**, still no segment read) — the
    bucket membership is exactly what the filter would accept, so the count is
    scan-identical;
  - any other filter streams live records through the same `forEachMatch` path
    `Scan` uses and increments a counter, so it never buffers a result slice or a
    whole-collection data map. `Count(f)` always equals `len(Scan(f))`.
- `Exists(key)` is a single `IndexLookup("_key", key)` — an **O(1)** in-memory
  hit with no segment read, so it stays flat regardless of collection size. A
  collection that has never taken a keyed write has no `_key` index and reports
  `false` for every key.

### Aggregations (count / group-by / numeric)

`Aggregate` (in `engine/aggregate.go`) computes a `count` and the numeric
aggregations `sum`/`avg`/`min`/`max` over the records matching a filter, optionally
grouped by a field, and **streams one group at a time** — the collection is never
materialised on either side of the wire.

- **Same scan, same index use.** Records are visited through the exact
  `forEachMatch` path `Count`/`Scan` use, so an aggregation over a filter a
  secondary index can serve (an `eq` or a range on an indexed field) walks only the
  indexed candidate set; anything else streams live segments, skipping stale
  versions via the primary index. The filter is therefore honoured identically to
  `Find` — non-matching records simply never reach an accumulator.
- **Bounded by groups, not rows.** Each matching record is folded into its group's
  running accumulator (`count`, running `sum`, `min`/`max`, and a numeric-value
  count) and then discarded; only the per-group accumulators are retained, so peak
  memory scales with the number of **distinct group values**, not the collection
  size. Groups are emitted in ascending key order using the same type-aware
  `query.Compare` the sort and filter use, so the stream is deterministic.
- **Numeric rules match the filter.** A field value contributes to
  `sum`/`avg`/`min`/`max` only when it is numeric under `query.AsNumber` — the same
  numeric-vs-string rule the `gt`/`lt` operators apply — and `avg` divides `sum` by
  that numeric count (SQL `AVG`: absent/non-numeric values are ignored, not zero).
  A record whose field is missing or non-numeric still counts toward `count`.
- **Whole-set count is free.** An ungrouped count with no numeric field short-
  circuits to `Count`, so it is answered straight from the primary/secondary index
  without reading a segment.
- **Embeddable.** `Aggregate` takes an `AggregateSpec` and emits plain
  `GroupResult` structs; the server maps those onto the streamed `AggregateResponse`
  messages (the group key becomes a type-preserving `google.protobuf.Value`). The
  engine imports no grpc/proto, keeping `make deps-check` green.

### TTL / expiring records

A record can carry an expiry **deadline**, after which it is invisible to reads
and reclaimed by compaction. The deadline is stored as a Unix-nanosecond
timestamp on the segment entry (`store.Entry.ExpiresAt`, `json:"expires_at,omitempty"`)
and mirrored onto the in-memory `IndexEntry`, so a read can drop an expired
record without touching disk. Like `rev`, it is folded into the entry CRC only
when non-zero, so a segment line or `index.json` written before TTLs existed
decodes as *never expires* and still verifies — fully backward compatible. A
Unix-nano `int64` (not a `time.Time`) is used precisely so `omitempty` drops it
when unset; a zero `time.Time` struct would still serialise on every line.

Deadlines are set three ways, in precedence order:

- **Explicit per-record** — `InsertWithExpiry(data, when)` /
  `UpdateWithExpiry(id, data, when)` stamp an exact instant. Over the wire these
  surface as `ttl_seconds` (relative) on the `Insert`/`InsertMany`/`Update`
  RPCs; the server converts `now + ttl_seconds` into the absolute deadline the
  engine stores.
- **Per-collection default** — `CreateCollectionWithDefaultTTL(name, ttl)` (RPC
  field `default_ttl_seconds`, CLI `create-collection --default-ttl`) pins a
  default for one collection. It is persisted in that collection's `meta.json`
  (`default_ttl_seconds`) and reloaded on open, so it survives restarts and
  **overrides** the server-wide default for that collection. It is stored
  separately from the inherited global default (a plain collection persists no
  value and keeps tracking the live `--default-ttl`), so changing the global
  later still affects collections that never set their own.
- **Server-wide default** — `CollectionConfig.DefaultTTL` (server
  `--default-ttl`) stamps `now + TTL` on every insert that carries no explicit
  deadline. Zero (the default) means records never expire.

Per-record `ttl_seconds` is rejected inside a transaction (the transaction
staging path does not yet carry a deadline); transaction inserts still honor the
collection/server default.

A plain `Update` keeps a record's existing deadline (**sticky**) — it is a
data-only write, not a TTL refresh; `UpdateWithExpiry` is the way to move the
deadline. Compare-and-swap and transaction updates likewise preserve the
deadline; transaction inserts honor the default TTL.

Two mechanisms make expiry effective:

1. **Defensive read filtering** — `Get` (hence `FindByID`/`GetByKey` and indexed
   scan candidates) and the streaming scan liveness check both drop any record
   whose deadline has passed. This makes a record invisible **the instant** it
   expires, before any background work runs, and independent of clock skew
   between the reaper and the reader.
2. **Reaping + reclamation** — a reaper runs on the compactor cadence
   (`reapExpired`), appends delete tombstones for expired ids, and removes them
   from the primary and secondary indexes. Compaction additionally drops expired
   entries during `resolveEntries`, so space is reclaimed even if the reaper has
   not yet run.

---

## Change Feed (Watch)

Every write emits a `WatchEvent` to all live `Watch` subscribers. Each
subscriber gets its own buffered channel (`--watch-buffer`, default 64).

### Overflow signal

Delivery is non-blocking: a write never waits on a slow consumer. If a
subscriber's buffer is full, the event is dropped and the watcher is marked
*overflowed*. Once that channel drains, the next emit delivers a single
sentinel `WatchEvent` with op `OVERFLOW` (no record) before normal events
resume — so a consumer that fell behind learns it missed writes and can resync
(re-read the affected records) instead of silently losing them. Exactly one
overflow sentinel is delivered per overflow episode.

Server-side `Watch` filters are applied *after* the buffer, so an `OVERFLOW`
sentinel always bypasses the filter and reaches the client regardless of
whether the dropped events would have matched.

---

## Concurrency Model

**Directory-level exclusive lock:**
When `engine.Open` is called, it acquires an exclusive OS-level advisory lock (using `flock` on Unix or `LockFileEx` on Windows) on a `LOCK` file in the data directory. This fast-fails any second attempt to open the same directory (from another process, or another DB instance within the same process) with `ErrDatabaseLocked`. This protects the append-only segments and segment-rotation logic from concurrent writers, which would otherwise silently corrupt data. To share access across multiple processes, run the gRPC server.

**Pessimistic locking per collection using `sync.RWMutex`:**

| Operation | Lock |
|---|---|
| Insert / Update / Delete | Write lock |
| FindById / Scan | Read lock |
| Compaction (rebuild phase) | Write lock (brief) |

Multiple concurrent reads proceed without blocking each other. The write lock is held only for the duration of the file append + in-memory index update, which is typically microseconds.

The compaction goroutine acquires the write lock only during the final atomic segment swap — reads and writes are unblocked for the entire resolve + rewrite phase.

---

## Background Compactor

Runs as a goroutine per collection. Two trigger conditions (whichever fires first):

1. **Dirty ratio**: >30% of entries in sealed segments are stale (overwritten or deleted)
2. **Time interval**: every 5 minutes (configurable)

A third, explicit trigger exists — see [On-demand compaction](#on-demand-compaction).

### Compaction algorithm

```
1. Snapshot sealed segment list (read lock, release)
2. Check dirty ratio — skip if below threshold
3. Scan all sealed segments, keep latest entry per id, drop deletes
4. Write resolved entries to new temp segments (no lock held)
5. Durably record the swap intent (compact.manifest: renames + removals)
6. Acquire write lock
7. Atomic rename: temp → final segment files (replacing reused names)
8. Delete old segments whose names were not reused
9. Rebuild primary index and all secondary indexes
10. Release write lock
11. Persist updated indexes to disk
12. Retire the swap manifest
13. Fire OnCompaction hook (used by Prometheus metrics)
```

### Compaction and Segment Rotation Interaction

While compaction runs (steps 3-5), concurrent writes might fill the active segment and trigger a segment rotation, sealing the active segment and creating a new one. To prevent collisions and dropped data:
- **Naming:** Compaction reuses the file names of the segments it read, and draws any additional names from a globally monotonic sequence (`segSeq`). This guarantees its output files never overwrite segments that were newly sealed during the pass.
- **Swap:** At step 7, compaction only replaces the segments it explicitly snapshotted. Any segments sealed during the pass (which were appended to `c.sealed` outside the snapshot) are preserved and appended after the new segments.

### Compaction and concurrent point reads

A point read (`Get`, `GetByKey`, `FindByID`, an index-driven scan candidate)
resolves the record's location from the primary index under the read lock and
then reads the segment file *without* the lock, so reads never hold up writers
during disk I/O. The swap (steps 6–10) moves every sealed record: a location
resolved before it names a file that may no longer exist, or a reused file name
whose offset now holds different bytes.

Point reads are therefore optimistic. The collection keeps a layout generation
that the swap bumps under the write lock, before the first rename. A read
samples it together with the index lookup and re-checks it after the segment
read; if a swap began in between, the result (value or error) is discarded and
the read is retried against the new layout. After three overlapping swaps the
read is performed under the read lock, which excludes the swap, so it always
terminates. Rotation needs no such handling: sealing a segment changes neither
its path nor its offsets.

Full scans (`ScanStream` without a usable index) do not have this protection
yet: they walk a snapshot of the segment list and can fail or miss records if a
swap lands mid-scan.

### Crash consistency

The swap (steps 6–12) is crash-atomic. The manifest written in step 5 is an
fsynced intent record: if the process dies anywhere before step 12, the next
open rolls the swap forward idempotently — outstanding renames applied, listed
removals deleted, leftover `.compact_*` temps discarded — and rebuilds the
primary and secondary indexes from the resulting segments. Temps found
*without* a manifest mean the pass died while still writing them; the old
segments remain authoritative and the temps are discarded.

Open also refuses to trust a persisted `index.json` blindly: even when its
checksum validates, every entry must point inside a segment file that actually
exists. A dangling reference (the signature of an index persisted against a
layout that later changed) triggers a full rebuild from the segments, which are
always the source of truth. Swap manifests are excluded from snapshots for the
same reason `index.json` is: they record absolute paths into the source
directory, and the archived segment set is always swap-consistent.

### Rebalancer

After compaction, adjacent segments smaller than 10% of `SegmentMaxSize` are merged to prevent segment count bloat from many small leftover files.

### On-demand compaction

Operators can force a compaction pass without waiting for the dirty-ratio or
timer trigger — for example to reclaim space immediately or to quiesce a
collection before a backup. `Collection.CompactNow()` (exposed as the `Compact`
RPC and `scriva-cli compact <collection>`) runs the same algorithm as the
background pass with two differences:

- **The dirty-ratio gate is skipped** (`compact(force=true)`), so the merge runs
  even when stale entries are below the 30% threshold.
- **It is synchronous** — the call returns only after the pass (and the reaper
  that precedes it) has fully completed, so a client knows the collection is
  compacted when the RPC returns.

Background and on-demand passes are serialized by a dedicated `compactMu`, so a
timer-triggered run and a forced run can never concurrently snapshot, remove,
and rename the same sealed segments. A closed collection refuses to compact,
and `Close()` itself takes `compactMu`: it waits for an in-flight pass to
finish (segment swap and index persist included) before persisting the final
index, and a pass that was still blocked on the lock when Close finished
aborts instead of mutating the layout afterwards.

---

## Transactions

> **Planned format — not implemented.** Cross-collection transactions (XTx) —
> atomic, durable writes spanning several collections of one data directory —
> are designed but **not shipped**: no released binary writes or reads the
> format it describes, and the harness in `internal/xtxspec` is not wired into
> `Open`. The design (coordinator authority via a root-level `xtx.journal`
> `COMMIT` record, redo-only stamped entries, presumed abort, fsync ordering
> S2 < S3 < S4, DB-level recovery state machine, canonical lock order, conflict
> and isolation semantics, error outcomes, and the old-binary compatibility
> fence) is in
> [design-cross-collection-transactions.md](design-cross-collection-transactions.md);
> the per-boundary crash checklist and frozen wire fixtures are in
> [design-xtx-fault-boundaries.md](design-xtx-fault-boundaries.md). Single-node,
> single data directory only — distributed/XA transactions are out of scope.
> Existing `CommitTx` (single collection) is unchanged.

Transactions are optimistic and scoped to a single collection. Operations are staged in memory and applied atomically on commit:

```
BeginTx   → allocate tx_id, create in-memory staging buffer
Insert / Update / Delete (with tx_id) → append to staging buffer (no disk write)
CommitTx  → acquire write lock → apply all staged ops sequentially → release
RollbackTx → discard staging buffer
```

Staged operations bypass the normal single-operation write path; the write lock is held for the entire commit batch.

### Idle transaction expiry

Open transactions live only in server memory, so a client that calls `BeginTx`
and then disconnects without committing or rolling back would otherwise leak its
staging buffer and reserved id forever. A background sweeper in the `TxManager`
reaps any transaction whose last staged op is older than `--tx-timeout`
(default 5m; set `0` to disable). Reaping an abandoned transaction is equivalent
to a rollback — nothing was ever written to disk, so the staged buffer is simply
discarded. A subsequent `CommitTx`/`RollbackTx` on a reaped id returns
`NotFound`.

---

## Durability

Writes are appended to the active segment with a single `write(2)`. Whether that
write is flushed to stable storage (via `fsync(2)`) before the operation is
acknowledged is controlled by the **sync mode** (`--sync`):

| Mode | Behaviour | Crash-loss window | Throughput |
|---|---|---|---|
| `none` (default) | Never fsyncs explicitly; relies on the OS page-cache flush | All not-yet-flushed writes | Highest |
| `interval` | A per-collection goroutine fsyncs the active segment every `--sync-interval` (default 1s) | At most one interval | High |
| `always` | fsyncs after every write, before acknowledging it | Zero (for acknowledged writes) | Lowest |

`always` holds the collection write lock across the fsync, so it serializes
durable writes — correct, but the slowest option. `interval` is the recommended
middle ground for most workloads. Sealing a segment and `Close()` always fsync
regardless of mode.

What each mode means for the other durability-relevant events:

| Event | `none` | `interval` / `always` |
|---|---|---|
| Segment rotation | sealing fsyncs the old segment | same, plus an `fsync` of the directory for the new file |
| Index persist (`index.json`, `sidx_*.json`) | fsyncs the active segment first so coverage never claims bytes a crash could lose; metadata files use temp → fsync → rename | same, plus directory fsync |
| Compaction swap | manifest, renames and unlinks are fsynced in order | same |
| `Close()` / `SIGTERM` | flushes, persists the index and releases the lock | same |
| After `kill -9` | nothing acknowledged is lost (the data is in the page cache); open trims a torn tail and replays the index from its covered prefix | same |
| After power loss | the tail since the last OS flush may be lost | loss bounded by the interval (`interval`) or nil for acknowledged writes (`always`) |

A crash or power loss can drop the latest writes, but open never leaves a half-applied one: a torn final line is trimmed, `CommitTx` is all-or-nothing, and the index is validated against the segments rather than trusted.

> Pick the mode that matches your data's value. `none` is appropriate for caches
> and rebuildable data; `always` for data you cannot afford to lose on power loss.

## Crash Safety

- **Partial write recovery**: on segment open, the last line is validated. Any partial line (from a crash mid-write) is detected and truncated before the segment is used.
- **Bit-rot detection**: each segment entry carries a CRC32C checksum verified on read. A single flipped byte in a sealed segment that still parses as JSON is caught (`store.ErrCorruptEntry`) rather than silently returning wrong data. See *Per-entry checksums* above.
- **Index recovery**: on startup, both the primary index and each secondary index checksum are verified. A mismatch triggers a full rebuild by replaying all segment entries.
- **Atomic segment swap**: compaction uses `os.Rename` which is atomic on POSIX filesystems. The old segments are only deleted after the new ones are in place.
- **Durable metadata writes**: `index.json`, `sidx_*.json`, and `meta.json` are written with a write-temp → `fsync` → atomic `rename` → directory `fsync` sequence, so a crash can never leave a half-written or invisible file. Directory `fsync` after creating or rotating a segment (under `--sync=interval`/`always`) ensures the new segment file's directory entry survives a crash too. (Directory `fsync` is a no-op on Windows, which does not support it; the atomic rename still holds.)
- **Id-counter recovery**: `meta.json` is persisted on segment rotation and on `Close`, not on every write. On startup the counter is reconciled against the highest id present in the active segment (which always holds the most recently assigned id), so a crash that lost an unsynced `meta.json` can never cause id reuse.

Note that partial-line recovery protects against *torn* writes (an incomplete
final line), not against *lost* writes — a write acknowledged under `--sync=none`
can still be lost if the machine loses power before the OS flushes its page
cache. Use `--sync=interval` or `--sync=always` to bound or eliminate that window.

### Partial writes and segment poisoning

A failed `Append` (ENOSPC, EIO, short write) may leave bytes on disk even when it reports zero written. The segment therefore **always truncates back to its last known-good size** after a failed write, and the collection also rolls back an already-appended prefix of a multi-entry write (batch, transaction commit) when a later append or fsync fails. Memory state (index, size) only advances after a write fully succeeded, so a failed write is invisible to readers and to the next reopen.

If the rollback truncate itself fails, the tail of the file is unknown and appending after it could glue a record onto garbage. The segment is then **poisoned**: every later append returns `engine.ErrSegmentPoisoned` (wrapping the original cause) until the process restarts. Reads and other collections keep working. Poisoning increments `scriva_segment_poisoned_total` and `scriva_append_errors_total{reason="poisoned"}`. The recovery is to fix the underlying fault (disk space, device health), restart so that open re-validates and trims the tail, then run `scriva verify` if you saw it. A record too large to be scanned back is refused up front (`ErrRecordTooLarge`) rather than written.

### Directory lock and filesystem assumptions

- **Single writer.** Opening a database takes an exclusive, non-blocking advisory lock (`flock` on Unix, `LockFileEx` on Windows) on `<data>/LOCK`; the lock is released by `Close()` and by process exit, including `kill -9`, so a crashed owner never leaves a stale lock. The lock is per open file description, so a second `Open` in the *same* process is refused too.
- **Local filesystems.** ScrivaDB assumes POSIX-like local-filesystem semantics: atomic `rename`, `fsync` that really reaches stable storage, and working `flock`. If the filesystem does not support the lock (some NFS setups) open fails with an `unsupported file system` error rather than running unprotected. Network or FUSE filesystems that merely accept the lock without enforcing it across hosts are **not** safe for a shared data directory; never point two hosts at one directory.
- **Directory fsync.** New, rotated and renamed files are made durable by an `fsync` of the parent directory under `--sync=interval`/`always` (a no-op on Windows).
- **No lock-free readers.** Other processes (`scriva verify` on a live directory, backup scripts) see an unsynchronized view; use `scriva-cli backup` against a running server, or stop the server first.

### Upgrading from v1 index files and mixed binaries

Index and secondary-index files written by earlier releases (no `version`, absolute segment paths, no coverage) are **v1**. The first open by the new engine cannot prove a v1 file current, so it rebuilds the index from the segments once (`scriva_recovery_total{kind="rebuild"}` increments) and rewrites it as v2; the next open is a normal clean reopen. See *v1 index upgrade* in the table above for the cost. No manual step and no segment rewrite is involved: the segment format is unchanged.

Going back is safe for data: a pre-v2 binary reads the v2 `index.json`, fails its (v1-style) checksum, treats it as stale and rebuilds from segments, rewriting it as v1. It does not understand the directory lock, the integrity gate or `repair`, so never run an old and a new binary on one directory at the same time, and do not use an old binary's `kill -9` recovery as a substitute for `scriva verify`.

### Recovery cost and performance guardrails

Measured with `engine/bench_*_test.go` (300,000 records of ~170 B, 4 MiB
segments, `SyncModeNone`, Intel i7-7700HQ; wall time per `OpenCollection`;
medians of 3 alternating runs of compiled test binaries on a noisy shared host).
"Baseline" is `main` before the integrity work (`f643158`); "Before" is the
integration branch before the perf fix; "Now" includes it.
Reproduce: `SCRIVA_BENCH_N=300000 go test ./engine -run xxx -bench 'Open|Persist' -benchtime=2x`.

| Scenario | Baseline | Before | Now | Notes |
|---|---|---|---|---|
| Clean reopen (valid v2 coverage) | 1.21 s | 1.37 s | 0.94 s | sealed segments validated by length + tail fingerprint; index checksum verified without re-marshalling. **Superseded by full-prefix checksums** (next paragraph) |
| Crash reopen, tail of 1k / 10k records | n/a (stale index trusted silently) | 3.7 s / 3.8 s | 0.93 s / 0.96 s | clean open + O(tail); a replayed index is persisted by the background persister, not synchronously on open |
| v1 index upgrade (one-time rebuild) | n/a | see `BenchmarkOpenV1Upgrade` | unchanged | rewritten as v2; next open is a clean reopen |
| One index persist pass | n/a | O(data) hashing + 2 marshals | O(segments) hashing + 1 marshal (first pass after open re-uses the hashes verified at open) | background goroutine; writers are not blocked |

**Full-prefix coverage (correctness fix).** Sealed segments were fingerprinted by their last 64 KiB only, which left earlier bytes covered by a spot check alone. Coverage now hashes whole prefixes. Measured on the same 300,000-record set (`-bench 'OpenClean|PersistIndexes' -benchtime=5x`, one noisy run each, same host, before = tail fingerprint, after = full checksum): `OpenClean` 0.87 s → 1.04 s (+20%), `PersistIndexes` 0.71 s → 1.01 s (a cold persist now hashes all sealed segments once; later passes are memoized). The extra cost is SHA-256 over the data at open, roughly the price of one sequential read of the segments; it is the price of proving the prefix rather than sampling it.

Write path (`Insert`/`Delete`, `none`/`interval`, ns/op medians): `Insert`
36.0 µs baseline, 64.9 µs before, 38.7 µs now; `Delete` 16.4 µs / 15.3 µs /
17.2 µs. The success path issues exactly the same syscalls as before. Rotation
only *requests* a persist, which is debounced (see above), so the full-index
encode no longer runs every ~25k inserts.

---

## Cross-collection transaction journal (internal, not yet user-visible)

The protocol is specified in [design-cross-collection-transactions.md](design-cross-collection-transactions.md). Only its root-level *coordinator journal* and *version gate* exist so far (`engine/xtx_journal.go`, `engine/xtx_format.go`, `engine/xtx_errors.go`); no code path writes them yet, so a database that never runs an XTx is byte-identical to before.

**Files (root of the data directory, reserved `xtx.` prefix).**

| File | Role |
|---|---|
| `xtx.format` | one JSON line `{format, min_reader, features, created_by, crc}`; durable (file + dir fsync) before the journal |
| `xtx.journal` | NDJSON: header `{magic, v, epoch, gen?, created, crc}` then `commit` / `abort` / `retire` records, each with a CRC32C over its canonical JSON |
| `xtx.journal.tmp` | checkpoint rewrite, deleted at open |

**Open.** `DB.Open` runs the gate before any collection: leftover `xtx.journal.tmp` is removed; `xtx.format` with `min_reader` above this binary's level fails with `ErrFormatTooNew`, an unknown feature / journal version / journal without format fails with `ErrXTxUnsupported`; a corrupt format fails with `ErrXTxFormatCorrupt`. The journal is then parsed front to back:

- an unterminated or zero-filled *final* line is a torn tail and is truncated + fsynced (a never-acknowledged record);
- any complete record that fails its CRC, does not parse, has an unknown kind/field, a txid of another epoch, a non-canonical participant list or digest, **anywhere**, is `ErrXTxJournalCorrupt` (also matches `ErrIntegrity`) and open refuses — a lost `COMMIT` is never read as an abort;
- `COMMIT` and `ABORT` (or differing contents) for one txid is `ErrXTxDecisionConflict`.

A root with neither file is a legacy root and opens exactly as before. `OpenCollection` / `CreateCollection` reject names with the `xtx.` prefix (`ErrReservedName`), and a root *directory* with that prefix refuses the open. Primary and secondary index recovery are transaction-aware: they replay only plain entries plus stamped entries whose coordinator decision is committed, and they exclude aborted or incomplete prepares. Compaction follows the same decision table and preserves unresolved and unretired committed stamped entries as coordinator evidence and as the materialized survivor, so derived indexes rebuild from the same committed effects without losing proof. Open-time recovery does not retire committed journal decisions by itself; coordinator evidence must not be garbage-collected until a later atomic checkpoint can prove it safe. `SnapshotTo` archives `xtx.format` and `xtx.journal` under its XTx consistency barrier. Offline `VerifyDir` checks the coordinator graph; `Repair` takes its verified backup first, rebuilds derived state using committed decisions only, and refuses irreconcilable coordinator metadata rather than fabricating or deleting evidence.

**Internal API for later stages.** `commit(key, parts)` allocates the txid (`<epoch 16 hex>-<seq 16 hex>`, seq under the journal mutex), appends `COMMIT` with one `write` and fsyncs; `abort` / `retire` append advisory/recovery records; `decision`, `decisions`, `status`, `statusByKey` read the in-memory decision table (`COMMITTED`, `ABORTED`, `PENDING`, `UNKNOWN`, `EXPIRED`); `observeSeq` restores the counter from stamped entries; `checkpoint` is the atomic GC rewrite (tmp + fsync + rename + dir fsync, `gen`+1, same epoch; only retired transactions, never the highest-seq record). A failed append is rolled back by truncation; a failed rollback or fsync poisons the journal (`ErrXTxDurability`), and an fsync failure on commit reports `ErrXTxOutcomeUnknown` without claiming an outcome.

## Backup / snapshot

`DB.SnapshotTo(io.Writer)` (the `Snapshot` RPC and `scriva-cli backup`) writes a
**gzip-compressed tar** of the whole database — one entry per collection file,
named `<collection>/<file>`. Because the on-disk format is just append-only
NDJSON plus small sidecar files, a backup is a plain file copy; restore is a
plain extract:

```bash
scriva-cli backup db.tar.gz
tar xzf db.tar.gz -C ./data      # then start the server with --data ./data
```

**Consistency** is layered to match ScrivaDB's guarantees without a global stop:

- The DB registry is held read-locked for the whole archive, so no collection is
  created, dropped, or reopened mid-snapshot.
- Each collection's files are copied while its **own read lock** is held, so no
  write, rotation, or compaction can mutate them during the copy — the archive
  captures a per-collection point in time. (ScrivaDB has no cross-collection
  transactions, so per-collection consistency is the strongest meaningful
  guarantee.)
- Segments are append-only, so even the active segment is captured at a valid
  entry boundary — the copy simply ends at the current file size.

When XTx is enabled, the archive also contains root-level `xtx.format` and
`xtx.journal`. `SnapshotTo` takes an exclusive DB-level XTx barrier from before
those files are copied until every collection is copied; `CommitXTx` holds the
shared side for its complete prepare/decision/materialization sequence. Restore
therefore reopens using the same authoritative coordinator evidence as the
source database; `xtx.journal.tmp` and derived `index.json` are never archived.

**What is and isn't archived:** segment files (`seg_*.ndjson`), `meta.json`, and
the secondary indexes (`sidx_*.json`, refreshed from memory just before the copy)
are included. The primary `index.json` is **deliberately excluded**: it stores
absolute segment paths and its checksum only guards its own contents, so a copied
index would reference the source directory and could be silently stale. The
restored collection rebuilds a correct primary index from its segments the first
time it is opened (the same [index recovery](#crash-safety) path used after a
crash), which is also why a backup taken under concurrent writes always restores
to a consistent state.

The RPC streams the archive in 64 KiB chunks (`SnapshotChunk`); it is gRPC-only
because binary streaming does not map cleanly onto the REST gateway.

---

## Replication (leader → follower)

ScrivaDB's append-only segment log *is already a write-ahead log*, which makes
leader→follower log shipping the natural HA primitive (R1). A follower stays
consistent with a leader by tailing its committed writes and applying them
through the normal write path, so its primary index, secondary indexes, keys,
revisions, and TTLs all end up identical to the leader's.

### Global LSN and the commit feed

When leader-side replication is enabled (`CollectionConfig.ReplicationRingSize >
0`, which the server sets by default), the DB owns a small **replication broker**.
Every committed entry — after it has been appended and fsynced under the
collection write lock — is published to the broker, which assigns it the next
**LSN** (a monotonic, DB-global sequence number) and records it. Publishing
happens *inside* the collection's write critical section, so entries from one
collection keep their commit order and all collections share one consistent total
order. The broker keeps the most recent entries in a bounded in-memory ring
(default 8192) so a briefly-disconnected follower can resume from memory rather
than re-fetching a whole snapshot.

The engine exposes this as plain Go types — `ReplicationEntry`,
`DB.SubscribeReplication`, `DB.ApplyReplication`, `DB.ReplicationStatus` — and
never imports gRPC/protobuf; the server maps them onto the `Replicate` and
`ReplicationStatus` RPCs. This keeps the embeddable engine dependency-free
(`make deps-check`), and leaves the embedded/default write path untouched when
replication is off (ring size 0 → no broker, no LSN cost).

### Bootstrap, then tail

A fresh follower catches up in two phases:

1. **Snapshot bootstrap.** The follower reads the leader's current LSN watermark
   `L` (via `ReplicationStatus`) *before* pulling a `Snapshot`, then extracts the
   snapshot into its data directory. Because the watermark is read first, the
   snapshot is guaranteed to contain every entry with `lsn ≤ L`.
2. **Stream tail.** The follower opens `Replicate(from_lsn = L)`; the leader ships
   the buffered backlog (`lsn > L`) and then live commits as they happen. The
   follower applies each entry and advances its **applied-LSN**, persisted to
   `replication.json` at the data-dir root so a restart resumes from exactly
   where it left off.

### Idempotent apply → no gaps, no duplicates

Apply is idempotent at the **record-revision** level: an insert/update whose id
already sits at an equal-or-newer revision is skipped, and a delete of an
already-absent id is skipped. This is the correctness backbone:

- A few entries can legitimately appear in *both* the snapshot and the tail (they
  raced in after the watermark was read) — re-applying them is a no-op.
- A resumed follower that re-requests from a slightly stale applied-LSN re-applies
  the overlap harmlessly.

So a follower converges to the leader's exact state under continuous writes, and
recovers from a disconnect with **neither a gap nor a duplicate**. Applied entries
also fan out to the follower's own Watch subscribers.

Replication is **asynchronous** (bounded lag). The leader tracks, per connected
follower, the highest LSN it has shipped; `ReplicationStatus` reports the leader
LSN and each follower's shipped LSN and lag. A follower that falls further behind
than the ring can hold — or whose consumer stalls and overflows its buffer — is
told to re-bootstrap with `FAILED_PRECONDITION`.

### Read replicas & follower reads (R2)

A follower serves **read RPCs** — `Find`, `FindById`, `FindByKey`, `Aggregate`,
and the read-only observability RPCs (`CollectionStats`, `ListCollections`,
`ListIndexes`, `Watch`) — directly from its applied state, so read traffic scales
horizontally: point read clients at any follower and writes at the leader.

Role-aware routing lives in the **server layer**; the engine owns only the role
*flag*. When a node is started as a follower (`--replicate-from`), the server
installs a single pair of gRPC interceptors (`server.ReadOnlyInterceptors`) that
refuse every mutating RPC with `FAILED_PRECONDITION` and the message *"read-only
replica; write to the leader"*. The guard is keyed on the generated method-name
constants and centralised in one place — adding a new write RPC is a one-line
addition to its `writeMethods` set. The check is **dynamic**: each call consults
`DB.IsFollower()`, so a promotion (R3) lifts the guard live without a restart.
The engine stays free of any gRPC/protobuf dependency; it exposes the role flag
(`DB.IsFollower`) and the applied-LSN watermark (`DB.AppliedLSN`).

Because replication is asynchronous, a follower read may be **stale** by the
follower's current lag. That bound is *observable*: `ReplicationStatusResponse`
carries an additive `applied_lsn` field (the node's follower watermark; 0 on a
leader). A client bounds staleness by reading a follower's `applied_lsn` and
diffing it against the leader's `leader_lsn` — the gap is the maximum number of
committed writes the follower has not yet applied. Records themselves never go
backwards: apply is idempotent by revision, and each applied entry advances the
persisted watermark monotonically.

### Manual failover & role management (R3)

After a leader loss, an operator recovers write availability by **promoting** a
caught-up follower. The engine owns a `role` flag (leader by default; follower
when opened with `CollectionConfig.Follower`, which the server sets in
`--replicate-from` mode) and exposes `DB.Promote(maxLag, force)` as plain Go —
still no gRPC/protobuf in the engine. The server's admin `Promote` RPC (POST
`/v1/replication/promote`) maps onto it; the CLI wraps it as `scriva-cli promote`.

A promotion:

1. **Checks the guard.** It refuses a node that is not a follower
   (`ErrNotFollower` → `FAILED_PRECONDITION`) and, unless forced, a follower whose
   **lag** exceeds the configured ceiling (`ErrReplicaLagExceeded` →
   `FAILED_PRECONDITION`). Lag is the *last-known leader LSN* minus the applied
   LSN. The follower learns the leader's LSN from the replication feed and from a
   `ReplicationStatus` probe on each (re)connect (`DB.NoteLeaderLSN`); when the
   leader is lost, that value is frozen at the last observation — exactly the
   "how far behind was I when the leader died?" a failover check needs. The
   ceiling is `--promote-max-lag` (default 0 = must be fully caught up); `--force`
   overrides it, accepting the loss of the leader's un-replicated tail.
2. **Flips the role** to leader. Because the read-only guard reads the role on
   every call, writes start flowing immediately — no restart.
3. **Reseeds the LSN counter** above the replicated tail (the applied watermark),
   so the new leader never reissues an LSN the old leader already assigned.
4. **Stops the apply loop.** The engine invokes a server-registered hook
   (`DB.SetPromoteHook`) that cancels the follower's tail context, so the new
   leader no longer replicates from its dead upstream.

Promotion is **one-way**: a promoted node is an ordinary leader. Repointing
clients and any surviving followers at the new leader is an operator step (see
[operations.md](operations.md)); **automatic leader election (consensus) remains
explicitly out of scope**. A leader restart keeps LSNs monotonic (the
last-assigned LSN is persisted) but its in-memory ring starts empty, so a
follower mid-catch-up may need to re-bootstrap.

`Promote` requires a **read-write** API key — the admin boundary until per-key
admin ACLs land in S3.

---

## Network Layer

```
┌───────────────────────────────────────────────┐
│  scriva binary                                │
│                                               │
│  ┌────────────────┐   ┌──────────────────────┐│
│  │ gRPC/TCP :5433 │   │ REST gateway :8080   ││
│  │ (optional TLS) │   │ (grpc-gateway)       ││
│  └───────┬────────┘   └──────────┬───────────┘│
│          │                       │             │
│  ┌───────▼───────────────────────▼───────────┐ │
│  │ Unix socket /tmp/scriva.sock              │ │
│  │ (local connections, always insecure)      │ │
│  └───────────────────────┬───────────────────┘ │
│                          │                     │
│  ┌───────────────────────▼───────────────────┐ │
│  │ engine.DB                                 │ │
│  └───────────────────────────────────────────┘ │
│                                               │
│  ┌────────────────────────────────────────┐   │
│  │ Prometheus metrics :9090/metrics       │   │
│  └────────────────────────────────────────┘   │
└───────────────────────────────────────────────┘
```

- **TCP gRPC listener** — optional TLS via `--tls-cert` / `--tls-key`. `server.ServerTLSConfig` builds the `*tls.Config`: when both flags are set, `credentials.NewTLS()` is used; otherwise `insecure.NewCredentials()`. When `--tls-client-ca` and a non-`off` `--tls-client-auth` are also set, it adds the client-CA pool and the `tls.ClientAuthType` (**mutual TLS**, S1) — see [Mutual TLS](#mutual-tls-s1).
- **REST gateway** — dials the TCP gRPC server on the internal loopback. Uses `InsecureSkipVerify` for this internal hop (the cert may be self-signed). Under `--tls-client-auth require` the TCP server would reject this certless loopback dial, so the gateway is routed over the Unix socket (`NewRESTGatewayUnix`) instead.
- **Unix socket** — always uses `insecure.NewCredentials()`. The CLI auto-detects this socket and prefers it for zero-overhead local connections.
- **Metrics HTTP server** — serves Prometheus exposition format at `/metrics`. Disabled when `--metrics-addr` is empty.

### Auth

All gRPC calls (TCP and Unix socket) pass through unary and stream interceptors backed by an `auth.Authenticator`. Each request's `x-api-key` metadata header is matched against the configured key set using `crypto/subtle.ConstantTimeCompare` — the lookup compares against *every* key without short-circuiting, so response timing never reveals which (or whether a) key matched.

**Scoped keys.** A key resolves to a principal with a scope of either `read` or `read-write`. The interceptor classifies each RPC by its method name: mutating RPCs (`Insert`, `Update`, `Delete`, `CreateCollection`, `DropCollection`, `EnsureIndex`, `DropIndex`, `Compact`, and the transaction verbs) require `read-write`; the rest (`Find`, `FindById`, `ListCollections`, `ListIndexes`, `CollectionStats`, `Watch`, `Snapshot`) are reads. A read-scoped key presenting on a write RPC is rejected with `PermissionDenied` (distinct from the `Unauthenticated` returned for a missing/unknown key). Unknown method names are treated as writes, so a read-only key can never slip through a newly added RPC that predates its classification.

**Key sources.** Keys come from the config file's `keys:` list (`{key, name, scope}` entries). The legacy single `--api-key` / `SCRIVA_API_KEY` still works and is registered as an additional `read-write` key named `default`, so existing single-key and no-auth (empty) deployments are unchanged.

**Rotation.** The active key set lives behind an `atomic.Pointer`; sending the server `SIGHUP` re-reads the config file and swaps in the new set atomically, with in-flight requests finishing against the set they started on. Keys can therefore be added, removed, or re-scoped without a restart.

**Per-collection ACLs (S3).** A key entry may carry an optional `collections:` allow-list. At key-set build time it is resolved into a `map[string]struct{}` on the principal (a **nil** map means *unrestricted* — the backward-compatible default; a non-nil map confines the principal to exactly its members). Enforcement is layered *on top of* scope, per RPC, and lives in one place in the auth interceptor. After the principal is resolved and deposited into the audit sink (so an ACL denial is still attributed to the real caller, mirroring a scope denial), the interceptor extracts the target collection from the request via a narrow `interface{ GetCollection() string }` assertion — the accessor the generated request protos expose — and rejects the call with `PermissionDenied` when the collection is outside a non-nil allow-list. Unary RPCs are checked before the handler runs. For **streaming** RPCs the collection is not knowable until the client's first message, so the check is deferred to the wrapped `ServerStream`'s `RecvMsg` and fires once, on the first (collection-bearing) request of `Watch`/`Find`/`Aggregate`. RPCs whose request has **no** collection field (e.g. `ListCollections`) don't satisfy the interface and are therefore never collection-scoped — a restricted key may still call them. Certificate-authenticated principals (below) resolve with a nil allow-list and so reach all collections; per-certificate ACLs are out of scope. Because the allow-list is part of the resolved key set, `SIGHUP` reload picks up ACL edits just like scope edits.

#### Mutual TLS (S1)

When `auth.WithCertAuth(true)` is enabled (the server sets it from `ServerTLSConfig` whenever a client-CA and a non-`off` client-auth mode are configured), the same `Authenticator` accepts a **verified client certificate** as an alternative credential. The composition inside `authorize` is deliberate and backward compatible:

1. If key auth is configured and the request carries an `x-api-key`, the key is validated as before — a valid key resolves the principal (and its scope), an **invalid** key is rejected outright with `Unauthenticated` (no silent fallback to the certificate).
2. Only when **no** API key is presented does it fall back to the peer certificate. `principalFromPeerCert` reads `credentials.TLSInfo` from the gRPC `peer` and inspects `ConnectionState.VerifiedChains`. That field is populated **only after** the TLS stack has chained the leaf up to a configured `ClientCA` (`RequireAndVerifyClientCert` or `VerifyClientCertIfGiven`), so a non-empty verified chain *is* proof of trust — an untrusted or unsigned cert fails the handshake and never reaches the interceptor.
3. The cert principal's name is the leaf's subject **Common Name**, falling back to the first DNS/email/URI **SAN**; its scope is **read-write**. A CA-signed client cert is treated as an operator-issued trusted identity (mirroring how `--api-key` becomes a `read-write` `default` principal). Per-certificate scoping and per-collection ACLs are deferred to S3; the resolved principal flows onto the request context exactly like an API-key principal, so that future work composes uniformly.

The two `--tls-client-auth` modes differ only at the transport: `require` (`RequireAndVerifyClientCert`) rejects any connection without a valid client cert during the handshake; `verify-if-given` (`VerifyClientCertIfGiven`) lets certless clients connect (authenticating by API key) while still verifying a cert when one is presented. mTLS is **off by default** and requires server TLS — `ServerTLSConfig` fails loudly if a client-CA or a non-`off` mode is set without `--tls-cert`/`--tls-key`.

### Interceptor pipeline

Both gRPC servers install the same interceptor chain, in this order:

```
[tracing] → auth → limiter → logging → metrics → handler
```

Auth runs first (of the always-present interceptors): on success it resolves the principal and attaches it to the request context (via a stream wrapper for streaming RPCs). The limiter runs next so it can read that principal — it applies the per-key rate limit and the in-flight semaphore, shedding over-budget calls before they reach the handler. Logging runs after the limiter so a shed call is still logged (with its `RESOURCE_EXHAUSTED` code). Metrics is innermost and records the Prometheus request histogram. Because logging and metrics sit *inside* auth, a call rejected by auth is not double-counted as a served request. **The limiter is chained only when at least one limit is configured**, so the default (unlimited) path keeps the exact `auth → logging → metrics` chain and adds no overhead. **Tracing, when enabled (`--otlp-endpoint`), is chained *outermost*** — before auth — so its per-RPC span wraps the whole handler (including the status of a call rejected by auth or the limiter) and its span-bearing context flows down through the chain into the engine scan hook; when tracing is off, it adds no interceptor at all.

### Request logging

The server owns a single `*slog.Logger` (`log/slog`, no third-party dependency), built from `--log-level` and `--log-format`. The logging interceptor (`server/logging.go`) writes exactly one record per RPC — `method`, `principal`, `duration`, `code` — at `info` for success and `error` for failure, letting an operator filter noise with the level while still capturing every error. The **engine package never imports the logger**: it stays embeddable and dependency-free, surfacing anything it needs to report through the existing `engine.CollectionConfig` hooks (the same rule metrics follows via `OnCompaction`), and `make deps-check` enforces this.

### Health & readiness

The standard `grpc.health.v1.Health` service (`server/health.go`) is registered on both the TCP and Unix gRPC servers via a shared `HealthService`. It starts `NOT_SERVING`, is marked `SERVING` once the listeners are accepting connections, and is flipped back to `NOT_SERVING` at the start of graceful shutdown — so a load balancer stops routing new work while `GracefulStop` drains the in-flight RPCs. Two HTTP probes are registered directly on the grpc-gateway mux: `GET /healthz` (liveness — `200` whenever the process can answer) and `GET /readyz` (readiness — `200` when the DB is open and the data directory accepts a probe write, else `503` with the reason). Readiness is deliberately data-plane aware: a full or read-only data volume makes the node *unready* without making it *dead*, so it is pulled from rotation rather than restarted.

### Backpressure & rate limiting

The `Limiter` (`server/limits.go`) provides two independent, opt-in defences against resource exhaustion, both surfaced through the unary and stream interceptors described above. Like metrics and logging, this is a **server-layer** concern — the limiter reaches for `golang.org/x/time/rate`, which the embeddable `engine`/`store`/`query` packages must never import (`make deps-check` enforces it).

- **In-flight semaphore (`--max-inflight`).** A counting semaphore of fixed capacity is acquired at the start of every RPC and released when it returns. Acquisition is *non-blocking*: when the ceiling is saturated the interceptor returns `RESOURCE_EXHAUSTED` immediately rather than queueing, so the server sheds load instead of accumulating goroutines, file descriptors, and memory behind a saturated CPU. A streaming RPC holds its slot for the whole stream lifetime, which correctly counts a long-lived `Watch` or `Snapshot` against the ceiling.

- **Per-principal token bucket (`--rate-limit`).** Each API-key principal (the resolved `name` the auth interceptor put on the context) gets its **own** `rate.Limiter`, created lazily on first request and stored in a mutex-guarded map. The rate is the configured requests/sec and the burst is one second's worth of budget (rounded up). Because the buckets are keyed by principal, throttling one key can never consume another key's budget. An unauthenticated deployment funnels every call into a single shared `"anonymous"` bucket.

Both controls default to their zero value (unlimited / disabled). `NewLimiter` reports `Enabled()` only when at least one is active, and `cmd/scriva` chains the limiter interceptors solely in that case — so the common, un-limited deployment pays nothing. `grpc.MaxConcurrentStreams` (`--max-concurrent-streams`) is set directly as a `grpc.ServerOption` on both servers, capping the HTTP/2 streams a single connection may multiplex; it is orthogonal to the server-wide in-flight ceiling.

### Tracing (OpenTelemetry)

Distributed tracing (`server/tracing.go`) is **opt-in and off by default**: `cmd/scriva` builds an OTel SDK `TracerProvider` and chains the tracing interceptors only when `--otlp-endpoint` is set. The provider batches spans to an OTLP/gRPC collector, tags them with a `service.name=scriva` resource, and samples with a **parent-based** sampler over `TraceIDRatioBased(--otlp-sample-ratio)` — so an upstream sampling decision propagated on the trace context is honoured, and the ratio governs only the traces ScrivaDB roots. On graceful shutdown the provider is `Shutdown` (with a bounded timeout) to flush any spans still buffered.

**Interceptor span.** `TracingInterceptors` (unary **and** stream) starts one span per RPC, named after the full method (`/scriva.v1.Scriva/Find`) with span kind *server*, tagged `rpc.method` and — once the call returns — `rpc.grpc.status_code`; a non-OK result additionally marks the span errored and records the error. For streaming RPCs the interceptor wraps the `ServerStream` so its `Context()` carries the span, exactly as the auth interceptor does for the principal. Chained outermost, the span becomes the parent of everything downstream.

**Engine hook.** The rule that keeps the engine embeddable applies here too: **the `engine`/`store`/`query` packages import no OpenTelemetry code** (`make deps-check` enforces it). Instead, the engine exposes timing through the same `engine.CollectionConfig` hook pattern used for metrics — a new `OnScan(ctx, collection, dur)` hook fired by `ScanStream`, alongside the existing `OnCompaction(collection, dur)`. The **server** owns the SDK and turns those callbacks into spans: `ScanTraceHook` starts an `engine.scan` span parented on the scan's context (which, because the span-bearing context threads down from the interceptor, nests it under the RPC span), and `CompactionTraceHook` records a root `engine.compaction` span (compaction is a background task with no request context). The scan hook receives the scan's `context.Context` precisely so its span can attach to the caller's; the compaction hook takes none, so its span stands alone. Both reconstruct their start/end timestamps from the reported duration so the span's extent matches the real work. The metrics `OnCompaction` hook and the tracing one are **composed** in `cmd/scriva` (metrics first, then tracing) so enabling tracing never displaces Prometheus compaction timing.

The net effect: a slow `Find` produces a trace spanning gateway → gRPC (`/scriva.v1.Scriva/Find`) → `engine.scan`, making it obvious whether the cost was in transport, a saturated limiter, or a large collection scan.

### Keyed CRUD, Upsert & CAS on the wire (N1)

The [caller-supplied string keys](#caller-supplied-string-keys),
[revisions/compare-and-swap](#revisions-and-compare-and-swap), and
[key-based upsert](#key-based-upsert) the engine has always had are surfaced over
gRPC/REST by a thin handler layer in `server/grpc.go` — the handlers **map
straight onto the engine methods and add no logic of their own**:

| RPC | REST | Engine method |
|---|---|---|
| `Insert` (with `key`) | `POST /v1/{collection}/records` | `InsertWithKey` |
| `Upsert` | `POST /v1/{collection}/records:upsert` | `Upsert` |
| `FindByKey` | `GET /v1/{collection}/keys/{key}` | `GetByKey` |
| `UpdateByKey` | `PUT /v1/{collection}/keys/{key}` | `UpdateByKey` |
| `DeleteByKey` | `DELETE /v1/{collection}/keys/{key}` | `DeleteByKey` |
| `UpdateIfRev` | `POST /v1/{collection}/keys/{key}:cas` | `UpdateIfRev` |

There is deliberately **no `InsertWithKey` RPC**: a keyed create is expressed by
setting the additive `key` field on the existing `Insert` request (empty = the
unchanged server-assigned-id behaviour). A keyed insert bypasses the transaction
and per-record-TTL paths (the engine's keyed insert supports neither), which the
handler rejects with `InvalidArgument`.

**Error-code mapping.** The handlers translate the engine's *typed* errors into
gRPC status codes, so a client sees a stable code rather than an opaque string:

| Engine error | gRPC code |
|---|---|
| `engine.ErrKeyNotFound` | `NotFound` |
| `engine.ErrDuplicateKey` | `AlreadyExists` |
| `engine.ErrReservedField` (data sets `_key` directly) | `InvalidArgument` |

`UpdateIfRev` is **not** an error path: a stale revision or a missing key returns
`swapped=false` with no error (mirroring the engine's `(false, nil)` no-op), so a
client distinguishes "someone else won the race, retry" from "the call failed".

**`key`/`rev` on responses.** `Record` gained `key` (field 5) and `rev` (field 6),
and `InsertResponse`/`UpdateResponse` gained the same pair — all additive field
numbers, so pre-N1 clients are unaffected. The read handlers populate them from
the engine's `Record{ID, Key, Rev, …}` (via `Get`/`GetByKey`) and, for streaming
`Find`, from `ScanResult.Rev` plus the `_key` field carried in `data`. `Watch`
events carry the key but no revision (the change feed does not track it), so their
`rev` is `0`.

---

## Web Admin UI

A browser-based admin UI lives at `clients/web/` (React 18 + TypeScript + Vite + Tailwind CSS, dark theme). It communicates exclusively with the REST gateway at `:8080` — no direct gRPC.

**CORS** — `server/rest.go` includes a CORS middleware that adds the necessary `Access-Control-Allow-*` headers so the browser can reach the gateway from a different origin (e.g., the Vite dev server at `localhost:5173`).

**Watch streaming** — grpc-gateway does not support the server-streaming shape used by the `Watch` RPC. A custom HTTP handler in `server/watch_rest.go` fills this gap: it opens a gRPC `Watch` stream internally and forwards each event to the browser as a `text/event-stream` (ReadableStream).

**Vite dev proxy** — during local development, Vite proxies all `/v1` requests from `localhost:5173` to `localhost:8080`, so no CORS issue arises in the dev workflow. The proxy is configured in `clients/web/vite.config.ts`.

---

## Observability

ScrivaDB exposes Prometheus metrics via a dedicated HTTP server (default `:9090/metrics`):

| Metric | Type | Labels |
|---|---|---|
| `scriva_collection_records_total` | Gauge | `collection` |
| `scriva_collection_segments_total` | Gauge | `collection` |
| `scriva_compaction_runs_total` | Counter | `collection` |
| `scriva_compaction_duration_seconds` | Histogram | `collection` |
| `scriva_grpc_request_duration_seconds` | Histogram | `method`, `code` |
| `scriva_scan_rows_scanned` | Histogram | `collection` |
| `scriva_recovery_total`, `_duration_seconds`, `_bytes_total` | Counter/Histogram | `collection`, `kind` |
| `scriva_integrity_open_total` | Counter | `collection`, `policy`, `outcome` |
| `scriva_integrity_findings_total` | Counter | `collection`, `severity`, `code` |
| `scriva_append_total`, `scriva_append_bytes_total` | Counter | `collection` |
| `scriva_append_errors_total` | Counter | `collection`, `reason` |
| `scriva_segment_poisoned_total` | Counter | `collection` |
| `scriva_dir_lock_total` | Counter | `result` |

Per-collection gauges are sampled at scrape time via a custom `DBCollector`. Compaction metrics are recorded via an `OnCompaction` hook injected into `CollectionConfig` at startup. gRPC request duration is recorded by a unary interceptor chained after the auth interceptor. `scriva_scan_rows_scanned` records the rows examined by each `Find`, fed from the engine's `ScanStats` through a server-layer scan-observer hook (never from inside the engine) — see [Slow-query log & scan stats](#slow-query-log--scan-stats).

The recovery, integrity, append, poison and lock series follow the same rule: the engine only calls `CollectionConfig` hooks (`OnIndexRecovery`, `OnIntegrity`, `OnAppend`, `OnSegmentPoisoned`, `OnLock`) and a `Logger`; the server (and the embedded façade options) inject the Prometheus/`slog` implementations.

### Fail-closed open policy

`recoverIndex` is the single choke point where open might touch history. A tail replay applies only the bytes after the persisted coverage; a strict decode failure there falls through to a full rebuild. Every full rebuild first runs `integrityGate`: one tolerant scan of all segments through the same checks as `Verify` (bad regions, glued lines, duplicate/conflicting ids, revision regressions). Findings of severity `data-corruption` or `conflict` make open return `*engine.OpenIntegrityError` (matches `engine.ErrIntegrity`, carries the `CollectionReport`) under `fail`/`rebuild-index-only`; under `report` the index and secondary indexes are rebuilt from the salvaged records instead and the report is kept on `Collection.OpenIntegrityReport()`. A refused open closes the files it opened and releases the directory lock. Conflicts inside a trusted persisted-index tail are not re-derived at open (that would defeat O(tail) reopen); `Verify` reports them.

For **distributed tracing** (opt-in OpenTelemetry, `--otlp-endpoint`), which complements these pull-based metrics with per-request spans across the gateway → gRPC → engine-scan hops, see [Tracing (OpenTelemetry)](#tracing-opentelemetry) above.
