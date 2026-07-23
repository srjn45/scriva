# Encryption at Rest — Design Proposal

> **Status:** Design *review-complete* — *not yet implemented*. It describes
> transparent field- and record-level encryption of data written to local
> segment files, targeting the embedded/local use case. The design (§1–13) is
> unblocked; all open questions are resolved (§13). **A fresh agent picking this
> up for implementation should read §14 (Implementation plan & handoff) — it is
> the self-contained build runbook (staging, git/workflow constraints, repo
> gotchas).** Only the human go-ahead to start building is pending.

---

## 1. Motivation & threat model

ScrivaDB is primarily an **embedded, local-storage** engine: an
application calls `scriva.Open(...)` and reads/writes structured data through the
façade. In that model the "client" and the "server" are the **same process**, so
there is no separate untrusted caller to bypass — the engine write path is the
one and only route to disk, and therefore the correct place to enforce an
encryption invariant.

We want: **sensitive data written to the local NDJSON segment files is
unreadable without a key**, at two granularities —

- **Field-level** — encrypt specific fields (`password`, `ssn`, `api_token`)
  while the rest of the record stays plaintext and queryable.
- **Record-level** — encrypt the whole record into a single blob, keeping only
  an explicit allow-list of index columns in plaintext.

### In scope (what this defends)

- A stolen laptop or disk.
- A leaked or copied backup tarball / filesystem snapshot.
- Another OS user or process reading the `data/` directory or `cat`-ing the
  `seg_*.ndjson` files.
- A curious/malicious operator inspecting files at rest.

### Explicitly out of scope (non-goals)

- **A compromised running process.** While the DB is open, the key and decrypted
  plaintext live in memory; at-rest encryption cannot defend against a process
  that has already been compromised. Accept this explicitly.
- **Network confidentiality.** Handled separately by TLS/mTLS on the gRPC/REST
  surface (see `architecture.md` → Network Layer). A deployment that must hide
  data from its *own* server should layer client-side encryption on top; that is
  the operator's choice and orthogonal to this feature.
- **Searching on encrypted fields.** Encrypted fields are fully opaque on disk
  (see §5). Equality lookup via a blind index is a possible future addition, not
  part of this proposal.

---

## 2. Core principle: ciphertext lives in the stored map

Encrypt/decrypt happens **only at the collection API boundary** (`Insert`,
`Update`, `Upsert`, `Get`, `ScanStream`). The `store.Entry.Data` map that is
serialized to the segment always holds **ciphertext** for encrypted fields. This
keeps every downstream subsystem key-oblivious:

| Subsystem | Effect |
|---|---|
| **Compaction** | Rewrites entries through `store.Encode` — copies opaque ciphertext blobs, never needs the key. |
| **Index rebuild** | Reads ciphertext from segments; safe because encrypted fields are never indexed (§5). |
| **CRC32C checksum** | Computed over the stored (ciphertext) form — coexists with the AEAD tag; still detects bit-rot. |
| **Primary index** | Stores only `{segment, offset}` — no record data, nothing to encrypt. |
| **Backup / snapshot** | A tar of the segment files is therefore *already encrypted*. See §8. |
| **Watch events** | Emitted from the write path where plaintext is in hand; in-process subscribers (same trust domain) see plaintext. |

The only components that ever touch the key are the encryption module and the
collection read/write boundary that calls it.

---

## 3. Envelope format

Each encrypted value is a self-describing string:

```
enc:v1:<key-id>:<base64url( nonce || AEAD-ciphertext-with-tag )>
```

The `enc:v1:` shown here is **illustrative**; the real marker is a distinctive,
rare reserved magic (not this human-readable prefix), and user writes whose value
begins with it are rejected — see §13 #12, which makes the marker an infallible
"is this encrypted?" discriminator on read.

- marker — reserved sentinel so a value can be recognized as encrypted without
  out-of-band schema (also lets decrypt detect an *un*-encrypted legacy value and
  pass it through — see §9).
- `v1` — envelope version, for future format changes.
- `<key-id>` — identifies which key encrypted this value, enabling **key
  rotation**: old segments keep their old-key blobs; new writes use the current
  key; a decrypt selects the key by id. (See §7.)
- Payload — `nonce ‖ ciphertext‖tag`, base64url-encoded.

### Cipher

**XChaCha20-Poly1305** (`golang.org/x/crypto/chacha20poly1305`):

- AEAD — confidentiality **and** tamper-detection in one primitive; a modified
  ciphertext is *rejected*, never silently decrypted to garbage.
- 192-bit (24-byte) nonce — a fresh `crypto/rand` nonce per write is safe with no
  counter bookkeeping. ScrivaDB's append-only model produces a new write (hence a
  new nonce) on every insert/update, so nonce reuse is a non-issue.

AES-256-GCM is a reasonable stdlib-only alternative (96-bit nonce, hardware
accel), but the passphrase path already depends on `x/crypto` for Argon2id, so
XChaCha20-Poly1305 (same module) is the chosen cipher — see §13 #3.

The AEAD **associated data** binds each blob to its context — `collection ‖ field
name` (field-level) or `collection ‖ "secure_data"` (record-level) — so a
ciphertext cannot be copied from one field/collection into another and still
decrypt.

---

## 4. Two modes, one primitive

Both granularities apply the same encrypt-value primitive with different field
selection, expressed by a per-collection `EncryptionPolicy`:

### Field-level (deny-list)

```
policy: { mode: fields, fields: ["password", "ssn"] }

on disk:  {"email":"a@b.com","password":"enc:v1:k1:aGVs...","age":30}
in app:   {"email":"a@b.com","password":"hunter2","age":30}
```

Non-listed fields stay plaintext and fully queryable/indexable.

### Record-level (allow-list / envelope)

```
policy: { mode: record, index_fields: ["id_col", "tenant"] }

on disk:  {"id_col":"u-123","tenant":"acme","secure_data":"enc:v1:k1:9f8a..."}
in app:   {full record}
```

The whole data map (minus the allow-listed index fields and the reserved `_key`)
is serialized, encrypted, and stored under `secure_data`. On read it is
decrypted and merged back. Only the allow-listed fields are queryable.

The policy is persisted in the collection's `meta.json` so it survives restart
and is applied consistently by every writer.

---

## 5. Queryability constraint (the one real tradeoff)

An encrypted field is opaque, so the engine cannot filter, range, sort, or index
on it. Enforced at three points:

1. **`EnsureIndex` / `EnsureUniqueIndex` on an encrypted field is rejected**
   (typed `ErrFieldEncrypted`). Otherwise the plaintext value would be persisted
   into `sidx_<field>.json` and leak straight back onto disk.
2. **Filters / sorts referencing an encrypted field are rejected** at scan
   planning (`InvalidArgument`), rather than silently never-matching.
3. **`ScanStream` decrypts a record's blobs *after* filtering/projection** on the
   plaintext fields, immediately before the record is yielded to the caller — so
   the plaintext form of an encrypted field never participates in a query.

The reserved `_key` field (used for keyed CRUD via its unique index) **must stay
plaintext** and cannot be listed as encrypted — keyed lookups depend on it. This
is validated when the policy is set.

For fields you never query *by* (passwords, tokens, secrets), this constraint
costs nothing. Equality lookup on an encrypted field — via a blind index
(`HMAC(key, value)` in a separate indexed column) — is deferred; it is a
strictly additive future change and does not affect this design.

---

## 6. Key management

Since the deployment is embedded/local, the application supplies the key at open
time. Three converging entry points feed one internal `Keyring`:

```go
// Raw 32-byte key — for apps that already manage keys (e.g. from a KMS).
scriva.Open(dir, scriva.WithEncryptionKey(key []byte))

// Human-supplied passphrase — derived via Argon2id with a per-DB random salt
// persisted in meta.json.
scriva.Open(dir, scriva.WithPassphrase(pw string))

// Pluggable provider — power users wire in OS keychain / Vault / KMS.
scriva.Open(dir, scriva.WithKeyProvider(p KeyProvider))
```

```go
type KeyProvider interface {
    // Current returns the key to encrypt new writes with, and its id.
    Current(ctx context.Context) (id string, key []byte, err error)
    // ByID returns a (possibly retired) key for decrypting old blobs.
    ByID(ctx context.Context, id string) (key []byte, err error)
}
```

`WithEncryptionKey` and `WithPassphrase` are thin built-in providers over a
static keyring.

### Wrong-key detection

On `Open`, a **key-check value** stored in `meta.json` — an AEAD-encrypted known
constant under the current key — is verified. A wrong key/passphrase fails fast
with a clear `ErrWrongEncryptionKey`, instead of producing garbage decrypts on
the first read.

### `meta.json` additions

```jsonc
{
  // ...existing fields...
  "encryption": {
    "policy":     { "mode": "fields", "fields": ["password"] },
    "kdf":        { "algo": "argon2id", "salt": "<base64>", "params": {...} },
    "key_check":  "enc:v1:k1:<base64>",
    "current_key_id": "k1"
  }
}
```

The KDF salt and key-check are non-secret and safe to store in cleartext; they
reveal nothing without the passphrase.

---

## 7. Key rotation

Because every blob carries its `<key-id>`, rotation is incremental and
non-blocking:

1. Add a new current key (`k2`) via the provider; new writes encrypt under `k2`.
2. Old segments keep `k1` blobs; decrypt selects the key by id, so reads keep
   working across both.
3. A **re-encrypting compaction pass** (optional, operator-triggered) rewrites
   old entries under the current key, after which `k1` can be retired. This reuses
   the existing compaction machinery — it already rewrites every surviving entry
   through `Encode`; the only addition is decrypt-with-old / encrypt-with-current
   in that rewrite. Until it runs, both keys must remain available.

---

## 8. Backup implications

`DB.SnapshotTo` tars the on-disk segment files, which are already ciphertext — so
**backups are encrypted for free**, and a leaked backup tarball is useless
without the key. Two consequences to document loudly for operators:

- **The key must be backed up separately.** A backup without the key (or
  passphrase) is permanently unrecoverable — this is the point, but it is also a
  foot-gun.
- `meta.json` (KDF salt + key-check + policy) is included in the snapshot, so a
  restored collection knows how to derive/verify the key and which fields are
  encrypted. It contains no secret material.

---

## 9. Backward compatibility & migration

- **Legacy plaintext records.** A value without the `enc:` sentinel is passed
  through unchanged on read, so a collection that predates encryption keeps
  working. Enabling a policy encrypts data **going forward**; historical
  plaintext is upgraded lazily as records are rewritten (update) or eagerly by a
  one-shot re-encrypting compaction pass (§7).
- **Disabling encryption** is symmetric: turn the policy off and let compaction
  rewrite blobs back to plaintext (requires the key to still be available).
- **CRC & existing entries.** Encryption changes only the *value bytes* inside
  `data`; the entry envelope (`id`, `op`, `rev`, `expires_at`, `crc`) is
  unchanged, and the CRC is recomputed over the ciphertext form on encode — no
  segment-format version bump is required.

---

## 10. Configuration surface

**Embedded façade** (root package `scriva`, `scriva.go`):

```go
db, _ := scriva.Open("./data",
    scriva.WithPassphrase(os.Getenv("SCRIVA_PASSPHRASE")),
    scriva.WithCollectionEncryption("users",
        scriva.EncryptFields("password", "ssn")),      // field-level
    scriva.WithCollectionEncryption("audit",
        scriva.EncryptRecord("id", "tenant")),          // record-level, plaintext index cols
)
```

**Server** (`cmd/scriva`): out of scope for the primary use case, but the same
policy could be expressed in `server` config (`keys`-style YAML) if a networked
deployment wants at-rest encryption; the key would come from an env var /
`--encryption-passphrase` / file. Left as a follow-up.

---

## 11. Where it hooks in (implementation sketch)

Following the repo's layering discipline (`make deps-check`: `engine`/`store`/
`query` stay dependency-light):

- **New package `crypto/` (or `engine/enc.go`)** — the AEAD primitive, envelope
  format, `Keyring`, `KeyProvider`, Argon2id KDF. Self-contained.
- **`engine/collection_config.go`** — add `EncryptionPolicy` + `Keyring` to
  `CollectionConfig`.
- **`engine/collection.go`** — encrypt on the `Insert`/`Update`/`Upsert` write
  path (before building `store.Entry`); decrypt in `Get`.
- **`engine/scan.go`** — decrypt in `ScanStream` after filter/projection; reject
  filters/sorts on encrypted fields at planning.
- **`engine/secondary_index.go`** — reject `EnsureIndex` on encrypted fields.
- **`engine/meta.go`** — persist/load the `encryption` block; verify key-check on
  open.
- **`store/`** — **unchanged.** It only ever sees ciphertext; this is deliberate.

---

## 12. Policy changes & migration

Adding or removing a field from the encryption list (and key rotation, §7) needs
existing records brought to the new configuration. The design does this
**without a big-bang migration and without a separate metadata collection** —
per-record encryption state is carried by the value itself.

### The read invariant (policy-independent)

Reads are driven by the per-value **`enc:` sentinel**, not by the current policy:

> A value that begins with `enc:` is decrypted (key selected by its `<key-id>`);
> any other value is passed through as plaintext.

Because each value self-describes whether it is encrypted, a collection in *any*
mixed state — legacy plaintext, new ciphertext, half-migrated — reads correctly
with zero coordination. The policy governs only **writes** (what to encrypt going
forward), never reads. No side-table is needed, and none can drift out of sync
with the data, since the marker is physically attached to the value.

### How records migrate

1. **Lazily, via the append-only log.** Every update re-encodes the record under
   the *current* policy, so hot records migrate themselves just by being written.
2. **In bulk, via compaction.** The compactor already rewrites every surviving
   entry; migration free-rides on that scan, encrypting/decrypting fields to match
   the current policy as it rewrites. A forced `CompactNow` migrates eagerly on
   demand. This is the same machinery as key rotation (§7) and enable/disable
   (§9).

So a field add/remove is: update the policy in `meta.json` → new writes conform
immediately → compaction backfills the tail → reads were correct throughout. No
downtime.

### Two notions of "done"

A policy `epoch` (a counter in `meta.json`, bumped on each policy change) is
mirrored onto each entry and onto the in-memory `IndexEntry` — exactly as `rev`
and `expires_at` already are — so completion can be judged without extra I/O:

| Completion | Condition | Cost | Gates |
|---|---|---|---|
| **Functional** — reads correct under new config | every **live** `IndexEntry` is at the current epoch | O(n) in-memory index walk, **no disk reads** | knowing the DB behaves per the new config |
| **Security** — no old-form bytes at rest | functional **+** stale entries reclaimed by compaction | one compaction pass | retiring an old key; proving no plaintext remains; making a de-encrypted field indexable |

Only **live** records matter for *functional* completeness — stale (superseded)
rows are never read, so checking them for correctness is wasted work. But a stale
row still **physically holds its old-form bytes on disk** (old plaintext, or
old-key ciphertext) until compaction reclaims it, so the *security* guarantee is
not met until those entries are purged. Since compaction is also the migration
engine, one completed forced pass delivers both states at once: it rewrites live
rows to the new policy **and** drops the stale ones.

Consequently, operations that depend on old bytes being physically gone —
retiring an old key, confirming no plaintext remains for a newly-encrypted field,
or building an index on a field just removed from encryption — are gated on
**security** completion (a compaction pass), not merely the live-index check.

### Caveats

- **Key availability.** Any key that still encrypts on-disk blobs (live *or*
  stale) must remain resolvable via the `KeyProvider` until migration reaches
  security completion; only then may it be retired.
- **Marker collision.** A literal plaintext value beginning with the reserved
  marker is **rejected on write** (`ErrReservedPrefix`), so it can never be misread
  as ciphertext on read — the marker is an infallible discriminator. See §13 #12.

## 13. Design decisions

All questions raised during design review are resolved below; each entry records
the decision and its rationale. Items marked **DEFER** are intentional
future-work scoping, not unknowns.

1. **Blind index — ✅ RESOLVED: DEFER.** Follows directly from the design's
   "fully opaque encrypted fields" decision (no equality search on encrypted
   fields). Blind-index equality lookup is a strictly-additive future extension.
2. **Argon2id parameters — ✅ RESOLVED.** Baseline defaults: memory 64 MiB,
   iterations 3, parallelism 1, 32-byte key, 16-byte random salt (OWASP-style),
   tunable via an advanced option for constrained/large hardware.
3. **Cipher choice — ✅ RESOLVED: XChaCha20-Poly1305.** The passphrase path
   already depends on `golang.org/x/crypto` (Argon2id), so AES-256-GCM's
   stdlib-only advantage buys nothing; XChaCha20-Poly1305 (same module) is the
   consistent choice, and its 192-bit random nonce needs no counter bookkeeping.
4. **Server-side key provisioning — ✅ RESOLVED: DEFER.** The embedded façade
   options cover the primary use case; add `--encryption-passphrase` / env / file
   if and when the networked path adopts at-rest encryption.
5. **Key scope — ✅ RESOLVED: one DB-wide keyring.** A single keyring (with
   `key-id`s for rotation) protects every collection — simplest to configure and
   reason about. The `key-id` scheme leaves room to add per-collection keys later
   with no format change, so this is not a one-way door.

### Architectural (resolved during review)

6. **Replication key distribution — ✅ RESOLVED.** Replication ships the stored
   **ciphertext** form (plaintext never crosses the wire); followers apply entries
   **verbatim** without re-encrypting and need **no key to replicate** (encrypted
   fields aren't indexed; `_key`/index columns are plaintext). Keys are provisioned
   **out of band, per node** — the replication protocol and snapshot carry the
   policy, KDF salt, and `key_check` but **never key material**; the restored
   `key_check` lets a follower verify it holds the right key. Followers' keyrings
   must track the leader's `key-id`s across rotation (a blob under an unresolvable
   `key-id` fails that read with a typed error). **Keyless followers are allowed**
   as encrypted cold-standbys: they store and forward ciphertext, serve reads of
   non-encrypted fields, return a typed error on encrypted-field reads, and must be
   given the key before promotion to leader.
7. **Scope of an encrypted "field" — ✅ RESOLVED.** **Top-level field names only
   for v1.** A top-level field's value is encrypted *whole* — a scalar, or an
   entire nested object/array serialized to one blob and decrypted back on read —
   so "encrypt this nested object" is already covered when the sensitive data sits
   under a single top-level key. The write/read transform stays non-recursive
   (`data[field]`). "Some leaves sensitive, siblings queryable under a shared
   parent" is modeled by flattening the sensitive leaf to the top level (free under
   the schemaless model) or by record-level mode. Encrypting a whole subtree also
   leaks *less* structure than per-leaf encryption (see #8). Reserved
   `_key`/`id`/`rev` remain non-encryptable. **Dotted-path / nested-leaf
   encryption is a deferred future extension** (needs a path syntax, array
   handling, and decrypt-and-merge-back).
8. **Metadata leakage — ✅ RESOLVED.** Accepted and documented; **no padding in
   v1.** The explicit guarantee boundary:
   - **Protected:** the plaintext *value* of every encrypted field.
   - **Visible on disk:** field-level mode — the encrypted field's *name* and its
     ciphertext *length*; record-level mode — the `secure_data` blob *length* and
     the allow-listed plaintext index columns; both — record count and field
     presence/absence.
   - **Guidance:** fixed-format secrets (SSN, card numbers, fixed tokens) leak
     nothing via length; for variable-length secrets where length is sensitive,
     use record-level mode. Optional length-bucketing/padding is a **future
     enhancement**, and field-name hiding is already available via record-level
     mode. This is a conscious, documented trade-off matching industry norm
     (LUKS/TDE/field-level libraries do not pad by default).
9. **Decrypt-failure semantics — ✅ RESOLVED.** Two typed errors:
   `ErrKeyUnavailable` (blob's `key-id` unresolvable — operational, retry after
   providing the key; expected on a keyless follower) vs `ErrDecryptFailed` (AEAD
   tag mismatch or malformed envelope — an integrity/tamper event, mirroring
   `store.ErrCorruptEntry`); both carry context (collection, id, field,
   segment/offset, key-id). **Lazy decrypt** — only encrypted fields that survive
   projection and are actually returned are decrypted, so a query returning no
   encrypted field needs no key and cannot fail here (this is what lets keyless
   followers serve most reads). **Fail-closed** — `Get`/`ScanStream` propagate the
   typed error with context; no silent skip, redaction, or partial results,
   consistent with existing CRC (`ErrCorruptEntry`) behavior. A re-encrypting
   compaction pass that cannot decrypt an old blob leaves the entry untouched
   (still valid under its old key) and reports — never drops data. An optional
   skip-and-report scan mode is a deferred future enhancement.

### Minor — resolved by the queryability rule

10. **Aggregate / Count / projection on encrypted fields — ✅ RESOLVED.** Follows
    from the opaque-field rule: reject numeric aggregations (`sum`/`avg`/`min`/
    `max`) and `eq`-count on an encrypted field (as for filters/sorts/index);
    projection decrypts at yield, only for encrypted fields it selects (per #9's
    lazy decrypt).
11. **Watch plaintext exposure — ✅ RESOLVED: document.** `Watch` emits decrypted
    data to in-process subscribers (same trust domain); for a networked subscriber
    the plaintext relies on transport TLS (consistent with the §1 threat model). A
    keyless follower's Watch handles encrypted fields exactly as reads do (#9 lazy
    decrypt + `ErrKeyUnavailable`). No mechanism change.
12. **Marker collision — ✅ RESOLVED: reserve + reject on write.** Use a
    distinctive, rare magic marker (not the human-readable `enc:v1:` shown in
    examples) and **reject any user write whose value begins with it**
    (`ErrReservedPrefix`), so the marker is an infallible discriminator on read —
    collisions are prevented, not merely detected. AEAD failure (#9
    `ErrDecryptFailed`) remains the backstop for corruption/tampering, not for
    collision.

---

## 14. Implementation plan & handoff

> This section is the operational runbook: everything a fresh agent needs to
> **build** this feature that is not already captured in the design above. The
> design (§1–13) is review-complete and unblocked; only the human go-ahead to
> start building is pending. **Nothing here changes the design — it records
> sequencing, constraints, and known gotchas.**

### 14.1 Status

- **Design:** complete, all 12 questions resolved (§13). No open blockers.
- **Implementation:** **not started.** No `crypto/` package, no engine hooks, no
  façade options exist yet.
- **Next step:** on explicit user go-ahead, run the staged pipeline in §14.2.

### 14.2 Suggested build staging (one PR per stage)

Each stage is a short-lived agent in its own **isolated git worktree**, landing
**one PR**, tests green (`make test`, race detector on) before the next starts.
Stages are ordered by dependency; do not parallelize 1→2→3.

1. **`crypto/` primitive** — AEAD (XChaCha20-Poly1305), envelope encode/decode
   (`<magic>:v1:<key-id>:<b64url(nonce‖ct+tag)>`), `Keyring`, `KeyProvider`
   interface, Argon2id KDF (64 MiB / 3 / 1 / 32B key / 16B salt), the typed
   errors (`ErrReservedPrefix`, `ErrKeyUnavailable`, `ErrDecryptFailed`,
   `ErrWrongEncryptionKey`, `ErrFieldEncrypted`). Fully unit-tested, self-contained,
   no engine imports. **Maps to §3, §6, §9, §13 #2/#3/#12.**
2. **Engine hooks** — encrypt/decrypt at the collection boundary
   (`engine/collection.go` Insert/Update/Upsert/Get, `engine/scan.go` ScanStream
   after filter/projection), `EncryptionPolicy` + `Keyring` on
   `engine/collection_config.go`, `meta.json` `encryption` block + wrong-key
   check on open (`engine/meta.go`), reject index/filter/sort/aggregate on
   encrypted fields (`engine/secondary_index.go`, scan planning). **Maps to §2,
   §5, §11.** `store/` stays **unchanged** (it only sees ciphertext).
3. **Migration + re-encrypting compaction** — policy `epoch` mirrored onto
   `IndexEntry`; lazy migration via append; bulk migration + key rotation +
   enable/disable via the re-encrypting compaction pass; functional vs security
   completion checks. **Maps to §7, §9, §12.**
4. **Façade options + docs** — `scriva.WithEncryptionKey` / `WithPassphrase` /
   `WithKeyProvider`, `WithCollectionEncryption` + `EncryptFields` /
   `EncryptRecord` on `scriva.go`; update `docs/getting-started.md`,
   `docs/architecture.md`, `README.md` key-properties, and mark the `ROADMAP.md`
   item done (per CLAUDE.md doc conventions). **Maps to §6, §10.**

### 14.3 Workflow constraints (must obey)

- **Git commit email MUST be `29410402+srjn45@users.noreply.github.com`** — any
  other author email causes the push to be rejected.
- Each stage runs as a **warden agent in an isolated worktree**, landing exactly
  **one PR**. Do not bundle stages.
- Conventional commits (`feat(crypto):`, `feat(engine):`, `docs:` …) per CLAUDE.md.
- `make test` (race detector, `-count=1`) must pass before opening each PR;
  integration tests use a real in-process gRPC server — never mock the engine.

### 14.4 Repo-state gotchas for the next agent

- **Rebrand landed.** The façade is the root package `scriva` with `scriva.Open`
  (commit `cf5652f`). All code samples in this doc use `scriva.*` accordingly.
  The API is **frozen at v1.0.0** — new encryption options are additive.
- **Illustrative marker.** `enc:v1:` throughout this doc is a readable stand-in;
  the real marker is a distinctive reserved magic string (§13 #12). Choose it in
  stage 1 and reject user writes bearing it (`ErrReservedPrefix`).
- **Concurrency.** If other agents share this repo path, check
  `who_is_editing_file` before touching shared engine files
  (`engine/collection.go`, `engine/meta.go`, `engine/scan.go`, `scriva.go`) and
  reconcile via the inbox before committing.

### 14.5 Definition of done (whole feature)

Field- and record-level encryption configurable via the façade; encrypted values
opaque on disk under the reserved marker; wrong-key fails fast on open; reads
policy-independent and fail-closed; rotation and enable/disable work via
compaction; all four doc surfaces updated and the ROADMAP item checked. Every
stage's PR merged with `make test` green.
