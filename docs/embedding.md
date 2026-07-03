# Embedding FileDB

FileDB's storage engine is a plain Go library. The `engine`, `store`, and
`query` packages are public, so you can open a database, read and write records,
and watch for changes **entirely in-process** — no server, no gRPC, and no
network. This is the right choice when your program is the only writer and you
want the durability and query model of FileDB without running a separate
daemon.

```go
import "github.com/srjn45/filedbv2/engine"

db, err := engine.Open("./data", engine.CollectionConfig{})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

col, err := db.CreateCollection("tasks") // or db.Collection("tasks") if it exists
if err != nil {
    log.Fatal(err)
}

id, _, err := col.Insert(map[string]any{"title": "write docs"})
```

A zero-valued `CollectionConfig{}` is safe: every field falls back to its
default (see `engine.DefaultWatchBufferSize`, `engine.SyncModeNone`, …).

---

## The `filedb` façade (recommended entry point)

`engine.Open` opens every collection under a single config. When your program
hosts several collections — each wanting its own durability or compaction
settings — the `filedb` package is the ergonomic front door. It opens a store
rooted at a directory, lazily opens-or-creates named collections with
per-collection options, and applies **embedded-friendly defaults** so you don't
have to.

A complete warden-shaped store — `sessions`, `events`, `messages`, `context`,
and a per-write-durable `spend` ledger — stands up in a handful of lines:

```go
import (
    "github.com/srjn45/filedbv2/engine"
    "github.com/srjn45/filedbv2/filedb"
)

db, err := filedb.Open("./data")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

sessions := db.MustCollection("sessions", filedb.WithUniqueIndex("name"))
events   := db.MustCollection("events")
messages := db.MustCollection("messages")
context  := db.MustCollection("context")
spend    := db.MustCollection("spend", filedb.WithCollectionSyncMode(engine.SyncModeAlways))

// CRUD goes straight to the returned *engine.Collection:
id, _, _ := sessions.InsertWithKey("sess-1", map[string]any{"name": "alpha", "status": "open"})
_ = events.Insert /* … */
_, _ = context.Upsert("cfg", map[string]any{"model": "opus"})
_ = id
```

- `Open(dir string, opts ...filedb.Option)` returns a `*filedb.DB`. Existing
  collections on disk are discovered automatically.
- `Collection(name, opts ...filedb.CollectionOption) (*engine.Collection, error)`
  and `MustCollection` (which panics on error, for init-time convenience) open a
  collection. The returned value is a plain `*engine.Collection`, so the full
  keyed / CAS / upsert / Watch API described below is available on it.
- **First call for a name wins.** Repeat calls return the same cached handle and
  ignore their options, so open each collection once at startup.
- Need something the façade doesn't wrap (e.g. `ListCollections`,
  `DropCollection`)? `db.Engine()` returns the underlying `*engine.DB`.

### Options

DB-wide defaults (passed to `filedb.Open`) — `WithSyncMode`, `WithSyncInterval`,
`WithSegmentMaxSize`, `WithCompactInterval`, `WithWatchBufferSize`.

Per-collection overrides (passed to `Collection`/`MustCollection`) —
`WithCollectionSyncMode`, `WithCollectionSyncInterval`,
`WithCollectionSegmentMaxSize`, `WithCollectionCompactInterval`,
`WithCollectionWatchBufferSize`, and `WithUniqueIndex(fields…)` (ensures a unique
secondary index on each field at open time, via `EnsureUniqueIndex`).

### Embedded durability default

The raw engine defaults to `SyncModeNone` — fastest, but a crash can lose
recently acknowledged writes. A DB opened through **`filedb.Open` defaults every
collection to `SyncModeInterval` at a 1s cadence** instead. This trades a bounded
(~1s) durability window for throughput: a crash loses at most the last interval's
writes, while the append-only, temp-then-rename segment format already rules out
torn or partial records. It is the right default for a local, single-writer
daemon that wants crash-safety without paying an `fsync` on every write.

A write path that genuinely needs per-write durability — a spend/ledger
collection, say — opts back in per collection:

```go
spend := db.MustCollection("spend", filedb.WithCollectionSyncMode(engine.SyncModeAlways))
```

`SyncModeAlways` fsyncs before each write is acknowledged; the override is scoped
to that one collection and is not clobbered by the global interval default, even
across reopen.

---

## String keys (caller-supplied primary keys)

`Insert` returns an engine-assigned `uint64` id. When your records already have
a natural string identity — a session id, an agent name, a context key — use the
keyed API to insert and address records by that string directly:

```go
_, _, err := col.InsertWithKey("sess-abc123", map[string]any{"status": "open"})

data, _, err := col.FindByKey("sess-abc123")           // O(1) lookup
_, err = col.UpdateByKey("sess-abc123", map[string]any{"status": "closed"})
err = col.DeleteByKey("sess-abc123")
```

- The key is stored in a reserved `_key` field (`engine.KeyField`) inside the
  record's data, enforced unique by a secondary index that `InsertWithKey`
  creates automatically on the first keyed write.
- Inserting a key that a live record already holds returns `engine.ErrDuplicateKey`.
- `FindByKey` / `UpdateByKey` / `DeleteByKey` on a key with no live record return
  `engine.ErrKeyNotFound`. Match both with `errors.Is`.
- `_key` is reserved: passing it to plain `Insert`/`Update` (or smuggling it into
  the `data` map of a keyed call) is rejected with `engine.ErrReservedField`. A
  record's key is fixed for its lifetime — `UpdateByKey` preserves it.
- Because `_key` is a normal data field, the key survives compaction, index
  rebuild, and reopen, and appears in `WatchEvent.Data["_key"]` on every event
  for that record.

The keyed methods interoperate with the numeric ones: a record inserted with
`InsertWithKey` is still readable by its `uint64` id via `FindByID`, and the
returned data includes the `_key` field.

---

## Revisions and compare-and-swap

Every record carries a monotonic **revision** (`rev`): it is `1` on insert and
increments by one on every update. The revision is available without reading the
record body via `Get`/`GetByKey`, which return a small `Record`:

```go
type Record struct {
    ID   uint64
    Key  string // the caller-supplied _key, or "" if none
    Rev  uint64
    Ts   time.Time
    Data map[string]any
}

rec, err := col.GetByKey("sess-abc123")
// rec.Rev is the current revision; ScanResult also carries Rev.
```

Two conditional-update primitives let you write **without an application-side
lock**. Both address the record by its string key, preserve the key, and apply
their write only if a condition on the *current* record still holds. The
condition check and the write happen in a single critical section, so two
goroutines racing the same swap can never both apply:

```go
// Optimistic-concurrency form: apply only if the record is still at expectedRev.
applied, err := col.UpdateIfRev("sess-abc123", rec.Rev,
    map[string]any{"status": "closed"})

// Predicate form: apply only if the current data still satisfies pred.
applied, err = col.UpdateIfMatch("sess-abc123",
    func(cur map[string]any) bool { return cur["status"] == "running" },
    map[string]any{"status": "exited"})
```

- Both return `(applied bool, err error)`. A **stale revision**, a **false
  predicate**, or a **missing key** is reported as `(false, nil)` — a clean
  no-op, never an error. A non-nil `err` means something actually went wrong
  (e.g. a reserved-field violation or an I/O failure).
- On success the revision bumps by one, exactly as a plain `UpdateByKey` would,
  and a normal update `WatchEvent` is emitted.
- `UpdateIfMatch`'s predicate runs under the collection write lock with the
  current committed data; keep it cheap, and do not call back into the
  collection or retain the map it is passed.

These map directly onto optimistic patterns like "advance status only if it is
still `running`" or "claim this job only if it is unclaimed" without any
read-modify-write race. Revisions survive compaction (the collapsed line keeps
its latest rev), index rebuild (revs are recomputed by replay order), and
reopen.

---

## Upsert (create-or-replace by key)

`Upsert` is a single-call "insert if absent, replace if present" on a string
key. It saves the common get-then-branch dance for archive/move-style flows
(e.g. writing a record to its final, closed state whether or not it already
exists):

```go
// Inserts a new record at rev 1 if "sess-abc123" is free…
rec, err := col.Upsert("sess-abc123", map[string]any{"status": "open"})

// …and replaces it in place (same id, rev bumped) if it already exists.
rec, err = col.Upsert("sess-abc123", map[string]any{"status": "archived"})
```

- The whole present/absent decision and the write happen in one critical
  section, so concurrent upserts on the same key serialise with **no lost
  updates** — the first inserts at `rev 1`, each later one replaces at `rev+1`.
- It returns the resulting `Record{ID, Key, Rev, Ts, Data}`, and emits an
  `OpInsert` `WatchEvent` for a create or an `OpUpdate` one for a replace.
- The key is stamped into `_key`, so passing `_key` inside `data` is rejected
  with `engine.ErrReservedField`; unique indexes on other fields still apply.
- A replace is an ordinary update entry, so the superseded versions collapse to
  a single live line on compaction.

---

## Watching changes (in-process subscriptions)

`Collection.Subscribe` gives you a live feed of every write to a collection.
It is the embedded equivalent of the `Watch` RPC, but without any server: the
events are delivered directly on a Go channel in the same process that performs
the writes.

```go
watcherID, events, cancel := col.Subscribe()
defer cancel() // unregisters the watcher and closes the channel
```

- `events` is a **buffered, receive-only** `<-chan engine.WatchEvent`. Its
  capacity is `CollectionConfig.WatchBufferSize` (default `64`).
- `cancel` removes the subscription and closes `events`. Always call it when you
  are done — a leaked watcher keeps receiving (and can force overflow on) every
  write. `defer cancel()` is the simplest correct pattern.
- Subscribe **before** you perform the writes you care about. Events are emitted
  synchronously on the write path, so anything written before you subscribe is
  not replayed.

Each write emits exactly one event, in commit order (including the individual
inserts/updates/deletes committed by a transaction):

```go
for ev := range events {
    switch ev.Op {
    case store.OpInsert, store.OpUpdate:
        // ev.ID, ev.Data are set
    case store.OpDelete:
        // ev.ID is set; ev.Data is nil
    case engine.OpOverflow:
        // you fell behind — see "The overflow contract" below
    }
}
```

### The `WatchEvent` shape

```go
type WatchEvent struct {
    Op   store.Op       // insert | update | delete | overflow
    ID   uint64         // record id (0 for the overflow sentinel)
    Data map[string]any // the record body; nil for delete and overflow
    Ts   time.Time      // commit timestamp (UTC)
}
```

The `Op` values come from the `store` package, except the watch-only
`engine.OpOverflow` sentinel:

| `Op`                 | Meaning                       | `ID`      | `Data`      |
|----------------------|-------------------------------|-----------|-------------|
| `store.OpInsert`     | a record was created          | record id | record body |
| `store.OpUpdate`     | a record was replaced         | record id | new body    |
| `store.OpDelete`     | a record was removed          | record id | `nil`       |
| `engine.OpOverflow`  | you missed writes — **resync** | `0`       | `nil`       |

`OpOverflow` is never written to a segment; it exists only on the subscription
channel.

### The overflow contract

Subscribers must never block writers. If your consumer falls behind and the
buffered channel fills up, the engine **drops** events rather than stalling the
write path. To make sure a slow consumer knows it lost continuity, the engine
guarantees:

> After one or more events are dropped for a subscriber, that subscriber
> receives **exactly one** `OpOverflow` sentinel — delivered once its channel
> has drained enough to accept it — before normal events resume.

The sentinel means **"there is a gap: you missed an unknown number of writes."**
It does **not** tell you which records changed. The only correct reaction is to
**stop assuming continuity and rebuild your view from a full `Scan`**, then keep
consuming live events from where the feed resumes:

```go
for ev := range events {
    if ev.Op == engine.OpOverflow {
        // Discard any incremental state and re-read the whole collection.
        all, err := col.Scan(nil) // nil filter = match all live records
        if err != nil {
            return err
        }
        rebuildFrom(all)
        continue
    }
    applyIncremental(ev)
}
```

Notes on the guarantee:

- You get **one** sentinel per gap, not one per dropped event. While the
  channel is still backed up, further events continue to be dropped and no
  second sentinel is queued until the first one has been delivered.
- Events received *before* the sentinel are real and ordered; the gap is
  strictly *at* the sentinel. Events received *after* it are again real and
  ordered — you have simply lost the ones in between.
- To reduce the chance of overflow for a bursty writer, either consume in a
  dedicated goroutine that does minimal work per event (hand off to a worker),
  or raise `CollectionConfig.WatchBufferSize`. Overflow handling is still
  required — it is a correctness contract, not merely a tuning knob.

---

## Runnable example

A complete, self-contained program lives at
[`examples/watch`](../examples/watch/main.go). It opens a collection,
subscribes, performs inserts/updates/deletes, and demonstrates reacting to an
`OpOverflow` sentinel with a full-`Scan` resync — all in one process with no
server running:

```bash
go run ./examples/watch
```

There is also an `Example_watch` in the `engine` package
(`engine/example_watch_test.go`) exercised by `make test`.
