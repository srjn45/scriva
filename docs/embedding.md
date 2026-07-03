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
