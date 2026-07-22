// Command watch is a self-contained example of consuming FileDB's change feed
// entirely in-process — no server, no gRPC, no network. It opens a collection,
// subscribes with Collection.Subscribe, performs writes, and demonstrates how a
// consumer must react to the OpOverflow resync sentinel.
//
// Run it with:
//
//	go run ./examples/watch
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/srjn45/scriva/engine"
)

func main() {
	dir, err := os.MkdirTemp("", "filedb-watch-example")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// A tiny watch buffer makes the overflow path easy to demonstrate below.
	db, err := engine.Open(dir, engine.CollectionConfig{WatchBufferSize: 4})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	col, err := db.CreateCollection("tasks")
	if err != nil {
		log.Fatal(err)
	}

	normalConsumer(col)
	overflowContract(col)
}

// normalConsumer subscribes and observes an insert, an update and a delete.
// Each write path emits exactly one WatchEvent, in commit order.
func normalConsumer(col *engine.Collection) {
	fmt.Println("== normal consumer ==")

	// Subscribe before writing so no events are missed. cancel unregisters the
	// watcher and closes the channel — always call it (defer is fine).
	_, events, cancel := col.Subscribe()
	defer cancel()

	id, _, err := col.Insert(map[string]any{"title": "write docs"})
	if err != nil {
		log.Fatal(err)
	}
	ev := <-events
	fmt.Printf("  %-6s id=%d title=%v\n", ev.Op, ev.ID, ev.Data["title"])

	if _, err := col.Update(id, map[string]any{"title": "write docs", "done": true}); err != nil {
		log.Fatal(err)
	}
	ev = <-events
	fmt.Printf("  %-6s id=%d done=%v\n", ev.Op, ev.ID, ev.Data["done"])

	if err := col.Delete(id); err != nil {
		log.Fatal(err)
	}
	ev = <-events
	// A delete event carries the id but no Data.
	fmt.Printf("  %-6s id=%d data=%v\n", ev.Op, ev.ID, ev.Data)
}

// overflowContract demonstrates the slow-consumer guarantee: if a subscriber
// falls behind and its buffer fills, the engine drops events rather than
// blocking writers, then delivers exactly one OpOverflow sentinel once the
// channel drains. The consumer must treat that sentinel as "I missed writes"
// and rebuild its view from a full Scan rather than assuming continuity.
func overflowContract(col *engine.Collection) {
	fmt.Println("== overflow contract ==")

	_, events, cancel := col.Subscribe()
	defer cancel()

	// Flood the 4-slot buffer without reading: the first few events are
	// buffered, the rest are dropped and the watcher is marked overflowed.
	for i := 0; i < 20; i++ {
		if _, _, err := col.Insert(map[string]any{"n": float64(i)}); err != nil {
			log.Fatal(err)
		}
	}

	// Drain what was buffered. These are real events that arrived before the
	// gap; the overflow sentinel has not been queued yet.
	buffered := drain(events)
	fmt.Printf("  received %d buffered events before the gap\n", buffered)

	// The next write has room in the drained channel, so it flushes the
	// OpOverflow sentinel ahead of the new event.
	if _, _, err := col.Insert(map[string]any{"n": float64(99)}); err != nil {
		log.Fatal(err)
	}

	// Consume the tail. A real consumer would range over the channel in a
	// dedicated goroutine; here the feed is quiescent, so a non-blocking drain
	// is enough to make the demo deterministic.
	for {
		select {
		case ev := <-events:
			if ev.Op == engine.OpOverflow {
				// The correct reaction: do NOT assume continuity. Rebuild the
				// view by re-reading the whole collection.
				all, err := col.Scan(nil)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("  OVERFLOW: missed writes — resynced via Scan, %d live records\n", len(all))
				continue
			}
			fmt.Printf("  %-6s id=%d n=%v (post-resync tail)\n", ev.Op, ev.ID, ev.Data["n"])
		default:
			return
		}
	}
}

// drain reads every event currently buffered on ch without blocking and returns
// how many it read.
func drain(ch <-chan engine.WatchEvent) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}
