package engine_test

import (
	"fmt"
	"os"

	"github.com/srjn45/filedbv2/engine"
)

// Example_watch shows an in-process consumer subscribing to a collection and
// observing insert, update and delete events. Everything runs in the same
// process: there is no server, no gRPC, and no network involved.
func Example_watch() {
	dir, err := os.MkdirTemp("", "filedb-watch-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	col, err := db.CreateCollection("tasks")
	if err != nil {
		panic(err)
	}

	// Subscribe before writing so that no events are missed. The returned
	// channel is buffered (WatchBufferSize); cancel unregisters the watcher
	// and closes the channel.
	_, events, cancel := col.Subscribe()
	defer cancel()

	// Every write path emits exactly one event, in commit order.
	id, _, _ := col.Insert(map[string]any{"title": "write docs"})
	ev := <-events
	fmt.Printf("%s title=%v\n", ev.Op, ev.Data["title"])

	_, _ = col.Update(id, map[string]any{"title": "write docs", "done": true})
	ev = <-events
	fmt.Printf("%s done=%v\n", ev.Op, ev.Data["done"])

	_ = col.Delete(id)
	ev = <-events
	// A delete event carries the id but no Data.
	fmt.Printf("%s id=%d data=%v\n", ev.Op, ev.ID, ev.Data)

	// Output:
	// insert title=write docs
	// update done=true
	// delete id=1 data=map[]
}
