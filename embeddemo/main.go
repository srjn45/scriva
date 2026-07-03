// Command embeddemo is a minimal example of embedding the FileDB storage
// engine directly into a Go program. It imports only the public engine
// package — no grpc, protobuf, prometheus, or cobra — and exercises the basic
// open/insert/read lifecycle.
//
// Its real job is to run in CI: if the engine ever grows a server-only
// dependency, this module will fail to build in isolation (see `make
// deps-check`), catching the regression before it ships.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/srjn45/filedbv2/engine"
)

func main() {
	dir, err := os.MkdirTemp("", "embeddemo-")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	users, err := db.CreateCollection("users")
	if err != nil {
		log.Fatalf("create collection: %v", err)
	}

	id, _, err := users.Insert(map[string]any{"name": "ada", "role": "engineer"})
	if err != nil {
		log.Fatalf("insert: %v", err)
	}

	rec, _, err := users.FindByID(id)
	if err != nil {
		log.Fatalf("find: %v", err)
	}

	fmt.Printf("embedded engine OK: inserted id=%d record=%v\n", id, rec)
}
