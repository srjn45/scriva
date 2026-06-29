BINARY_DIR   := bin
FILEDB       := $(BINARY_DIR)/filedb
FILEDB_CLI   := $(BINARY_DIR)/filedb-cli
PROTO_DIR    := proto
PROTO_GEN    := internal/pb

GO           := go
GOFLAGS      := -trimpath
LDFLAGS      := -s -w

.PHONY: all build proto openapi test bench lint run cli clean release help

all: build

## build: compile both binaries into bin/
build:
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(FILEDB)     ./cmd/filedb
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(FILEDB_CLI) ./cmd/filedb-cli
	@echo "Built: $(FILEDB) and $(FILEDB_CLI)"

## proto: generate Go code from proto/filedb.proto (requires buf)
proto:
	@which buf > /dev/null 2>&1 || (echo "ERROR: buf not found. Install: https://buf.build/docs/installation" && exit 1)
	buf generate
	@echo "Proto generated in $(PROTO_GEN)/proto"

## openapi: generate the OpenAPI/Swagger spec from proto into docs/openapi (requires buf)
openapi:
	@which buf > /dev/null 2>&1 || (echo "ERROR: buf not found. Install: https://buf.build/docs/installation" && exit 1)
	buf generate
	@echo "OpenAPI spec generated in docs/openapi/"

## test: run all tests with race detector and coverage
test:
	$(GO) test ./... -race -count=1 -coverprofile=coverage.out
	$(GO) tool cover -func=coverage.out | tail -1

## bench: run engine microbenchmarks (insert/find/scan) with allocation stats
bench:
	$(GO) test ./internal/engine -bench '.' -benchmem -run '^$$'

## lint: run golangci-lint
lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "Install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## run: start the filedb server (requires bin/filedb)
run: build
	$(FILEDB) serve --data ./data --api-key dev-key

## cli: start the interactive CLI
cli: build
	$(FILEDB_CLI) --api-key dev-key

## release: dry-run goreleaser snapshot build
release:
	@which goreleaser > /dev/null 2>&1 || (echo "Install goreleaser: https://goreleaser.com/install/" && exit 1)
	goreleaser release --snapshot --clean

## clean: remove build artifacts
clean:
	rm -rf $(BINARY_DIR) coverage.out dist/

## help: list available targets
help:
	@grep -E '^## ' Makefile | sed 's/## //' | column -t -s ':'
