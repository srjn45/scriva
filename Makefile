BINARY_DIR   := bin
FILEDB       := $(BINARY_DIR)/filedb
FILEDB_CLI   := $(BINARY_DIR)/filedb-cli
PROTO_DIR    := proto
PROTO_GEN    := internal/pb

GO           := go
GOFLAGS      := -trimpath
LDFLAGS      := -s -w

# Packages that must stay embeddable (no server-only dependencies) and the
# module-path fragments they are forbidden from pulling in. Keep in sync with
# the embeddemo module and the deps-check CI job.
EMBED_PKGS    := ./engine ./store ./query
FORBIDDEN_DEPS := grpc|protobuf|prometheus|cobra|grpc-gateway

.PHONY: all build proto openapi test bench fuzz lint deps-check run cli clean release help

# FUZZTIME controls how long each fuzz target runs (override on the CLI).
FUZZTIME     ?= 10s

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

## fuzz: run engine crash-recovery + compaction fuzz targets (FUZZTIME=10s)
fuzz:
	$(GO) test ./internal/engine -run '^$$' -fuzz 'FuzzSegmentRecovery$$' -fuzztime=$(FUZZTIME)
	$(GO) test ./internal/engine -run '^$$' -fuzz 'FuzzCompaction$$'      -fuzztime=$(FUZZTIME)

## lint: run golangci-lint
lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "Install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## deps-check: assert the embeddable engine packages stay free of server-only deps
deps-check:
	@echo "Checking $(EMBED_PKGS) for forbidden dependencies ($(FORBIDDEN_DEPS))..."
	@if $(GO) list -deps $(EMBED_PKGS) | grep -E -i '$(FORBIDDEN_DEPS)'; then \
		echo "ERROR: engine/store/query must not depend on grpc, protobuf, prometheus, cobra, or grpc-gateway."; \
		echo "The engine is meant to be embeddable; metrics enter only via the OnCompaction hook."; \
		exit 1; \
	fi
	@echo "OK: no forbidden dependencies in the embeddable packages"
	@echo "Building embeddemo/ (engine-only consumer)..."
	@cd embeddemo && $(GO) build -o /dev/null ./...
	@echo "OK: embeddemo builds against the public engine package"

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
