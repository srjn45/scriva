---
title: Install
description: Install the FileDB v2 server and CLI — prebuilt binary, Docker, package managers, or build from source.
---

FileDB v2 ships two binaries: **`filedb`** (server) and **`filedb-cli`**
(client). Pick whichever install path fits your environment.

## Option 1 — Download a binary

Prebuilt archives are attached to every [GitHub Release](https://github.com/srjn45/FileDBv2/releases)
for linux, darwin, and windows on amd64/arm64.

```bash
# Linux amd64
curl -L https://github.com/srjn45/filedbv2/releases/latest/download/filedbv2_linux_amd64.tar.gz | tar xz
sudo mv filedb filedb-cli /usr/local/bin/
```

## Option 2 — Docker

```bash
docker run -d \
  -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e FILEDB_API_KEY=my-secret-key \
  ghcr.io/srjn45/filedbv2:latest serve --data /data
```

Or with Compose:

```bash
FILEDB_API_KEY=dev-key docker compose up -d
```

gRPC is exposed on `:5433`, REST on `:8080`, and data persists in a named
`filedb-data` volume.

## Option 3 — Build from source

Requires **Go 1.22+**.

```bash
git clone https://github.com/srjn45/filedbv2
cd filedbv2
make build          # produces bin/filedb and bin/filedb-cli
make test           # run the suite with the race detector
```

## Option 4 — Embed it (no install)

If you only want the database inside a Go program, you don't install anything —
just add the module:

```bash
go get github.com/srjn45/filedbv2/filedb   # ergonomic façade (recommended)
go get github.com/srjn45/filedbv2/engine   # lower-level storage engine
```

See the [Embedding guide](/FileDBv2/guides/embedding/).

## Verify

```bash
filedb --version
filedb-cli --version
```

Continue to the [Quickstart](/FileDBv2/start/quickstart/).
