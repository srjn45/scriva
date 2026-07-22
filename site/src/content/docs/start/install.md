---
title: Install
description: Install the ScrivaDB server and CLI — prebuilt binary, Docker, package managers, or build from source.
---

ScrivaDB ships two binaries: **`scriva`** (server) and **`scriva-cli`**
(client). Pick whichever install path fits your environment.

## Option 1 — Download a binary

Prebuilt archives are attached to every [GitHub Release](https://github.com/srjn45/scriva/releases)
for linux, darwin, and windows on amd64/arm64.

```bash
# Linux amd64
curl -L https://github.com/srjn45/scriva/releases/latest/download/scriva_linux_amd64.tar.gz | tar xz
sudo mv scriva scriva-cli /usr/local/bin/
```

## Option 2 — Docker

```bash
docker run -d \
  -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e SCRIVA_API_KEY=my-secret-key \
  ghcr.io/srjn45/scriva:latest serve --data /data
```

Or with Compose:

```bash
SCRIVA_API_KEY=dev-key docker compose up -d
```

gRPC is exposed on `:5433`, REST on `:8080`, and data persists in a named
`scriva-data` volume.

## Option 3 — Build from source

Requires **Go 1.22+**.

```bash
git clone https://github.com/srjn45/scriva
cd scriva
make build          # produces bin/scriva and bin/scriva-cli
make test           # run the suite with the race detector
```

## Option 4 — Embed it (no install)

If you only want the database inside a Go program, you don't install anything —
just add the module:

```bash
go get github.com/srjn45/scriva         # ergonomic façade (recommended)
go get github.com/srjn45/scriva/engine  # lower-level storage engine
```

See the [Embedding guide](/scriva/guides/embedding/).

## Verify

```bash
scriva --version
scriva-cli --version
```

Continue to the [Quickstart](/scriva/start/quickstart/).
