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
# Linux amd64 — asset names include the version: scriva_<version>_<os>_<arch>.tar.gz
VERSION=1.2.1   # replace with the release you want
curl -L "https://github.com/srjn45/scriva/releases/download/v${VERSION}/scriva_${VERSION}_linux_amd64.tar.gz" | tar xz
sudo mv scriva scriva-cli /usr/local/bin/
```

Releases also attach `.deb` and `.rpm` packages and `checksums.txt`. There is no
apt/yum repository — download the package and install it with `dpkg -i` / `rpm -i`.
A Homebrew cask (`brew install --cask srjn45/scriva/scriva`), a Scoop manifest and
the AUR package `scriva-bin` are published by the release job only when its
publishing secrets are configured, so they may lag behind or be absent for a version.

## Option 2 — Docker

```bash
docker run -d \
  -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e SCRIVA_API_KEY=my-secret-key \
  ghcr.io/srjn45/scriva:latest \
  --grpc-addr 0.0.0.0:5433 --rest-addr 0.0.0.0:8080
```

The published image's entrypoint is `scriva serve --data /data`; extra arguments
are appended (do not repeat `serve`). The listeners default to `127.0.0.1` inside
the container, so pass the two address flags as above for the published ports to be
reachable. Binding all interfaces makes the server refuse to start unless an API key
or TLS is configured. The image built from the repo `Dockerfile` (used by Compose)
already bakes those flags into its entrypoint.

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
