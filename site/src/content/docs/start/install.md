---
title: Install
description: Install, verify and upgrade the ScrivaDB server and CLI — Homebrew, Scoop, .deb/.rpm, prebuilt binary, Docker, or build from source.
---

ScrivaDB ships two binaries: **`scriva`** (server) and **`scriva-cli`**
(client). Both are statically linked (no CGO) and there is nothing else to install.

| You are on… | Easiest path |
|---|---|
| macOS or Linux with Homebrew | [Homebrew](#homebrew-macos-linux) |
| Windows | [Scoop](#scoop-windows) |
| Debian / Ubuntu / Fedora / RHEL | [`.deb` / `.rpm`](#deb--rpm-linux) |
| You have Go installed | [Go install](#go-install) |
| Any server or container platform | [Docker](#docker) |
| Anything else | [Prebuilt binary](#prebuilt-binary) |
| A Go program that just needs a database | [Embed it](#embed-it-no-install) |

## Homebrew (macOS, Linux)

```bash
brew install --cask srjn45/scriva/scriva
```

This installs both `scriva` and `scriva-cli` from the
[`srjn45/homebrew-scriva`](https://github.com/srjn45/homebrew-scriva) tap.

## Scoop (Windows)

```powershell
scoop bucket add scriva https://github.com/srjn45/scoop-scriva
scoop install scriva
```

## `.deb` / `.rpm` (Linux)

Every [GitHub Release](https://github.com/srjn45/scriva/releases/latest) attaches
packages for amd64 and arm64. They install both binaries into `/usr/bin`.

```bash
VERSION=1.6.0   # the release you want
ARCH=amd64      # or arm64

# Debian / Ubuntu
curl -LO "https://github.com/srjn45/scriva/releases/download/v${VERSION}/scriva_${VERSION}_linux_${ARCH}.deb"
sudo dpkg -i "scriva_${VERSION}_linux_${ARCH}.deb"

# Fedora / RHEL
sudo rpm -i "https://github.com/srjn45/scriva/releases/download/v${VERSION}/scriva_${VERSION}_linux_${ARCH}.rpm"
```

There is no apt or yum repository, so these packages do not update themselves, and they
do not ship a systemd unit — see [Operations & security](/scriva/guides/operations-security/)
for running the server as a service.

## Go install

With Go 1.24 or later, install the standalone server and CLI directly into
`$GOBIN` (or `$GOPATH/bin`):

```bash
go install github.com/srjn45/scriva/cmd/scriva@v1.6.0
go install github.com/srjn45/scriva/cmd/scriva-cli@v1.6.0
```

Replace `v1.6.0` with the release you want. Ensure that destination is on your
`PATH`; use a tagged version rather than `@latest` for a repeatable deployment.

## Prebuilt binary

Archives for linux, macOS and windows on amd64/arm64 are attached to every release as
`scriva_<version>_<os>_<arch>.tar.gz` (`.zip` on Windows).

```bash
VERSION=1.6.0   # the release you want
OS=linux        # linux | darwin
ARCH=amd64      # amd64 | arm64
curl -LO "https://github.com/srjn45/scriva/releases/download/v${VERSION}/scriva_${VERSION}_${OS}_${ARCH}.tar.gz"
curl -LO "https://github.com/srjn45/scriva/releases/download/v${VERSION}/checksums.txt"
grep "scriva_${VERSION}_${OS}_${ARCH}.tar.gz$" checksums.txt | sha256sum -c -
# macOS: replace sha256sum with shasum -a 256
tar xzf "scriva_${VERSION}_${OS}_${ARCH}.tar.gz"
sudo mv scriva scriva-cli /usr/local/bin/
```

## Docker

Multi-arch (amd64 + arm64) images are published to GitHub Container Registry as
`ghcr.io/srjn45/scriva`. Tags are `latest` and one per release (`v1.6.0`). **Pin a
version tag in production** so a restart never upgrades you by surprise.

```bash
docker run -d \
  -p 5433:5433 -p 8080:8080 \
  -v $(pwd)/data:/data \
  -e SCRIVA_API_KEY=my-secret-key \
  ghcr.io/srjn45/scriva:v1.6.0 \
  --grpc-addr 0.0.0.0:5433 --rest-addr 0.0.0.0:8080
```

The image's entrypoint is `scriva serve --data /data`; extra arguments are appended
(do not repeat `serve`). Listeners default to `127.0.0.1` inside the container, so pass
the two address flags as above for the published ports to be reachable. Binding all
interfaces makes the server refuse to start unless an API key or TLS is configured.
The image contains the server only; use `scriva-cli` from another install path or
the REST/gRPC API.

With Compose (from a clone of the repository):

```bash
SCRIVA_API_KEY=dev-key docker compose up -d
```

gRPC is on `:5433`, REST on `:8080`, and data persists in a named `scriva-data` volume.

## Build from source

Requires **Go 1.24+**.

```bash
git clone https://github.com/srjn45/scriva
cd scriva
make build          # produces bin/scriva and bin/scriva-cli
make test           # run the suite with the race detector
```

## Embed it (no install)

If you only want the database inside a Go program, install nothing — add the module:

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

Release builds print the release version; a build from source prints `dev`.
Continue to the [Quickstart](/scriva/start/quickstart/).

## Upgrading

ScrivaDB has no self-update command: you upgrade by replacing the binaries (or the image
tag) through the same channel you installed from, then restarting the server.

| Installed with | Upgrade |
|---|---|
| Homebrew | `brew upgrade --cask srjn45/scriva/scriva` |
| Scoop | `scoop update scriva` |
| `.deb` / `.rpm` | download the new package and `dpkg -i` / `rpm -U` it |
| Go install | repeat both `go install` commands with the new version |
| Binary | repeat the download steps with the new version |
| Docker | change the image tag, then `docker compose pull && docker compose up -d` (or `docker pull` and recreate the container) |
| Embedded | `go get github.com/srjn45/scriva@v1.6.0` |

The Homebrew and Scoop packages are updated by the release job when it has its
publishing credentials, so a brand-new release can take a moment to appear there.

Before you upgrade a server with data you care about:

1. Read the [changelog](https://github.com/srjn45/scriva/blob/main/CHANGELOG.md) for
   the versions you are crossing; **Upgrade note** entries call out behaviour changes
   (for example, the v1.6 secure-by-default listeners).
2. Take a backup — see [Durability & backup](/scriva/guides/durability-and-backup/).
3. Stop the old server cleanly, swap the binaries, and start the new one. Never run two
   versions against the same data directory at once.

Within a major version the wire API and client SDKs stay compatible, and older data
directories open as-is (index files are rebuilt once on first open when needed).
The exception is **cross-collection transactions**: once a data directory has run one,
it can no longer be opened by a pre-v1.5 server. Plan rollbacks around your backup, and
see the [upgrade, downgrade and recovery playbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md#upgrade-downgrade-and-recovery-playbook)
and [Atomic batches & upgrading roots](/scriva/guides/atomic-batches/).
