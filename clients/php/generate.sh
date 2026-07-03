#!/usr/bin/env bash
# Regenerate PHP protobuf + gRPC stubs from proto/filedb.proto using buf.
#
# The stubs are vendored under clients/php/src/Proto/ so that `composer require`
# users don't need protoc. Regenerate them whenever proto/filedb.proto changes.
#
# Requirements:
#   - buf (https://buf.build/docs/installation) — the only tool needed; the
#     protoc-gen-php and grpc-php plugins are pulled from the Buf Schema Registry.
#
# The plugin versions are pinned to match the `google/protobuf: ^3.25` runtime
# declared in composer.json. protocolbuffers/php:v25.1 emits the pre-4.x codegen
# style (Google\Protobuf\Internal\RepeatedField, GPBUtil::checkX) that the 3.25
# runtime expects. Bump these together with the composer constraint.
#
# Usage (run from anywhere):
#   clients/php/generate.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$SCRIPT_DIR/src/Proto"

PROTOBUF_PHP_VERSION="v25.1"
GRPC_PHP_VERSION="v1.62.0"

if ! command -v buf >/dev/null 2>&1; then
  echo "ERROR: buf not found. Install it: https://buf.build/docs/installation" >&2
  exit 1
fi

# Generate from a temp workspace rooted at the proto directory so that the
# generated GPBMetadata class is \GPBMetadata\Filedb (file path "filedb.proto"),
# matching the vendored layout.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp "$REPO_ROOT/proto/filedb.proto" "$WORK/filedb.proto"

cat > "$WORK/buf.yaml" <<'YAML'
version: v2
modules:
  - path: .
deps:
  - buf.build/googleapis/googleapis
lint:
  use:
    - DEFAULT
YAML

cat > "$WORK/buf.gen.yaml" <<YAML
version: v2
plugins:
  - remote: buf.build/protocolbuffers/php:${PROTOBUF_PHP_VERSION}
    out: OUT_PLACEHOLDER
  - remote: buf.build/grpc/php:${GRPC_PHP_VERSION}
    out: OUT_PLACEHOLDER
YAML
sed -i "s#OUT_PLACEHOLDER#$OUT_DIR#g" "$WORK/buf.gen.yaml"

echo "Regenerating PHP stubs into $OUT_DIR ..."
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
( cd "$WORK" && buf dep update >/dev/null 2>&1 || true; buf generate --template buf.gen.yaml )

echo "Done. Run 'composer install' in $SCRIPT_DIR to install dependencies."
