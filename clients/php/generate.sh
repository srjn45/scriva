#!/usr/bin/env bash
# Regenerate PHP gRPC + protobuf stubs from proto/filedb.proto.
#
# Requirements:
#   - protoc (protobuf compiler): https://github.com/protocolbuffers/protobuf/releases
#   - grpc_php_plugin: built from https://github.com/grpc/grpc (tools/run_tests/helper_scripts/build_php.sh)
#       or installed via: pecl install grpc
#   - The google/api proto includes (clone https://github.com/googleapis/googleapis)
#
# Usage (run from the repo root):
#   clients/php/generate.sh
#
# The script places all generated files under clients/php/src/Proto/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROTO_FILE="$REPO_ROOT/proto/filedb.proto"
OUT_DIR="$SCRIPT_DIR/src/Proto"

# Find grpc_php_plugin (try common locations)
GRPC_PHP_PLUGIN="${GRPC_PHP_PLUGIN:-$(which grpc_php_plugin 2>/dev/null || true)}"
if [[ -z "$GRPC_PHP_PLUGIN" ]]; then
  echo "ERROR: grpc_php_plugin not found. Set GRPC_PHP_PLUGIN=/path/to/grpc_php_plugin" >&2
  exit 1
fi

# Find google API proto includes (required for google/api/annotations.proto)
GOOGLE_APIS_DIR="${GOOGLE_APIS_DIR:-}"
if [[ -z "$GOOGLE_APIS_DIR" ]]; then
  # Try common locations
  for d in /usr/local/include /usr/include "$REPO_ROOT/third_party/googleapis"; do
    if [[ -f "$d/google/api/annotations.proto" ]]; then
      GOOGLE_APIS_DIR="$d"
      break
    fi
  done
fi

PROTO_PATH_FLAGS="-I $REPO_ROOT/proto"
if [[ -n "$GOOGLE_APIS_DIR" ]]; then
  PROTO_PATH_FLAGS="$PROTO_PATH_FLAGS -I $GOOGLE_APIS_DIR"
fi

mkdir -p "$OUT_DIR"

echo "Generating PHP stubs from $PROTO_FILE ..."
protoc $PROTO_PATH_FLAGS \
  --php_out="$OUT_DIR" \
  --grpc_out="$OUT_DIR" \
  --plugin=protoc-gen-grpc="$GRPC_PHP_PLUGIN" \
  "$PROTO_FILE"

echo "Done. Stubs written to $OUT_DIR"
echo ""
echo "Run 'composer install' in $SCRIPT_DIR to install dependencies."
