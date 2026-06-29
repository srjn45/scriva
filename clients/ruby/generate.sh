#!/usr/bin/env bash
# generate.sh — regenerate Ruby gRPC stubs from proto/filedb.proto.
#
# Requirements:
#   gem install grpc-tools   (provides grpc_tools_ruby_protoc)
#
# Output goes to lib/filedbv2/proto/ — commit the results.
#
# A minimal copy of google/api/{annotations,http}.proto is vendored under
# ./proto/google/api/ so codegen is self-contained (not required at runtime).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROTO_SRC="$(cd "$HERE/../../proto" && pwd)"
DEPS_DIR="$HERE/proto"
OUT_DIR="$HERE/lib/filedbv2/proto"

mkdir -p "$OUT_DIR"

grpc_tools_ruby_protoc \
  -I "$PROTO_SRC" \
  -I "$DEPS_DIR" \
  --ruby_out="$OUT_DIR" \
  --grpc_out="$OUT_DIR" \
  "$PROTO_SRC/filedb.proto"

echo "Done. Stubs written to $OUT_DIR"
