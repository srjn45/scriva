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

# grpc_tools emits a bare `require 'filedb_pb'` in the service stub, which only
# resolves if OUT_DIR happens to be on the load path. Rewrite it to the
# package-relative path so `require "filedbv2/proto/filedb_services_pb"` works
# from anywhere on the gem's load path.
SERVICES_FILE="$OUT_DIR/filedb_services_pb.rb"
if [ -f "$SERVICES_FILE" ]; then
  sed -i.bak "s#^require 'filedb_pb'#require 'filedbv2/proto/filedb_pb'#" "$SERVICES_FILE"
  rm -f "$SERVICES_FILE.bak"
fi

echo "Done. Stubs written to $OUT_DIR"
