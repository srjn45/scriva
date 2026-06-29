#!/usr/bin/env bash
# generate.sh — regenerate the Python gRPC stubs from proto/filedb.proto.
#
# Requirements:
#   pip install "grpcio-tools>=1.60"
#   (or: pip install -e ".[codegen]" from this directory)
#
# Output goes to src/filedbv2/proto/ — commit the results.
#
# The canonical proto lives at ../../proto/filedb.proto. It imports
# google/api/annotations.proto, which is not bundled with grpc_tools; a minimal
# copy is vendored under ./proto/google/api/ so codegen is self-contained.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROTO_SRC="$(cd "$HERE/../../proto" && pwd)"
DEPS_DIR="$HERE/proto"
OUT_DIR="$HERE/src/filedbv2/proto"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I "$PROTO_SRC" \
  -I "$DEPS_DIR" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_SRC/filedb.proto"

# grpc_tools emits `import filedb_pb2` (package-relative is not the default).
# Rewrite it to a package-relative import so `from filedbv2.proto import ...`
# works regardless of sys.path.
GRPC_FILE="$OUT_DIR/filedb_pb2_grpc.py"
if [ -f "$GRPC_FILE" ]; then
  sed -i.bak 's/^import filedb_pb2 as/from . import filedb_pb2 as/' "$GRPC_FILE"
  rm -f "$GRPC_FILE.bak"
fi

# Ensure the generated directory is an importable package.
touch "$OUT_DIR/__init__.py"

echo "Done. Stubs written to $OUT_DIR"
