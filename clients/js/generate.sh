#!/usr/bin/env bash
# generate.sh — refresh the vendored proto used by the client.
#
# The client loads the schema dynamically at runtime with @grpc/proto-loader
# (see src/client.ts), so there is no static stub codegen step: the only build
# artifact it needs is a copy of the canonical proto. This script copies
# ../../proto/filedb.proto into ./proto/ (alongside the vendored google/api
# dependencies) so the published package is self-contained. Commit the result.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROTO_SRC="$(cd "$HERE/../../proto" && pwd)"
DEST_DIR="$HERE/proto"

cp "$PROTO_SRC/filedb.proto" "$DEST_DIR/filedb.proto"

echo "Done. Vendored proto refreshed at $DEST_DIR/filedb.proto"
