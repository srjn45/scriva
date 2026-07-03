// embeddemo is a standalone module that embeds the FileDB storage engine
// directly, importing only the public `engine` package. It exists to prove —
// and, via CI, to keep proving — that the engine can be linked into an
// application without dragging in the server's grpc/protobuf/prometheus/cobra
// dependency tree.
module github.com/srjn45/filedbv2/embeddemo

go 1.24.0

require github.com/srjn45/filedbv2 v0.0.0

// Build against the engine in this repository, not a published version.
replace github.com/srjn45/filedbv2 => ../
