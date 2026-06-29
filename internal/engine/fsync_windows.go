//go:build windows

package engine

// fsyncDir is a no-op on Windows, which does not support fsync on a directory
// handle. The atomic rename performed by writeFileAtomic still guarantees that
// readers never observe a partial file.
func fsyncDir(_ string) error { return nil }
