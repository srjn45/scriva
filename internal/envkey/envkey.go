// Package envkey resolves the API key from the environment, preferring the
// current SCRIVA_API_KEY variable while still honoring the legacy
// FILEDB_API_KEY name for one release cycle.
package envkey

import (
	"fmt"
	"os"
)

// Current is the supported environment variable holding the API key.
const Current = "SCRIVA_API_KEY"

// Legacy is the pre-rebrand environment variable name, still accepted as a
// fallback. It emits a deprecation warning when used and will be removed.
const Legacy = "FILEDB_API_KEY"

// APIKey returns the API key from the environment. It reads SCRIVA_API_KEY
// first and, only when that is unset or empty, falls back to the legacy
// FILEDB_API_KEY — printing a one-time deprecation warning to stderr when it
// does so. It returns "" when neither is set.
func APIKey() string {
	if v := os.Getenv(Current); v != "" {
		return v
	}
	if v := os.Getenv(Legacy); v != "" {
		fmt.Fprintf(os.Stderr, "warning: %s is deprecated; set %s instead\n", Legacy, Current)
		return v
	}
	return ""
}
