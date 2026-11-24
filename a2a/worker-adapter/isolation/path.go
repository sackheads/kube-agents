package isolation

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// AgyBinDir is the directory agy prepends to its shell's PATH, relative to
// HOME. agy creates it if it is missing.
const AgyBinDir = ".gemini/antigravity-cli/bin"

// TrustedPathDir is where the image's broker-routed tools (kubectl, gcloud)
// live. A command a turn runs must find them there, not in a directory
// searched before it.
const TrustedPathDir = "/usr/local/bin"

// AgySearchedFirst is every directory agy's shell searches before
// TrustedPathDir: agy's own bin under home, then each entry of path ahead
// of TrustedPathDir. Relative and empty entries (the working directory)
// are kept, as the shell would search them.
func AgySearchedFirst(home, path string) []string {
	dirs := []string{filepath.Join(home, AgyBinDir)}
	for _, d := range strings.Split(path, ":") {
		if filepath.Clean(d) == TrustedPathDir {
			break
		}
		dirs = append(dirs, d)
	}
	return dirs
}

// CheckSearchPath fails closed if a turn's commands could put a program
// ahead of TrustedPathDir: if any of dirs is writable by uid (with primary
// group gid), or is missing where uid could create it, or is relative or
// empty (the working directory, which the turn writes).
func CheckSearchPath(dirs []string, uid, gid int) error {
	for _, d := range dirs {
		if d == "" || !filepath.IsAbs(d) {
			return fmt.Errorf("isolation: PATH searches %q before %s, a directory relative to the turn's working directory", d, TrustedPathDir)
		}
		existing, err := nearestExisting(d)
		if err != nil {
			return fmt.Errorf("isolation: PATH entry %s: %w", d, err)
		}
		fi, err := os.Stat(existing)
		if err != nil {
			return fmt.Errorf("isolation: PATH entry %s: %w", d, err)
		}
		if writableBy(fi, uid, gid) {
			if existing == d {
				return fmt.Errorf("isolation: PATH entry %s, searched before %s, is writable by uid %d", d, TrustedPathDir, uid)
			}
			return fmt.Errorf("isolation: PATH entry %s, searched before %s, is missing and uid %d could create it in %s", d, TrustedPathDir, uid, existing)
		}
	}
	return nil
}

// nearestExisting is d, or its nearest ancestor that exists.
func nearestExisting(d string) (string, error) {
	for p := filepath.Clean(d); ; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if p == filepath.Dir(p) {
			return "", errors.New("no ancestor exists")
		}
	}
}
