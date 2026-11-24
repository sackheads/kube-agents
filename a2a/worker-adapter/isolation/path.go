package isolation

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
// ahead of TrustedPathDir. For each of dirs it requires:
//   - an absolute path (an empty or relative entry is the turn's working
//     directory, which the turn writes);
//   - no component from / down, nor the target chain of any symlink among
//     them, writable by uid (with primary group gid), since a writable
//     ancestor can rename the entry away and put another in its place;
//   - a missing entry only where uid could not create it, which is the same
//     rule applied to the components that exist;
//   - every entry inside it a regular file or directory not writable by uid,
//     or a symlink whose target passes the same checks.
func CheckSearchPath(dirs []string, uid, gid int) error {
	for _, d := range dirs {
		if d == "" || !filepath.IsAbs(d) {
			return fmt.Errorf("isolation: PATH searches %q before %s, a directory relative to the turn's working directory", d, TrustedPathDir)
		}
		if err := checkChain(filepath.Clean(d), uid, gid, 0); err != nil {
			return fmt.Errorf("isolation: PATH entry %s, searched before %s: %w", d, TrustedPathDir, err)
		}
		ents, err := os.ReadDir(d)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("isolation: PATH entry %s: %w", d, err)
		}
		for _, e := range ents {
			p := filepath.Join(d, e.Name())
			if err := checkEntry(p, uid, gid, 0); err != nil {
				return fmt.Errorf("isolation: PATH entry %s, searched before %s: %w", d, TrustedPathDir, err)
			}
		}
	}
	return nil
}

// maxLinkDepth bounds how many symlinks checkChain and checkEntry follow.
const maxLinkDepth = 16

// checkChain checks every existing component of the absolute path p, from
// / down: none may be writable by uid, and a symlink among them is checked
// through its target's own chain. Components below the first missing one
// do not exist, so they are covered by their nearest existing parent.
func checkChain(p string, uid, gid, depth int) error {
	if depth > maxLinkDepth {
		return fmt.Errorf("more than %d symlinks", maxLinkDepth)
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	cur := "/"
	// stickyAbove is set while the last component was a sticky directory
	// that uid could otherwise write: then this one must exist and not be
	// uid's, since a sticky directory lets uid rename only its own entries.
	stickyAbove := ""
	for i := -1; i < len(parts); i++ {
		if i >= 0 {
			if parts[i] == "" {
				continue
			}
			cur = filepath.Join(cur, parts[i])
		}
		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			if stickyAbove != "" {
				return fmt.Errorf("%s is missing, and uid %d could create it in the sticky %s", cur, uid, stickyAbove)
			}
			return nil // the parent checked above decides who could create it
		}
		if err != nil {
			return err
		}
		if stickyAbove != "" && ownedBy(fi, uid) {
			return fmt.Errorf("%s is owned by uid %d in the sticky %s", cur, uid, stickyAbove)
		}
		stickyAbove = ""
		if fi.Mode()&fs.ModeSymlink != 0 {
			// A link's own bits mean nothing; its parent (checked) decides
			// who can replace it. Its target must pass on its own.
			target, err := filepath.EvalSymlinks(cur)
			if err != nil {
				return err
			}
			if err := checkChain(target, uid, gid, depth+1); err != nil {
				return fmt.Errorf("%s links to %s: %w", cur, target, err)
			}
			continue
		}
		if writableBy(fi, uid, gid) {
			if fi.IsDir() && fi.Mode()&fs.ModeSticky != 0 && !ownedBy(fi, uid) && i < len(parts)-1 {
				stickyAbove = cur
				continue
			}
			return fmt.Errorf("%s is writable by uid %d", cur, uid)
		}
	}
	return nil
}

// ownedBy reports whether fi belongs to uid; unknown ownership counts as
// uid's, failing closed.
func ownedBy(fi fs.FileInfo, uid int) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return !ok || int(st.Uid) == uid
}

// checkEntry checks one entry of a searched-first directory: a regular file
// or directory not writable by uid, or a symlink whose target's chain and
// target pass.
func checkEntry(p string, uid, gid, depth int) error {
	if depth > maxLinkDepth {
		return fmt.Errorf("more than %d symlinks", maxLinkDepth)
	}
	fi, err := os.Lstat(p)
	if err != nil {
		return err
	}
	switch t := fi.Mode().Type(); {
	case t&fs.ModeSymlink != 0:
		target, err := filepath.EvalSymlinks(p)
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		if err := checkChain(target, uid, gid, depth+1); err != nil {
			return fmt.Errorf("%s links to %s: %w", p, target, err)
		}
		return nil
	case t != 0 && t != fs.ModeDir:
		return fmt.Errorf("%s is neither a file, a directory nor a symlink", p)
	}
	if writableBy(fi, uid, gid) {
		return fmt.Errorf("%s is writable by uid %d", p, uid)
	}
	return nil
}
