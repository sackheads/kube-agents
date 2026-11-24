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

// maxLinkDepth bounds how many symlink hops a walk follows.
const maxLinkDepth = 16

// checkChain walks the absolute path p one component at a time, following
// each symlink one hop at a time (os.Readlink, resolved against the
// directory that holds the link), and requires every existing directory and
// file it passes through to be beyond uid's reach (writableBy, with the
// sticky-directory rule below). Each link's containing directory is among
// them, so no hop of a chain can be retargeted. It returns the final
// component's Lstat, or nil if the path ends in a missing component that
// uid could not create.
func checkChain(p string, uid, gid, _ int) error {
	_, err := walkChecked(p, uid, gid)
	return err
}

func walkChecked(p string, uid, gid int) (fs.FileInfo, error) {
	if !filepath.IsAbs(p) {
		return nil, fmt.Errorf("%s is not absolute", p)
	}
	root, err := os.Lstat("/")
	if err != nil {
		return nil, err
	}
	if writableBy(root, uid, gid) {
		return nil, fmt.Errorf("/ is writable by uid %d", uid)
	}
	queue := splitPath(p)
	cur := "/" // the real, symlink-free path walked so far
	last := root
	hops := 0
	// stickyAbove is set while the last directory was sticky and otherwise
	// writable by uid: then the next component must exist and not be uid's,
	// since a sticky directory lets uid rename only its own entries.
	stickyAbove := ""
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			cur = filepath.Dir(cur) // cur is real, so this is the kernel's ".."
			stickyAbove = ""
			if last, err = os.Lstat(cur); err != nil {
				return nil, err
			}
			continue
		}
		next := filepath.Join(cur, name)
		fi, err := os.Lstat(next)
		if errors.Is(err, fs.ErrNotExist) {
			if stickyAbove != "" {
				return nil, fmt.Errorf("%s is missing, and uid %d could create it in the sticky %s", next, uid, stickyAbove)
			}
			return nil, nil // the parent, checked, decides who could create it
		}
		if err != nil {
			return nil, err
		}
		if stickyAbove != "" && ownedBy(fi, uid) {
			return nil, fmt.Errorf("%s is owned by uid %d in the sticky %s", next, uid, stickyAbove)
		}
		stickyAbove = ""
		if fi.Mode()&fs.ModeSymlink != 0 {
			// A link's own bits mean nothing; cur, its directory, is checked.
			if hops++; hops > maxLinkDepth {
				return nil, fmt.Errorf("more than %d symlink hops at %s", maxLinkDepth, next)
			}
			target, err := os.Readlink(next)
			if err != nil {
				return nil, err
			}
			if filepath.IsAbs(target) {
				cur = "/"
			}
			queue = append(splitPath(target), queue...)
			continue
		}
		if writableBy(fi, uid, gid) {
			if fi.IsDir() && fi.Mode()&fs.ModeSticky != 0 && !ownedBy(fi, uid) && len(queue) > 0 {
				stickyAbove = next
			} else {
				return nil, fmt.Errorf("%s is writable by uid %d", next, uid)
			}
		}
		cur, last = next, fi
	}
	return last, nil
}

func splitPath(p string) []string {
	return strings.Split(strings.Trim(p, "/"), "/")
}

// checkEntry checks one entry of a searched-first directory: the walk to
// it, every symlink hop included, passes checkChain's rule, and what it
// finally names is a regular file or a directory.
func checkEntry(p string, uid, gid, _ int) error {
	fi, err := walkChecked(p, uid, gid)
	if err != nil {
		return err
	}
	if fi == nil {
		return fmt.Errorf("%s names nothing", p)
	}
	if t := fi.Mode().Type(); t != 0 && t != fs.ModeDir {
		return fmt.Errorf("%s is neither a file, a directory nor a symlink to one", p)
	}
	return nil
}

// ownedBy reports whether fi belongs to uid; unknown ownership counts as
// uid's, failing closed.
func ownedBy(fi fs.FileInfo, uid int) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return !ok || int(st.Uid) == uid
}
