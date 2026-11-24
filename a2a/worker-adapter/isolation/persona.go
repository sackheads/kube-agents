package isolation

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

// AgyPersonaNames are the only names a persona directory may hold: the
// rules files and the skills directory a persona renders. Nothing that
// configures behaviour beyond the model's context (hooks, MCP servers,
// plugins, settings) may appear there. The agy image's
// .gemini/config/{GEMINI.md,AGENTS.md,skills} link to these (#6715).
var AgyPersonaNames = []string{"AGENTS.md", "GEMINI.md", "skills"}

// CheckPersonaDir verifies the directory a persona is rendered into before
// a turn: it exists, is a real directory, holds nothing but
// AgyPersonaNames (and, under skills, nothing that is not a directory or a
// regular file), and neither it nor anything in it is writable by uid, the
// harness's user. Another user renders it; a harness that could write it
// would configure its own next turn. It fails closed on anything else.
func CheckPersonaDir(dir string, uid, gid int) error {
	if err := checkRoot("persona dir", dir); err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("isolation: persona dir: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("isolation: persona dir %s is not a directory", dir)
	}
	if writableBy(fi, uid, gid) {
		return fmt.Errorf("isolation: persona dir %s is writable by uid %d", dir, uid)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("isolation: persona dir: %w", err)
	}
	for _, e := range ents {
		if !slices.Contains(AgyPersonaNames, e.Name()) {
			return fmt.Errorf("isolation: persona dir %s holds %q, which is not a persona entry", dir, e.Name())
		}
	}
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("isolation: persona dir: %w", err)
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("isolation: persona dir: %w", err)
		}
		if t := info.Mode().Type(); t != 0 && t != fs.ModeDir {
			return fmt.Errorf("isolation: persona entry %s is neither a directory nor a regular file", p)
		}
		if writableBy(info, uid, gid) {
			return fmt.Errorf("isolation: persona entry %s is writable by uid %d", p, uid)
		}
		return nil
	})
}

// writableBy reports whether the permission bits let uid (with primary
// group gid) write fi. Root is not considered: the harness never runs as
// root.
func writableBy(fi fs.FileInfo, uid, gid int) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return true // unknown ownership: fail closed
	}
	perm := fi.Mode().Perm()
	switch {
	case int(st.Uid) == uid:
		return perm&0o200 != 0
	case int(st.Gid) == gid:
		return perm&0o020 != 0
	default:
		return perm&0o002 != 0
	}
}
