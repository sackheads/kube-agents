package isolation

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAgySearchedFirstStopsAtTheTrustedDir(t *testing.T) {
	got := AgySearchedFirst("/opt/agy-home", "/opt/tools/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/bin")
	want := []string{"/opt/agy-home/.gemini/antigravity-cli/bin", "/opt/tools/bin", "/usr/local/sbin"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := AgySearchedFirst("/h", "/usr/local/bin/:/usr/bin"); !slices.Equal(got, []string{"/h/.gemini/antigravity-cli/bin"}) {
		t.Errorf("a trailing slash on the trusted dir: %q", got)
	}
	if got := AgySearchedFirst("/h", "::/usr/local/bin"); len(got) != 3 {
		t.Errorf("empty entries dropped: %q", got)
	}
}

func TestCheckSearchPath(t *testing.T) {
	ro := tempDir(t)
	must(t, os.Chmod(ro, 0o555))
	roParentMissing := filepath.Join(ro, "bin") // missing, parent read-only
	rw := tempDir(t)
	rwParentMissing := filepath.Join(rw, "bin") // missing, parent writable

	if err := CheckSearchPath([]string{ro, roParentMissing}, otherUID, otherGID); err != nil {
		t.Fatalf("read-only dirs: %v", err)
	}
	if err := CheckSearchPath([]string{ro}, os.Getuid(), os.Getgid()); err == nil {
		t.Fatal("a dir agy owns passed; its owner can chmod it")
	}
	for name, c := range map[string]struct {
		dirs []string
		want string
	}{
		"a writable dir":                   {[]string{ro, rw}, "is writable"},
		"a missing dir agy could create":   {[]string{rwParentMissing}, "could create it"},
		"the working directory, empty":     {[]string{""}, "relative"},
		"the working directory, relative":  {[]string{"bin"}, "relative"},
		"an other-writable dir for others": {[]string{otherWritable(t)}, "is writable"},
	} {
		t.Run(name, func(t *testing.T) {
			uid, gid := os.Getuid(), os.Getgid()
			if name == "an other-writable dir for others" {
				uid, gid = otherUID, otherGID
			}
			err := CheckSearchPath(c.dirs, uid, gid)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v; want one saying %q", err, c.want)
			}
		})
	}
}

func otherWritable(t *testing.T) string {
	t.Helper()
	d := tempDir(t)
	must(t, os.Chmod(d, 0o777))
	return d
}
