package workeradapter

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestToolChildrenGetEverythingButCredentialsAndRoutes: a name with a
// credential word, and every model, provider, bus or chat route, is withheld,
// whatever else it is; everything else passes, the task's parameters and
// trigger, the session kube-proxy settings and the image's tool settings
// among them.
func TestToolChildrenGetEverythingButCredentialsAndRoutes(t *testing.T) {
	never := []string{
		"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL",
		"GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_GEMINI_BASE_URL",
		"GEMINI_API_KEY", "OPENAI_API_KEY", "XAI_API_KEY", "GROQ_API_KEY",
		"AICORE_CLIENT_SECRET", "AI_API_URL", "GW_CLIENT_SECRET",
		"NATS_URL", "NATS_PASSWORD", "A2A_BUS_TOKEN_FILE",
		"LITELLM_MASTER_KEY", "AGY_LLM_GATEWAY_URL",
		"HF_TOKEN", "GITHUB_TOKEN", "GH_TOKEN", "SLACK_BOT_TOKEN", "AWS_SECRET_ACCESS_KEY",
		"DECISION_MEMORY_LLM_API_KEY", "SOME_FUTURE_SECRET", "DB_PASSWORD", "ACME_PRIVATE_KEY_FILE",
		"api_key", "apiKey", "ACME_ACCESSTOKEN", "ACME-API-KEY", "dbPassword", "MY_APIKEY",
	}
	keep := []string{
		"PATH", "HOME", "LANG", "LC_ALL", "TERM", "TZ", "KUBECONFIG",
		"CREDENTIAL_PROXY_URL", "CREDENTIAL_PROXY_TOKEN_FILE", "CREDENTIAL_PROXY_TASK_TOKEN_FILE",
		"PROFILE_TASK_PROJECT", "PROFILE_TASK_DRY_RUN", "PROFILE_TRIGGER_STREAM", "PROFILE_TRIGGER_ORIGIN",
		"KUBE_PROXY_URL", "KUBEAGENTS_SCOPE_DECLARED", "FLEET_AUDIT_REPORT_STORE", "GKE_PROJECT_ID",
		"GOOGLE_CLOUD_UNIVERSE_DOMAIN", "KUBE_AGENTS_VCS_ROOT", "SANDBOX_MEMORY_LIMIT_BYTES",
		"KUBE_AGENTS_VCS_AUTHOR_NAME", "KUBE_AGENTS_VCS_AUTHOR_EMAIL", "KUBEAGENTS_KUBECTL_COST_TIER",
	}
	var env []string
	for _, n := range append(slices.Clone(never), keep...) {
		env = append(env, n+"=v-"+n)
	}
	got := toolChildEnv(env)
	for _, n := range never {
		if slices.ContainsFunc(got, func(kv string) bool { return strings.HasPrefix(kv, n+"=") }) {
			t.Errorf("%s reaches a tool child", n)
		}
	}
	for _, n := range keep {
		if !slices.Contains(got, n+"=v-"+n) {
			t.Errorf("%s was withheld from a tool child, which may need it", n)
		}
	}
	names := WithheldFromToolChildren(env)
	slices.Sort(names)
	want := slices.Clone(never)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Errorf("withheld names\n got %q\nwant %q", names, want)
	}
}

// TestASecretSourcedVariableIsWithheld: a variable the operator names as
// Secret-sourced (EnvToolWithheld) is withheld whatever its name, and a name
// the list doesn't hold, with no credential word, passes.
func TestASecretSourcedVariableIsWithheld(t *testing.T) {
	env := []string{EnvToolWithheld + "=ACME_DSN, ACME_ENDPOINT", "ACME_DSN=postgres://u:p@db", "ACME_ENDPOINT=https://x", "ACME_REGION=eu"}
	got := toolChildEnv(env)
	for _, n := range []string{"ACME_DSN", "ACME_ENDPOINT"} {
		if slices.ContainsFunc(got, func(kv string) bool { return strings.HasPrefix(kv, n+"=") }) {
			t.Errorf("Secret-sourced %s reaches a tool child", n)
		}
	}
	if !slices.Contains(got, "ACME_REGION=eu") {
		t.Error("a name the list doesn't hold was withheld")
	}
	if w := WithheldFromToolChildren(env); !slices.Equal(w, []string{"ACME_DSN", "ACME_ENDPOINT"}) {
		t.Errorf("withheld %q", w)
	}
}

// TestAToolChildBuiltFromTheFilteredEnvCannotPrintACredential runs a real
// child with the filtered environment, as a tool would, and has it print its
// environment.
func TestAToolChildBuiltFromTheFilteredEnvCannotPrintACredential(t *testing.T) {
	cmd := exec.Command("/usr/bin/env")
	cmd.Env = toolChildEnv([]string{"PATH=/usr/bin:/bin", "ANTHROPIC_API_KEY=sk-secret-value", "GOOGLE_GEMINI_BASE_URL=http://gw", "NATS_PASSWORD=bus-secret", "HF_TOKEN=hf-secret", "HOME=/h"})
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"sk-secret-value", "http://gw", "bus-secret", "hf-secret"} {
		if strings.Contains(string(out), s) {
			t.Errorf("a tool child printed %q", s)
		}
	}
	if !strings.Contains(string(out), "HOME=/h") {
		t.Error("an allowlisted variable did not reach the child")
	}
}

// toolWrapperVars is every variable the image's tool wrappers read, by the
// file that reads it: a tool child must see each one, or the wrapper takes
// another path (a session pod's kubectl falls back to the broker, which
// refuses the session caller).
var toolWrapperVars = map[string][]string{
	"deploy/sandbox/session-kubectl.sh":                        {"CREDENTIAL_PROXY_EXEC_URL", "CREDENTIAL_PROXY_REFUSAL_FILE", "CREDENTIAL_PROXY_URL", "HOME", "KUBE_PROXY_MODE", "KUBE_PROXY_URL", "KUBEAGENTS_KUBECTL_COST_TIER", "SANDBOX_MEMORY_LIMIT_BYTES"},
	"deploy/sandbox/kubectl_context_registered.sh":             {"CREDENTIAL_PROXY_TOKEN_FILE", "HOME", "KUBE_PROXY_CA_FILE", "KUBE_PROXY_URL", "KUBECONFIG"},
	"agents/platform/scripts/credential_proxy_client.py":       {"CREDENTIAL_PROXY_TOKEN_FILE", "CREDENTIAL_PROXY_URL", "HERMES_HOME", "KUBE_PROXY_CA_FILE", "KUBE_PROXY_HOST_CONTEXT", "KUBE_PROXY_URL", "KUBECONFIG"},
	"deploy/sandbox/kubectl-admit.py":                          {"KUBEAGENTS_KUBECTL_COST_TIER", "KUBECTL_ADMIT_ON_BUSY", "KUBECTL_ADMIT_GUARD_PATH", "KUBECTL_ADMIT_RUN_DIR"},
	"deploy/sandbox/shell-memory-guard.py":                     {"SANDBOX_MEMORY_LIMIT_BYTES"},
	"agents/platform/scripts/vcs_client.py":                    {"KUBE_AGENTS_LOCAL_GIT", "KUBE_AGENTS_VCS_AUTHOR_NAME", "KUBE_AGENTS_VCS_ROOT", "KUBE_AGENTS_VCS_TTL_HOURS"},
	"a2a/worker-adapter/refusal.go (the write-refusal record)": {"CREDENTIAL_PROXY_REFUSAL_FILE", "CREDENTIAL_PROXY_REFUSAL_TASK"},
}

// TestToolChildrenGetWhatTheToolWrappersRead: every variable the image's
// tool wrappers are known to read reaches a tool child, and each wrapper
// file still mentions the variables listed for it.
func TestToolChildrenGetWhatTheToolWrappersRead(t *testing.T) {
	for file, vars := range toolWrapperVars {
		path, _, _ := strings.Cut(file, " ")
		src, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, v := range vars {
			if !strings.Contains(string(src), v) {
				t.Errorf("%s no longer mentions %s: update toolWrapperVars", path, v)
			}
			if Withheld(v) {
				t.Errorf("%s (read by %s) is withheld from tool children", v, path)
			}
		}
	}
}

// toolFiles is every script the worker image installs from the repository:
// the .py and .sh sources of a2a/Dockerfile.worker's COPY lines (a copied
// directory contributes its scripts), build-stage copies (--from) aside.
func toolFiles(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "Dockerfile.worker"))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.ReplaceAll(string(b), "\\\n", " ")
	var out []string
	for _, line := range strings.Split(joined, "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || f[0] != "COPY" {
			continue
		}
		var srcs []string
		from := false
		for _, w := range f[1:] {
			if strings.HasPrefix(w, "--from") {
				from = true
			}
			if !strings.HasPrefix(w, "--") {
				srcs = append(srcs, w)
			}
		}
		if from || len(srcs) < 2 {
			continue
		}
		for _, src := range srcs[:len(srcs)-1] {
			path := filepath.Join("..", "..", src)
			fi, err := os.Stat(path)
			if err != nil {
				continue
			}
			if fi.IsDir() {
				_ = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
					if err == nil && !d.IsDir() && (strings.HasSuffix(p, ".py") || strings.HasSuffix(p, ".sh")) {
						rel, _ := filepath.Rel(filepath.Join("..", ".."), p)
						out = append(out, rel)
					}
					return nil
				})
			} else if strings.HasSuffix(src, ".py") || strings.HasSuffix(src, ".sh") {
				out = append(out, src)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// toolChildWithheldOnPurpose is a variable a tool file reads that tool
// children still don't get, with the reason: a credential or route the tool
// can do without (the broker holds credentials; tool children get paths and
// settings).
var toolChildWithheldOnPurpose = map[string]string{}

var (
	pyEnvRead    = regexp.MustCompile(`(?:environ\.get|environ\[|getenv)\(?\s*["']([A-Z][A-Z0-9_]+)["']`)
	pyEnvConst   = regexp.MustCompile(`_ENVS?\s*=\s*\(?((?:\s*["'][A-Z][A-Z0-9_]+["']\s*,?)+)`)
	quotedName   = regexp.MustCompile(`["']([A-Z][A-Z0-9_]+)["']`)
	shEnvRead    = regexp.MustCompile(`\$\{?([A-Z][A-Z0-9_]+)`)
	shAssignment = regexp.MustCompile(`(?m)^\s*(?:local\s+|readonly\s+|export\s+)?([A-Z][A-Z0-9_]+)=`)
)

// envNamesRead is every environment variable name a tool file reads, as
// far as a scan can tell: Python's environ/getenv calls and *_ENV(S) string
// constants, and a shell script's $NAME uses minus the names it assigns
// itself.
func envNamesRead(path string, src []byte) []string {
	names := map[string]bool{}
	if strings.HasSuffix(path, ".py") {
		for _, m := range pyEnvRead.FindAllSubmatch(src, -1) {
			names[string(m[1])] = true
		}
		for _, m := range pyEnvConst.FindAllSubmatch(src, -1) {
			for _, q := range quotedName.FindAllSubmatch(m[1], -1) {
				names[string(q[1])] = true
			}
		}
	} else {
		local := map[string]bool{}
		for _, m := range shAssignment.FindAllSubmatch(src, -1) {
			local[string(m[1])] = true
		}
		for _, m := range shEnvRead.FindAllSubmatch(src, -1) {
			if n := string(m[1]); !local[n] {
				names[n] = true
			}
		}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// TestEveryVariableAToolFileReadsReachesToolChildren scans every tool file
// the image installs for the environment variables it reads: each must
// reach a tool child, or be withheld on purpose with a reason.
func TestEveryVariableAToolFileReadsReachesToolChildren(t *testing.T) {
	for _, path := range toolFiles(t) {
		src, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Errorf("%s: %v", path, err)
			continue
		}
		for _, n := range envNamesRead(path, src) {
			if _, ok := toolChildWithheldOnPurpose[n]; ok {
				continue
			}
			if Withheld(n) {
				t.Errorf("%s reads %s, which tool children don't get: allow it, or list it in toolChildWithheldOnPurpose with the reason", path, n)
			}
		}
	}
}

// TestTheScanFindsWhatTheWrappersAreKnownToRead checks the scanner against
// the hand-kept list: every variable toolWrapperVars names for a scanned file
// is one the scan finds there, so a scan that went blind would fail here.
func TestTheScanFindsWhatTheWrappersAreKnownToRead(t *testing.T) {
	for file, vars := range toolWrapperVars {
		path, _, _ := strings.Cut(file, " ")
		if !slices.Contains(toolFiles(t), path) {
			continue
		}
		src, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		found := envNamesRead(path, src)
		for _, v := range vars {
			if !slices.Contains(found, v) {
				t.Errorf("the scan of %s misses %s, which it reads", path, v)
			}
		}
	}
}

// TestTheOperatorRendersTheSameWithheldListName: the operator, a separate
// module, renders lib.EnvToolWithheld for profile workers under its own
// constant; the two must stay the same name.
func TestTheOperatorRendersTheSameWithheldListName(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "k8s-operator", "internal", "controller", "agentprofile_pool.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `envToolWithheld = "`+lib.EnvToolWithheld+`"`) {
		t.Errorf("the operator's envToolWithheld is not %q", lib.EnvToolWithheld)
	}
}
