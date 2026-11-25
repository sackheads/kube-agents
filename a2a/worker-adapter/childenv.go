package workeradapter

import (
	"strings"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// A harness starts the processes a turn's tools run, such as a shell command
// or an MCP stdio server, as the adapter's user. Those children get the
// environment minus three things, so no credential the pod carries reaches
// them:
//
//   - every variable the operator sources from a Secret, which it names in
//     EnvToolWithheld on the container (so a secret added later is withheld
//     by construction);
//   - every name with a credential word in it (KEY, TOKEN, SECRET, PASSWORD,
//     AUTH, CRED and their variants), whatever its source;
//   - the model, provider, bus and chat routes: where the model, the bus and
//     the chat backends are, and their keys.
//
// Everything else passes, as it does for any harness today: the task's
// parameters and trigger (PROFILE_TASK_*, PROFILE_TRIGGER_*), the session
// kube-proxy settings, the image's tool settings, and whatever a tool file
// reads that is not a credential.
//
// CREDENTIAL_PROXY_* passes on purpose, credential words and all: the
// broker-routed wrappers need the broker's URL, the pod's own projected
// broker token (CREDENTIAL_PROXY_TOKEN_FILE) and the task's token
// (CREDENTIAL_PROXY_TASK_TOKEN_FILE), and the broker scopes what either can
// do.
//
// Withholding a *_FILE variable hides only the path. The file stays readable
// to the adapter's user wherever it is mounted, so a secret kept in a file
// needs file modes or another user to be out of a tool child's reach.

// EnvToolWithheld (lib.EnvToolWithheld) is the comma-separated list of the
// container's variables sourced from a Secret (an env entry's secretKeyRef).
// Worker containers take no envFrom Secret, whose keys aren't known when the
// pod is rendered. The list itself names variables, not values, and passes.
const EnvToolWithheld = lib.EnvToolWithheld

// toolChildAlwaysPassedPrefix passes despite its credential words (see above).
const toolChildAlwaysPassedPrefix = "CREDENTIAL_PROXY_"

// credentialWords are the name parts (split on "_") that mark a credential.
var credentialWords = map[string]bool{
	"KEY": true, "KEYS": true, "APIKEY": true,
	"TOKEN": true, "TOKENS": true,
	"SECRET": true, "SECRETS": true,
	"PASSWORD": true, "PASSWD": true, "PASSPHRASE": true,
	"AUTH": true, "CRED": true, "CREDS": true, "CREDENTIAL": true, "CREDENTIALS": true,
	"PRIVATE": true, "COOKIE": true,
}

// credentialSuffixes mark a credential when a name part ends with one,
// however the words run together (ACCESSTOKEN, APIKEY, DBPASSWORD).
var credentialSuffixes = []string{"TOKEN", "TOKENS", "SECRET", "SECRETS", "PASSWORD", "PASSWD", "KEY", "KEYS", "CREDENTIALS"}

// nameParts is name upper-cased and split on anything not a letter or a
// digit and at each lower-to-upper case change, so "api_key", "apiKey" and
// "API-KEY" all give API and KEY.
func nameParts(name string) []string {
	var parts []string
	var cur strings.Builder
	var prevLower bool
	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, strings.ToUpper(cur.String()))
			cur.Reset()
		}
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			cur.WriteRune(r)
			prevLower = true
		case r >= 'A' && r <= 'Z':
			if prevLower {
				flush()
			}
			cur.WriteRune(r)
			prevLower = false
		case r >= '0' && r <= '9':
			cur.WriteRune(r)
		default:
			flush()
			prevLower = false
		}
	}
	flush()
	return parts
}

// toolChildWithheldPrefixes and toolChildWithheldNames are the model,
// provider, bus and chat routes: an endpoint or a key a tool has no use for.
var (
	toolChildWithheldPrefixes = []string{
		"ANTHROPIC_", "GEMINI_", "OPENAI_", "XAI_", "GROQ_", "MISTRAL_", "COHERE_",
		"AICORE_", "AI_API_", "GW_", "LITELLM_", "AGY_LLM_", "VERTEX_", "BEDROCK_", "AWS_",
		"DECISION_MEMORY_LLM_", "HF_",
		"NATS_", "A2A_BUS_",
		"SLACK_", "DISCORD_", "TEAMS_", "TELEGRAM_",
	}
	toolChildWithheldNames = []string{
		"GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_GEMINI_BASE_URL",
		"GOOGLE_GENAI_USE_VERTEXAI", "GITHUB_TOKEN", "GH_TOKEN",
	}
)

// Withheld reports whether a turn's tool children must not see the variable
// name on its own merits: a credential word, or a model, bus or chat route.
// The Secret-sourced names come from the environment itself
// (WithheldFromToolChildren, toolChildEnv).
func Withheld(name string) bool {
	if strings.HasPrefix(name, toolChildAlwaysPassedPrefix) {
		return false
	}
	for _, part := range nameParts(name) {
		if credentialWords[part] {
			return true
		}
		for _, w := range credentialSuffixes {
			if strings.HasSuffix(part, w) {
				return true
			}
		}
	}
	for _, p := range toolChildWithheldPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	for _, n := range toolChildWithheldNames {
		if name == n {
			return true
		}
	}
	return false
}

// secretSourced is the set of names env's EnvToolWithheld lists.
func secretSourced(env []string) map[string]bool {
	out := map[string]bool{}
	for _, kv := range env {
		name, value, _ := strings.Cut(kv, "=")
		if name != EnvToolWithheld {
			continue
		}
		for _, n := range strings.Split(value, ",") {
			if n = strings.TrimSpace(n); n != "" {
				out[n] = true
			}
		}
	}
	return out
}

func withheldIn(name string, secrets map[string]bool) bool {
	return secrets[name] || Withheld(name)
}

// WithheldFromToolChildren is the names in env (KEY=value entries) that a
// turn's tool children must not see, for a harness's withhold list.
func WithheldFromToolChildren(env []string) []string {
	secrets := secretSourced(env)
	var out []string
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if withheldIn(name, secrets) {
			out = append(out, name)
		}
	}
	return out
}

// toolChildEnv is env without the variables a turn's tool children must not
// see.
func toolChildEnv(env []string) []string {
	secrets := secretSourced(env)
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if !withheldIn(name, secrets) {
			out = append(out, kv)
		}
	}
	return out
}
