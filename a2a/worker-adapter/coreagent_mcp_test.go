package workeradapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
	"github.com/gke-labs/kube-agents/a2a/lib/coreagentwire"
	"github.com/gke-labs/kube-agents/a2a/lib/persona"
)

var relayServer = persona.MCPServer{
	Name: "developer_knowledge", Command: "/worker-adapter",
	Args:  []string{"mcp-relay", "http://broker:8080/v1/mcp/developer_knowledge"},
	Env:   map[string]string{"CREDENTIAL_PROXY_URL": "http://broker:8080"},
	Tools: []string{"search_documents"},
}

// A turn's servers are the persona's, then the delegate server when the turn
// may delegate, field for field.
func TestACoreAgentTurnCarriesThePersonaServersThenTheDelegate(t *testing.T) {
	delegate := persona.MCPServer{Name: "a2a", Command: "/worker-adapter", Args: []string{"delegate-mcp"}, Env: map[string]string{"A2A_DELEGATE_SOCKET": "/run/d.sock"}, Tools: []string{"delegate"}}
	got := coreAgentTurnMCP([]persona.MCPServer{relayServer}, &delegate)
	want := []coreagentwire.MCPServer{
		{Name: relayServer.Name, Command: relayServer.Command, Args: relayServer.Args, Env: relayServer.Env, Tools: relayServer.Tools},
		{Name: "a2a", Command: "/worker-adapter", Args: []string{"delegate-mcp"}, Env: delegate.Env, Tools: []string{"delegate"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("servers = %+v\nwant %+v", got, want)
	}
	if got := coreAgentTurnMCP(nil, nil); len(got) != 0 {
		t.Errorf("no persona servers and no delegate gave %+v", got)
	}
	// The wire copy is its own: changing it leaves the persona's alone.
	got[0].Args[0] = "changed"
	if relayServer.Args[0] != "mcp-relay" {
		t.Error("the turn's servers share the persona's slices")
	}
}

// Each turn's driver carries the persona's servers, rebuilt from source: a
// turn without a delegate doesn't keep one an earlier turn had. The Claude
// Code driver, which takes its servers as flags, is left alone.
func TestEachTurnsEnvelopeCarriesItsOwnServers(t *testing.T) {
	delegate := persona.MCPServer{Name: "a2a", Command: "/worker-adapter", Tools: []string{"delegate"}}
	pod := Driver(newCoreAgentDriver())
	first := turnDriver(pod, []persona.MCPServer{relayServer}, &delegate)
	second := turnDriver(first, []persona.MCPServer{relayServer}, nil)
	for i, tc := range []struct {
		d    Driver
		want []string
	}{{first, []string{"developer_knowledge", "a2a"}}, {second, []string{"developer_knowledge"}}} {
		line, err := tc.d.EncodeUser("hello")
		if err != nil {
			t.Fatal(err)
		}
		var env coreagentwire.Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, s := range env.MCP {
			names = append(names, s.Name)
		}
		if !reflect.DeepEqual(names, tc.want) {
			t.Errorf("turn %d servers = %v, want %v", i+1, names, tc.want)
		}
	}
	if _, ok := turnDriver(claudeDriver{}, []persona.MCPServer{relayServer}, nil).(claudeDriver); !ok {
		t.Error("the Claude Code driver was changed")
	}
}

// Through a profile worker's own run on core-agent: a profile that delegates
// gets the delegate server in its turn's stdin envelope, after the persona's
// servers, as the same command, args and socket the Claude Code render gives
// its --mcp-config entry, with only the delegate tool; a task with a via, and
// a profile that doesn't delegate, get none, and keep the persona's servers.
func TestACoreAgentProfileTurnGetsTheDelegateServerExceptOnATaskWithVia(t *testing.T) {
	fixture, err := filepath.Abs("testdata/coreagent/text-only.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		delegate bool
		via      bool
		want     bool
	}{
		{"opted in", true, false, true},
		{"opted in, a task with via", true, true, false},
		{"not opted in", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := startServer(t)
			d := newFakeDispatcher(t, url)
			root := t.TempDir()
			harness := stub(t, fmt.Sprintf(`
read -r first || exit 1
printf '%%s' "$first" > %s/envelope
cat %q
`, root, fixture))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := profileWorkerConfig(t, url, harness)
			cfg.Driver = newCoreAgentDriver()
			pw := ProfileWorkerConfig{
				Tier: pwTier, ReadyAddr: "", deadlineMargin: time.Second,
				PersonaDir: coreAgentPersona(t), Harness: persona.CoreAgent, ConfigRoot: root,
				Relay: persona.Relay{URL: "http://credential-proxy:8765/mcp", Command: "/bin/true", Args: []string{"{url}"}},
			}
			if tc.delegate {
				cfg.DelegateSocket = delegateSock(t)
				pw.Delegate = ProfileDelegate("/usr/local/bin/worker-adapter", cfg.DelegateSocket)
			}
			ln, addr := readyListener(t)
			pw.ReadyAddr, pw.readyListener = addr, ln
			runErr := make(chan error, 1)
			go func() { runErr <- RunProfileWorker(ctx, cfg, pw) }()
			select {
			case err := <-runErr:
				t.Fatalf("the worker stopped before it was ready: %v", err)
			case <-time.After(500 * time.Millisecond):
			}
			waitReady(t, addr)

			const task = "task-coreagent-delegate"
			env := profileSubmission(t, task, "audit", nil)
			if tc.via {
				env.Authority = json.RawMessage(`{"via":[{"taskId":"parent-1"}]}`)
			}
			d.offer(task, env, 5)
			d.nextAccept(5 * time.Second)
			d.send(lib.DispatchMessage{Type: lib.DispatchConfirm, TaskID: task})
			if _, final := drainLeases(d, task, 20*time.Second); !final {
				t.Fatal("no final renewal")
			}
			raw, err := os.ReadFile(filepath.Join(root, "envelope"))
			if err != nil {
				t.Fatal(err)
			}
			var got coreagentwire.Envelope
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("the turn's envelope doesn't parse: %v\n%s", err, raw)
			}
			var delegate *coreagentwire.MCPServer
			personaServers := 0
			for i, s := range got.MCP {
				if s.Name == persona.DelegateServer {
					delegate = &got.MCP[i]
					if i != len(got.MCP)-1 {
						t.Errorf("the delegate server is not after the persona's: %+v", got.MCP)
					}
					continue
				}
				personaServers++
			}
			if personaServers == 0 {
				t.Errorf("the persona's servers are gone: %+v", got.MCP)
			}
			if (delegate != nil) != tc.want {
				t.Fatalf("delegate server in the envelope: %v, want %v\n%+v", delegate != nil, tc.want, got.MCP)
			}
			if hasDelegateAllow(got.Policy) != tc.want {
				t.Errorf("delegate allow rule in the policy: %v, want %v\n%v", hasDelegateAllow(got.Policy), tc.want, got.Policy.Allow)
			}
			if delegate == nil {
				return
			}
			if delegate.Command != "/usr/local/bin/worker-adapter" || !reflect.DeepEqual(delegate.Args, []string{DelegateMCPCommand}) {
				t.Errorf("the delegate server starts %s %v, want the worker's %s subcommand", delegate.Command, delegate.Args, DelegateMCPCommand)
			}
			if !reflect.DeepEqual(delegate.Tools, []string{DelegateToolName}) {
				t.Errorf("the delegate server offers %v, want only %s", delegate.Tools, DelegateToolName)
			}
			if delegate.Env[EnvDelegateSocket] != cfg.DelegateSocket {
				t.Errorf("the delegate server dials %q, want the task's socket %q", delegate.Env[EnvDelegateSocket], cfg.DelegateSocket)
			}
		})
	}
}

// hasDelegateAllow reports whether a turn's policy lets the gate pass a call
// to the delegate tool.
func hasDelegateAllow(p coreagentwire.Policy) bool {
	name := persona.DelegateServer + "_" + DelegateToolName
	return slices.Contains(p.Allow, "mcp:"+name) && slices.Contains(p.Allow, "mcp:"+name+" *")
}

// recordingCoreAgentStub is a core-agent harness that keeps each run's stdin
// envelope as envelope.<n> under dir and answers with a recorded text turn.
func recordingCoreAgentStub(t *testing.T, dir string) []string {
	t.Helper()
	fixture, err := filepath.Abs("testdata/coreagent/text-only.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return stub(t, fmt.Sprintf(`
dir=%q
n=$(( $(cat "$dir/n" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$dir/n"
read -r first || exit 1
printf '%%s' "$first" > "$dir/envelope.$n"
cat %q
`, dir, fixture))
}

func readEnvelope(t *testing.T, dir string, n int) coreagentwire.Envelope {
	t.Helper()
	var env coreagentwire.Envelope
	if err := json.Unmarshal([]byte(readRun(t, dir, "envelope", n)), &env); err != nil {
		t.Fatalf("run %d's envelope doesn't parse: %v", n, err)
	}
	return env
}

// Through a reused session pod's own runs on core-agent: a human turn gets
// the delegate server, at that turn's own socket with the gateway's targets
// for it, and the gate's allow rule for the delegate tool beside the
// session's own grant; a wake gets neither, and nothing the first turn was
// given carries over.
func TestACoreAgentSessionTurnGetsTheDelegateServerAndItsGrantExceptOnAWake(t *testing.T) {
	url := startServer(t)
	c := testClient(t, url)
	const session = "chat-otter-ca1"
	dir := t.TempDir()
	sock := delegateSock(t)
	targets := lib.DelegateTargets{Targets: []lib.DelegateTarget{{Name: "platform"}, {Name: "cluster", Description: "reads one cluster"}}}
	part, err := lib.DelegateTargetsPart(targets)
	if err != nil {
		t.Fatal(err)
	}
	submitWithParts(t, c, session, "task-ca-1", "use cluster", part)
	cfg := reuseConfig(url, "task-ca-1", session, recordingCoreAgentStub(t, dir))
	cfg.DelegateSocket = sock
	cfg.Driver = NewCoreAgentDriver(coreagentwire.Policy{Tools: []string{"read_file"}, Allow: []string{"read_file:/workspace/*"}}, nil)
	cfg.DelegateMCP = DelegateServerFor(ProfileDelegate("/usr/local/bin/worker-adapter", sock))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runAdapter(ctx, cfg)
	waitState(t, c, session, "task-ca-1", lib.StateCompleted)

	first := readEnvelope(t, dir, 1)
	if len(first.MCP) != 1 || first.MCP[0].Name != persona.DelegateServer {
		t.Fatalf("the human turn's servers = %+v, want the delegate server alone", first.MCP)
	}
	d := first.MCP[0]
	if d.Env[EnvDelegateSocket] == "" || d.Env[EnvDelegateSocket] == sock {
		t.Errorf("the reused pod's turn dials %q, want a socket of its own beside %q", d.Env[EnvDelegateSocket], sock)
	}
	var got lib.DelegateTargets
	if err := json.Unmarshal([]byte(d.Env[EnvDelegateTargets]), &got); err != nil || len(got.Targets) != 2 || got.Targets[1].Name != "cluster" {
		t.Errorf("the turn's targets %q (%v), want the gateway's two", d.Env[EnvDelegateTargets], err)
	}
	if !hasDelegateAllow(first.Policy) || !slices.Contains(first.Policy.Allow, "read_file:/workspace/*") {
		t.Errorf("the human turn's allow rules = %v, want the session's and the delegate tool's", first.Policy.Allow)
	}

	submitWithAuthority(t, c, session, "task-ca-2", "the delegation came back", authorityWithVia(t, c, "task-ca-2", session))
	waitRun(t, dir, "envelope", 2)
	waitState(t, c, session, "task-ca-2", lib.StateCompleted)
	wake := readEnvelope(t, dir, 2)
	if len(wake.MCP) != 0 {
		t.Errorf("the wake's servers = %+v, want none", wake.MCP)
	}
	if hasDelegateAllow(wake.Policy) || !slices.Contains(wake.Policy.Allow, "read_file:/workspace/*") {
		t.Errorf("the wake's allow rules = %v, want the session's only", wake.Policy.Allow)
	}
	cancel()
	waitOutcome(t, done, 30*time.Second)
}

// One turn's delegate server: the turn's socket over the template's, the
// submission's delegateTargets part when it has one, no stale list when it
// hasn't, nothing for a turn that may not delegate, and the template left as
// it was.
func TestTurnDelegateServerCarriesTheTurnsSocketAndTargets(t *testing.T) {
	tmpl := DelegateServerFor(ProfileDelegate("/self", "/run/base.sock"))
	if tmpl == nil || tmpl.Name != persona.DelegateServer || !reflect.DeepEqual(tmpl.Args, []string{DelegateMCPCommand}) || !reflect.DeepEqual(tmpl.Tools, []string{DelegateToolName}) {
		t.Fatalf("delegateMCPServer = %+v", tmpl)
	}
	if DelegateServerFor(persona.Delegate{}) != nil {
		t.Error("the zero Delegate gave a server")
	}
	tmpl.Env[EnvDelegateTargets] = `{"targets":[{"name":"stale"}]}`

	targets := lib.DelegateTargets{Targets: []lib.DelegateTarget{{Name: "platform"}, {Name: "e2e5"}}}
	part, err := lib.DelegateTargetsPart(targets)
	if err != nil {
		t.Fatal(err)
	}
	msg := func(parts ...lib.Part) *lib.Envelope {
		raw, err := json.Marshal(lib.Message{Role: "user", Parts: append([]lib.Part{{Kind: "text", Text: "hi"}}, parts...)})
		if err != nil {
			t.Fatal(err)
		}
		return &lib.Envelope{Payload: raw}
	}

	s := turnDelegateServer(tmpl, true, "/run/turn-1.sock", msg(part))
	if s.Env[EnvDelegateSocket] != "/run/turn-1.sock" {
		t.Errorf("socket %q, want the turn's", s.Env[EnvDelegateSocket])
	}
	var gotTargets lib.DelegateTargets
	if err := json.Unmarshal([]byte(s.Env[EnvDelegateTargets]), &gotTargets); err != nil || len(gotTargets.Targets) != 2 || gotTargets.Targets[1].Name != "e2e5" {
		t.Errorf("targets %q (%v), want the turn's two", s.Env[EnvDelegateTargets], err)
	}
	if s := turnDelegateServer(tmpl, true, "/run/turn-2.sock", msg()); s.Env[EnvDelegateTargets] != "" {
		t.Errorf("a turn with no part kept a list: %q", s.Env[EnvDelegateTargets])
	}
	if turnDelegateServer(tmpl, false, "/run/turn-3.sock", msg(part)) != nil || turnDelegateServer(nil, true, "/run/t.sock", msg()) != nil || turnDelegateServer(tmpl, true, "", msg()) != nil {
		t.Error("a turn that may not delegate got a server")
	}
	if tmpl.Env[EnvDelegateSocket] != "/run/base.sock" || tmpl.Env[EnvDelegateTargets] == "" {
		t.Errorf("the template was changed: %+v", tmpl.Env)
	}
}

// coreAgentPersona is the adapter tests' example persona without its script
// skill, which core-agent refuses for a persona that grants no shell tool.
func coreAgentPersona(t *testing.T) string {
	t.Helper()
	src, dst := "../lib/persona/testdata/example", t.TempDir()
	for _, rel := range []string{"persona.yaml", "mcp.yaml", "instructions.md", "skills/notes/SKILL.md"} {
		b, err := os.ReadFile(filepath.Join(src, rel))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, rel), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}
