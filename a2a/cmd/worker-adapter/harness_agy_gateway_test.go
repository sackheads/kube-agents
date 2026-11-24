package main

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/gke-labs/kube-agents/a2a/lib"
	"github.com/gke-labs/kube-agents/a2a/lib/persona"
	workeradapter "github.com/gke-labs/kube-agents/a2a/worker-adapter"
)

// fakeAgyEnv makes this test binary act as agy for one model call
// (TestFakeAgy): it reads its gateway from its env and POSTs one
// streamGenerateContent there, as agy in gateway mode does.
const fakeAgyEnv = "WORKER_ADAPTER_TEST_FAKE_AGY"

func TestFakeAgy(t *testing.T) {
	if os.Getenv(fakeAgyEnv) != "1" {
		t.Skip("helper for TestAnAgyWorkerCallsTheGatewayThroughOneProxy")
	}
	for _, key := range agyWithheldEnv[2:] {
		if _, set := os.LookupEnv(key); set {
			fmt.Fprintf(os.Stderr, "agy's env has %s\n", key)
			os.Exit(5)
		}
	}
	gw, model := os.Getenv(agyGatewayURLEnv), os.Getenv(agyGatewayModelsEnv)
	resp, err := http.Post(gw+"/v1beta/models/"+model+":streamGenerateContent?alt=sse", "application/json", strings.NewReader(`{"contents":[]}`))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(6)
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
	if resp.StatusCode != http.StatusOK {
		os.Exit(7)
	}
	os.Exit(0)
}

// recordingGateway is the install's gateway: it records each call's path
// and headers and answers one SSE chunk.
type recordingGateway struct {
	mu    sync.Mutex
	calls []*http.Request
}

func (g *recordingGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.calls = append(g.calls, r.Clone(r.Context()))
	g.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "data: {\"candidates\":[]}\n\n")
}

func (g *recordingGateway) seen() []*http.Request {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.calls)
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestAnAgyWorkerCallsTheGatewayThroughOneProxy: the pod points agy at
// another gateway and carries a key; the env the adapter builds (builder,
// then startAgyGateway) points agy at the loopback proxy, with the
// adapter's model, and carries neither. Two agy processes in a row, as
// #6710's per-turn respawn runs them on one env, both reach the install's
// gateway through the one proxy, on /v1beta/..., with none of the pod's
// credentials. A path off the proxy's allowlist is refused, and after Close
// the proxy is gone.
func TestAnAgyWorkerCallsTheGatewayThroughOneProxy(t *testing.T) {
	gw := &recordingGateway{}
	up := httptest.NewServer(gw)
	t.Cleanup(up.Close)
	stray := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("the pod's own AGY_LLM_GATEWAY_URL was called: %s", r.URL.Path)
	}))
	t.Cleanup(stray.Close)

	pod := map[string]string{
		modelBaseURLEnv:     up.URL,
		modelAPIKeyEnv:      "sk-pod",
		agyGatewayURLEnv:    stray.URL,
		agyGatewayModelsEnv: "pod-model",
		"A2A_MODEL":         "gemini-default",
	}
	var podEnv []string
	for k, v := range pod {
		podEnv = append(podEnv, k+"="+v)
	}
	podEnv = append(podEnv, "PATH="+os.Getenv("PATH"))
	started := 0
	start := func(gateway string, log *slog.Logger) (*workeradapter.GatewayProxy, error) {
		started++
		return workeradapter.StartGatewayProxy(gateway, log)
	}
	env, closer, err := startAgyGateway(persona.Antigravity, agyBuilder{}.env(podEnv, nil), func(k string) string { return pod[k] }, start, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	proxy := closer.(*workeradapter.GatewayProxy)
	if started != 1 {
		t.Fatalf("started %d proxies, want 1", started)
	}
	if got := envValue(env, agyGatewayURLEnv); got != proxy.URL || !strings.HasPrefix(got, "http://127.0.0.1:") {
		t.Errorf("%s=%q, want the proxy's loopback URL %q", agyGatewayURLEnv, got, proxy.URL)
	}
	if got := envValue(env, agyGatewayModelsEnv); got != "gemini-default" {
		t.Errorf("%s=%q, want the adapter's model", agyGatewayModelsEnv, got)
	}
	for _, key := range []string{modelAPIKeyEnv, modelBaseURLEnv} {
		if n := envCount(env, key); n != 0 {
			t.Errorf("agy's env carries %s", key)
		}
	}
	for _, key := range []string{agyGatewayURLEnv, agyGatewayModelsEnv} {
		if n := envCount(env, key); n != 1 {
			t.Errorf("agy's env sets %s %d times, want once", key, n)
		}
	}

	for turn := 1; turn <= 2; turn++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFakeAgy$")
		cmd.Env = append(slices.Clone(env), fakeAgyEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("agy process %d: %v: %s", turn, err, out)
		}
		if !strings.Contains(string(out), "candidates") {
			t.Errorf("agy process %d got %q, want the gateway's answer", turn, out)
		}
	}
	calls := gw.seen()
	if len(calls) != 2 {
		t.Fatalf("the gateway saw %d calls, want 2", len(calls))
	}
	for _, r := range calls {
		if r.URL.Path != "/v1beta/models/gemini-default:streamGenerateContent" {
			t.Errorf("gateway path %q", r.URL.Path)
		}
		if a := r.Header.Get("Authorization"); strings.Contains(a, "sk-pod") {
			t.Errorf("the pod's key reached the gateway from agy's side: %q", a)
		}
	}

	resp, err := http.Get(proxy.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("a path off the allowlist: %d, want 404", resp.StatusCode)
	}
	if len(gw.seen()) != 2 {
		t.Errorf("a refused path reached the gateway")
	}

	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if resp, err := http.Post(proxy.URL+"/v1beta/models/m:generateContent", "application/json", strings.NewReader("{}")); err == nil {
		resp.Body.Close()
		t.Errorf("the proxy still answers after Close: %d", resp.StatusCode)
	}
}

// TestTheAgyProxyGatewayFollowsClaudesRule: with no ANTHROPIC_BASE_URL the
// proxy forwards to the install's inference-gateway, and the model
// defaults to model-default.
func TestTheAgyProxyGatewayFollowsClaudesRule(t *testing.T) {
	var gotGateway string
	start := func(gateway string, log *slog.Logger) (*workeradapter.GatewayProxy, error) {
		gotGateway = gateway
		return &workeradapter.GatewayProxy{URL: "http://127.0.0.1:9"}, nil
	}
	env, _, err := startAgyGateway(persona.Antigravity, nil, func(string) string { return "" }, start, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if gotGateway != defaultModelBaseURL {
		t.Errorf("gateway %q, want %q", gotGateway, defaultModelBaseURL)
	}
	if got := envValue(env, agyGatewayModelsEnv); got != defaultModel {
		t.Errorf("model %q, want %q", got, defaultModel)
	}
}

// TestAClaudeWorkerStartsNoGatewayProxy: Claude reaches the gateway
// itself; its env comes back as it went in.
func TestAClaudeWorkerStartsNoGatewayProxy(t *testing.T) {
	start := func(string, *slog.Logger) (*workeradapter.GatewayProxy, error) {
		t.Fatal("a proxy was started for Claude")
		return nil, nil
	}
	in := []string{"ANTHROPIC_API_KEY=k", "ANTHROPIC_BASE_URL=http://g"}
	env, closer, err := startAgyGateway(persona.ClaudeCode, slices.Clone(in), func(string) string { return "" }, start, quietLog())
	if err != nil || closer != nil || !slices.Equal(env, in) {
		t.Errorf("claude: env %q closer %v err %v, want the env unchanged and no proxy", env, closer, err)
	}
}

// TestAProxyThatCannotStartRefusesTheAgyWorker: the error comes back, with
// no env, so run() exits rather than starting agy pointed elsewhere.
func TestAProxyThatCannotStartRefusesTheAgyWorker(t *testing.T) {
	start := func(string, *slog.Logger) (*workeradapter.GatewayProxy, error) {
		return nil, errors.New("listen: no ports")
	}
	env, closer, err := startAgyGateway(persona.Antigravity, []string{"HOME=/h"}, func(string) string { return "" }, start, quietLog())
	if err == nil || env != nil || closer != nil {
		t.Errorf("env %q closer %v err %v, want a refusal", env, closer, err)
	}
}

// TestRunRefusesAnAgyWorkerWhoseProxyCannotStart: run() exits 1 when the
// gateway URL is one the proxy won't forward to, before it dials the bus
// or runs any harness.
func TestRunRefusesAnAgyWorkerWhoseProxyCannotStart(t *testing.T) {
	code, stderr := runChild(t, "",
		"TASK_ID=task-wiring", "PROFILE=p", "NATS_URL=nats://127.0.0.1:1", "A2A_WORKDIR="+t.TempDir(),
		lib.EnvHarness+"=antigravity", lib.EnvHarnessUnisolated+"="+lib.HarnessUnisolatedAllow, "A2A_HARNESS_PATH=/bin/false",
		modelBaseURLEnv+"=ftp://inference-gateway")
	if code != 1 || !strings.Contains(stderr, "agy's gateway proxy") {
		t.Errorf("exit %d, stderr %q; want 1 and the proxy's refusal", code, stderr)
	}
	// It stops there: no dial of the bus, no harness turn.
	if strings.Contains(stderr, "adapter run failed") || strings.Contains(stderr, "nats") {
		t.Errorf("run() went on past the refusal: %q", stderr)
	}
}

func envValue(env []string, key string) string {
	v := ""
	for _, kv := range env {
		if k, val, ok := strings.Cut(kv, "="); ok && k == key {
			v = val
		}
	}
	return v
}

func envCount(env []string, key string) int {
	n := 0
	for _, kv := range env {
		if k, _, ok := strings.Cut(kv, "="); ok && k == key {
			n++
		}
	}
	return n
}
