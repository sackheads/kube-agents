package workeradapter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixedPolicy knows the names in known and grants those in granted.
type fixedPolicy struct{ known, granted map[string]bool }

func (p fixedPolicy) Known(n string) bool   { return p.known[n] }
func (p fixedPolicy) Granted(n string) bool { return p.granted[n] }

func policyOf(known []string, granted ...string) fixedPolicy {
	p := fixedPolicy{known: map[string]bool{}, granted: map[string]bool{}}
	for _, n := range known {
		p.known[n] = true
	}
	for _, n := range granted {
		p.granted[n] = true
	}
	return p
}

var agyTools = []string{"read_file", "write_file", "run_command", "search_web", "send_message", "ask_question"}

// scriptedGateway records each request body and answers with status, type
// and body.
type scriptedGateway struct {
	status int
	ctype  string
	body   string
	mu     sync.Mutex
	bodies [][]byte
}

func (g *scriptedGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	g.mu.Lock()
	g.bodies = append(g.bodies, b)
	g.mu.Unlock()
	if g.ctype != "" {
		w.Header().Set("Content-Type", g.ctype)
	}
	if g.status != 0 {
		w.WriteHeader(g.status)
	}
	_, _ = io.WriteString(w, g.body)
}

func (g *scriptedGateway) seen() [][]byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([][]byte(nil), g.bodies...)
}

func proxyWith(t *testing.T, h http.Handler, policy ToolPolicy) string {
	t.Helper()
	up := httptest.NewServer(h)
	t.Cleanup(up.Close)
	var opts GatewayProxyOptions
	if policy != nil {
		opts.Tools = policy
	}
	p, err := StartGatewayProxyWith(up.URL, slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p.URL
}

type gotResponse struct {
	status int
	body   string
	errMsg string
	state  string
}

func post(t *testing.T, proxy, method, body string) gotResponse {
	t.Helper()
	resp, err := http.Post(proxy+"/v1beta/models/m:"+method, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	got := gotResponse{status: resp.StatusCode, body: string(b)}
	var e struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal(b, &e) == nil {
		got.errMsg, got.state = e.Error.Message, e.Error.Status
	}
	return got
}

const sseDone = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hi\"}]}}]}\r\n\r\n" +
	"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\" there\"}]},\"finishReason\":\"STOP\"}]}\r\n\r\n"

// TestTheProxyMapsTheGatewaysFailuresToWhatAgyCanSee: agy reads a 400, an
// in-stream error and a cut-short stream as success, so a gateway refusal
// becomes a 403 and a gateway failure a 503, before any header, and the
// gateway's own body never reaches the harness.
func TestTheProxyMapsTheGatewaysFailuresToWhatAgyCanSee(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		ctype  string
		body   string
		method string
		want   int
	}{
		{name: "gateway 400", status: 400, body: `{"error":"GWMARK"}`, want: 403},
		{name: "gateway 401", status: 401, body: `GWMARK`, want: 403},
		{name: "gateway 429", status: 429, body: `GWMARK`, want: 503},
		{name: "gateway 500", status: 500, body: `GWMARK`, want: 503},
		{name: "gateway 503", status: 503, body: `GWMARK`, want: 503},
		{name: "stream with no finishReason", ctype: "text/event-stream", body: "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"GWMARK\"}]}}]}\n\n", want: 503},
		{name: "last event not closed", ctype: "text/event-stream", body: strings.TrimSuffix(sseDone, "\r\n"), want: 503},
		{name: "in-stream error", ctype: "text/event-stream", body: "data: {\"error\":{\"code\":500,\"message\":\"GWMARK\"}}\n\n", want: 503},
		{name: "event that doesn't parse", ctype: "text/event-stream", body: "data: {\"candidates\":[GWMARK\n\n", want: 503},
		{name: "empty stream", ctype: "text/event-stream", body: "", want: 503},
		{name: "JSON with no finishReason", ctype: "application/json", body: `{"candidates":[{"content":{"parts":[{"text":"GWMARK"}]}}]}`, method: "generateContent", want: 503},
		{name: "finished stream", ctype: "text/event-stream", body: sseDone, want: 200},
		{name: "blocked prompt", ctype: "application/json", body: `{"promptFeedback":{"blockReason":"SAFETY"},"x":"GWMARK"}`, method: "generateContent", want: 403},
		{name: "finish then error event", ctype: "text/event-stream", body: sseDone + "data: {\"error\":{\"code\":500,\"message\":\"GWMARK\"}}\n\n", want: 503},
		{name: "error beside a finish", ctype: "text/event-stream", body: "data: {\"candidates\":[{\"finishReason\":\"STOP\"}],\"error\":{\"message\":\"GWMARK\"}}\n\n", want: 503},
		{name: "event: error", ctype: "text/event-stream", body: "event: error\ndata: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"GWMARK\"}]},\"finishReason\":\"STOP\"}]}\n\n", want: 503},
		{name: "text after the finish", ctype: "text/event-stream", body: sseDone + "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"GWMARK\"}]}}]}\n\n", want: 503},
		{name: "DONE sentinel after the finish", ctype: "text/event-stream", body: sseDone + "data: [DONE]\n\n", want: 200},
		{name: "JSON error with a finish", ctype: "application/json", body: `{"candidates":[{"finishReason":"STOP"}],"error":{"message":"GWMARK"}}`, method: "generateContent", want: 503},
		{name: "countTokens", ctype: "application/json", body: `{"totalTokens":7}`, method: "countTokens", want: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := proxyWith(t, &scriptedGateway{status: tc.status, ctype: tc.ctype, body: tc.body}, nil)
			method := tc.method
			if method == "" {
				method = "streamGenerateContent"
			}
			got := post(t, proxy, method, `{"contents":[]}`)
			if got.status != tc.want {
				t.Fatalf("status %d, want %d (body %q)", got.status, tc.want, got.body)
			}
			if tc.want == 200 {
				if got.body != tc.body {
					t.Errorf("body %q, want the gateway's %q byte for byte", got.body, tc.body)
				}
				return
			}
			if strings.Contains(got.body, "GWMARK") {
				t.Errorf("the gateway's body reached the harness: %q", got.body)
			}
			wantState := map[int]string{403: "PERMISSION_DENIED", 503: "UNAVAILABLE"}[tc.want]
			if got.state != wantState {
				t.Errorf("error status %q, want %q", got.state, wantState)
			}
		})
	}
}

// TestAStreamThatDropsIsRetryable: the gateway promises more than it sends
// and hangs up; the harness gets the 503, not the part that came.
func TestAStreamThatDropsIsRetryable(t *testing.T) {
	proxy := proxyWith(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "100000")
		// Whole-looking so far: only the hang-up says it was cut short.
		_, _ = io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"GWMARK\"}]},\"finishReason\":\"STOP\"}]}\n\n")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}), nil)
	got := post(t, proxy, "streamGenerateContent", `{}`)
	if got.status != 503 || strings.Contains(got.body, "GWMARK") {
		t.Errorf("status %d body %q, want 503 and none of the partial answer", got.status, got.body)
	}
}

// recordedAgyRequest is shaped like a request agy 1.5.13 sends in gateway
// mode: the conversation, with a tool result carrying a redaction marker,
// non-ASCII text, and escapes an encoder would rewrite (\u2028, \u003c,
// \/), keys in agy's order and odd spacing; then one declaration per tools
// entry, plus one entry with two, which agy hasn't been seen to send.
const recordedAgyRequest = `{"contents":[{"role":"user", "parts":[{"text":"<USER_REQUEST>\nlist the repo café ✓ \u2028 \u003cb> a\/b\n</USER_REQUEST>"}]},` +
	`{"role":"model","parts":[{"functionCall":{"name":"run_command","args":{"CommandLine":"git remote -v"}}}]},` +
	`{"role":"user","parts":[{"functionResponse":{"name":"run_command","response":{"output":"origin https://[REDACTED_GITHUB_TOKEN]@github.com/x/y.git é\t\"q\""}}}]}],` +
	`"systemInstruction":{"parts":[{"text":"You are a coding agent."}]},` +
	`"tools":[{"functionDeclarations":[{"name":"read_file", "description":"Read a <file>","parameters":{"type":"OBJECT"}}]},` +
	`{"functionDeclarations":[{"name":"run_command","description":"Run a shell command","parameters":{"type":"OBJECT"}}]},` +
	` {"functionDeclarations":[{"name":"search_web","description":"Search"}]},` +
	`{"functionDeclarations":[{"name":"ask_question"},{"name":"send_message","description":"Message"}]}],` +
	`"labels":{"agy":"1"}, "generationConfig":{"temperature":1,"thinkingConfig":{"includeThoughts":true}}}`

// TestTheProxyStripsUngrantedToolsAndNothingElse: G1. The gateway gets the
// request with only the ungranted declarations gone: every byte before and
// after the tools array is the one agy sent, the redaction marker and the
// escapes included, a fully granted entry is unchanged, and a part-granted
// entry keeps its granted declaration.
func TestTheProxyStripsUngrantedToolsAndNothingElse(t *testing.T) {
	gw := &scriptedGateway{ctype: "text/event-stream", body: sseDone}
	proxy := proxyWith(t, gw, policyOf(agyTools, "read_file", "send_message"))
	if got := post(t, proxy, "streamGenerateContent", recordedAgyRequest); got.status != 200 {
		t.Fatalf("status %d body %q", got.status, got.body)
	}
	sent := string(gw.seen()[0])
	i := strings.Index(recordedAgyRequest, `"tools":`) + len(`"tools":`)
	j := strings.Index(recordedAgyRequest, `,"labels"`)
	wantTools := `[{"functionDeclarations":[{"name":"read_file", "description":"Read a <file>","parameters":{"type":"OBJECT"}}]},` +
		`{"functionDeclarations":[{"name":"send_message","description":"Message"}]}]`
	want := recordedAgyRequest[:i] + wantTools + recordedAgyRequest[j:]
	if sent != want {
		t.Errorf("the gateway got\n%s\nwant\n%s", sent, want)
	}
	if !strings.HasPrefix(sent, recordedAgyRequest[:i]) || !strings.HasSuffix(sent, recordedAgyRequest[j:]) {
		t.Errorf("bytes outside the tools array changed")
	}
}

// TestTheProxyRefusesWhatThePolicyCantVouchFor: E. A tool the policy
// doesn't know, a built-in tool, a toolConfig, a second tools key and an
// encoded body are refused with the 403, and nothing reaches the gateway.
func TestTheProxyRefusesWhatThePolicyCantVouchFor(t *testing.T) {
	for _, tc := range []struct{ name, body, ctEnc string }{
		{name: "unknown tool", body: `{"tools":[{"functionDeclarations":[{"name":"read_file"}]},{"functionDeclarations":[{"name":"new_tool"}]}]}`},
		{name: "unknown and ungranted", body: `{"tools":[{"functionDeclarations":[{"name":"generate_image"}]}]}`},
		{name: "built-in tool", body: `{"tools":[{"googleSearch":{}}]}`},
		{name: "built-in beside declarations", body: `{"tools":[{"functionDeclarations":[{"name":"read_file"}],"codeExecution":{}}]}`},
		{name: "toolConfig", body: `{"tools":[],"toolConfig":{"functionCallingConfig":{"mode":"ANY"}}}`},
		{name: "tools twice", body: `{"tools":[],"tools":[{"functionDeclarations":[{"name":"run_command"}]}]}`},
		{name: "nameless declaration", body: `{"tools":[{"functionDeclarations":[{"description":"x"}]}]}`},
		{name: "not an object", body: `[1]`},
		{name: "trailing data", body: `{"tools":[]} {"tools":[]}`},
		{name: "nested countTokens request", body: `{"generateContentRequest":{"tools":[{"functionDeclarations":[{"name":"new_tool"}]}]}}`},
		{name: "encoded body", body: `{}`, ctEnc: "gzip"},
		{name: "name in another case", body: `{"tools":[{"functionDeclarations":[{"name":"run_command","NAME":"read_file"}]}]}`},
		{name: "name twice", body: `{"tools":[{"functionDeclarations":[{"name":"write_file","name":"read_file"}]}]}`},
		{name: "declarations twice in an entry", body: `{"tools":[{"functionDeclarations":[{"name":"write_file"}],"functionDeclarations":[{"name":"read_file"}]}]}`},
		{name: "tools in another case", body: `{"tools":[],"Tools":[{"functionDeclarations":[{"name":"write_file"}]}]}`},
		{name: "snake-case toolConfig", body: `{"tools":[],"tool_config":{"function_calling_config":{"mode":"ANY"}}}`},
		{name: "snake-case nested request", body: `{"generate_content_request":{"tools":[{"functionDeclarations":[{"name":"new_tool"}]}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw := &scriptedGateway{ctype: "text/event-stream", body: sseDone}
			proxy := proxyWith(t, gw, policyOf(agyTools, "read_file", "run_command"))
			req, _ := http.NewRequest(http.MethodPost, proxy+"/v1beta/models/m:streamGenerateContent", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.ctEnc != "" {
				req.Header.Set("Content-Encoding", tc.ctEnc)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 403 {
				t.Errorf("status %d, want 403", resp.StatusCode)
			}
			if n := len(gw.seen()); n != 0 {
				t.Errorf("the refused request reached the gateway")
			}
		})
	}
}

// TestTheProxyRefusesACallToAToolItDidntOffer: G2. The model's answer calls
// a tool the forwarded request didn't offer (stripped, or never there): the
// harness gets the 403 naming it, and none of the answer. A call to an
// offered tool comes back byte for byte.
func TestTheProxyRefusesACallToAToolItDidntOffer(t *testing.T) {
	call := func(name string) string {
		return "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}]}}]}\n\n" +
			"data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"" + name + "\",\"args\":{}}}]},\"finishReason\":\"STOP\"}]}\n\n"
	}
	req := `{"tools":[{"functionDeclarations":[{"name":"read_file"}]},{"functionDeclarations":[{"name":"run_command"}]}]}`
	for _, tc := range []struct {
		name, tool string
		want       int
	}{
		{name: "granted", tool: "read_file", want: 200},
		{name: "stripped", tool: "run_command", want: 403},
		{name: "never offered", tool: "write_file", want: 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := call(tc.tool)
			proxy := proxyWith(t, &scriptedGateway{ctype: "text/event-stream", body: body}, policyOf(agyTools, "read_file"))
			got := post(t, proxy, "streamGenerateContent", req)
			if got.status != tc.want {
				t.Fatalf("status %d, want %d (%q)", got.status, tc.want, got.body)
			}
			if tc.want == 200 && got.body != body {
				t.Errorf("body %q, want the gateway's byte for byte", got.body)
			}
			if tc.want == 403 && (got.state != "PERMISSION_DENIED" || !strings.Contains(got.errMsg, tc.tool) || strings.Contains(got.body, "ok")) {
				t.Errorf("refusal %q, want PERMISSION_DENIED naming %s and none of the answer", got.body, tc.tool)
			}
		})
	}
	t.Run("no policy", func(t *testing.T) {
		body := call("write_file")
		proxy := proxyWith(t, &scriptedGateway{ctype: "text/event-stream", body: body}, nil)
		if got := post(t, proxy, "streamGenerateContent", req); got.status != 200 || got.body != body {
			t.Errorf("without a policy: status %d body %q, want the answer as it came", got.status, got.body)
		}
	})
}

// TestARequestWithNoToolsOffersNone: with a policy, a request without tools
// passes unchanged, and any functionCall in its answer is refused.
func TestARequestWithNoToolsOffersNone(t *testing.T) {
	gw := &scriptedGateway{ctype: "application/json", body: `{"candidates":[{"content":{"parts":[{"functionCall":{"name":"read_file"}}]},"finishReason":"STOP"}]}`}
	proxy := proxyWith(t, gw, policyOf(agyTools, "read_file"))
	body := `{"contents":[{"parts":[{"text":"x"}]}]}`
	if got := post(t, proxy, "generateContent", body); got.status != 403 {
		t.Errorf("status %d, want 403", got.status)
	}
	if s := gw.seen(); len(s) != 1 || !bytes.Equal(s[0], []byte(body)) {
		t.Errorf("the gateway got %q, want the body unchanged", s)
	}
}

// TestTheHarnessGetsTheAnswerPlainWhenItAsksForGzip: the proxy doesn't pass
// the harness's Accept-Encoding on, so a gateway that compresses when asked
// is decompressed by the proxy's own transport, the proxy can read the
// answer, and the harness gets it plain.
func TestTheHarnessGetsTheAnswerPlainWhenItAsksForGzip(t *testing.T) {
	proxy := proxyWith(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			_, _ = io.WriteString(w, sseDone)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, _ = io.WriteString(zw, sseDone)
		_ = zw.Close()
	}), nil)
	req, _ := http.NewRequest(http.MethodPost, proxy+"/v1beta/models/m:streamGenerateContent", strings.NewReader("{}"))
	req.Header.Set("Accept-Encoding", "gzip")
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != sseDone || resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("status %d encoding %q body %q, want 200 and the answer plain", resp.StatusCode, resp.Header.Get("Content-Encoding"), b)
	}
}

// TestAGetWithABodyIsRefusedUnderAPolicy: a GET's body would go to the
// gateway unchecked, so with a policy it is refused.
func TestAGetWithABodyIsRefusedUnderAPolicy(t *testing.T) {
	gw := &scriptedGateway{ctype: "text/event-stream", body: sseDone}
	proxy := proxyWith(t, gw, policyOf(agyTools, "read_file"))
	req, _ := http.NewRequest(http.MethodGet, proxy+"/v1beta/models/m:streamGenerateContent", strings.NewReader(`{"tools":[{"functionDeclarations":[{"name":"run_command"}]}]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 || len(gw.seen()) != 0 {
		t.Errorf("status %d, gateway calls %d; want 403 and none", resp.StatusCode, len(gw.seen()))
	}
}

// TestRefusalsQuoteNamesShortAndEscaped: a name from the request or answer
// reaches agy's result escaped and cut to 64 bytes.
func TestRefusalsQuoteNamesShortAndEscaped(t *testing.T) {
	long := strings.Repeat("a", 200) + "\n"
	body := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"" + strings.ReplaceAll(long, "\n", "\\n") + "\"}}]},\"finishReason\":\"STOP\"}]}\n\n"
	proxy := proxyWith(t, &scriptedGateway{ctype: "text/event-stream", body: body}, policyOf(agyTools, "read_file"))
	got := post(t, proxy, "streamGenerateContent", `{}`)
	if got.status != 403 || strings.Contains(got.errMsg, strings.Repeat("a", 65)) || strings.Contains(got.errMsg, "\n") {
		t.Errorf("status %d message %q, want 403 with the name cut and escaped", got.status, got.errMsg)
	}
}

// TestTheProxyHoldsAtMostAFewAnswersAtOnce: calls past
// gatewayProxyMaxHeldCalls wait for a slot rather than each holding an
// answer.
func TestTheProxyHoldsAtMostAFewAnswersAtOnce(t *testing.T) {
	var mu sync.Mutex
	inFlight, peak := 0, 0
	release := make(chan struct{})
	proxy := proxyWith(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		<-release
		mu.Lock()
		inFlight--
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseDone)
	}), nil)
	var wg sync.WaitGroup
	for i := 0; i < gatewayProxyMaxHeldCalls+3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(proxy+"/v1beta/models/m:streamGenerateContent", "application/json", strings.NewReader("{}"))
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := inFlight
		mu.Unlock()
		if n >= gatewayProxyMaxHeldCalls {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if peak != gatewayProxyMaxHeldCalls {
		t.Errorf("peak %d calls at the gateway at once, want %d", peak, gatewayProxyMaxHeldCalls)
	}
}
