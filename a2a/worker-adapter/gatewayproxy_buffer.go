package workeradapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// The gateway proxy holds each model call whole before the harness sees any
// of it (#6714, #6711). The Antigravity CLI reports a failed turn only for an
// HTTP 403 before any response header (status ERROR, not retried) or, after
// its retries, a 5xx before any header. A 400, an error event inside the
// stream, a stream that drops, or a stream cut short all read to it as a
// successful turn whose result is whatever text arrived. So the proxy reads
// the gateway's whole answer, checks it, and only then sends a status: the
// answer as it came, or one of two errors.
//
//   - gatewayProxyRefusedStatus (403 PERMISSION_DENIED): a policy refusal
//     (ToolPolicy) or a gateway refusal (a 4xx). Retrying gets the same
//     answer.
//   - gatewayProxyRetryStatus (503 UNAVAILABLE): the gateway failed, timed
//     out, was over the size cap, or its answer was cut short or did not
//     parse. agy retries it, a bounded number of times, then fails the turn.
//
// The cost is that agy's progress text arrives per model call, not per
// token.
const (
	gatewayProxyRefusedStatus = http.StatusForbidden
	gatewayProxyRetryStatus   = http.StatusServiceUnavailable
	// A request over the size cap is refused with the 403, not a 413, which
	// agy would read as an empty successful turn.
	gatewayProxyTooLargeMessage = "gateway proxy: the request is over the size limit"
	// gatewayProxyMaxHeldCalls bounds how many answers the proxy holds at
	// once, each up to gatewayProxyMaxResponseBytes. agy makes one call at a
	// time, and its subagents a few more.
	gatewayProxyMaxHeldCalls = 4
)

// ToolPolicy is the tool allowlist the proxy enforces on an agy worker's
// model calls (#6711), since agy can't be told to drop a tool it offers the
// model. Known reports whether name is a tool this agy version is known to
// offer; Granted whether the worker's persona grants it. The grant is fixed
// for the worker's life.
type ToolPolicy interface {
	Known(name string) bool
	Granted(name string) bool
}

// GatewayProxyOptions are the gateway proxy's optional settings.
type GatewayProxyOptions struct {
	// Tools, when set, is enforced on every call:
	//   - E: a request that offers a tool Tools doesn't know, a tool that
	//     isn't a function declaration, or a toolConfig is refused;
	//   - G1: declarations Tools doesn't grant are removed from the request
	//     before it is forwarded, and nothing else in the body changes;
	//   - G2: an answer whose functionCall names a tool the forwarded request
	//     didn't offer is refused.
	// Nil enforces nothing about tools; the proxy still holds each answer
	// whole.
	Tools ToolPolicy
}

// errToolPolicy is a request ToolPolicy refuses (E).
var errToolPolicy = errors.New("tool policy")

// offeredKey carries the tools a forwarded request offered, from the request
// handler to ModifyResponse.
type offeredKey struct{}

// offeredTools is what the forwarded request offered; nil when no policy is
// enforced.
type offeredTools map[string]bool

// gatewayError is the Gemini-format error body the proxy answers with.
func gatewayError(status int, message string) []byte {
	name := "PERMISSION_DENIED"
	if status == gatewayProxyRetryStatus {
		name = "UNAVAILABLE"
	}
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": status, "message": message, "status": name}})
	return b
}

// writeGatewayError answers a request the proxy refuses before forwarding.
func writeGatewayError(w http.ResponseWriter, status int, message string) {
	body := gatewayError(status, message)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// replaceResponse turns the gateway's response into one of the proxy's
// errors, before any of it reaches the harness.
func replaceResponse(resp *http.Response, status int, message string) {
	body := gatewayError(status, message)
	resp.StatusCode = status
	resp.Status = fmt.Sprintf("%d %s", status, http.StatusText(status))
	resp.Header = http.Header{"Content-Type": {"application/json"}}
	setBufferedBody(resp, body)
}

func setBufferedBody(resp *http.Response, body []byte) {
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.TransferEncoding = nil
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
}

// holdResponse is the proxy's ModifyResponse: read the whole answer, check
// it, and replay it or replace it. Messages name the status or the tool, and
// nothing from the body, since agy writes them into the turn's result.
func holdResponse(resp *http.Response, policy ToolPolicy) error {
	body, err := io.ReadAll(&cappedBody{r: resp.Body, left: gatewayProxyMaxResponseBytes})
	_ = resp.Body.Close()
	if err != nil {
		if errors.Is(err, errGatewayResponseTooLarge) {
			replaceResponse(resp, gatewayProxyRetryStatus, "gateway proxy: the model's answer was over the size limit")
		} else {
			replaceResponse(resp, gatewayProxyRetryStatus, "gateway proxy: the model's answer was cut short")
		}
		return nil
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		replaceResponse(resp, gatewayProxyRetryStatus, fmt.Sprintf("gateway proxy: the gateway answered HTTP %d", resp.StatusCode))
		return nil
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		replaceResponse(resp, gatewayProxyRefusedStatus, fmt.Sprintf("gateway proxy: the gateway refused the call (HTTP %d)", resp.StatusCode))
		return nil
	}
	countTokens := strings.HasSuffix(resp.Request.URL.Path, ":countTokens")
	calls, err := answerCalls(body, resp.Header.Get("Content-Type"), countTokens)
	switch {
	case errors.Is(err, errAnswerBlocked):
		replaceResponse(resp, gatewayProxyRefusedStatus, "gateway proxy: the model blocked the prompt")
		return nil
	case err != nil:
		replaceResponse(resp, gatewayProxyRetryStatus, "gateway proxy: the model's answer was cut short, malformed or an error")
		return nil
	}
	if offered, _ := resp.Request.Context().Value(offeredKey{}).(offeredTools); policy != nil && offered != nil {
		for _, name := range calls {
			if !offered[name] {
				replaceResponse(resp, gatewayProxyRefusedStatus, fmt.Sprintf("gateway proxy: the model called %s, a tool this worker doesn't grant", quoteShort(name)))
				return nil
			}
		}
	}
	setBufferedBody(resp, body)
	return nil
}

// geminiAnswer is the part of a GenerateContentResponse the proxy reads.
// Error is any error object the gateway put in the stream.
type geminiAnswer struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				FunctionCall *struct {
					Name string `json:"name"`
				} `json:"functionCall"`
			} `json:"parts"`
		} `json:"content"`
		FinishReason string `json:"finishReason"`
	} `json:"candidates"`
	PromptFeedback *struct {
		BlockReason string `json:"blockReason"`
	} `json:"promptFeedback"`
	Error json.RawMessage `json:"error"`
}

// Why answerCalls refuses an answer it parsed.
var (
	errAnswerCutShort = errors.New("cut short or malformed")
	errAnswerBlocked  = errors.New("prompt blocked")
)

// answerCalls returns every functionCall name in an answer. It fails with
// errAnswerCutShort when the answer is cut short, doesn't parse, or carries
// an error (an error object in any chunk, or an SSE `event: error`), and
// with errAnswerBlocked when the gateway's model blocked the prompt, which
// agy would otherwise record as an empty successful turn. A generation
// answer is whole when its last chunk carries a finishReason: a chunk after
// the finish means the stream went on past its end. An SSE answer also
// needs every event closed by a blank line. countTokens answers carry no
// finishReason and need only parse.
func answerCalls(body []byte, contentType string, countTokens bool) ([]string, error) {
	var chunks [][]byte
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/event-stream") {
		events, err := sseData(body)
		if err != nil {
			return nil, err
		}
		chunks = events
	} else {
		chunks = [][]byte{body}
	}
	if len(chunks) == 0 {
		return nil, errAnswerCutShort
	}
	var calls []string
	finished, blocked := false, false
	for _, c := range chunks {
		var a geminiAnswer
		if err := json.Unmarshal(c, &a); err != nil {
			return nil, errAnswerCutShort
		}
		if len(a.Error) > 0 && !bytes.Equal(a.Error, []byte("null")) {
			return nil, errAnswerCutShort
		}
		finished = false
		for _, cand := range a.Candidates {
			for _, p := range cand.Content.Parts {
				if p.FunctionCall != nil {
					calls = append(calls, p.FunctionCall.Name)
				}
			}
			if cand.FinishReason != "" {
				finished = true
			}
		}
		if a.PromptFeedback != nil && a.PromptFeedback.BlockReason != "" {
			blocked = true
		}
	}
	switch {
	case blocked:
		return nil, errAnswerBlocked
	case !finished && !countTokens:
		return nil, errAnswerCutShort
	}
	return calls, nil
}

// sseData returns the data of each event in an SSE body, over the bytes
// without copying the body. Every event must end with a blank line: one that
// doesn't was cut off. An `event: error` fails the answer. A `data: [DONE]`
// sentinel carries no chunk.
func sseData(body []byte) ([][]byte, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	if !bytes.HasSuffix(body, []byte("\n\n")) && !bytes.HasSuffix(body, []byte("\r\n\r\n")) {
		return nil, errAnswerCutShort
	}
	var out [][]byte
	var data [][]byte
	flush := func() {
		if len(data) > 0 {
			out = append(out, bytes.Join(data, []byte("\n")))
			data = nil
		}
	}
	for len(body) > 0 {
		line := body
		if k := bytes.IndexByte(body, '\n'); k >= 0 {
			line, body = body[:k], body[k+1:]
		} else {
			body = nil
		}
		line = bytes.TrimSuffix(line, []byte("\r"))
		switch {
		case len(line) == 0:
			flush()
		case bytes.HasPrefix(line, []byte("data:")):
			v := bytes.TrimPrefix(bytes.TrimPrefix(line, []byte("data:")), []byte(" "))
			if !bytes.Equal(bytes.TrimSpace(v), []byte("[DONE]")) {
				data = append(data, v)
			}
		case bytes.HasPrefix(line, []byte("event:")):
			if bytes.EqualFold(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("event:"))), []byte("error")) {
				return nil, errAnswerCutShort
			}
		}
	}
	flush()
	return out, nil
}

// applyToolPolicy is E and G1 on a request body: it returns the body with
// ungranted declarations removed and the tool names it still offers. Only
// the tools value changes; every other byte comes through as it was, the
// conversation's tool results, with any redaction markers, included. A
// countTokens call may nest the request as generateContentRequest, which
// gets the same treatment.
func applyToolPolicy(body []byte, policy ToolPolicy) ([]byte, offeredTools, error) {
	offered := offeredTools{}
	// Google's JSON accepts a field's proto name (snake_case) as well as its
	// JSON name, so both spellings are refused or rewritten. Keys that
	// differ only in case are refused first (strictKeys), so no reader can
	// take a different copy from this one.
	top, err := strictKeys(body)
	if err != nil {
		return nil, nil, err
	}
	// A key that folds to a policy field but isn't one of its exact
	// spellings ("Tools", "TOOL_CONFIG", one with a Kelvin sign) is refused:
	// this check reads exact keys, and a lenient reader downstream might not.
	for k := range top {
		norm := strings.ReplaceAll(k, "_", "")
		for _, field := range []string{"tools", "toolConfig", "generateContentRequest"} {
			if strings.EqualFold(norm, field) && !policyFieldSpellings[k] {
				return nil, nil, fmt.Errorf("%w: %s is not a spelling this proxy checks", errToolPolicy, quoteShort(k))
			}
		}
	}
	out := body
	for _, key := range []string{"toolConfig", "tool_config"} {
		out, err = rewriteObjectValue(out, key, func([]byte) ([]byte, error) {
			return nil, fmt.Errorf("%w: a toolConfig", errToolPolicy)
		})
		if err != nil {
			return nil, nil, err
		}
	}
	out, err = rewriteObjectValue(out, "tools", func(raw []byte) ([]byte, error) { return filterTools(raw, policy, offered) })
	if err != nil {
		return nil, nil, err
	}
	for _, key := range []string{"generateContentRequest", "generate_content_request"} {
		out, err = rewriteObjectValue(out, key, func(raw []byte) ([]byte, error) {
			inner, innerOffered, err := applyToolPolicy(raw, policy)
			for name := range innerOffered {
				offered[name] = true
			}
			return inner, err
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return out, offered, nil
}

// policyFieldSpellings are the exact keys applyToolPolicy reads: the JSON
// name and the proto name of each field it checks.
var policyFieldSpellings = map[string]bool{
	"tools": true, "toolConfig": true, "tool_config": true,
	"generateContentRequest": true, "generate_content_request": true,
}

// strictKeys returns a JSON object's top-level keys, and refuses one where
// two keys are equal ignoring case: Go's struct decoding folds case and
// keeps the last copy, the gateway's parser may keep the first, and a tool
// policy can only vouch for a body every reader sees the same way.
func strictKeys(obj []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("%w: not a JSON object", errToolPolicy)
	}
	fields := map[string]json.RawMessage{}
	var seen []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errToolPolicy, err)
		}
		key, _ := tok.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("%w: %v", errToolPolicy, err)
		}
		// Unicode simple folding, as encoding/json and other lenient
		// readers match keys, not only ASCII case.
		for _, prior := range seen {
			if strings.EqualFold(prior, key) {
				return nil, fmt.Errorf("%w: %s appears twice", errToolPolicy, quoteShort(key))
			}
		}
		seen = append(seen, key)
		fields[key] = raw
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, fmt.Errorf("%w: not a JSON object", errToolPolicy)
	}
	return fields, nil
}

// quoteShort quotes a name from a request or answer for an error message
// agy writes into the turn's result: escaped, and at most 64 bytes of it.
func quoteShort(s string) string {
	if len(s) > 64 {
		s = s[:64] + "..."
	}
	return strconv.Quote(s)
}

// rewriteObjectValue replaces the value of key in the JSON object body with
// fn's result, splicing it into the original bytes so nothing else changes.
// No key: body unchanged. A key that appears twice is refused, since the
// gateway's parser and this one might pick different copies.
func rewriteObjectValue(body []byte, key string, fn func([]byte) ([]byte, error)) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("%w: the request is not a JSON object", errToolPolicy)
	}
	start, end := -1, -1
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errToolPolicy, err)
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("%w: %v", errToolPolicy, err)
		}
		if tok != key {
			continue
		}
		if start >= 0 {
			return nil, fmt.Errorf("%w: %s appears twice", errToolPolicy, quoteShort(key))
		}
		end = int(dec.InputOffset())
		start = end - len(raw)
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, fmt.Errorf("%w: the request is not a JSON object", errToolPolicy)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: data after the request object", errToolPolicy)
	}
	if start < 0 {
		return body, nil
	}
	repl, err := fn(body[start:end])
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(body)-(end-start)+len(repl))
	out = append(out, body[:start]...)
	out = append(out, repl...)
	return append(out, body[end:]...), nil
}

// filterTools is E and G1 on a tools array. An entry whose declarations are
// all granted comes through byte for byte; one with none left is dropped;
// one with some left is re-encoded with those. Anything but
// functionDeclarations in an entry (a built-in tool such as googleSearch) is
// refused, as is a declaration Tools doesn't know.
func filterTools(raw []byte, policy ToolPolicy, offered offeredTools) ([]byte, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("%w: tools is not an array", errToolPolicy)
	}
	kept := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		fields, err := strictKeys(entry)
		if err != nil {
			return nil, err
		}
		for k := range fields {
			if k != "functionDeclarations" {
				return nil, fmt.Errorf("%w: a tools entry offers %s", errToolPolicy, quoteShort(k))
			}
		}
		var decls []json.RawMessage
		if err := json.Unmarshal(fields["functionDeclarations"], &decls); err != nil {
			return nil, fmt.Errorf("%w: functionDeclarations is not an array", errToolPolicy)
		}
		var granted []json.RawMessage
		for _, d := range decls {
			declFields, err := strictKeys(d)
			if err != nil {
				return nil, err
			}
			var name string
			if err := json.Unmarshal(declFields["name"], &name); err != nil || name == "" {
				return nil, fmt.Errorf("%w: a declaration has no name", errToolPolicy)
			}
			if !policy.Known(name) {
				return nil, fmt.Errorf("%w: the harness offers %s, a tool it isn't known to have", errToolPolicy, quoteShort(name))
			}
			if policy.Granted(name) {
				granted = append(granted, d)
				offered[name] = true
			}
		}
		switch {
		case len(granted) == len(decls) && len(decls) > 0:
			kept = append(kept, entry)
		case len(granted) > 0:
			b, err := json.Marshal(map[string][]json.RawMessage{"functionDeclarations": granted})
			if err != nil {
				return nil, err
			}
			kept = append(kept, b)
		}
	}
	return append(append([]byte{'['}, bytes.Join(kept, []byte{','})...), ']'), nil
}

// holdRequest is E and G1 on the incoming request, when a policy is set: the
// body is read whole, rewritten, and put back, and the tools it still offers
// ride on the context for holdResponse. It answers the refusal itself and
// returns nil when the request may not go on.
func holdRequest(w http.ResponseWriter, r *http.Request, policy ToolPolicy) *http.Request {
	if policy == nil {
		return r
	}
	if ce := r.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		writeGatewayError(w, gatewayProxyRefusedStatus, "gateway proxy: an encoded request body can't be checked")
		return nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeGatewayError(w, gatewayProxyRefusedStatus, gatewayProxyTooLargeMessage)
			return nil
		}
		http.Error(w, "request body unreadable", http.StatusBadRequest)
		return nil
	}
	var offered offeredTools
	if r.Method == http.MethodPost {
		body, offered, err = applyToolPolicy(body, policy)
		if err != nil {
			writeGatewayError(w, gatewayProxyRefusedStatus, "gateway proxy: "+err.Error())
			return nil
		}
	} else {
		if len(body) > 0 {
			writeGatewayError(w, gatewayProxyRefusedStatus, "gateway proxy: a GET with a body can't be checked")
			return nil
		}
		offered = offeredTools{}
	}
	r = r.WithContext(context.WithValue(r.Context(), offeredKey{}, offered))
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return r
}
