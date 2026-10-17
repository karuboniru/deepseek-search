package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func assertSearchCacheLog(t *testing.T, payload any, callbackID string) {
	t.Helper()
	want := map[string]any{
		"host_callback_id": callbackID,
		"level":            "info",
		"message":          "DeepSeek search restored historical search context from server cache",
		"fields": map[string]any{
			"plugin": "deepseek-search",
			"event":  "search_context_cache_fallback",
		},
	}
	// The exact allowlist keeps credentials, queries, results, and other request
	// context out of the diagnostic while retaining host request correlation.
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("cache fallback log = %#v, want %#v", payload, want)
	}
}

func TestLogSearchCacheRestoreUsesRequestScopeAndIgnoresHostFailure(t *testing.T) {
	previous := hostCall
	t.Cleanup(func() { hostCall = previous })
	for _, hostErr := range []error{nil, errors.New("host logger unavailable")} {
		calls := 0
		hostCall = func(method string, payload any) (json.RawMessage, error) {
			calls++
			if method != "host.log" {
				t.Fatalf("host method = %q, want host.log", method)
			}
			assertSearchCacheLog(t, payload, "request-log-callback")
			return nil, hostErr
		}
		ctx := context.WithValue(context.Background(), callbackIDContextKey{}, "request-log-callback")
		logSearchCacheRestore(ctx)
		if calls != 1 {
			t.Fatalf("host log calls = %d, want 1", calls)
		}
	}
}

func TestConfiguredEngineLogsCacheFallbackWithoutFailingRequest(t *testing.T) {
	previousHost, previousConfig := hostCall, activeConfig.Load()
	t.Cleanup(func() {
		hostCall = previousHost
		activeConfig.Store(previousConfig)
	})
	if err := configure(liveTestJSON(lifecycleRequest{ConfigYAML: []byte("enabled: true\napi_key: secret-cache-test-key\nmodels:\n  - name: cache-search\n    upstream: cache-upstream\n")})); err != nil {
		t.Fatal(err)
	}
	logs, searches, modelCalls := 0, 0, 0
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.http.operation_open":
			return json.RawMessage(`{"operation_id":"cache-search-operation"}`), nil
		case "host.http.do_stream":
			searches++
			return json.RawMessage(`{"status_code":200,"stream_id":"cache-search-stream"}`), nil
		case "host.http.stream_read":
			return liveTestJSON(hostHTTPStreamChunk{Payload: []byte(`{"results":[{"title":"secret-cache-search-result","url":"https://example.com/private-result"}]}`), Done: true}), nil
		case "host.http.cancel", "host.http.stream_close":
			return json.RawMessage(`{}`), nil
		case "host.log":
			logs++
			assertSearchCacheLog(t, payload, "cache-replay-request")
			return nil, errors.New("host logger unavailable")
		case "host.model.execute":
			modelCalls++
			request := payload.(hostModelRequest)
			if request.HostCallbackID != "cache-replay-request" {
				t.Fatalf("upstream callback ID = %q", request.HostCallbackID)
			}
			for _, restored := range []string{"secret-cache-query", "secret-cache-search-result", "function_call_output"} {
				if !bytes.Contains(request.Body, []byte(restored)) {
					t.Fatalf("cache replay lost %q: %s", restored, request.Body)
				}
			}
			return liveTestJSON(hostModelResponse{StatusCode: 200, Body: []byte(`{"id":"cache-replay-answer","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Cache replay completed"}]}]}`)}), nil
		default:
			t.Fatalf("unexpected host method %q", method)
			return nil, nil
		}
	}
	seedCalls := 0
	ctx := context.WithValue(context.Background(), callbackIDContextKey{}, "cache-seed-request")
	initial, err := currentConfig().engine.Run(ctx, "cache-tenant", []byte(`{"model":"cache-search","input":"Question","tools":[{"type":"web_search"}]}`), func(context.Context, []byte) ([]byte, error) {
		seedCalls++
		if seedCalls == 1 {
			return []byte(`{"id":"cache-search-round","status":"completed","output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"cache-private-call","arguments":"{\"action\":\"search\",\"query\":\"secret-cache-query\"}"}]}`), nil
		}
		return []byte(`{"id":"cache-seed-answer","status":"completed","output":[]}`), nil
	})
	if err != nil {
		t.Fatalf("seed search history: %v", err)
	}
	var response struct {
		Output []map[string]json.RawMessage `json:"output"`
	}
	if err := json.Unmarshal(initial, &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Output) != 1 || liveString(response.Output[0]["type"]) != "web_search_call" {
		t.Fatalf("seed output = %s", initial)
	}
	// A legacy client returns the public item without its embedded capsule.
	delete(response.Output[0], "encrypted_content")
	input := []any{map[string]string{"role": "user", "content": "Question"}, response.Output[0], map[string]string{"role": "user", "content": "Follow up"}}
	request := executorRequest{
		Model: "cache-search", Format: "openai-response", HostCallbackID: "cache-replay-request",
		Metadata: map[string]any{"caller_scope": "cache-tenant"},
		Payload:  liveTestJSON(map[string]any{"model": "cache-search", "input": input, "tools": []any{map[string]string{"type": "web_search"}}}),
	}
	replayed, err := runEngine(context.Background(), currentConfig(), request)
	if err != nil || !bytes.Contains(replayed, []byte("Cache replay completed")) {
		t.Fatalf("logging failure affected replay: %v, %s", err, replayed)
	}
	if logs != 1 || searches != 1 || modelCalls != 1 {
		t.Fatalf("cache replay logs=%d searches=%d model calls=%d, want 1 each", logs, searches, modelCalls)
	}
}
