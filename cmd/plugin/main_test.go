package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRouteConfiguredResponsesModelsIncludingContinuation(t *testing.T) {
	config := []byte("enabled: true\napi_key: test-key\nmodels:\n  - name: search-gpt\n    upstream: gpt-upstream\n    provider: codex\n")
	request, _ := json.Marshal(lifecycleRequest{ConfigYAML: config})
	if _, err := handleMethod("plugin.register", request); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		model, format string
		want          bool
	}{
		{"search-gpt", "openai-response", true},
		{"search-gpt", "responses", true},
		{"search-gpt", "claude", false},
		{"other", "openai-response", false},
	} {
		raw, _ := json.Marshal(routeRequest{RequestedModel: test.model, SourceFormat: test.format})
		result, err := handleMethod("model.route", raw)
		if err != nil {
			t.Fatal(err)
		}
		var env struct {
			Result struct{ Handled bool }
		}
		if err := json.Unmarshal(result, &env); err != nil {
			t.Fatal(err)
		}
		if env.Result.Handled != test.want {
			t.Fatalf("route model=%q format=%q handled=%v, want %v", test.model, test.format, env.Result.Handled, test.want)
		}
	}
	var reg struct {
		Result struct {
			Capabilities struct {
				WebSearchModels []string `json:"web_search_models"`
			} `json:"capabilities"`
		} `json:"result"`
	}
	raw, _ := handleMethod("plugin.register", request)
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatal(err)
	}
	if len(reg.Result.Capabilities.WebSearchModels) != 1 || reg.Result.Capabilities.WebSearchModels[0] != "search-gpt" {
		t.Fatalf("web search models = %#v", reg.Result.Capabilities.WebSearchModels)
	}
}

func TestHostModelStreamCollectsCompletedResponseAndCloses(t *testing.T) {
	previous := hostCall
	defer func() { hostCall = previous }()
	completed := `{"type":"response.completed","response":{"id":"resp_host","object":"response","status":"completed","output":[{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":3,"output_tokens":1}}}`
	frames := []byte("event: response.completed\ndata: " + completed + "\n\n")
	read := 0
	closed := false
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			request := payload.(hostModelRequest)
			if request.EntryProtocol != "openai-response" || request.ExitProtocol != "openai-response" || request.Model != "gpt-upstream" || request.ForcedProvider != "codex" || request.HostCallbackID != "callback-1" {
				t.Fatalf("unexpected host model request: %#v", request)
			}
			var body map[string]any
			if err := json.Unmarshal(request.Body, &body); err != nil {
				t.Fatal(err)
			}
			if body["stream"] != true || body["model"] != "gpt-upstream" {
				t.Fatalf("body = %#v", body)
			}
			return json.RawMessage(`{"status_code":200,"stream_id":"upstream-1"}`), nil
		case "host.model.stream_read":
			var chunk []byte
			if read == 0 {
				chunk = frames[:19]
			} else {
				chunk = frames[19:]
			}
			read++
			raw, _ := json.Marshal(struct {
				Payload []byte `json:"payload"`
				Done    bool   `json:"done"`
			}{chunk, read == 2})
			return raw, nil
		case "host.model.stream_close":
			closed = true
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected host method %s", method)
			return nil, nil
		}
	}
	response, err := executeHostModel(context.Background(), executorRequest{HostCallbackID: "callback-1"}, modelConfig{Name: "search-gpt", Upstream: "gpt-upstream", Provider: "codex"}, []byte(`{"model":"search-gpt","input":"hi","stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	if !closed {
		t.Fatal("host stream was not closed")
	}
	if !strings.Contains(string(response), `"id":"resp_host"`) || !strings.Contains(string(response), `"input_tokens":3`) {
		t.Fatalf("response = %s", response)
	}
}

func TestResponseSSEIncludesOutputAndTerminalUsage(t *testing.T) {
	response := []byte(`{"id":"resp_1","object":"response","status":"completed","output":[{"id":"call_1","type":"function_call","name":"lookup","arguments":"{\"q\":\"test\"}"},{"id":"msg_1","type":"message","content":[{"type":"output_text","text":"answer","annotations":[{"type":"url_citation","url":"https://example.com"}]}]}],"usage":{"input_tokens":3,"output_tokens":1}}`)
	sse, err := responseSSE(response)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []string{"response.created", "response.function_call_arguments.done", "response.output_text.done", "response.output_item.done", "response.completed"} {
		if !strings.Contains(string(sse), "event: "+event+"\n") {
			t.Fatalf("missing %s: %s", event, sse)
		}
	}
	if !strings.Contains(string(sse), `"input_tokens":3`) {
		t.Fatal("terminal usage missing")
	}
	if !strings.Contains(string(sse), `"annotations"`) || strings.Contains(string(sse), `"Text"`) {
		t.Fatal("content annotations or wire casing lost")
	}
	if !strings.Contains(string(sse), `"arguments":""`) {
		t.Fatal("function call added event must start with empty arguments")
	}
}

func TestCollectorRequiresCompletedEvent(t *testing.T) {
	collector := newResponsesCollector()
	if err := collector.feed([]byte("event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\"}}\n\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.result(); err == nil {
		t.Fatal("output item without response.completed was accepted")
	}
	if err := collector.feed(make([]byte, maxModelStreamBytes)); err == nil {
		t.Fatal("cumulative stream size limit was not enforced")
	}
}

func TestResponseSSERejectsIncomplete(t *testing.T) {
	if _, err := responseSSE([]byte(`{"status":"incomplete","output":[]}`)); err == nil {
		t.Fatal("incomplete response was emitted as completed")
	}
}

func TestCompactDelegatesNonstreamWithAlt(t *testing.T) {
	previous := hostCall
	defer func() { hostCall = previous }()
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		if method != "host.model.execute" {
			t.Fatalf("method = %q", method)
		}
		request := payload.(hostModelRequest)
		if request.Stream || request.Alt != "responses/compact" || request.Model != "gpt-upstream" || request.ForcedProvider != "codex" {
			t.Fatalf("request = %#v", request)
		}
		var body map[string]any
		if err := json.Unmarshal(request.Body, &body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != false || body["model"] != "gpt-upstream" {
			t.Fatalf("body = %#v", body)
		}
		raw, _ := json.Marshal(hostModelResponse{StatusCode: 200, Body: []byte(`{"id":"resp_compact","output":[]}`)})
		return raw, nil
	}
	response, err := executeHostModelNonstream(context.Background(), executorRequest{HostCallbackID: "callback-1", Alt: "responses/compact"}, modelConfig{Upstream: "gpt-upstream", Provider: "codex"}, []byte(`{"model":"search-gpt","input":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response), "resp_compact") {
		t.Fatalf("response = %s", response)
	}
}
