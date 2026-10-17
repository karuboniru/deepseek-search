package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type searchOptionsTransport func(*http.Request) (*http.Response, error)

func (f searchOptionsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Reset the reservation after each fake response so tests never wait on rate limits.
func searchOptionsClient(t *testing.T) (*DeepSeekClient, *[]map[string]any) {
	t.Helper()
	requests := new([]map[string]any)
	var client *DeepSeekClient
	transport := searchOptionsTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		*requests = append(*requests, body)
		client.searchGate.mu.Lock()
		client.searchGate.next = time.Time{}
		client.searchGate.mu.Unlock()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"results":[]}`))}, nil
	})
	var err error
	client, err = NewDeepSeekClient(ClientOptions{APIKey: "test", Count: 7, NeedContent: true, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	return client, requests
}

func assertSearchOptionsBody(t *testing.T, body map[string]any, query string, count int, content bool) {
	t.Helper()
	if body["query"] != query || body["count"] != float64(count) || body["need_content"] != content {
		t.Fatalf("search body = %v, want query=%q count=%d need_content=%v", body, query, count, content)
	}
}

func TestDeepSeekSearchOptionsOverrideAndRestoreDefaults(t *testing.T) {
	client, requests := searchOptionsClient(t)
	count, content := 1, false
	for _, test := range []struct {
		query string
		opts  SearchOptions
		count int
		need  bool
	}{
		{query: "both", opts: SearchOptions{Count: &count, NeedContent: &content}, count: 1},
		{query: "default", count: 7, need: true},
		{query: "content only", opts: SearchOptions{NeedContent: &content}, count: 7},
		{query: "count only", opts: SearchOptions{Count: &count}, count: 1, need: true},
	} {
		if _, err := client.SearchWithOptions(context.Background(), test.query, test.opts); err != nil {
			t.Fatal(err)
		}
		assertSearchOptionsBody(t, (*requests)[len(*requests)-1], test.query, test.count, test.need)
	}
	count = 50
	if _, err := client.SearchWithOptions(context.Background(), "maximum", SearchOptions{Count: &count}); err != nil {
		t.Fatal(err)
	}
	assertSearchOptionsBody(t, (*requests)[4], "maximum", 50, true)
	if _, err := client.Search(context.Background(), "legacy default"); err != nil {
		t.Fatal(err)
	}
	assertSearchOptionsBody(t, (*requests)[5], "legacy default", 7, true)
	if client.opts.Count != 7 || !client.opts.NeedContent {
		t.Fatalf("request overrides mutated client defaults: %+v", client.opts)
	}
}

func TestDeepSeekSearchOptionsRejectInvalidCountBeforeHTTP(t *testing.T) {
	client, requests := searchOptionsClient(t)
	for _, count := range []int{-1, 0, 51} {
		if _, err := client.SearchWithOptions(context.Background(), "valid", SearchOptions{Count: &count}); err == nil {
			t.Errorf("accepted count %d", count)
		}
	}
	if len(*requests) != 0 {
		t.Fatalf("invalid options reached HTTP: %v", *requests)
	}
}

func TestDeepSeekSearchOptionsStayIndependentDuringConcurrentCalls(t *testing.T) {
	firstStarted, releaseFirst := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	bodies := make(map[string]map[string]any)
	client, err := NewDeepSeekClient(ClientOptions{APIKey: "test", Count: 7, NeedContent: true, HTTPClient: &http.Client{Transport: searchOptionsTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			return nil, err
		}
		query, _ := body["query"].(string)
		mu.Lock()
		bodies[query] = body
		mu.Unlock()
		if query == "override" {
			close(firstStarted)
			<-releaseFirst
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"results":[]}`))}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	count, content := 2, false
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.SearchWithOptions(context.Background(), "override", SearchOptions{Count: &count, NeedContent: &content})
		firstDone <- err
	}()
	<-firstStarted
	client.searchGate.mu.Lock()
	client.searchGate.next = time.Time{}
	client.searchGate.mu.Unlock()
	_, errDefault := client.Search(context.Background(), "default")
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if errDefault != nil {
		t.Fatal(errDefault)
	}
	assertSearchOptionsBody(t, bodies["override"], "override", 2, false)
	assertSearchOptionsBody(t, bodies["default"], "default", 7, true)
}

func optionsModelOutput(t *testing.T, id, arguments string) string {
	t.Helper()
	call, err := json.Marshal(map[string]string{"type": "function_call", "name": searchFunction, "call_id": id, "arguments": arguments})
	if err != nil {
		t.Fatal(err)
	}
	return string(modelOutput(call))
}

func TestEngineSearchOptionsPerCallRoundAndRequest(t *testing.T) {
	client, requests := searchOptionsClient(t)
	engine := NewEngine(client, Options{})
	firstRound, _ := object([]byte(optionsModelOutput(t, "one", `{"action":"search","query":"override","count":3,"need_content":false}`)))
	var calls []json.RawMessage
	if err := json.Unmarshal(firstRound["output"], &calls); err != nil {
		t.Fatal(err)
	}
	calls = append(calls, searchCall("same round", "call default"))
	model, _ := scriptedModel(t,
		string(modelOutput(calls...)),
		optionsModelOutput(t, "two", `{"action":"search","queries":["multi one","multi two"],"count":50,"need_content":false}`),
		optionsModelOutput(t, "three", `{"action":"search","query":"round default"}`),
		`{"output":[]}`,
	)
	request := []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`)
	if _, err := engine.Run(context.Background(), "tenant", request, model); err != nil {
		t.Fatal(err)
	}
	model, _ = scriptedModel(t, optionsModelOutput(t, "next", `{"action":"search","query":"request default"}`), `{"output":[]}`)
	if _, err := engine.Run(context.Background(), "tenant", request, model); err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 6 {
		t.Fatalf("HTTP requests = %v, want six searches", *requests)
	}
	assertSearchOptionsBody(t, (*requests)[0], "override", 3, false)
	assertSearchOptionsBody(t, (*requests)[1], "call default", 7, true)
	assertSearchOptionsBody(t, (*requests)[2], "multi one", 50, false)
	assertSearchOptionsBody(t, (*requests)[3], "multi two", 50, false)
	assertSearchOptionsBody(t, (*requests)[4], "round default", 7, true)
	assertSearchOptionsBody(t, (*requests)[5], "request default", 7, true)
}

func TestEngineRejectsInvalidSearchOptionsAsToolErrors(t *testing.T) {
	for _, option := range []string{
		`"count":null`, `"count":0`, `"count":-1`, `"count":51`, `"count":1.5`, `"count":1.0`, `"count":1e1`, `"count":"2"`, `"count":true`, `"count":[]`, `"count":{}`, `"count":999999999999999999999999`,
		`"need_content":null`, `"need_content":"false"`, `"need_content":0`, `"need_content":[]`, `"need_content":{}`,
	} {
		t.Run(option, func(t *testing.T) {
			client, requests := searchOptionsClient(t)
			engine := NewEngine(client, Options{})
			model, modelRequests := scriptedModel(t, optionsModelOutput(t, "invalid", `{"action":"search","query":"valid",`+option+`}`), `{"output":[]}`)
			result, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
			if err != nil {
				t.Fatalf("option error should reach model as tool output: %v", err)
			}
			if len(*requests) != 0 {
				t.Fatalf("invalid options reached HTTP: %v", *requests)
			}
			if len(*modelRequests) != 2 || !strings.Contains(string((*modelRequests)[1]), "search_error") || !strings.Contains(string(result), `"status":"failed"`) {
				t.Fatalf("missing failed tool result: model=%s public=%s", *modelRequests, result)
			}
		})
	}
}

func TestEngineRejectsSearchOverridesForLegacySearcherAndOtherActions(t *testing.T) {
	for _, args := range []string{
		`{"action":"search","query":"valid","count":2}`,
		`{"action":"search","query":"valid","need_content":false}`,
		`{"action":"open_page","url":"https://example.com","count":2}`,
		`{"action":"open_page","url":"https://example.com","need_content":false}`,
		`{"action":"find_in_page","url":"https://example.com","pattern":"Go","count":2}`,
		`{"action":"find_in_page","url":"https://example.com","pattern":"Go","need_content":false}`,
	} {
		t.Run(args, func(t *testing.T) {
			searcher := new(fakeSearcher)
			engine := NewEngine(searcher, Options{})
			model, requests := scriptedModel(t, optionsModelOutput(t, "invalid", args), `{"output":[]}`)
			_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
			if err != nil {
				t.Fatal(err)
			}
			if len(searcher.queries) != 0 || len(searcher.urls) != 0 || !strings.Contains(string((*requests)[1]), "search_error") {
				t.Fatalf("unsupported overrides silently ignored: queries=%v urls=%v history=%s", searcher.queries, searcher.urls, (*requests)[1])
			}
		})
	}
}

func TestSearchToolSchemaHasOptionalSearchOptions(t *testing.T) {
	var tool struct {
		Description string `json:"description"`
		Parameters  struct {
			Properties map[string]struct {
				Type        string `json:"type"`
				Minimum     int    `json:"minimum"`
				Maximum     int    `json:"maximum"`
				Description string `json:"description"`
			} `json:"properties"`
			Required []string `json:"required"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(searchToolDefinition), &tool); err != nil {
		t.Fatal(err)
	}
	count := tool.Parameters.Properties["count"]
	if count.Type != "integer" || count.Minimum != 1 || count.Maximum != 50 || tool.Parameters.Properties["need_content"].Type != "boolean" {
		t.Fatalf("missing option types or count bounds: %s", searchToolDefinition)
	}
	for _, description := range []string{tool.Description, tool.Parameters.Properties["need_content"].Description} {
		if !strings.Contains(description, "context") || !strings.Contains(description, "summaries") {
			t.Fatalf("missing content context-cost guidance: %q", description)
		}
	}
	for _, required := range tool.Parameters.Required {
		if required == "count" || required == "need_content" {
			t.Fatalf("search override %q must remain optional", required)
		}
	}
}
