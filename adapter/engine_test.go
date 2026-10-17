package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type fakeSearcher struct {
	queries []string
	urls    []string
}

func (f *fakeSearcher) Search(_ context.Context, query string) (json.RawMessage, error) {
	f.queries = append(f.queries, query)
	return json.RawMessage(`{"results":[{"title":"Result","url":"https://example.com/page"}]}`), nil
}

func (f *fakeSearcher) Fetch(_ context.Context, url string) (json.RawMessage, error) {
	f.urls = append(f.urls, url)
	return json.RawMessage(`{"content":"A page about Go"}`), nil
}

func scriptedModel(t *testing.T, responses ...string) (Model, *[][]byte) {
	t.Helper()
	requests := new([][]byte)
	model := func(_ context.Context, body []byte) ([]byte, error) {
		*requests = append(*requests, append([]byte(nil), body...))
		index := len(*requests) - 1
		if index >= len(responses) {
			t.Fatalf("unexpected model call %d", index+1)
		}
		return []byte(responses[index]), nil
	}
	return model, requests
}

func TestSearchLoopPreservesReasoningAndPrivateReplay(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, requests := scriptedModel(t,
		`{"id":"upstream_1","output":[{"type":"reasoning","encrypted_content":"opaque"},{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go docs\"}"}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}`,
		`{"id":"upstream_2","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Result","annotations":[{"type":"url_citation","url":"https://example.com/page"}]}]}],"status":"completed","usage":{"input_tokens":7,"output_tokens":11,"total_tokens":18},"custom_top_level":{"kept":true}}`,
	)
	result, err := engine.Run(context.Background(), "tenant-a", []byte(`{"model":"deepseek","input":"Question","tools":[{"type":"web_search"}],"reasoning":{"effort":"high"}}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 2 || len(searcher.queries) != 1 || searcher.queries[0] != "Go docs" {
		t.Fatalf("requests=%d searches=%v", len(*requests), searcher.queries)
	}
	first, _ := object((*requests)[0])
	if !strings.Contains(string(first["tools"]), searchFunction) || string(first["reasoning"]) != `{"effort":"high"}` {
		t.Fatalf("tool rewrite lost request fields: %s", (*requests)[0])
	}
	second, _ := object((*requests)[1])
	if !strings.Contains(string(second["input"]), `"encrypted_content":"opaque"`) || !strings.Contains(string(second["input"]), `"function_call_output"`) {
		t.Fatalf("private history missing: %s", second["input"])
	}
	if strings.Contains(string(result), searchFunction) || strings.Contains(string(result), "function_call_output") {
		t.Fatalf("private shim leaked: %s", result)
	}
	public, _ := object(result)
	var output []map[string]json.RawMessage
	if err := json.Unmarshal(public["output"], &output); err != nil {
		t.Fatal(err)
	}
	if len(output) != 3 || stringValue(output[0]["type"]) != "reasoning" || stringValue(output[1]["type"]) != "web_search_call" || stringValue(output[2]["type"]) != "message" {
		t.Fatalf("unexpected public output: %s", public["output"])
	}
	if string(public["custom_top_level"]) != `{"kept":true}` {
		t.Fatalf("top-level fields lost: %s", result)
	}
	if string(public["usage"]) != `{"input_tokens":9,"output_tokens":14,"total_tokens":23}` {
		t.Fatalf("usage was not aggregated: %s", public["usage"])
	}
	if !strings.Contains(string(output[1]["action"]), "https://example.com/page") {
		t.Fatalf("source missing: %s", output[1]["action"])
	}
}

func TestMultipleSearchCallsAndRoundsHaveUniquePublicIDs(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"reused","arguments":"{\"action\":\"search\",\"query\":\"one\"}"},{"type":"function_call","name":"__deepseek_web_search","call_id":"second","arguments":"{\"action\":\"search\",\"query\":\"two\"}"}]}`,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"reused","arguments":"{\"action\":\"search\",\"query\":\"three\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Three results"}]}]}`,
	)
	result, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 3 || len(searcher.queries) != 3 {
		t.Fatalf("requests=%d queries=%v", len(*requests), searcher.queries)
	}
	response, _ := object(result)
	var output []map[string]json.RawMessage
	_ = json.Unmarshal(response["output"], &output)
	if len(output) != 4 {
		t.Fatalf("output=%s", response["output"])
	}
	ids := make(map[string]bool)
	for _, item := range output[:3] {
		id := stringValue(item["id"])
		if stringValue(item["type"]) != "web_search_call" || ids[id] || id == "" {
			t.Fatalf("duplicate or missing web ID: %s", response["output"])
		}
		ids[id] = true
	}
}

func TestExpandedTranscriptKeepsSearchRoundBoundaries(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	firstModel, _ := scriptedModel(t,
		`{"output":[{"type":"reasoning","encrypted_content":"round-one"},{"type":"function_call","name":"__deepseek_web_search","call_id":"same","arguments":"{\"action\":\"search\",\"query\":\"one\"}"}]}`,
		`{"output":[{"type":"reasoning","encrypted_content":"round-two"},{"type":"function_call","name":"__deepseek_web_search","call_id":"same","arguments":"{\"action\":\"search\",\"query\":\"two\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), firstModel)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := object(first)
	var output []json.RawMessage
	_ = json.Unmarshal(response["output"], &output)
	input := append([]json.RawMessage{json.RawMessage(`{"role":"user","content":"Question"}`)}, output...)
	inputBody, _ := json.Marshal(input)
	secondModel, requests := scriptedModel(t, `{"output":[]}`)
	_, err = engine.Run(context.Background(), "tenant", []byte(`{"input":`+string(inputBody)+`,"tools":[{"type":"web_search"}]}`), secondModel)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := object((*requests)[0])
	var history []map[string]json.RawMessage
	_ = json.Unmarshal(request["input"], &history)
	want := []string{"", "reasoning", "function_call", "function_call_output", "reasoning", "function_call", "function_call_output", "message"}
	if len(history) != len(want) {
		t.Fatalf("history length %d, want %d: %s", len(history), len(want), request["input"])
	}
	for i, typ := range want {
		if stringValue(history[i]["type"]) != typ {
			t.Fatalf("history[%d].type=%q want %q: %s", i, stringValue(history[i]["type"]), typ, request["input"])
		}
	}
	if len(searcher.queries) != 2 {
		t.Fatalf("replay repeated search: %v", searcher.queries)
	}
}

func TestEmptyModelOutputIsArray(t *testing.T) {
	engine := NewEngine(nil, Options{})
	model, _ := scriptedModel(t, `{"output":[]}`)
	result, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question"}`), model)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := object(result)
	if string(response["output"]) != "[]" {
		t.Fatalf("empty output = %s", response["output"])
	}
}

func TestPublicResponseRestoresToolMetadataAfterPrivateShim(t *testing.T) {
	for _, test := range []struct {
		name   string
		choice string
	}{
		{name: "forced search", choice: `,"tool_choice":{"type":"web_search"}`},
		{name: "no explicit choice"},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := NewEngine(new(fakeSearcher), Options{})
			model, _ := scriptedModel(t,
				`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}]}`,
				`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}],"tools":[{"type":"function","name":"__deepseek_web_search"}],"tool_choice":{"type":"function","name":"__deepseek_web_search"},"model":"upstream-internal","extra":{"preserved":true}}`,
			)
			result, err := engine.Run(context.Background(), "tenant", []byte(`{"model":"deepseek-public","input":"Question","tools":[{"type":"web_search","search_context_size":"short"}]`+test.choice+`}`), model)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(result), searchFunction) {
				t.Fatalf("private shim leaked into public response: %s", result)
			}
			response, _ := object(result)
			if string(response["tools"]) != `[{"type":"web_search","search_context_size":"short"}]` || stringValue(response["model"]) != "deepseek-public" || string(response["extra"]) != `{"preserved":true}` {
				t.Fatalf("public metadata not restored: %s", result)
			}
			if test.choice == "" && len(response["tool_choice"]) > 0 {
				t.Fatalf("internal tool_choice leaked: %s", response["tool_choice"])
			}
			if test.choice != "" && string(response["tool_choice"]) != `{"type":"web_search"}` {
				t.Fatalf("original tool_choice not restored: %s", response["tool_choice"])
			}
		})
	}
}

func TestForcedWebSearchRelaxesAfterToolResult(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}],"tool_choice":{"type":"web_search"}}`), model)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := object((*requests)[0])
	second, _ := object((*requests)[1])
	if !strings.Contains(string(first["tool_choice"]), searchFunction) || string(second["tool_choice"]) != `"auto"` {
		t.Fatalf("tool choice did not relax: first=%s second=%s", first["tool_choice"], second["tool_choice"])
	}
}

func TestUnsupportedSearchFiltersFailBeforeModelCall(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	called := false
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search","filters":{"allowed_domains":["example.com"]}}]}`), func(context.Context, []byte) ([]byte, error) {
		called = true
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "filters are unsupported") || called {
		t.Fatalf("filters silently ignored: error=%v model_called=%v", err, called)
	}
}

func TestExternalWebAccessFalseIsAcceptedWithoutShimField(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search","external_web_access":false}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 1 || strings.Contains(string((*requests)[0]), "external_web_access") {
		t.Fatalf("flag changed search behavior or leaked into shim: searches=%v request=%s", searcher.queries, (*requests)[0])
	}
}

func TestExpandedTranscriptRestoresWebCallWithoutSearchingAgain(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	firstModel, _ := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"reused_call_id","arguments":"{\"action\":\"search\",\"query\":\"Go docs\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer one"}]}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant-a", []byte(`{"input":"First question","tools":[{"type":"web_search"}]}`), firstModel)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse, _ := object(first)
	var firstOutput []json.RawMessage
	_ = json.Unmarshal(firstResponse["output"], &firstOutput)
	var web json.RawMessage
	for _, item := range firstOutput {
		obj, _ := object(item)
		if stringValue(obj["type"]) == "web_search_call" {
			web = item
		}
	}
	if len(web) == 0 {
		t.Fatalf("web_search_call missing: %s", first)
	}
	if _, err := engine.Run(context.Background(), "tenant-b", []byte(`{"input":[`+string(web)+`]}`), firstModel); err == nil {
		t.Fatal("other tenant restored a web item")
	}
	input, _ := json.Marshal([]json.RawMessage{
		json.RawMessage(`{"role":"user","content":"Compacted summary"}`),
		web,
		json.RawMessage(`{"role":"user","content":"Second question"}`),
	})
	secondModel, requests := scriptedModel(t, `{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer two"}]}]}`)
	second, err := engine.Run(context.Background(), "tenant-a", []byte(`{"input":`+string(input)+`,"tools":[{"type":"web_search"}]}`), secondModel)
	if err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 1 || len(*requests) != 1 {
		t.Fatalf("replay repeated search: searches=%v model_calls=%d", searcher.queries, len(*requests))
	}
	request, _ := object((*requests)[0])
	if strings.Contains(string(request["input"]), "First question") || !strings.Contains(string(request["input"]), "Compacted summary") || !strings.Contains(string(request["input"]), "function_call_output") {
		t.Fatalf("compacted replay was not restored: %s", request["input"])
	}
	webObj, _ := object(web)
	if !strings.Contains(string(request["input"]), stringValue(webObj["id"])) || strings.Contains(string(second), searchFunction) {
		t.Fatalf("public web item was not restored privately: request=%s response=%s", request["input"], second)
	}
}

func TestCustomToolCallWaitsForMatchingOutput(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"custom_tool_call","name":"code","call_id":"custom_1","input":"print(1)"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question"}`), model)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := object(first)
	id := stringValue(response["id"])
	if _, err := engine.Run(context.Background(), "tenant", []byte(`{"previous_response_id":"`+id+`","input":[{"type":"function_call_output","call_id":"custom_1","output":"wrong type"}]}`), model); err == nil {
		t.Fatal("mismatched custom tool output accepted")
	}
	_, err = engine.Run(context.Background(), "tenant", []byte(`{"previous_response_id":"`+id+`","input":[{"type":"custom_tool_call_output","call_id":"custom_1","output":"1"}]}`), model)
	if err != nil || len(*requests) != 2 {
		t.Fatalf("custom tool did not resume: error=%v calls=%d", err, len(*requests))
	}
}

func TestIncompleteModelResponseDoesNotSearch(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, _ := scriptedModel(t, `{"status":"incomplete","output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}]}`)
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err == nil || len(searcher.queries) != 0 {
		t.Fatalf("incomplete response invoked search: error=%v queries=%v", err, searcher.queries)
	}
}

func TestExpandedMixedCallsValidateClientResults(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	firstModel, _ := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"search_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Checking"}]},{"type":"custom_tool_call","name":"code","call_id":"client_1","input":"print(1)"}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), firstModel)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := object(first)
	var output []json.RawMessage
	_ = json.Unmarshal(response["output"], &output)
	input := append([]json.RawMessage{json.RawMessage(`{"role":"user","content":"Question"}`)}, output...)
	input = append(input, json.RawMessage(`{"type":"custom_tool_call_output","call_id":"client_1","output":"1"}`))
	inputBody, _ := json.Marshal(input)
	secondModel, requests := scriptedModel(t, `{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`)
	_, err = engine.Run(context.Background(), "tenant", []byte(`{"input":`+string(inputBody)+`,"tools":[{"type":"web_search"}]}`), secondModel)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 1 || len(searcher.queries) != 1 || !strings.Contains(string((*requests)[0]), `"custom_tool_call_output"`) || !strings.Contains(string((*requests)[0]), `"function_call_output"`) {
		t.Fatalf("expanded transcript did not restore both results: requests=%v searches=%v", *requests, searcher.queries)
	}
	modelRequest, _ := object((*requests)[0])
	var history []map[string]json.RawMessage
	_ = json.Unmarshal(modelRequest["input"], &history)
	if len(history) < 6 || stringValue(history[1]["type"]) != "function_call" || stringValue(history[2]["type"]) != "message" || stringValue(history[3]["type"]) != "custom_tool_call" || stringValue(history[4]["type"]) != "function_call_output" {
		t.Fatalf("parallel calls were split by a hidden result: %s", modelRequest["input"])
	}
	bad := append([]json.RawMessage(nil), input...)
	bad[len(bad)-1] = json.RawMessage(`{"type":"function_call_output","call_id":"client_1","output":"wrong"}`)
	badBody, _ := json.Marshal(bad)
	if _, err := engine.Run(context.Background(), "tenant", []byte(`{"input":`+string(badBody)+`,"tools":[{"type":"web_search"}]}`), secondModel); err == nil {
		t.Fatal("mismatched expanded client result accepted")
	}
}

func TestSearchQueriesAndFindPageSnippets(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"search_1","arguments":"{\"action\":\"search\",\"queries\":[\"one\",\"two\"]}"},{"type":"function_call","name":"__deepseek_web_search","call_id":"find_1","arguments":"{\"action\":\"find_in_page\",\"url\":\"https://example.com/page\",\"pattern\":\"GO\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(searcher.queries) != 2 || len(searcher.urls) != 1 {
		t.Fatalf("queries=%v urls=%v", searcher.queries, searcher.urls)
	}
	request, _ := object((*requests)[1])
	var history []map[string]json.RawMessage
	_ = json.Unmarshal(request["input"], &history)
	found := false
	for _, item := range history {
		if stringValue(item["type"]) != "function_call_output" {
			continue
		}
		var content map[string]json.RawMessage
		if json.Unmarshal([]byte(stringValue(item["output"])), &content) == nil && string(content["total_matches"]) == "1" && strings.Contains(string(content["matches"]), `"line_number":1`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("find_in_page did not return a numbered snippet: %s", request["input"])
	}
}

func TestCompactHydratesWebHistoryAndReplacesItWithCompaction(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	firstModel, _ := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Old answer"}]}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant", []byte(`{"model":"deepseek","input":"Old question","tools":[{"type":"web_search"}]}`), firstModel)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse, _ := object(first)
	var firstOutput []json.RawMessage
	_ = json.Unmarshal(firstResponse["output"], &firstOutput)
	var web json.RawMessage
	for _, item := range firstOutput {
		obj, _ := object(item)
		if stringValue(obj["type"]) == "web_search_call" {
			web = item
		}
	}
	compactInput, _ := json.Marshal([]json.RawMessage{json.RawMessage(`{"role":"user","content":"Old question"}`), web})
	compactModel, compactRequests := scriptedModel(t, `{"output":[{"type":"compaction","encrypted_content":"opaque-summary"}]}`)
	compacted, err := engine.Compact(context.Background(), "tenant", []byte(`{"model":"deepseek","input":`+string(compactInput)+`}`), compactModel)
	if err != nil {
		t.Fatal(err)
	}
	if len(*compactRequests) != 1 || !strings.Contains(string((*compactRequests)[0]), "function_call_output") || len(searcher.queries) != 1 {
		t.Fatalf("compact did not hydrate private result: requests=%v searches=%v", *compactRequests, searcher.queries)
	}
	compactedResponse, _ := object(compacted)
	nextModel, nextRequests := scriptedModel(t, `{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"New answer"}]}]}`)
	_, err = engine.Run(context.Background(), "tenant", []byte(`{"previous_response_id":"`+stringValue(compactedResponse["id"])+`","input":"New question"}`), nextModel)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string((*nextRequests)[0]), "Old question") || !strings.Contains(string((*nextRequests)[0]), "opaque-summary") {
		t.Fatalf("compaction did not replace old history: %s", (*nextRequests)[0])
	}
}

func TestMixedClientCallsPauseUntilAllResultsAndIsolateTenant(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, requests := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"client_lookup","call_id":"client_1","arguments":"{}"},{"type":"function_call","name":"__deepseek_web_search","call_id":"search_1","arguments":"{\"action\":\"open_page\",\"url\":\"https://example.com/page\"}"},{"type":"function_call","name":"client_lookup","call_id":"client_2","arguments":"{}"}]}`,
		`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`,
	)
	first, err := engine.Run(context.Background(), "tenant-a", []byte(`{"model":"deepseek","input":"Question","tools":[{"type":"web_search"},{"type":"function","name":"client_lookup","parameters":{"type":"object"}}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 1 || len(searcher.urls) != 1 {
		t.Fatalf("model continued before client tools: requests=%d fetches=%v", len(*requests), searcher.urls)
	}
	response, _ := object(first)
	id := stringValue(response["id"])
	if _, err := engine.Run(context.Background(), "tenant-b", []byte(`{"previous_response_id":"`+id+`","input":[{"type":"function_call_output","call_id":"client_1","output":"x"}]}`), model); err == nil {
		t.Fatal("cross-tenant resume succeeded")
	}
	partial, err := engine.Run(context.Background(), "tenant-a", []byte(`{"previous_response_id":"`+id+`","input":[{"type":"function_call_output","call_id":"client_1","output":"one"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 1 {
		t.Fatal("model resumed with an unresolved client tool")
	}
	paused, _ := object(partial)
	if !strings.Contains(string(paused["output"]), "client_2") || strings.Contains(string(paused["output"]), "client_1") {
		t.Fatalf("wrong pending calls: %s", paused["output"])
	}
	final, err := engine.Run(context.Background(), "tenant-a", []byte(`{"previous_response_id":"`+stringValue(paused["id"])+`","input":[{"type":"function_call_output","call_id":"client_2","output":"two"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	if len(*requests) != 2 || !strings.Contains(string((*requests)[1]), `"output":"one"`) || !strings.Contains(string((*requests)[1]), `"output":"two"`) || !strings.Contains(string((*requests)[1]), "function_call_output") {
		t.Fatalf("resume lost results: %s", (*requests)[1])
	}
	if !strings.Contains(string(final), "Done") {
		t.Fatalf("missing final answer: %s", final)
	}
	if _, err := engine.Run(context.Background(), "tenant-a", []byte(`{"previous_response_id":"`+id+`","input":[{"type":"function_call_output","call_id":"wrong","output":"x"}]}`), model); err == nil {
		t.Fatal("wrong call ID accepted")
	}
}

func TestSearchCannotRunWithoutDeclaration(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	model, _ := scriptedModel(t, `{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_1","arguments":"{\"action\":\"search\",\"query\":\"secret\"}"}]}`)
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"model":"deepseek","input":"Question"}`), model)
	if err == nil || !strings.Contains(err.Error(), "undeclared") {
		t.Fatalf("undeclared call error = %v", err)
	}
}

func TestModelErrorDoesNotSaveState(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	_, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("upstream unavailable")
	})
	if err == nil || !strings.Contains(err.Error(), "upstream unavailable") {
		t.Fatalf("model error = %v", err)
	}
}
