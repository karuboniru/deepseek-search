package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func capsuleOutput(t *testing.T, response []byte) []json.RawMessage {
	t.Helper()
	obj, err := object(response)
	if err != nil {
		t.Fatal(err)
	}
	var output []json.RawMessage
	if err := json.Unmarshal(obj["output"], &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func capsuleRequest(t *testing.T, input []json.RawMessage) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"input": input})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func capsuleHistory(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	obj, err := object(body)
	if err != nil {
		t.Fatal(err)
	}
	var history []json.RawMessage
	if err := json.Unmarshal(obj["input"], &history); err != nil {
		t.Fatal(err)
	}
	return history
}

func capsuleWithoutContent(t *testing.T, item json.RawMessage) json.RawMessage {
	t.Helper()
	obj, err := object(item)
	if err != nil {
		t.Fatal(err)
	}
	delete(obj, "encrypted_content")
	body, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func capsuleFixture(t *testing.T, tenant string) (*Engine, *fakeSearcher, json.RawMessage) {
	t.Helper()
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, _ := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"original","arguments":"{\"action\":\"search\",\"query\":\"private search\"}"}]}`,
		`{"output":[]}`,
	)
	response, err := engine.Run(context.Background(), tenant, []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	output := capsuleOutput(t, response)
	if len(output) != 1 {
		t.Fatalf("expected one search item: %s", response)
	}
	return engine, searcher, output[0]
}

func TestSearchContextCapsuleIsVersionedAndSelfContained(t *testing.T) {
	_, _, web := capsuleFixture(t, "tenant")
	obj, _ := object(web)
	content := stringValue(obj["encrypted_content"])
	var capsule map[string]json.RawMessage
	if content == "" || json.Unmarshal([]byte(content), &capsule) != nil {
		t.Fatalf("encrypted_content must hold a plaintext JSON capsule: %s", web)
	}
	if stringValue(capsule["format"]) != "deepseek-search-context" || string(capsule["version"]) != "1" {
		t.Fatalf("missing capsule format/version: %s", content)
	}
	for _, key := range []string{"round", "items", "call", "result"} {
		if len(capsule[key]) == 0 {
			t.Fatalf("capsule missing %s: %s", key, content)
		}
	}
	call, _ := object(capsule["call"])
	result, _ := object(capsule["result"])
	id := stringValue(obj["id"])
	if stringValue(call["call_id"]) != id || stringValue(result["call_id"]) != id || stringValue(call["name"]) != searchFunction || !strings.Contains(stringValue(result["output"]), "Result") {
		t.Fatalf("capsule lost the original paired call/result: %s", content)
	}
}

func TestSearchContextCapsuleReplaysAfterRestartAndOverridesCache(t *testing.T) {
	engine, searcher, web := capsuleFixture(t, "tenant")
	obj, _ := object(web)
	id := stringValue(obj["id"])
	record, ok := engine.states.getWeb("tenant", id)
	if !ok {
		t.Fatal("search cache missing")
	}
	record.result = json.RawMessage(`{"type":"function_call_output","call_id":"` + id + `","output":"cache was poisoned"}`)
	engine.states.putWeb(id, record)
	for _, test := range []struct {
		name   string
		engine *Engine
	}{
		{name: "capsule takes priority over cache", engine: engine},
		{name: "fresh engine has no cache", engine: NewEngine(searcher, Options{})},
	} {
		t.Run(test.name, func(t *testing.T) {
			model, requests := scriptedModel(t, `{"output":[]}`)
			if _, err := test.engine.Run(context.Background(), "tenant", capsuleRequest(t, []json.RawMessage{web}), model); err != nil {
				t.Fatal(err)
			}
			history := capsuleHistory(t, (*requests)[0])
			if len(history) != 2 || !strings.Contains(string(history[1]), "Result") || strings.Contains(string(history[1]), "poisoned") {
				t.Fatalf("did not restore capsule result: %s", (*requests)[0])
			}
			for _, item := range history {
				itemObj, _ := object(item)
				if stringValue(itemObj["call_id"]) != id {
					t.Fatalf("unpaired restored item: %s", item)
				}
			}
		})
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("historical search was reexecuted: %v", searcher.queries)
	}
}

func TestSearchContextCacheFallbackAndMissingHistoryPlaceholder(t *testing.T) {
	engine, searcher, web := capsuleFixture(t, "tenant-a")
	legacy := capsuleWithoutContent(t, web)
	for _, test := range []struct {
		name        string
		engine      *Engine
		tenant      string
		item        json.RawMessage
		placeholder bool
	}{
		{name: "legacy cached item", engine: engine, tenant: "tenant-a", item: legacy},
		{name: "unknown ID", engine: NewEngine(searcher, Options{}), tenant: "tenant-a", item: legacy, placeholder: true},
		{name: "another tenant cannot use cache", engine: engine, tenant: "tenant-b", item: legacy, placeholder: true},
		{name: "native web call", engine: engine, tenant: "tenant-a", item: json.RawMessage(`{"type":"web_search_call","id":"native_web","status":"completed","action":{"type":"open_page","url":"https://example.org/old"}}`), placeholder: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			model, requests := scriptedModel(t, `{"output":[]}`)
			if _, err := test.engine.Run(context.Background(), test.tenant, capsuleRequest(t, []json.RawMessage{test.item}), model); err != nil {
				t.Fatal(err)
			}
			history := capsuleHistory(t, (*requests)[0])
			if len(history) != 2 {
				t.Fatalf("historical call/result not paired: %s", (*requests)[0])
			}
			call, _ := object(history[0])
			result, _ := object(history[1])
			if stringValue(call["type"]) != "function_call" || stringValue(call["call_id"]) == "" || stringValue(call["call_id"]) != stringValue(result["call_id"]) || stringValue(result["type"]) != "function_call_output" {
				t.Fatalf("historical call/result not paired: %s", (*requests)[0])
			}
			unavailable := strings.Contains(stringValue(result["output"]), "historical_search_unavailable")
			if unavailable != test.placeholder || (test.placeholder && strings.Contains(stringValue(result["output"]), "Result")) {
				t.Fatalf("unexpected historical result: %s", result)
			}
		})
	}
	if len(searcher.queries) != 1 || len(searcher.urls) != 0 {
		t.Fatalf("historical search was reexecuted: searches=%v fetches=%v", searcher.queries, searcher.urls)
	}
}

func TestSearchContextCapsulesPreserveExactRoundAndClientToolOrdering(t *testing.T) {
	searcher := new(fakeSearcher)
	producer := NewEngine(searcher, Options{})
	model, _ := scriptedModel(t,
		`{"output":[{"type":"reasoning","encrypted_content":"round-one"},{"type":"function_call","name":"__deepseek_web_search","call_id":"same","arguments":"{\"action\":\"search\",\"query\":\"one\"}"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Searching"}]},{"type":"function_call","name":"__deepseek_web_search","call_id":"other","arguments":"{\"action\":\"search\",\"query\":\"two\"}"}]}`,
		`{"output":[{"type":"reasoning","encrypted_content":"round-two"},{"type":"custom_tool_call","name":"code","call_id":"client","input":"print(1)"},{"type":"function_call","name":"__deepseek_web_search","call_id":"same","arguments":"{\"action\":\"search\",\"query\":\"three\"}"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Waiting"}]}]}`,
	)
	response, err := producer.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	responseObj, _ := object(response)
	stored, ok := producer.states.get("tenant", stringValue(responseObj["id"]))
	if !ok {
		t.Fatal("producer state missing")
	}
	clientResult := json.RawMessage(`{"type":"custom_tool_call_output","call_id":"client","output":"1"}`)
	input := append([]json.RawMessage{json.RawMessage(`{"role":"user","content":"Question"}`)}, capsuleOutput(t, response)...)
	input = append(input, clientResult)
	replayModel, requests := scriptedModel(t, `{"output":[]}`)
	if _, err := NewEngine(searcher, Options{}).Run(context.Background(), "tenant", capsuleRequest(t, input), replayModel); err != nil {
		t.Fatal(err)
	}
	got := capsuleHistory(t, (*requests)[0])
	want := append(append([]json.RawMessage(nil), stored.history...), clientResult)
	if len(got) != len(want) {
		t.Fatalf("history length=%d want=%d: %s", len(got), len(want), (*requests)[0])
	}
	for i := range want {
		var gotValue, wantValue any
		_ = json.Unmarshal(got[i], &gotValue)
		_ = json.Unmarshal(want[i], &wantValue)
		gotJSON, _ := json.Marshal(gotValue)
		wantJSON, _ := json.Marshal(wantValue)
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Fatalf("history[%d]=%s want=%s", i, gotJSON, wantJSON)
		}
	}
	if len(searcher.queries) != 3 {
		t.Fatalf("replay repeated search: %v", searcher.queries)
	}
}

func TestSearchContextMalformedCapsuleFailsBeforeModelAndCacheFallback(t *testing.T) {
	engine, searcher, web := capsuleFixture(t, "tenant")
	obj, _ := object(web)
	var capsule map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stringValue(obj["encrypted_content"])), &capsule); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(map[string]json.RawMessage)
		raw  json.RawMessage
	}{
		{name: "invalid JSON", raw: json.RawMessage(`"{"`)},
		{name: "non-string capsule", raw: json.RawMessage(`{}`)},
		{name: "unknown format", edit: func(c map[string]json.RawMessage) { c["format"] = json.RawMessage(`"foreign"`) }},
		{name: "unknown version", edit: func(c map[string]json.RawMessage) { c["version"] = json.RawMessage(`999`) }},
		{name: "wrong public ID", edit: func(c map[string]json.RawMessage) { c["id"] = json.RawMessage(`"different"`) }},
		{name: "missing result", edit: func(c map[string]json.RawMessage) { delete(c, "result") }},
		{name: "unpaired result", edit: func(c map[string]json.RawMessage) {
			c["result"] = json.RawMessage(`{"type":"function_call_output","call_id":"wrong","output":"{}"}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			copyObj := make(map[string]json.RawMessage, len(obj))
			for key, value := range obj {
				copyObj[key] = value
			}
			if test.raw != nil {
				copyObj["encrypted_content"] = test.raw
			} else {
				copyCapsule := make(map[string]json.RawMessage, len(capsule))
				for key, value := range capsule {
					copyCapsule[key] = value
				}
				test.edit(copyCapsule)
				body, _ := json.Marshal(copyCapsule)
				copyObj["encrypted_content"], _ = json.Marshal(string(body))
			}
			item, _ := json.Marshal(copyObj)
			called := false
			_, err := engine.Run(context.Background(), "tenant", capsuleRequest(t, []json.RawMessage{item}), func(context.Context, []byte) ([]byte, error) {
				called = true
				return []byte(`{"output":[]}`), nil
			})
			if err == nil || called {
				t.Fatalf("malformed capsule silently used cache/placeholder: err=%v called=%v", err, called)
			}
		})
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("malformed capsule repeated search: %v", searcher.queries)
	}
}

func TestSearchContextConcurrentTenantCacheIsolation(t *testing.T) {
	engine, searcher, web := capsuleFixture(t, "owner")
	legacy := capsuleWithoutContent(t, web)
	body := capsuleRequest(t, []json.RawMessage{legacy})
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			tenant := fmt.Sprintf("other-%d", index)
			if index%2 == 0 {
				tenant = "owner"
			}
			_, err := engine.Run(context.Background(), tenant, body, func(_ context.Context, request []byte) ([]byte, error) {
				unavailable := strings.Contains(string(request), "historical_search_unavailable")
				if unavailable != (tenant != "owner") {
					t.Errorf("tenant %s got another tenant's search history: %s", tenant, request)
				}
				return []byte(`{"output":[]}`), nil
			})
			if err != nil {
				t.Errorf("tenant %s replay failed: %v", tenant, err)
			}
		}(i)
	}
	workers.Wait()
	if len(searcher.queries) != 1 {
		t.Fatalf("concurrent replay repeated search: %v", searcher.queries)
	}
}

func TestSearchContextFullReplayRecoversMissingPreviousResponse(t *testing.T) {
	_, searcher, web := capsuleFixture(t, "tenant")
	engine := NewEngine(searcher, Options{})
	for _, test := range []struct {
		name    string
		input   []json.RawMessage
		wantErr bool
	}{
		{name: "capsule supplies missing state", input: []json.RawMessage{json.RawMessage(`{"role":"user","content":"Restored summary"}`), web}},
		{name: "legacy item supplies placeholder", input: []json.RawMessage{capsuleWithoutContent(t, web)}},
		{name: "new message cannot replace missing state", input: []json.RawMessage{json.RawMessage(`{"role":"user","content":"New question"}`)}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"previous_response_id": "missing-response", "input": test.input})
			called := false
			_, err := engine.Run(context.Background(), "tenant", body, func(_ context.Context, request []byte) ([]byte, error) {
				called = true
				if !strings.Contains(string(request), "function_call_output") || strings.Contains(string(request), "missing-response") {
					t.Errorf("recovery did not replace missing state with expanded history: %s", request)
				}
				return []byte(`{"output":[]}`), nil
			})
			if (err != nil) != test.wantErr || called == test.wantErr {
				t.Fatalf("recovery err=%v model_called=%v wantErr=%v", err, called, test.wantErr)
			}
		})
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("recovery repeated search: %v", searcher.queries)
	}
}

func TestSearchContextCompletedObserverItemAlreadyReplays(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	model, _ := scriptedModel(t,
		`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"search","arguments":"{\"action\":\"search\",\"query\":\"one\"}"}]}`,
		`{"output":[]}`,
	)
	replayed := false
	_, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model, testObserver{search: func(_ int, item json.RawMessage) error {
		obj, _ := object(item)
		if stringValue(obj["status"]) == "in_progress" {
			return nil
		}
		if stringValue(obj["encrypted_content"]) == "" {
			t.Fatal("completed search event has no replay capsule")
		}
		replayModel, requests := scriptedModel(t, `{"output":[]}`)
		if _, err := NewEngine(searcher, Options{}).Run(context.Background(), "tenant", capsuleRequest(t, []json.RawMessage{item}), replayModel); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string((*requests)[0]), "Result") {
			t.Fatalf("completed event lost search output: %s", (*requests)[0])
		}
		replayed = true
		return nil
	}})
	if err != nil || !replayed || len(searcher.queries) != 1 {
		t.Fatalf("completed event replay: err=%v replayed=%v searches=%v", err, replayed, searcher.queries)
	}
}

func TestSearchContextReplayStillEnforcesHistoryBounds(t *testing.T) {
	_, searcher, web := capsuleFixture(t, "tenant")
	body := capsuleRequest(t, []json.RawMessage{web})
	for _, test := range []struct {
		name string
		opts Options
	}{
		{name: "expanded item count", opts: Options{MaxHistoryItems: 1}},
		{name: "decoded capsule bytes", opts: Options{MaxHistoryBytes: 64}},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			_, err := NewEngine(searcher, test.opts).Run(context.Background(), "tenant", body, func(context.Context, []byte) ([]byte, error) {
				called = true
				return []byte(`{"output":[]}`), nil
			})
			if err == nil || called {
				t.Fatalf("historical payload bypassed bounds: err=%v model_called=%v", err, called)
			}
		})
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("oversized history repeated search: %v", searcher.queries)
	}
}

func TestSearchContextCapsuleReclaimsOnlyItsTenantCacheSlots(t *testing.T) {
	for _, test := range []struct {
		name         string
		replayTenant string
		cacheTenant  string
		wantErr      bool
		wantRemoved  bool
	}{
		{name: "valid capsule reclaims every queue slot", replayTenant: "owner", cacheTenant: "owner", wantRemoved: true},
		{name: "wrong replay tenant cannot remove owner cache", replayTenant: "other", cacheTenant: "owner", wantErr: true},
		{name: "valid capsule cannot remove other tenant cache", replayTenant: "owner", cacheTenant: "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, _, web := capsuleFixture(t, "owner")
			obj, _ := object(web)
			id := stringValue(obj["id"])
			record, _ := engine.states.getWeb("owner", id)
			record.tenant = test.cacheTenant
			engine.states.putWeb(id, record)
			engine.states.putWeb("unrelated", webRecord{tenant: "owner"})
			model, _ := scriptedModel(t, `{"output":[]}`)
			_, err := engine.Run(context.Background(), test.replayTenant, capsuleRequest(t, []json.RawMessage{web}), model)
			if (err != nil) != test.wantErr {
				t.Fatalf("capsule replay error=%v wantErr=%v", err, test.wantErr)
			}
			_, retained := engine.states.getWeb(test.cacheTenant, id)
			if retained == test.wantRemoved {
				t.Fatalf("cache retained=%v wantRemoved=%v", retained, test.wantRemoved)
			}
			engine.states.mu.Lock()
			for _, queued := range engine.states.webOrder {
				if queued == id && test.wantRemoved {
					t.Errorf("capsule replay left a stale eviction queue slot: %v", engine.states.webOrder)
				}
			}
			if test.wantRemoved && (len(engine.states.webItems) != 1 || len(engine.states.webOrder) != 1 || engine.states.webOrder[0] != "unrelated") {
				t.Errorf("capsule did not reclaim its actual cache capacity: items=%d queue=%v", len(engine.states.webItems), engine.states.webOrder)
			}
			engine.states.mu.Unlock()
			if _, ok := engine.states.getWeb("owner", "unrelated"); !ok {
				t.Fatal("capsule evicted an unrelated cache item")
			}
		})
	}
}

func TestSearchContextCacheRestoreCallbackOnlyReportsCacheHits(t *testing.T) {
	for _, test := range []struct {
		name        string
		strip       bool
		fresh       bool
		otherTenant bool
		wantHits    int32
	}{
		{name: "capsule with cache does not report fallback"},
		{name: "capsule without cache does not report fallback", fresh: true},
		{name: "legacy cache hit reports fallback", strip: true, wantHits: 1},
		{name: "unknown legacy ID does not report fallback", strip: true, fresh: true},
		{name: "other tenant placeholder does not report fallback", strip: true, otherTenant: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, searcher, web := capsuleFixture(t, "owner")
			var hits atomic.Int32
			if test.fresh {
				engine = NewEngine(searcher, Options{})
			}
			engine.opts.OnSearchCacheRestore = func(context.Context) { hits.Add(1) }
			if test.strip {
				web = capsuleWithoutContent(t, web)
			}
			tenant := "owner"
			if test.otherTenant {
				tenant = "other"
			}
			model, _ := scriptedModel(t, `{"output":[]}`)
			if _, err := engine.Run(context.Background(), tenant, capsuleRequest(t, []json.RawMessage{web}), model); err != nil {
				t.Fatal(err)
			}
			if got := hits.Load(); got != test.wantHits {
				t.Fatalf("cache restore callbacks=%d want=%d", got, test.wantHits)
			}
		})
	}
}

func TestSearchContextConcurrentCapsuleReplayReclaimsCacheWithoutFallback(t *testing.T) {
	engine, searcher, web := capsuleFixture(t, "owner")
	var hits atomic.Int32
	engine.opts.OnSearchCacheRestore = func(context.Context) { hits.Add(1) }
	body := capsuleRequest(t, []json.RawMessage{web})
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := engine.Run(context.Background(), "owner", body, func(_ context.Context, request []byte) ([]byte, error) {
				if !strings.Contains(string(request), "Result") || strings.Contains(string(request), "historical_search_unavailable") {
					t.Errorf("concurrent capsule lost its result: %s", request)
				}
				return []byte(`{"output":[]}`), nil
			})
			if err != nil {
				t.Errorf("concurrent capsule replay: %v", err)
			}
		}()
	}
	workers.Wait()
	engine.states.mu.Lock()
	if len(engine.states.webItems) != 0 || len(engine.states.webOrder) != 0 {
		t.Errorf("concurrent capsule replay did not reclaim cache: items=%d queue=%v", len(engine.states.webItems), engine.states.webOrder)
	}
	engine.states.mu.Unlock()
	if hits.Load() != 0 || len(searcher.queries) != 1 {
		t.Fatalf("capsule replay used fallback/search: callbacks=%d searches=%v", hits.Load(), searcher.queries)
	}
}

func TestSearchContextIdenticalIDlessMessagesDoNotCrossRoundBoundary(t *testing.T) {
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{})
	message := json.RawMessage(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Searching"}]}`)
	round := 0
	model := func(context.Context, []byte) ([]byte, error) {
		round++
		switch round {
		case 1:
			return modelOutput(searchCall("a", "one"), message), nil
		case 2:
			return modelOutput(message, searchCall("b", "two")), nil
		default:
			return []byte(`{"output":[]}`), nil
		}
	}
	response, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := object(response)
	stored, _ := engine.states.get("tenant", stringValue(obj["id"]))
	input := append([]json.RawMessage{json.RawMessage(`{"role":"user","content":"Question"}`)}, capsuleOutput(t, response)...)
	replayModel, requests := scriptedModel(t, `{"output":[]}`)
	if _, err := NewEngine(searcher, Options{}).Run(context.Background(), "tenant", capsuleRequest(t, input), replayModel); err != nil {
		t.Fatal(err)
	}
	got := capsuleHistory(t, (*requests)[0])
	if len(got) != len(stored.history) {
		t.Fatalf("replay history length=%d want=%d: %s", len(got), len(stored.history), (*requests)[0])
	}
	for i, want := range stored.history {
		var gotValue, wantValue any
		_ = json.Unmarshal(got[i], &gotValue)
		_ = json.Unmarshal(want, &wantValue)
		gotJSON, _ := json.Marshal(gotValue)
		wantJSON, _ := json.Marshal(wantValue)
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Fatalf("round boundary moved history[%d]: got=%s want=%s", i, gotJSON, wantJSON)
		}
	}
}

func TestSearchContextCacheRestoreCallbackRunsOncePerMixedRequest(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	model, _ := scriptedModel(t,
		string(modelOutput(searchCall("a", "one"), searchCall("b", "two"), searchCall("c", "three"))),
		`{"output":[]}`,
	)
	response, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	output := capsuleOutput(t, response)
	input := []json.RawMessage{
		capsuleWithoutContent(t, output[0]),
		capsuleWithoutContent(t, output[1]),
		output[2],
		json.RawMessage(`{"type":"web_search_call","id":"native-old","status":"completed","action":{"type":"search","query":"historical"}}`),
	}
	type requestContextKey struct{}
	requestContext := context.WithValue(context.Background(), requestContextKey{}, "request-context")
	var hits atomic.Int32
	engine.opts.OnSearchCacheRestore = func(ctx context.Context) {
		if ctx.Value(requestContextKey{}) != "request-context" {
			t.Error("cache restore callback lost the request context")
		}
		hits.Add(1)
	}
	for request := int32(1); request <= 2; request++ {
		replayModel, _ := scriptedModel(t, `{"output":[]}`)
		if _, err := engine.Run(requestContext, "tenant", capsuleRequest(t, input), replayModel); err != nil {
			t.Fatal(err)
		}
		if got := hits.Load(); got != request {
			t.Fatalf("request %d with two cache hits reported %d total fallbacks; want one callback per request", request, got)
		}
	}
}

func TestSearchContextQuoteHeavyCapsuleSurvivesJSONExpansion(t *testing.T) {
	const historyLimit = 1 << 20
	data, err := json.Marshal(map[string]string{"content": strings.Repeat("\"\\", 80_000)})
	if err != nil {
		t.Fatal(err)
	}
	searcher := observedSearcher{search: func(context.Context, string) (json.RawMessage, error) { return data, nil }}
	engine := NewEngine(searcher, Options{MaxHistoryBytes: historyLimit})
	model, _ := scriptedModel(t,
		string(modelOutput(searchCall("a", "quote-heavy"))),
		`{"output":[]}`,
	)
	response, err := engine.Run(context.Background(), "tenant", []byte(`{"input":"Question","tools":[{"type":"web_search"}]}`), model)
	if err != nil {
		t.Fatal(err)
	}
	body := capsuleRequest(t, capsuleOutput(t, response))
	if len(body) <= historyLimit {
		t.Fatalf("fixture did not exercise escaped replay expansion: replay bytes=%d limit=%d", len(body), historyLimit)
	}
	replayModel, requests := scriptedModel(t, `{"output":[]}`)
	if _, err := NewEngine(searcher, Options{MaxHistoryBytes: historyLimit}).Run(context.Background(), "tenant", body, replayModel); err != nil {
		t.Fatalf("emitted historical context became irrecoverable after JSON escaping: %v", err)
	}
	history := capsuleHistory(t, (*requests)[0])
	var bytesInHistory int
	for _, item := range history {
		bytesInHistory += len(item)
	}
	if bytesInHistory > historyLimit {
		t.Fatalf("fixture exceeds decoded history limit: %d > %d", bytesInHistory, historyLimit)
	}
	if len(history) != 2 {
		t.Fatalf("quote-heavy call/result missing: history items=%d", len(history))
	}
	result, _ := object(history[1])
	if stringValue(result["output"]) != string(data) {
		t.Fatal("quote-heavy historical result changed during replay")
	}
}

func TestSearchContextWireAllowanceDoesNotExpandOrdinaryRequestFields(t *testing.T) {
	_, searcher, web := capsuleFixture(t, "tenant")
	const historyLimit = 8 << 10
	for _, field := range []string{"instructions", "tools"} {
		t.Run(field, func(t *testing.T) {
			request := map[string]any{"input": []json.RawMessage{web}}
			largeText := strings.Repeat("x", historyLimit+1)
			if field == "tools" {
				request[field] = []map[string]any{{"type": "function", "name": "client", "description": largeText, "parameters": map[string]string{"type": "object"}}}
			} else {
				request[field] = largeText
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if len(body) >= 4*historyLimit {
				t.Fatalf("fixture exceeds the replay wire allowance: %d", len(body))
			}
			called := false
			_, err = NewEngine(searcher, Options{MaxHistoryBytes: historyLimit}).Run(context.Background(), "tenant", body, func(context.Context, []byte) ([]byte, error) {
				called = true
				return []byte(`{"output":[]}`), nil
			})
			if err == nil || called {
				t.Fatalf("oversized %s bypassed ordinary request bounds: error=%v model_called=%v", field, err, called)
			}
		})
	}
	if len(searcher.queries) != 1 {
		t.Fatalf("oversized request repeated historical search: %v", searcher.queries)
	}
}
