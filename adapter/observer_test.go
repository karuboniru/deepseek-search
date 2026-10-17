package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type testObserver struct {
	start  func(json.RawMessage) error
	search func(int, json.RawMessage) error
}

func (o testObserver) Start(response json.RawMessage) error {
	if o.start != nil {
		return o.start(response)
	}
	return nil
}

func (o testObserver) Search(index int, item json.RawMessage) error {
	if o.search != nil {
		return o.search(index, item)
	}
	return nil
}

type observedSearcher struct {
	search func(context.Context, string) (json.RawMessage, error)
	fetch  func(context.Context, string) (json.RawMessage, error)
}

func (s observedSearcher) Search(ctx context.Context, q string) (json.RawMessage, error) {
	return s.search(ctx, q)
}

func (s observedSearcher) Fetch(ctx context.Context, url string) (json.RawMessage, error) {
	if s.fetch != nil {
		return s.fetch(ctx, url)
	}
	return s.search(ctx, url)
}

func searchCall(id, query string) json.RawMessage {
	args, _ := json.Marshal(map[string]string{"action": "search", "query": query})
	item, _ := json.Marshal(map[string]string{"type": "function_call", "name": searchFunction, "call_id": id, "arguments": string(args)})
	return item
}

func modelOutput(items ...json.RawMessage) []byte {
	response, _ := json.Marshal(map[string]any{"output": items, "status": "completed"})
	return response
}

func TestObserverPublicOrderAcrossRounds(t *testing.T) {
	var events []string
	engine := NewEngine(observedSearcher{search: func(_ context.Context, query string) (json.RawMessage, error) {
		events = append(events, "search:"+query)
		return json.RawMessage(`{"results":[{"url":"https://example.com"}]}`), nil
	}}, Options{})
	var start map[string]json.RawMessage
	var observed []json.RawMessage
	var indexes []int
	observer := testObserver{
		start: func(raw json.RawMessage) error {
			events = append(events, "start")
			start, _ = object(raw)
			if strings.Contains(string(raw), searchFunction) {
				t.Fatalf("private tool leaked: %s", raw)
			}
			if stringValue(start["object"]) != "response" || stringValue(start["status"]) != "in_progress" || string(start["output"]) != "[]" {
				t.Fatalf("invalid skeleton: %s", raw)
			}
			var createdAt int64
			if err := json.Unmarshal(start["created_at"], &createdAt); err != nil || createdAt <= 0 {
				t.Fatalf("invalid created_at: %s", raw)
			}
			for key, want := range map[string]string{"background": "false", "error": "null", "incomplete_details": "null", "usage": "null"} {
				if string(start[key]) != want {
					t.Fatalf("start %s=%s, want %s", key, start[key], want)
				}
			}
			return nil
		},
		search: func(index int, raw json.RawMessage) error {
			item, _ := object(raw)
			events = append(events, fmt.Sprintf("event:%d:%s", index, stringValue(item["status"])))
			observed = append(observed, append(json.RawMessage(nil), raw...))
			indexes = append(indexes, index)
			return nil
		},
	}
	round := 0
	model := func(_ context.Context, body []byte) ([]byte, error) {
		round++
		events = append(events, fmt.Sprintf("model:%d", round))
		switch round {
		case 1:
			return modelOutput(json.RawMessage(`{"type":"reasoning","id":"r1"}`), searchCall("same", "one"), searchCall("second", "two")), nil
		case 2:
			return modelOutput(json.RawMessage(`{"type":"reasoning","id":"r2"}`), searchCall("same", "three")), nil
		default:
			return modelOutput(json.RawMessage(`{"type":"message","id":"answer","content":[]}`)), nil
		}
	}
	result, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"model":"public-model","input":"question","tools":[{"type":"web_search_preview"}],"tool_choice":"required"}`), model, observer)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"start", "model:1", "event:1:in_progress", "search:one", "event:1:completed", "event:2:in_progress", "search:two", "event:2:completed", "model:2", "event:4:in_progress", "search:three", "event:4:completed", "model:3"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
	final, _ := object(result)
	if string(final["id"]) != string(start["id"]) || stringValue(start["id"]) == "" {
		t.Fatalf("response ID mismatch: %s / %s", start["id"], final["id"])
	}
	for _, key := range []string{"model", "tools", "tool_choice", "object", "created_at"} {
		if string(start[key]) != string(final[key]) {
			t.Fatalf("%s metadata differs", key)
		}
	}
	var output []json.RawMessage
	_ = json.Unmarshal(final["output"], &output)
	seen := map[string]bool{}
	for i := 0; i < len(observed); i += 2 {
		before, _ := object(observed[i])
		after, _ := object(observed[i+1])
		out, _ := object(output[indexes[i]])
		id := stringValue(before["id"])
		if id == "" || seen[id] || id != stringValue(after["id"]) || id != stringValue(out["id"]) {
			t.Fatalf("unstable search ID %q", id)
		}
		seen[id] = true
		if !strings.Contains(string(after["action"]), "https://example.com") {
			t.Fatal("completed search lacks sources")
		}
	}
	// The ID supplied to Start is immediately usable for a later public turn.
	follow, _ := json.Marshal(map[string]any{"previous_response_id": stringValue(start["id"]), "input": "follow up"})
	_, err = engine.RunObserved(context.Background(), "tenant", follow, func(_ context.Context, body []byte) ([]byte, error) {
		if !strings.Contains(string(body), "function_call_output") {
			t.Fatalf("private history missing: %s", body)
		}
		return []byte(`{"output":[]}`), nil
	}, testObserver{start: func(raw json.RawMessage) error {
		obj, _ := object(raw)
		if string(obj["previous_response_id"]) != string(start["id"]) || stringValue(obj["model"]) != "public-model" || string(obj["tools"]) != string(start["tools"]) {
			t.Fatalf("continuation metadata: %s", raw)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestObserverMixedToolsAndPendingPause(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	modelCalls := 0
	var starts []string
	var createdAt []string
	var searches int
	observer := testObserver{start: func(raw json.RawMessage) error {
		obj, _ := object(raw)
		starts = append(starts, stringValue(obj["id"]))
		createdAt = append(createdAt, string(obj["created_at"]))
		return nil
	}, search: func(_ int, _ json.RawMessage) error { searches++; return nil }}
	model := func(context.Context, []byte) ([]byte, error) {
		modelCalls++
		return modelOutput(searchCall("web", "one"), json.RawMessage(`{"type":"function_call","name":"client","call_id":"a","arguments":"{}"}`), json.RawMessage(`{"type":"function_call","name":"client","call_id":"b","arguments":"{}"}`)), nil
	}
	first, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"model":"m","input":"question","tools":[{"type":"web_search"},{"type":"function","name":"client"}]}`), model, observer)
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := object(first)
	if stringValue(obj["id"]) != starts[0] || modelCalls != 1 || searches != 2 {
		t.Fatalf("mixed execution: %s calls=%d searches=%d", first, modelCalls, searches)
	}
	body, _ := json.Marshal(map[string]any{"previous_response_id": starts[0], "input": []map[string]string{{"type": "function_call_output", "call_id": "a", "output": "ok"}}})
	paused, err := engine.RunObserved(context.Background(), "tenant", body, model, observer)
	if err != nil {
		t.Fatal(err)
	}
	obj, _ = object(paused)
	if len(starts) != 2 || string(obj["created_at"]) != createdAt[1] || stringValue(obj["object"]) != "response" || stringValue(obj["id"]) != starts[1] || starts[0] == starts[1] || modelCalls != 1 || searches != 2 {
		t.Fatalf("pending pause lost ID or invoked upstream: %s", paused)
	}
	var output []json.RawMessage
	_ = json.Unmarshal(obj["output"], &output)
	if len(output) != 1 || !strings.Contains(string(output[0]), `"call_id":"b"`) {
		t.Fatalf("pending output: %s", obj["output"])
	}
	body, _ = json.Marshal(map[string]any{"previous_response_id": starts[1], "input": []map[string]string{{"type": "function_call_output", "call_id": "b", "output": "ok"}}})
	_, err = engine.Run(context.Background(), "tenant", body, func(context.Context, []byte) ([]byte, error) { return []byte(`{"output":[]}`), nil })
	if err != nil {
		t.Fatalf("paused ID continuation failed: %v", err)
	}
}

func TestObserverFailureAbortsWithoutState(t *testing.T) {
	failure := errors.New("observer stopped")
	for _, phase := range []string{"start", "in_progress", "completed", "second_search"} {
		t.Run(phase, func(t *testing.T) {
			searches, models, callbacks := 0, 0, 0
			engine := NewEngine(observedSearcher{search: func(context.Context, string) (json.RawMessage, error) {
				searches++
				return json.RawMessage(`{"results":[]}`), nil
			}}, Options{})
			observer := testObserver{start: func(json.RawMessage) error {
				if phase == "start" {
					return failure
				}
				return nil
			}, search: func(_ int, raw json.RawMessage) error {
				callbacks++
				obj, _ := object(raw)
				if stringValue(obj["status"]) == phase || (phase == "second_search" && callbacks == 3) {
					return failure
				}
				return nil
			}}
			result, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"tools":[{"type":"web_search"}]}`), func(context.Context, []byte) ([]byte, error) {
				models++
				return modelOutput(searchCall("a", "one"), searchCall("b", "two")), nil
			}, observer)
			if result != nil || !errors.Is(err, failure) {
				t.Fatalf("result=%s err=%v", result, err)
			}
			wantSearches := map[string]int{"start": 0, "in_progress": 0, "completed": 1, "second_search": 1}[phase]
			if searches != wantSearches || (phase == "start" && models != 0) || (phase != "start" && models != 1) {
				t.Fatalf("searches=%d models=%d", searches, models)
			}
			if len(engine.states.items) != 0 || len(engine.states.webItems) != 0 {
				t.Fatal("failed request stored continuation state")
			}
		})
	}
}

func TestObserverCancellationAbortsWithoutTerminalOrState(t *testing.T) {
	for _, phase := range []string{"before_start", "start", "model", "in_progress", "search", "completed", "second_query", "wrapped_search_error"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var events []string
			searches := 0
			engine := NewEngine(observedSearcher{search: func(context.Context, string) (json.RawMessage, error) {
				searches++
				if phase == "search" || (phase == "second_query" && searches == 1) {
					cancel()
				}
				if phase == "wrapped_search_error" {
					return nil, fmt.Errorf("transport: %w", context.Canceled)
				}
				return json.RawMessage(`{"results":[]}`), nil
			}}, Options{})
			observer := testObserver{start: func(json.RawMessage) error {
				events = append(events, "start")
				if phase == "start" {
					cancel()
				}
				return nil
			}, search: func(_ int, raw json.RawMessage) error {
				obj, _ := object(raw)
				status := stringValue(obj["status"])
				events = append(events, status)
				if phase == status {
					cancel()
				}
				return nil
			}}
			if phase == "before_start" {
				cancel()
			}
			models := 0
			result, err := engine.RunObserved(ctx, "tenant", []byte(`{"tools":[{"type":"web_search"}]}`), func(context.Context, []byte) ([]byte, error) {
				models++
				if phase == "model" {
					cancel()
				}
				return modelOutput(json.RawMessage(`{"type":"function_call","name":"__deepseek_web_search","call_id":"a","arguments":"{\"queries\":[\"one\",\"two\"]}"}`)), nil
			}, observer)
			if result != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("result=%s error=%v", result, err)
			}
			if len(engine.states.items) != 0 || len(engine.states.webItems) != 0 {
				t.Fatal("cancelled request stored state")
			}
			if phase != "completed" && strings.Contains(strings.Join(events, ","), "completed") {
				t.Fatalf("terminal search emitted after cancellation: %v", events)
			}
			if phase == "before_start" && len(events) != 0 {
				t.Fatalf("pre-cancelled request emitted events: %v", events)
			}
			if phase == "second_query" && searches != 1 {
				t.Fatalf("cancelled multi-query continued: %d", searches)
			}
			if models > 1 {
				t.Fatal("cancelled search reached continuation model")
			}
		})
	}
}

func TestObserverSearchFailureStillCompletesResponse(t *testing.T) {
	engine := NewEngine(observedSearcher{search: func(context.Context, string) (json.RawMessage, error) { return nil, errors.New("provider unavailable") }}, Options{})
	var statuses []string
	modelCalls := 0
	_, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"tools":[{"type":"web_search"}]}`), func(_ context.Context, body []byte) ([]byte, error) {
		modelCalls++
		if modelCalls == 1 {
			return modelOutput(searchCall("a", "one")), nil
		}
		if !strings.Contains(string(body), "provider unavailable") {
			t.Fatalf("model lacks error result: %s", body)
		}
		return []byte(`{"output":[]}`), nil
	}, testObserver{search: func(_ int, raw json.RawMessage) error {
		obj, _ := object(raw)
		statuses = append(statuses, stringValue(obj["status"]))
		return nil
	}})
	if err != nil || !reflect.DeepEqual(statuses, []string{"in_progress", "failed"}) {
		t.Fatalf("statuses=%v err=%v", statuses, err)
	}
}

func TestObserverValidatesBeforeStart(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{})
	for _, body := range []string{`{"input":42}`, `{"previous_response_id":"missing"}`, `{"tools":[{"type":"web_search","filters":{"allowed_domains":["example.com"]}}]}`, `{"input":[{"type":"function_call_output","call_id":"orphan","output":"bad"}]}`} {
		_, err := engine.RunObserved(context.Background(), "tenant", []byte(body), func(context.Context, []byte) ([]byte, error) {
			t.Fatal("invalid request called model")
			return nil, nil
		}, testObserver{start: func(json.RawMessage) error { t.Fatal("invalid request emitted start"); return nil }})
		if err == nil {
			t.Fatalf("accepted invalid request %s", body)
		}
	}
}

func TestObserverConcurrentResponseIDs(t *testing.T) {
	engine := NewEngine(nil, Options{})
	const count = 12
	var wg sync.WaitGroup
	ids := make(chan string, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var id string
			result, err := engine.RunObserved(context.Background(), "tenant", []byte(`{"input":"hi"}`), func(context.Context, []byte) ([]byte, error) { return []byte(`{"output":[]}`), nil }, testObserver{start: func(raw json.RawMessage) error { obj, _ := object(raw); id = stringValue(obj["id"]); return nil }})
			if err != nil {
				t.Error(err)
				return
			}
			obj, _ := object(result)
			if id == "" || id != stringValue(obj["id"]) {
				t.Errorf("response ID mismatch: %s", result)
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate ID %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("completed %d requests", len(seen))
	}
}

func TestUnobservedResponsesPreserveUpstreamCreatedAt(t *testing.T) {
	engine := NewEngine(nil, Options{})
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%v", compact), func(t *testing.T) {
			model := func(context.Context, []byte) ([]byte, error) {
				return []byte(`{"object":"response","created_at":123,"output":[]}`), nil
			}
			var result []byte
			var err error
			if compact {
				result, err = engine.Compact(context.Background(), "tenant", []byte(`{"input":"hi"}`), model)
			} else {
				result, err = engine.Run(context.Background(), "tenant", []byte(`{"input":"hi"}`), model)
			}
			if err != nil {
				t.Fatal(err)
			}
			response, _ := object(result)
			if string(response["created_at"]) != "123" {
				t.Fatalf("upstream timestamp replaced: %s", result)
			}
		})
	}
}
