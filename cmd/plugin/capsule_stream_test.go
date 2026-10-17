package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/karuboniru/deepseek-search/adapter"
)

// Inspect every public field except the designated plaintext history carrier.
// Private calls remain forbidden as public items or protocol events.
func liveWithoutSearchCapsules(t *testing.T, payload []byte) []byte {
	t.Helper()
	var strip func(any)
	strip = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if value["type"] == "web_search_call" {
				delete(value, "encrypted_content")
			}
			for _, child := range value {
				strip(child)
			}
		case []any:
			for _, child := range value {
				strip(child)
			}
		}
	}
	var public []byte
	for _, line := range bytes.Split(payload, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: {")) {
			continue
		}
		var event any
		if err := json.Unmarshal(line[len("data: "):], &event); err != nil {
			t.Fatal(err)
		}
		strip(event)
		public = append(public, liveTestJSON(event)...)
	}
	return public
}

func TestExecuteStreamSearchCapsulesReplayAfterEngineRestart(t *testing.T) {
	clientCall := func(id string) json.RawMessage {
		return liveTestJSON(map[string]string{"id": id, "type": "function_call", "name": "client_lookup", "call_id": id, "arguments": `{"key":"value"}`})
	}
	reasoning, _ := liveReasoning("capsule_reasoning", "Find both sources")
	message, _ := liveText("capsule_message", "Waiting for client tools")
	answer, _ := liveText("capsule_answer", "All results restored")
	h := newLiveHostHarness(t,
		[]liveReadStep{{payload: liveComplete("upstream_capsule_mixed", reasoning, liveHiddenCall("capsule_private_1", "first capsule query"), clientCall("capsule_client_1"), liveHiddenCall("capsule_private_2", "second capsule query"), clientCall("capsule_client_2"), message)}},
		[]liveReadStep{{payload: liveComplete("upstream_capsule_baseline", answer)}},
		[]liveReadStep{{payload: liveComplete("upstream_capsule_completed_replay", answer)}},
		[]liveReadStep{{payload: liveComplete("upstream_capsule_done_replay", answer)}},
	)
	h.start(t, "capsule-first", []byte(`{"model":"live-search","input":"Original capsule question","tools":[{"type":"web_search"},{"type":"function","name":"client_lookup","parameters":{"type":"object"}}]}`))
	for _, want := range []string{"first capsule query", "second capsule query"} {
		select {
		case query := <-h.searches:
			if query != want {
				t.Fatalf("search = %q, want %q", query, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("search %q did not start", want)
		}
		h.searchPermit <- struct{}{}
	}
	liveWait(t, h.streams["capsule-first"].closed, "capsule public stream close")
	payload, closeErr := h.snapshot("capsule-first")
	if closeErr != "" {
		t.Fatalf("capsule stream failed: %q, %s", closeErr, payload)
	}
	responseID, completedOutput := liveTestCompleted(t, payload)
	var doneOutput []json.RawMessage
	doneCapsules := map[string]string{}
	for _, event := range reasoningEvents(t, payload) {
		if event.Type != "response.output_item.done" {
			continue
		}
		for len(doneOutput) <= event.OutputIndex {
			doneOutput = append(doneOutput, nil)
		}
		if doneOutput[event.OutputIndex] != nil {
			t.Fatalf("duplicate output_item.done at index %d", event.OutputIndex)
		}
		doneOutput[event.OutputIndex] = event.Item
		var item struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Capsule string `json:"encrypted_content"`
		}
		if err := json.Unmarshal(event.Item, &item); err != nil {
			t.Fatal(err)
		}
		if item.Type != "web_search_call" {
			continue
		}
		if item.Capsule == "" || !json.Valid([]byte(item.Capsule)) {
			t.Fatalf("search item.done has no plaintext JSON capsule: %s", event.Item)
		}
		var envelope struct {
			Format  string `json:"format"`
			Version int    `json:"version"`
		}
		if err := json.Unmarshal([]byte(item.Capsule), &envelope); err != nil || envelope.Format != "deepseek-search-context" || envelope.Version != 1 {
			t.Fatalf("unrecognized search capsule envelope: %s, %v", item.Capsule, err)
		}
		for _, want := range []string{"__deepseek_web_search", "private-argument-never-public", "function_call_output", "https://example.com"} {
			if !bytes.Contains([]byte(item.Capsule), []byte(want)) {
				t.Fatalf("search capsule omitted private context %q: %s", want, item.Capsule)
			}
		}
		doneCapsules[item.ID] = item.Capsule
	}
	completedCapsules := 0
	for _, raw := range completedOutput {
		var item struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Capsule string `json:"encrypted_content"`
		}
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatal(err)
		}
		if item.Type == "web_search_call" {
			completedCapsules++
			if item.Capsule == "" || doneCapsules[item.ID] != item.Capsule {
				t.Fatalf("completed search capsule differs from item.done: %s", raw)
			}
		}
	}
	if len(doneCapsules) != 2 || completedCapsules != 2 || len(doneOutput) != len(completedOutput) {
		t.Fatalf("stream output counts: done capsules=%d completed capsules=%d done items=%d completed items=%d", len(doneCapsules), completedCapsules, len(doneOutput), len(completedOutput))
	}
	results := []json.RawMessage{
		liveTestJSON(map[string]string{"type": "function_call_output", "call_id": "capsule_client_1", "output": "first-client-result"}),
		liveTestJSON(map[string]string{"type": "function_call_output", "call_id": "capsule_client_2", "output": "second-client-result"}),
	}
	h.start(t, "capsule-baseline", liveTestJSON(map[string]any{"model": "live-search", "previous_response_id": responseID, "input": results}))
	liveWait(t, h.streams["capsule-baseline"].closed, "baseline continuation close")
	baselinePayload, closeErr := h.snapshot("capsule-baseline")
	if closeErr != "" {
		t.Fatalf("baseline continuation failed: %q, %s", closeErr, baselinePayload)
	}
	h.mu.Lock()
	baseline := h.requests[len(h.requests)-1].Body
	h.mu.Unlock()
	var baselineRequest map[string]json.RawMessage
	if err := json.Unmarshal(baseline, &baselineRequest); err != nil {
		t.Fatal(err)
	}
	for _, source := range []struct {
		name   string
		output []json.RawMessage
	}{{"completed", completedOutput}, {"done", doneOutput}} {
		t.Run(source.name, func(t *testing.T) {
			// Retain no cached conversation or web records from the original Engine.
			config := *activeConfig.Load()
			config.engine = adapter.NewEngine(h, adapter.Options{})
			activeConfig.Store(&config)
			input := []json.RawMessage{liveTestJSON(map[string]string{"role": "user", "content": "Original capsule question"})}
			input = append(input, source.output...)
			input = append(input, results...)
			id := "capsule-replay-" + source.name
			h.start(t, id, liveTestJSON(map[string]any{"model": "live-search", "input": input, "tools": []any{map[string]string{"type": "web_search"}, map[string]any{"type": "function", "name": "client_lookup", "parameters": map[string]string{"type": "object"}}}}))
			liveWait(t, h.streams[id].closed, "fresh Engine replay close")
			replayed, closeErr := h.snapshot(id)
			if closeErr != "" || !bytes.Contains(replayed, []byte("All results restored")) {
				t.Fatalf("fresh Engine replay failed: %q, %s", closeErr, replayed)
			}
			h.mu.Lock()
			body := h.requests[len(h.requests)-1].Body
			h.mu.Unlock()
			var replayRequest map[string]json.RawMessage
			if err := json.Unmarshal(body, &replayRequest); err != nil {
				t.Fatal(err)
			}
			if !liveEqual(baselineRequest["input"], replayRequest["input"]) {
				t.Fatalf("fresh Engine changed private history or ordering:\nwant %s\n got %s", baselineRequest["input"], replayRequest["input"])
			}
			if len(h.searches) != 0 {
				t.Fatalf("fresh Engine repeated %d completed searches", len(h.searches))
			}
		})
	}
}
