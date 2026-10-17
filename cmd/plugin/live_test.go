package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type liveHarness struct {
	live   *liveResponse
	frames [][]byte
}

func newLiveHarness(t *testing.T) *liveHarness {
	t.Helper()
	h := &liveHarness{}
	h.live = newLiveResponse(func(raw []byte) error { h.frames = append(h.frames, append([]byte(nil), raw...)); return nil })
	if err := h.live.Start(json.RawMessage(`{"id":"resp_public","model":"public","status":"in_progress","output":[],"tools":[{"type":"web_search"}]}`)); err != nil {
		t.Fatal(err)
	}
	return h
}
func (h *liveHarness) event(t *testing.T, raw string) {
	t.Helper()
	if err := h.live.Event(json.RawMessage(raw)); err != nil {
		t.Fatal(err)
	}
}
func (h *liveHarness) all() string { return string(bytes.Join(h.frames, nil)) }
func (h *liveHarness) events(t *testing.T, kind string) []map[string]json.RawMessage {
	t.Helper()
	var result []map[string]json.RawMessage
	for _, frame := range h.frames {
		for _, line := range bytes.Split(frame, []byte("\n")) {
			if bytes.HasPrefix(line, []byte("data: {")) {
				obj, err := liveObject(line[6:])
				if err != nil {
					t.Fatal(err)
				}
				if liveString(obj["type"]) == kind {
					result = append(result, obj)
				}
			}
		}
	}
	return result
}
func liveFinal(output json.RawMessage) []byte {
	return liveJSON(map[string]any{"id": "resp_public", "status": "completed", "model": "public", "output": output})
}
func TestLiveEarlyTextAndCompletion(t *testing.T) {
	h := newLiveHarness(t)
	if err := h.live.BeginRound(); err != nil {
		t.Fatal(err)
	}
	h.event(t, `{"type":"response.created","response":{"id":"upstream","tools":[{"name":"__deepseek_web_search"}]}}`)
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"same","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	h.event(t, `{"type":"response.content_part.added","item_id":"same","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}`)
	h.event(t, `{"type":"response.output_text.delta","item_id":"same","output_index":0,"content_index":0,"delta":"hello"}`)
	if !strings.Contains(h.all(), `"delta":"hello"`) {
		t.Fatal("delta buffered until completion")
	}
	completed, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"same","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello world","annotations":[]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := liveObject(completed)
	if err = h.live.Complete(liveFinal(obj["output"])); err != nil {
		t.Fatal(err)
	}
	deltas := h.events(t, "response.output_text.delta")
	if len(deltas) != 2 || liveString(deltas[1]["delta"]) != " world" {
		t.Fatalf("unexpected deltas: %s", liveJSON(deltas))
	}
	if strings.Contains(h.all(), "__deepseek") || len(h.events(t, "response.created")) != 1 || len(h.events(t, "response.completed")) != 1 || strings.Count(h.all(), "[DONE]") != 1 {
		t.Fatal(h.all())
	}
	seq := 0
	for _, frame := range h.frames {
		for _, line := range bytes.Split(frame, []byte("\n")) {
			if bytes.HasPrefix(line, []byte("data: {")) {
				event, _ := liveObject(line[6:])
				var got int
				_ = json.Unmarshal(event["sequence_number"], &got)
				seq++
				if got != seq {
					t.Fatalf("sequence %d: %d", seq, got)
				}
			}
		}
	}
}
func TestLivePrivateBufferSearchAndRepeatedIDs(t *testing.T) {
	h := newLiveHarness(t)
	_ = h.live.BeginRound()
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"same","type":"function_call","call_id":"private_call","arguments":""}}`)
	h.event(t, `{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"same","delta":"SECRET_PRIVATE_ARGUMENTS"}`)
	if len(h.frames) != 2 {
		t.Fatal("unclassified function leaked")
	}
	round, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"same","type":"function_call","call_id":"private_call","name":"__deepseek_web_search","arguments":"SECRET_PRIVATE_ARGUMENTS","status":"completed"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var output []map[string]json.RawMessage
	obj, _ := liveObject(round)
	_ = json.Unmarshal(obj["output"], &output)
	if liveString(output[0]["call_id"]) != "private_call" || liveString(output[0]["id"]) == "same" {
		t.Fatal(string(round))
	}
	for _, status := range []string{"in_progress", "completed"} {
		if err = h.live.Search(0, liveJSON(map[string]any{"id": "ws_public", "type": "web_search_call", "status": status, "action": map[string]string{"type": "search", "query": "public query"}})); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.live.BeginRound()
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"same","type":"function_call","call_id":"client_call","name":"client_tool","arguments":""}}`)
	h.event(t, `{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"same","delta":"{}"}`)
	round2, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"same","type":"function_call","call_id":"client_call","name":"client_tool","arguments":"{}","status":"completed"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	obj2, _ := liveObject(round2)
	var second []json.RawMessage
	_ = json.Unmarshal(obj2["output"], &second)
	public := []json.RawMessage{h.live.output[0].current, second[0]}
	if err = h.live.Complete(liveFinal(liveJSON(public))); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(h.all(), "SECRET_PRIVATE_ARGUMENTS") || strings.Contains(h.all(), "__deepseek_web_search") || strings.Contains(h.all(), "private_call") {
		t.Fatal("private call leaked")
	}
	added := h.events(t, "response.output_item.added")
	if len(added) != 2 || string(added[1]["output_index"]) != "1" {
		t.Fatal(string(liveJSON(added)))
	}
	var one, two map[string]json.RawMessage
	_ = json.Unmarshal(output[0]["id"], new(string))
	one, _ = liveObject(second[0])
	two, _ = liveObject(liveJSON(output[0]))
	if liveString(one["id"]) == liveString(two["id"]) {
		t.Fatal("round IDs collide")
	}
	if len(h.events(t, "response.web_search_call.searching")) != 1 {
		t.Fatal("missing search progress")
	}
}
func TestLiveFallbackReasoningCustomRefusalAndAnnotations(t *testing.T) {
	h := newLiveHarness(t)
	_ = h.live.BeginRound()
	round, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"r","type":"reasoning","summary":[{"type":"summary_text","text":"summary"}],"content":[{"type":"reasoning_text","text":"thought"}]},{"id":"c","type":"custom_tool_call","call_id":"client","name":"custom","input":"custom input","status":"completed"},{"id":"m","type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"cannot comply"},{"type":"output_text","text":"answer","annotations":[{"type":"url_citation","url":"https://example.com","title":"source","start_index":0,"end_index":6}]}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := liveObject(round)
	if err = h.live.Complete(liveFinal(obj["output"])); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"response.reasoning_summary_text.delta", "response.reasoning_text.delta", "response.custom_tool_call_input.delta", "response.refusal.delta", "response.output_text.annotation.added"} {
		if len(h.events(t, kind)) != 1 {
			t.Errorf("missing %s", kind)
		}
	}
}
func TestLiveFailureAndMalformedEvents(t *testing.T) {
	for _, raw := range []string{`{`, `{}`, `{"type":"response.output_text.delta","output_index":-1}`, `{"type":"response.output_item.added","output_index":"0","item":{}}`, `{"type":"response.output_text.delta","output_index":0,"delta":"x"}`} {
		h := newLiveHarness(t)
		_ = h.live.BeginRound()
		if err := h.live.Event([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	h := newLiveHarness(t)
	_ = h.live.BeginRound()
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"a","type":"message","content":[],"status":"in_progress"}}`)
	h.event(t, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"a","delta":"partial"}`)
	if err := h.live.Fail(errors.New("SECRET_PRIVATE_ARGUMENTS")); err != nil {
		t.Fatal(err)
	}
	_ = h.live.Fail(errors.New("again"))
	if len(h.events(t, "response.failed")) != 1 || strings.Contains(h.all(), "SECRET_PRIVATE_ARGUMENTS") || strings.Contains(h.all(), "[DONE]") || !strings.Contains(h.all(), "partial") {
		t.Fatal(h.all())
	}
}
func TestLivePauseWithoutRoundsAndEmitFailure(t *testing.T) {
	h := newLiveHarness(t)
	if err := h.live.Complete(liveFinal(json.RawMessage(`[{"id":"pending","type":"function_call","name":"client","call_id":"call","arguments":"{}","status":"completed"}]`))); err != nil {
		t.Fatal(err)
	}
	if len(h.events(t, "response.output_item.added")) != 1 || len(h.events(t, "response.function_call_arguments.delta")) != 1 {
		t.Fatal(h.all())
	}
	broken := newLiveResponse(func([]byte) error { return errors.New("disconnected") })
	if err := broken.Start(json.RawMessage(`{"id":"r"}`)); err == nil {
		t.Fatal("emit error ignored")
	}
	if err := broken.Fail(errors.New("upstream")); err == nil {
		t.Fatal("lost emit error")
	}
}
func TestLiveRejectInconsistentOutput(t *testing.T) {
	h := newLiveHarness(t)
	_ = h.live.BeginRound()
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message","content":[]}}`)
	h.event(t, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"m","delta":"first"}`)
	if _, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"m","type":"message","content":[{"type":"output_text","text":"different"}]}]}`)); err == nil {
		t.Fatal("accepted contradictory delta")
	}
	h2 := newLiveHarness(t)
	_ = h2.live.BeginRound()
	h2.event(t, `{"type":"response.output_item.added","output_index":2,"item":{"id":"m","type":"message","content":[]}}`)
	if _, err := h2.live.EndRound([]byte(`{"status":"completed","output":[]}`)); err == nil {
		t.Fatal("accepted inconsistent output index")
	}
}

func TestLiveDoneReconcilesSuffixAndRejectsDivergence(t *testing.T) {
	for _, tc := range []struct {
		delta, done string
		wantError   bool
	}{
		{"a", "abc", false}, {"", "", false}, {"a", "", true}, {"abc", "other", true},
	} {
		t.Run(tc.delta+"/"+tc.done, func(t *testing.T) {
			h := newLiveHarness(t)
			_ = h.live.BeginRound()
			h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message","content":[]}}`)
			h.event(t, string(liveJSON(map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": tc.delta})))
			err := h.live.Event(liveJSON(map[string]any{"type": "response.output_text.done", "output_index": 0, "content_index": 0, "text": tc.done}))
			if (err != nil) != tc.wantError {
				t.Fatalf("done error: %v", err)
			}
			if tc.wantError {
				return
			}
			var text string
			for _, event := range h.events(t, "response.output_text.delta") {
				text += liveString(event["delta"])
			}
			if text != tc.done {
				t.Fatalf("deltas %q, done %q", text, tc.done)
			}
			final := liveJSON(map[string]any{"status": "completed", "output": []any{map[string]any{"id": "m", "type": "message", "content": []any{map[string]any{"type": "output_text", "text": tc.done}}}}})
			if _, err = h.live.EndRound(final); err != nil {
				t.Fatal(err)
			}
		})
	}
	h := newLiveHarness(t)
	_ = h.live.BeginRound()
	h.event(t, `{"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message","content":[]}}`)
	h.event(t, `{"type":"response.output_text.done","output_index":0,"content_index":0,"text":"original"}`)
	if _, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"m","type":"message","content":[{"type":"output_text","text":"changed"}]}]}`)); err == nil {
		t.Fatal("accepted a final snapshot contradicting text.done")
	}
}

func TestLiveCompleteRejectsUnreplacedPrivateSlot(t *testing.T) {
	h := newLiveHarness(t)
	if err := h.live.BeginRound(); err != nil {
		t.Fatal(err)
	}
	round, err := h.live.EndRound([]byte(`{"status":"completed","output":[{"id":"secret","type":"function_call","name":"__deepseek_web_search","call_id":"secret_call","arguments":"secret_args"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := liveObject(round)
	if err := h.live.Complete(liveFinal(obj["output"])); err == nil {
		t.Fatal("private slot accepted as public completion")
	}
	if strings.Contains(h.all(), "secret") || strings.Contains(h.all(), "__deepseek_web_search") {
		t.Fatal("private content leaked")
	}
}

func TestLiveItemIDsUniqueAcrossPublicRequests(t *testing.T) {
	var ids []string
	for _, responseID := range []string{"resp_first", "resp_second"} {
		live := newLiveResponse(func([]byte) error { return nil })
		if err := live.Start(liveJSON(map[string]any{"id": responseID})); err != nil {
			t.Fatal(err)
		}
		if err := live.BeginRound(); err != nil {
			t.Fatal(err)
		}
		raw, err := live.EndRound([]byte(`{"status":"completed","output":[{"id":"reused","type":"message","content":[{"type":"output_text","text":"answer"}]}]}`))
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			Output []struct {
				ID string `json:"id"`
			} `json:"output"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, response.Output[0].ID)
	}
	if ids[0] == "" || ids[0] == ids[1] {
		t.Fatalf("item IDs reused across responses: %v", ids)
	}
}
