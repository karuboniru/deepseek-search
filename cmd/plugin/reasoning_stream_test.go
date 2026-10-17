package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/karuboniru/deepseek-search/adapter"
)

type reasoningStreamEvent struct {
	Type           string          `json:"type"`
	SequenceNumber int             `json:"sequence_number"`
	ItemID         string          `json:"item_id"`
	OutputIndex    int             `json:"output_index"`
	SummaryIndex   int             `json:"summary_index"`
	ContentIndex   int             `json:"content_index"`
	Delta          string          `json:"delta"`
	Text           string          `json:"text"`
	Item           json.RawMessage `json:"item"`
	Part           json.RawMessage `json:"part"`
	Response       json.RawMessage `json:"response"`
}

func reasoningEvents(t *testing.T, raw []byte) []reasoningStreamEvent {
	t.Helper()
	var events []reasoningStreamEvent
	for _, frame := range bytes.Split(raw, []byte("\n\n")) {
		for _, line := range bytes.Split(frame, []byte("\n")) {
			if !bytes.HasPrefix(line, []byte("data: ")) || bytes.Equal(line, []byte("data: [DONE]")) {
				continue
			}
			var event reasoningStreamEvent
			if err := json.Unmarshal(line[len("data: "):], &event); err != nil {
				t.Fatal(err)
			}
			if event.SequenceNumber != len(events)+1 {
				t.Fatalf("sequence number = %d, want %d", event.SequenceNumber, len(events)+1)
			}
			events = append(events, event)
		}
	}
	return events
}

func TestResponseSSEReasoningSummaryLifecycle(t *testing.T) {
	response := []byte(`{"id":"resp_reasoning","status":"completed","output":[{"id":"rs_before","type":"reasoning","summary":[{"type":"summary_text","text":"Before searching"},{"type":"summary_text","text":""}]},{"id":"ws_1","type":"web_search_call","status":"completed","action":{"type":"search","query":"example"}},{"id":"rs_after","type":"reasoning","summary":[{"type":"summary_text","text":"After searching"}],"encrypted_content":"opaque"},{"id":"rs_encrypted","type":"reasoning","summary":[],"encrypted_content":"opaque-only"},{"id":"msg_answer","type":"message","content":[{"type":"output_text","text":"Answer"}]}]}`)
	sse, err := responseSSE(response)
	if err != nil {
		t.Fatal(err)
	}
	events := reasoningEvents(t, sse)
	for _, want := range []struct {
		id    string
		index int
		texts []string
	}{
		{"rs_before", 0, []string{"Before searching", ""}},
		{"rs_after", 2, []string{"After searching"}},
		{"rs_encrypted", 3, nil},
	} {
		var itemEvents []reasoningStreamEvent
		for _, event := range events {
			if event.ItemID == want.id {
				itemEvents = append(itemEvents, event)
			}
			if event.Type == "response.output_item.added" && event.OutputIndex == want.index {
				var item struct {
					Summary []json.RawMessage `json:"summary"`
				}
				if err := json.Unmarshal(event.Item, &item); err != nil {
					t.Fatal(err)
				}
				if len(item.Summary) != 0 {
					t.Fatalf("%s added with populated summary: %s", want.id, event.Item)
				}
			}
		}
		position := 0
		for summaryIndex, text := range want.texts {
			kinds := []string{"response.reasoning_summary_part.added"}
			if text != "" {
				kinds = append(kinds, "response.reasoning_summary_text.delta")
			}
			kinds = append(kinds, "response.reasoning_summary_text.done", "response.reasoning_summary_part.done")
			for _, kind := range kinds {
				if position >= len(itemEvents) {
					t.Fatalf("%s summary %d missing %s", want.id, summaryIndex, kind)
				}
				event := itemEvents[position]
				position++
				if event.Type != kind || event.OutputIndex != want.index || event.SummaryIndex != summaryIndex {
					t.Fatalf("%s summary %d event = %#v, want %s at output %d", want.id, summaryIndex, event, kind, want.index)
				}
				switch kind {
				case "response.reasoning_summary_text.delta":
					if event.Delta != text {
						t.Fatalf("reasoning delta = %q, want %q", event.Delta, text)
					}
				case "response.reasoning_summary_text.done":
					if event.Text != text {
						t.Fatalf("reasoning text = %q, want %q", event.Text, text)
					}
				case "response.reasoning_summary_part.added", "response.reasoning_summary_part.done":
					var part struct {
						Type string `json:"type"`
						Text string `json:"text"`
					}
					if err := json.Unmarshal(event.Part, &part); err != nil {
						t.Fatal(err)
					}
					wantText := text
					if kind == "response.reasoning_summary_part.added" {
						wantText = ""
					}
					if part.Type != "summary_text" || part.Text != wantText {
						t.Fatalf("reasoning part = %s, want summary_text %q", event.Part, wantText)
					}
				}
			}
		}
		if position != len(itemEvents) {
			t.Fatalf("%s has unexpected events: %#v", want.id, itemEvents[position:])
		}
	}
	assertReasoningOutputPreserved(t, response, events)
}

func TestResponseSSEReasoningTextLifecycle(t *testing.T) {
	response := []byte(`{"id":"resp_content","status":"completed","output":[{"id":"rs_content","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"Detailed reasoning"},{"type":"reasoning_text","text":""}]}]}`)
	sse, err := responseSSE(response)
	if err != nil {
		t.Fatal(err)
	}
	events := reasoningEvents(t, sse)
	for index, text := range []string{"Detailed reasoning", ""} {
		var kinds []string
		for _, event := range events {
			if event.ItemID != "rs_content" || event.ContentIndex != index {
				continue
			}
			kinds = append(kinds, event.Type)
			if event.OutputIndex != 0 {
				t.Fatalf("reasoning content output index = %d", event.OutputIndex)
			}
			if event.Type == "response.reasoning_text.delta" && event.Delta != text {
				t.Fatalf("reasoning content delta = %q, want %q", event.Delta, text)
			}
			if event.Type == "response.reasoning_text.done" && event.Text != text {
				t.Fatalf("reasoning content text = %q, want %q", event.Text, text)
			}
		}
		var want []string
		if text != "" {
			want = append(want, "response.reasoning_text.delta")
		}
		want = append(want, "response.reasoning_text.done")
		if !reflect.DeepEqual(kinds, want) {
			t.Fatalf("reasoning content %d events = %v, want %v", index, kinds, want)
		}
	}
	assertReasoningOutputPreserved(t, response, events)
}

type reasoningSearchStub struct{}

func (reasoningSearchStub) Search(context.Context, string) (json.RawMessage, error) {
	return json.RawMessage(`{"results":[{"title":"Example","url":"https://example.com"}]}`), nil
}

func (reasoningSearchStub) Fetch(context.Context, string) (json.RawMessage, error) {
	return json.RawMessage(`{"content":"Example page"}`), nil
}

func TestSearchRoundReasoningSurvivesResponseSSE(t *testing.T) {
	engine := adapter.NewEngine(reasoningSearchStub{}, adapter.Options{})
	before := json.RawMessage(`{"id":"rs_before_search","type":"reasoning","summary":[{"type":"summary_text","text":"Find a source"}],"encrypted_content":"opaque-before"}`)
	after := json.RawMessage(`{"id":"rs_after_search","type":"reasoning","summary":[{"type":"summary_text","text":"Use the source"}],"encrypted_content":"opaque-after"}`)
	requests := 0
	response, err := engine.Run(context.Background(), "reasoning-tenant", []byte(`{"model":"search-gpt","input":"Question","tools":[{"type":"web_search"}],"reasoning":{"effort":"high"}}`), func(_ context.Context, body []byte) ([]byte, error) {
		requests++
		if requests == 1 {
			return []byte(`{"id":"upstream_1","status":"completed","output":[` + string(before) + `,{"type":"function_call","name":"__deepseek_web_search","call_id":"search_1","arguments":"{\"action\":\"search\",\"query\":\"example\"}"}]}`), nil
		}
		if requests != 2 {
			t.Fatalf("unexpected model request %d", requests)
		}
		var continuation struct {
			Input     []json.RawMessage `json:"input"`
			Reasoning struct {
				Effort string `json:"effort"`
			} `json:"reasoning"`
		}
		if err := json.Unmarshal(body, &continuation); err != nil {
			t.Fatal(err)
		}
		if continuation.Reasoning.Effort != "high" {
			t.Fatalf("reasoning options lost in search continuation: %s", body)
		}
		if len(continuation.Input) < 2 || !bytes.Equal(continuation.Input[1], before) {
			t.Fatalf("reasoning context lost in search continuation: %s", body)
		}
		return []byte(`{"id":"upstream_2","status":"completed","output":[` + string(after) + `,{"id":"msg_result","type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer"}]}]}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("model requests = %d, want 2", requests)
	}
	sse, err := responseSSE(response)
	if err != nil {
		t.Fatal(err)
	}
	events := reasoningEvents(t, sse)
	var deltas []string
	for _, event := range events {
		if event.Type == "response.reasoning_summary_text.delta" {
			deltas = append(deltas, event.Delta)
		}
	}
	if want := []string{"Find a source", "Use the source"}; !reflect.DeepEqual(deltas, want) {
		t.Fatalf("search reasoning deltas = %v, want %v", deltas, want)
	}
	assertReasoningOutputPreserved(t, response, events)
}

func assertReasoningOutputPreserved(t *testing.T, response []byte, events []reasoningStreamEvent) {
	t.Helper()
	var original struct {
		Output []any `json:"output"`
	}
	if err := json.Unmarshal(response, &original); err != nil {
		t.Fatal(err)
	}
	doneItems := make([]any, len(original.Output))
	seenIndexes := make(map[int]bool)
	var completedOutput []any
	for _, event := range events {
		if event.Type == "response.output_item.done" {
			var item any
			if err := json.Unmarshal(event.Item, &item); err != nil {
				t.Fatal(err)
			}
			if event.OutputIndex < 0 || event.OutputIndex >= len(doneItems) || seenIndexes[event.OutputIndex] {
				t.Fatalf("invalid or repeated output index %d", event.OutputIndex)
			}
			seenIndexes[event.OutputIndex] = true
			doneItems[event.OutputIndex] = item
		}
		if event.Type == "response.completed" {
			var completed struct {
				Output []any `json:"output"`
			}
			if err := json.Unmarshal(event.Response, &completed); err != nil {
				t.Fatal(err)
			}
			completedOutput = completed.Output
		}
	}
	if !reflect.DeepEqual(doneItems, original.Output) || !reflect.DeepEqual(completedOutput, original.Output) {
		t.Fatalf("output changed: done=%#v completed=%#v want=%#v", doneItems, completedOutput, original.Output)
	}
}
