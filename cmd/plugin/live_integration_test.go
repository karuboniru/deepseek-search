package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/karuboniru/deepseek-search/adapter"
)

// Each gate blocks an upstream read, so assertions about live delivery do not
// depend on scheduling delays or the speed of a machine running the tests.
type liveReadStep struct {
	payload []byte
	gate    <-chan struct{}
	err     string
}

type liveUpstream struct {
	steps  []liveReadStep
	read   int
	closed chan struct{}
	once   sync.Once
}

type livePublicStream struct {
	payload []byte
	err     string
	closed  chan struct{}
}

type liveHostHarness struct {
	mu           sync.Mutex
	turns        []*liveUpstream
	requests     []hostModelRequest
	streams      map[string]*livePublicStream
	notify       chan struct{}
	stop         chan struct{}
	searches     chan string
	searchPermit chan struct{}
	failEmit     string
}

func newLiveHostHarness(t *testing.T, rounds ...[]liveReadStep) *liveHostHarness {
	t.Helper()
	h := &liveHostHarness{
		streams: make(map[string]*livePublicStream), notify: make(chan struct{}, 128),
		stop: make(chan struct{}), searches: make(chan string, 16), searchPermit: make(chan struct{}, 16),
	}
	for _, steps := range rounds {
		h.turns = append(h.turns, &liveUpstream{steps: steps, closed: make(chan struct{})})
	}
	previousHost, previousConfig := hostCall, activeConfig.Load()
	model := modelConfig{Name: "live-search", Upstream: "upstream", Provider: "codex"}
	activeConfig.Store(&runtimeConfig{
		pluginConfig: pluginConfig{Enabled: true},
		models:       map[string]modelConfig{model.Name: model},
		engine:       adapter.NewEngine(h, adapter.Options{}),
	})
	hostCall = h.call
	t.Cleanup(func() {
		close(h.stop)
		h.mu.Lock()
		var streams []*livePublicStream
		for _, stream := range h.streams {
			streams = append(streams, stream)
		}
		h.mu.Unlock()
		for _, stream := range streams {
			select {
			case <-stream.closed:
			case <-time.After(5 * time.Second):
				t.Error("stream did not stop during cleanup")
			}
		}
		hostCall = previousHost
		activeConfig.Store(previousConfig)
	})
	return h
}

func (h *liveHostHarness) Search(ctx context.Context, query string) (json.RawMessage, error) {
	select {
	case h.searches <- query:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.stop:
		return nil, errors.New("test stopped")
	}
	select {
	case <-h.searchPermit:
		return json.RawMessage(`{"results":[{"title":"Example","url":"https://example.com"}]}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.stop:
		return nil, errors.New("test stopped")
	}
}

func (h *liveHostHarness) Fetch(context.Context, string) (json.RawMessage, error) {
	return nil, errors.New("unexpected fetch")
}

func liveTestJSON(value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func (h *liveHostHarness) call(method string, payload any) (json.RawMessage, error) {
	switch method {
	case "host.model.execute_stream":
		h.mu.Lock()
		defer h.mu.Unlock()
		h.requests = append(h.requests, payload.(hostModelRequest))
		index := len(h.requests) - 1
		if index >= len(h.turns) {
			return nil, errors.New("unexpected upstream model round")
		}
		return liveTestJSON(hostModelStreamResponse{StatusCode: 200, StreamID: fmt.Sprint(index)}), nil
	case "host.model.stream_read", "host.model.stream_close":
		var index int
		if _, err := fmt.Sscan(payload.(map[string]string)["stream_id"], &index); err != nil {
			return nil, err
		}
		upstream := h.turns[index]
		if method == "host.model.stream_close" {
			upstream.once.Do(func() { close(upstream.closed) })
			return liveTestJSON(struct{}{}), nil
		}
		if upstream.read >= len(upstream.steps) {
			return nil, errors.New("unexpected read after scripted stream")
		}
		step := upstream.steps[upstream.read]
		upstream.read++
		if step.gate != nil {
			select {
			case <-step.gate:
			case <-upstream.closed:
				return nil, errors.New("upstream closed")
			case <-h.stop:
				return nil, errors.New("test stopped")
			}
		}
		return liveTestJSON(hostModelStreamReadResponse{Payload: step.payload, Error: step.err, Done: upstream.read == len(upstream.steps)}), nil
	case "host.stream.emit":
		request := payload.(streamEmitRequest)
		h.mu.Lock()
		stream := h.streams[request.StreamID]
		fail := h.failEmit != "" && bytes.Contains(request.Payload, []byte(h.failEmit))
		if !fail {
			stream.payload = append(stream.payload, request.Payload...)
		}
		h.mu.Unlock()
		select {
		case h.notify <- struct{}{}:
		default:
		}
		if fail {
			return nil, errors.New("client disconnected")
		}
		return liveTestJSON(struct{}{}), nil
	case "host.stream.close":
		request := payload.(map[string]string)
		h.mu.Lock()
		stream := h.streams[request["stream_id"]]
		stream.err = request["error"]
		close(stream.closed)
		h.mu.Unlock()
		return liveTestJSON(struct{}{}), nil
	default:
		return nil, fmt.Errorf("unexpected host method %s", method)
	}
}

func (h *liveHostHarness) start(t *testing.T, id string, body []byte) {
	t.Helper()
	h.mu.Lock()
	h.streams[id] = &livePublicStream{closed: make(chan struct{})}
	h.mu.Unlock()
	request := executorRequest{Model: "live-search", Format: "openai-response", Stream: true, Payload: body, StreamID: id, HostCallbackID: "callback-live", Metadata: map[string]any{"caller_scope": "live-tenant"}}
	raw, err := executeStream(liveTestJSON(request))
	if err != nil || !bytes.Contains(raw, []byte(`"ok":true`)) {
		h.mu.Lock()
		delete(h.streams, id)
		h.mu.Unlock()
		t.Fatalf("start stream: %s, %v", raw, err)
	}
}

func (h *liveHostHarness) snapshot(id string) ([]byte, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	stream := h.streams[id]
	return append([]byte(nil), stream.payload...), stream.err
}

func (h *liveHostHarness) waitPayload(t *testing.T, id, fragment string) []byte {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		payload, _ := h.snapshot(id)
		if bytes.Contains(payload, []byte(fragment)) {
			return payload
		}
		select {
		case <-h.notify:
		case <-h.streams[id].closed:
			payload, _ = h.snapshot(id)
			if bytes.Contains(payload, []byte(fragment)) {
				return payload
			}
			t.Fatalf("stream closed before %q: %s", fragment, payload)
		case <-deadline:
			t.Fatalf("no live %q while upstream is blocked: %s", fragment, payload)
		}
	}
}

func liveWait(t *testing.T, channel <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func liveFrame(kind string, fields map[string]any) []byte {
	fields["type"] = kind
	return []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", kind, liveTestJSON(fields)))
}

func liveComplete(id string, output ...json.RawMessage) []byte {
	return liveFrame("response.completed", map[string]any{"response": map[string]any{"id": id, "object": "response", "status": "completed", "output": output, "usage": map[string]int{"input_tokens": 3, "output_tokens": 1}}})
}

func liveReasoning(id, text string) (json.RawMessage, []byte) {
	item := liveTestJSON(map[string]any{"id": id, "type": "reasoning", "summary": []any{map[string]string{"type": "summary_text", "text": text}}})
	frames := liveFrame("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"id": id, "type": "reasoning", "summary": []any{}}})
	frames = append(frames, liveFrame("response.reasoning_summary_part.added", map[string]any{"item_id": id, "output_index": 0, "summary_index": 0, "part": map[string]string{"type": "summary_text", "text": ""}})...)
	frames = append(frames, liveFrame("response.reasoning_summary_text.delta", map[string]any{"item_id": id, "output_index": 0, "summary_index": 0, "delta": text})...)
	return item, frames
}

func liveText(id, text string) (json.RawMessage, []byte) {
	item := liveTestJSON(map[string]any{"id": id, "type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}})
	frames := liveFrame("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"id": id, "type": "message", "role": "assistant", "content": []any{}}})
	frames = append(frames, liveFrame("response.content_part.added", map[string]any{"item_id": id, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})...)
	frames = append(frames, liveFrame("response.output_text.delta", map[string]any{"item_id": id, "output_index": 0, "content_index": 0, "delta": text})...)
	return item, frames
}

func liveHiddenCall(id, query string) json.RawMessage {
	return liveTestJSON(map[string]any{"id": id, "type": "function_call", "name": "__deepseek_web_search", "call_id": id, "arguments": string(liveTestJSON(map[string]string{"action": "search", "query": query, "private_marker": "private-argument-never-public"}))})
}

func TestExecuteStreamDeliversLiveAcrossSearchRoundsAndContinues(t *testing.T) {
	searchRoundGate, finalGate := make(chan struct{}), make(chan struct{})
	reasoning, reasoningFrames := liveReasoning("reason_before", "Finding sources now")
	answer, answerFrames := liveText("message_answer", "Live answer")
	continued, _ := liveText("message_continued", "Follow-up")
	replayed, _ := liveText("message_replayed", "Replayed history")
	firstCalls := []json.RawMessage{liveHiddenCall("private_call_1", "first"), liveHiddenCall("private_call_2", "second")}
	privateFrames := liveFrame("response.output_item.added", map[string]any{"output_index": 1, "item": firstCalls[0]})
	privateFrames = append(privateFrames, liveFrame("response.function_call_arguments.delta", map[string]any{"output_index": 1, "item_id": "private_call_1", "delta": "private-argument-never-public"})...)
	h := newLiveHostHarness(t,
		[]liveReadStep{{payload: append(reasoningFrames, privateFrames...)}, {gate: searchRoundGate, payload: liveComplete("upstream_search_1", append([]json.RawMessage{reasoning}, firstCalls...)...)}},
		[]liveReadStep{{payload: liveComplete("upstream_search_2", liveHiddenCall("private_call_3", "third"))}},
		[]liveReadStep{{payload: answerFrames}, {gate: finalGate, payload: liveComplete("upstream_answer", answer)}},
		[]liveReadStep{{payload: liveComplete("upstream_followup", continued)}},
		[]liveReadStep{{payload: liveComplete("upstream_replayed", replayed)}},
	)
	h.start(t, "public-first", []byte(`{"model":"live-search","input":"Question","tools":[{"type":"web_search"}]}`))
	beforeSearch := h.waitPayload(t, "public-first", `"delta":"Finding sources now"`)
	if bytes.Contains(beforeSearch, []byte("response.completed")) {
		t.Fatal("completed before first upstream round was released")
	}
	close(searchRoundGate)
	for _, want := range []string{"first", "second", "third"} {
		select {
		case query := <-h.searches:
			if query != want {
				t.Fatalf("search = %q, want %q", query, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("search %q was not started", want)
		}
		progress := h.waitPayload(t, "public-first", `"type":"web_search_call"`)
		if bytes.Contains(progress, []byte("response.completed")) {
			t.Fatal("completed while search was blocked")
		}
		h.searchPermit <- struct{}{}
	}
	beforeComplete := h.waitPayload(t, "public-first", `"delta":"Live answer"`)
	if bytes.Contains(beforeComplete, []byte("response.completed")) {
		t.Fatal("completed before final upstream read was released")
	}
	close(finalGate)
	liveWait(t, h.streams["public-first"].closed, "first public stream close")
	payload, closeErr := h.snapshot("public-first")
	if closeErr != "" {
		t.Fatalf("stream error = %q, payload = %s", closeErr, payload)
	}
	publicFields := liveWithoutSearchCapsules(t, payload)
	for _, secret := range []string{"__deepseek_web_search", "private_call_", "private-argument-never-public", "response.function_call_arguments"} {
		if bytes.Contains(publicFields, []byte(secret)) {
			t.Fatalf("private tool data %q leaked: %s", secret, payload)
		}
	}
	events := reasoningEvents(t, payload)
	createdID, completedID := "", ""
	var initialOutput []json.RawMessage
	var lifecycleIDs []string
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
		if event.Type == "response.created" || event.Type == "response.in_progress" || event.Type == "response.completed" {
			var response struct {
				ID     string            `json:"id"`
				Output []json.RawMessage `json:"output"`
			}
			if err := json.Unmarshal(event.Response, &response); err != nil {
				t.Fatal(err)
			}
			if event.Type == "response.created" {
				createdID = response.ID
			} else if event.Type == "response.completed" {
				completedID = response.ID
				initialOutput = response.Output
				assertReasoningOutputPreserved(t, event.Response, events)
			}
			lifecycleIDs = append(lifecycleIDs, response.ID)
		}
	}
	if counts["response.created"] != 1 || counts["response.in_progress"] != 1 || counts["response.completed"] != 1 || counts["response.failed"] != 0 {
		t.Fatalf("public lifecycle counts = %#v", counts)
	}
	if createdID == "" || createdID != completedID || strings.HasPrefix(completedID, "upstream_") {
		t.Fatalf("public response IDs: created=%q completed=%q", createdID, completedID)
	}
	for _, id := range lifecycleIDs {
		if id != createdID {
			t.Fatalf("public lifecycle response ID = %q, want %q", id, createdID)
		}
	}
	if counts["response.output_item.done"] != 5 {
		t.Fatalf("done output items = %d, want reasoning, three searches, and answer", counts["response.output_item.done"])
	}
	h.start(t, "public-followup", liveTestJSON(map[string]any{"model": "live-search", "previous_response_id": completedID, "input": "Follow-up question"}))
	liveWait(t, h.streams["public-followup"].closed, "follow-up public stream close")
	followup, closeErr := h.snapshot("public-followup")
	if closeErr != "" || !bytes.Contains(followup, []byte("Follow-up")) {
		t.Fatalf("continuation failed: %q, %s", closeErr, followup)
	}
	h.mu.Lock()
	requests := append([]hostModelRequest(nil), h.requests...)
	h.mu.Unlock()
	if len(requests) != 4 {
		t.Fatalf("upstream rounds = %d, want 4", len(requests))
	}
	if bytes.Contains(requests[3].Body, []byte("previous_response_id")) || !bytes.Contains(requests[3].Body, []byte("Live answer")) || !bytes.Contains(requests[3].Body, []byte("function_call_output")) {
		t.Fatalf("public continuation did not restore private history: %s", requests[3].Body)
	}
	fullHistory := []json.RawMessage{liveTestJSON(map[string]string{"role": "user", "content": "Question"})}
	fullHistory = append(fullHistory, initialOutput...)
	fullHistory = append(fullHistory, liveTestJSON(map[string]string{"role": "user", "content": "Replay follow-up"}))
	h.start(t, "public-replayed", liveTestJSON(map[string]any{"model": "live-search", "input": fullHistory, "tools": []any{map[string]string{"type": "web_search"}}}))
	liveWait(t, h.streams["public-replayed"].closed, "full-history replay public stream close")
	replayPayload, closeErr := h.snapshot("public-replayed")
	if closeErr != "" || !bytes.Contains(replayPayload, []byte("Replayed history")) {
		t.Fatalf("full-history replay failed: %q, %s", closeErr, replayPayload)
	}
	h.mu.Lock()
	requests = append([]hostModelRequest(nil), h.requests...)
	h.mu.Unlock()
	if len(requests) != 5 {
		t.Fatalf("upstream rounds after replay = %d, want 5", len(requests))
	}
	var replayRequest struct {
		Input []struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"input"`
	}
	if err := json.Unmarshal(requests[4].Body, &replayRequest); err != nil {
		t.Fatal(err)
	}
	hiddenCalls, hiddenResults := 0, 0
	for _, item := range replayRequest.Input {
		if item.Type == "web_search_call" {
			t.Fatal("public search item reached upstream instead of restored private call")
		}
		if item.Type == "function_call" && item.Name == "__deepseek_web_search" {
			hiddenCalls++
		}
		if item.Type == "function_call_output" {
			hiddenResults++
		}
	}
	if hiddenCalls != 3 || hiddenResults != 3 || len(h.searches) != 0 {
		t.Fatalf("replay history calls=%d results=%d repeated searches=%d", hiddenCalls, hiddenResults, len(h.searches))
	}
}

func liveTestCompleted(t *testing.T, payload []byte) (string, []json.RawMessage) {
	t.Helper()
	events := reasoningEvents(t, payload)
	for _, event := range events {
		if event.Type != "response.completed" {
			continue
		}
		var response struct {
			ID     string            `json:"id"`
			Output []json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(event.Response, &response); err != nil {
			t.Fatal(err)
		}
		assertReasoningOutputPreserved(t, event.Response, events)
		return response.ID, response.Output
	}
	t.Fatalf("missing completed response: %s", payload)
	return "", nil
}

func TestExecuteStreamMixedClientCallsPauseUntilAllResults(t *testing.T) {
	gate := make(chan struct{})
	clientCall := func(id, arguments string) json.RawMessage {
		return liveTestJSON(map[string]string{"id": id, "type": "function_call", "name": "client_lookup", "call_id": id, "arguments": arguments})
	}
	firstClient := clientCall("client_call_1", `{"key":"first"}`)
	secondClient := clientCall("client_call_2", `{"key":"second"}`)
	clientFrames := liveFrame("response.output_item.added", map[string]any{"output_index": 0, "item": clientCall("client_call_1", "")})
	clientFrames = append(clientFrames, liveFrame("response.function_call_arguments.delta", map[string]any{"output_index": 0, "item_id": "client_call_1", "delta": `{"key":"first"}`})...)
	answer, _ := liveText("mixed_answer", "Client and search results used")
	h := newLiveHostHarness(t,
		[]liveReadStep{{payload: clientFrames}, {gate: gate, payload: liveComplete("upstream_mixed", firstClient, liveHiddenCall("private_mixed", "mixed query"), secondClient)}},
		[]liveReadStep{{payload: liveComplete("upstream_mixed_answer", answer)}},
	)
	h.start(t, "mixed-first", []byte(`{"model":"live-search","input":"Question","tools":[{"type":"web_search"},{"type":"function","name":"client_lookup","parameters":{"type":"object"}}]}`))
	beforeCompletion := h.waitPayload(t, "mixed-first", `"delta":"{\"key\":\"first\"}"`)
	if bytes.Contains(beforeCompletion, []byte("response.completed")) {
		t.Fatal("mixed response completed before client argument stream was released")
	}
	close(gate)
	select {
	case query := <-h.searches:
		if query != "mixed query" {
			t.Fatalf("mixed search query = %q", query)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mixed search did not start")
	}
	h.searchPermit <- struct{}{}
	liveWait(t, h.streams["mixed-first"].closed, "mixed first public stream close")
	pausedPayload, closeErr := h.snapshot("mixed-first")
	if closeErr != "" {
		t.Fatalf("mixed first stream failed: %q", closeErr)
	}
	publicFields := liveWithoutSearchCapsules(t, pausedPayload)
	for _, secret := range []string{"__deepseek_web_search", "private_mixed", "private-argument-never-public"} {
		if bytes.Contains(publicFields, []byte(secret)) {
			t.Fatalf("mixed response leaked private tool data %q: %s", secret, pausedPayload)
		}
	}
	pausedID, pausedOutput := liveTestCompleted(t, pausedPayload)
	if len(pausedOutput) != 3 || !bytes.Contains(pausedPayload, []byte(`"call_id":"client_call_2"`)) {
		t.Fatalf("mixed pause lost public calls: %s", pausedPayload)
	}
	h.mu.Lock()
	requestCount := len(h.requests)
	h.mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("model resumed before client results: rounds=%d", requestCount)
	}
	h.start(t, "mixed-partial", liveTestJSON(map[string]any{"model": "live-search", "previous_response_id": pausedID, "input": []any{map[string]string{"type": "function_call_output", "call_id": "client_call_1", "output": "first-result"}}}))
	liveWait(t, h.streams["mixed-partial"].closed, "partially resumed stream close")
	partialPayload, closeErr := h.snapshot("mixed-partial")
	if closeErr != "" {
		t.Fatalf("partial tool result failed: %q", closeErr)
	}
	partialID, pendingOutput := liveTestCompleted(t, partialPayload)
	if len(pendingOutput) != 1 || !bytes.Contains(pendingOutput[0], []byte(`"call_id":"client_call_2"`)) {
		t.Fatalf("partial resume returned wrong pending calls: %s", partialPayload)
	}
	h.mu.Lock()
	requestCount = len(h.requests)
	h.mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("model resumed with unresolved client result: rounds=%d", requestCount)
	}
	h.start(t, "mixed-resumed", liveTestJSON(map[string]any{"model": "live-search", "previous_response_id": partialID, "input": []any{map[string]string{"type": "function_call_output", "call_id": "client_call_2", "output": "second-result"}}}))
	liveWait(t, h.streams["mixed-resumed"].closed, "fully resumed stream close")
	resumedPayload, closeErr := h.snapshot("mixed-resumed")
	if closeErr != "" || !bytes.Contains(resumedPayload, []byte("Client and search results used")) {
		t.Fatalf("mixed resume failed: %q, %s", closeErr, resumedPayload)
	}
	liveTestCompleted(t, resumedPayload)
	h.mu.Lock()
	requests := append([]hostModelRequest(nil), h.requests...)
	h.mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("mixed upstream rounds = %d, want 2", len(requests))
	}
	for _, expected := range []string{"first-result", "second-result", "function_call_output", "__deepseek_web_search", "https://example.com"} {
		if !bytes.Contains(requests[1].Body, []byte(expected)) {
			t.Fatalf("resume lost %q: %s", expected, requests[1].Body)
		}
	}
	if len(h.searches) != 0 {
		t.Fatal("resuming client tools repeated the completed search")
	}
}

func TestExecuteStreamFailsAfterLivePartialOutput(t *testing.T) {
	gate := make(chan struct{})
	_, frames := liveText("partial_message", "Partial answer")
	h := newLiveHostHarness(t, []liveReadStep{{payload: frames}, {gate: gate, err: "upstream connection lost"}})
	h.start(t, "public-failed", []byte(`{"model":"live-search","input":"Question"}`))
	h.waitPayload(t, "public-failed", `"delta":"Partial answer"`)
	close(gate)
	liveWait(t, h.streams["public-failed"].closed, "failed public stream close")
	payload, closeErr := h.snapshot("public-failed")
	if !bytes.Contains(payload, []byte("event: response.failed\n")) || bytes.Contains(payload, []byte("event: response.completed\n")) {
		t.Fatalf("unexpected failure lifecycle: %s", payload)
	}
	if closeErr == "" {
		t.Fatal("failure did not reach host.stream.close")
	}
	liveWait(t, h.turns[0].closed, "failed upstream close")
}

func TestExecuteStreamEmitFailureClosesUpstream(t *testing.T) {
	gate := make(chan struct{})
	_, frames := liveText("disconnect_message", "Disconnect now")
	h := newLiveHostHarness(t, []liveReadStep{{payload: frames}, {gate: gate}})
	h.failEmit = `"delta":"Disconnect now"`
	h.start(t, "public-disconnected", []byte(`{"model":"live-search","input":"Question"}`))
	liveWait(t, h.turns[0].closed, "upstream close after client disconnect")
	liveWait(t, h.streams["public-disconnected"].closed, "disconnected public stream close")
	payload, closeErr := h.snapshot("public-disconnected")
	if !strings.Contains(closeErr, "client disconnected") || bytes.Contains(payload, []byte("response.completed")) {
		t.Fatalf("disconnect result: error=%q payload=%s", closeErr, payload)
	}
}
