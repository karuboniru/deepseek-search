package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestCollectorDeliversFragmentedMixedSSEBeforeCompletion(t *testing.T) {
	c := newResponsesCollector()
	var events []string
	c.onEvent = func(raw json.RawMessage) error {
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		events = append(events, event.Type)
		return nil
	}
	first := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"早\"}\r\n\r\n"
	second := "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"后\"}\n\n"
	data := []byte(first + second)
	for _, b := range data {
		if err := c.feed([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"response.output_text.delta", "response.reasoning_summary_text.delta"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("early events %v, want %v", events, want)
	}
	if _, err := c.result(); err == nil {
		t.Fatal("missing completion accepted")
	}
	// Exercise mixed separators in one chunk, where the earlier CRLF frame must win.
	c = newResponsesCollector()
	count := 0
	c.onEvent = func(json.RawMessage) error { count++; return nil }
	if err := c.feed(data); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("mixed frames delivered %d events", count)
	}
}

func TestHostModelEventFailureClosesWithoutFurtherReads(t *testing.T) {
	old := hostCall
	defer func() { hostCall = old }()
	sentinel := errors.New("client disconnected")
	reads, closes := 0, 0
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			return json.RawMessage(`{"status_code":200,"stream_id":"stream"}`), nil
		case "host.model.stream_read":
			reads++
			return json.Marshal(hostModelStreamReadResponse{Payload: []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"first\"}\n\n")})
		case "host.model.stream_close":
			closes++
			return json.RawMessage(`{}`), nil
		default:
			return nil, errors.New("unexpected callback")
		}
	}
	_, err := executeHostModelEvents(context.Background(), executorRequest{}, modelConfig{Upstream: "model"}, []byte(`{"input":"hello"}`), func(json.RawMessage) error { return sentinel })
	if !errors.Is(err, sentinel) || reads != 1 || closes != 1 {
		t.Fatalf("err=%v reads=%d closes=%d", err, reads, closes)
	}
}

func TestHostModelCompletionDoesNotWaitForEOF(t *testing.T) {
	old := hostCall
	defer func() { hostCall = old }()
	reads, closes := 0, 0
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.model.execute_stream":
			return json.RawMessage(`{"status_code":200,"stream_id":"terminal"}`), nil
		case "host.model.stream_read":
			reads++
			if reads > 1 {
				return nil, errors.New("read after terminal response")
			}
			return json.Marshal(hostModelStreamReadResponse{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"), Done: false})
		case "host.model.stream_close":
			closes++
			return json.RawMessage(`{}`), nil
		default:
			return nil, errors.New("unexpected callback")
		}
	}
	_, err := executeHostModel(context.Background(), executorRequest{}, modelConfig{Upstream: "model"}, []byte(`{"input":"hello"}`))
	if err != nil || reads != 1 || closes != 1 {
		t.Fatalf("err=%v reads=%d closes=%d", err, reads, closes)
	}
}
