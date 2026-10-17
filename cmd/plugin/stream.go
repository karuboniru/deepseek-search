package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const maxModelStreamBytes = 16 << 20

type hostModelRequest struct {
	EntryProtocol  string      `json:"entry_protocol"`
	ExitProtocol   string      `json:"exit_protocol"`
	Model          string      `json:"model"`
	Stream         bool        `json:"stream"`
	Body           []byte      `json:"body"`
	Headers        http.Header `json:"headers"`
	Query          url.Values  `json:"query"`
	Alt            string      `json:"alt"`
	ForcedProvider string      `json:"forced_provider,omitempty"`
	HostCallbackID string      `json:"host_callback_id,omitempty"`
}

type hostModelStreamResponse struct {
	StatusCode int    `json:"status_code"`
	StreamID   string `json:"stream_id"`
}

type hostModelResponse struct {
	StatusCode int    `json:"status_code"`
	Body       []byte `json:"body"`
}

func executeHostModelNonstream(ctx context.Context, exec executorRequest, model modelConfig, body []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("decode model request: %w", err)
	}
	modelJSON, _ := json.Marshal(model.Upstream)
	request["model"] = modelJSON
	request["stream"] = json.RawMessage("false")
	modelBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode model request: %w", err)
	}
	raw, err := hostCall("host.model.execute", hostModelRequest{
		EntryProtocol: "openai-response", ExitProtocol: "openai-response", Model: model.Upstream,
		Stream: false, Body: modelBody, Headers: exec.Headers, Query: exec.Query,
		Alt: exec.Alt, ForcedProvider: model.Provider, HostCallbackID: exec.HostCallbackID,
	})
	if err != nil {
		return nil, err
	}
	var response hostModelResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode host model response: %w", err)
	}
	if response.StatusCode >= 400 {
		return nil, fmt.Errorf("host model returned HTTP %d", response.StatusCode)
	}
	if len(response.Body) == 0 {
		return nil, errors.New("host model returned an empty response")
	}
	return response.Body, nil
}

type hostModelStreamReadResponse struct {
	Payload []byte `json:"payload"`
	Error   string `json:"error"`
	Done    bool   `json:"done"`
}

func executeHostModel(ctx context.Context, exec executorRequest, model modelConfig, body []byte) ([]byte, error) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("decode model request: %w", err)
	}
	modelJSON, _ := json.Marshal(model.Upstream)
	request["model"] = modelJSON
	request["stream"] = json.RawMessage("true")
	streamBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode model request: %w", err)
	}
	raw, err := hostCall("host.model.execute_stream", hostModelRequest{
		EntryProtocol: "openai-response", ExitProtocol: "openai-response",
		Model: model.Upstream, Stream: true, Body: streamBody,
		Headers: exec.Headers, Query: exec.Query, Alt: exec.Alt,
		ForcedProvider: model.Provider, HostCallbackID: exec.HostCallbackID,
	})
	if err != nil {
		return nil, err
	}
	var opened hostModelStreamResponse
	if err := json.Unmarshal(raw, &opened); err != nil {
		return nil, fmt.Errorf("decode host model stream: %w", err)
	}
	if opened.StreamID != "" {
		defer func() { _, _ = hostCall("host.model.stream_close", map[string]string{"stream_id": opened.StreamID}) }()
	}
	if opened.StatusCode >= 400 {
		return nil, fmt.Errorf("host model returned HTTP %d", opened.StatusCode)
	}
	if opened.StreamID == "" {
		return nil, errors.New("host model returned an empty stream ID")
	}
	stopCancelWatch := make(chan struct{})
	cancelWatchExited := make(chan struct{})
	defer func() { close(stopCancelWatch); <-cancelWatchExited }()
	go func() {
		defer close(cancelWatchExited)
		select {
		case <-ctx.Done():
			_, _ = hostCall("host.model.stream_close", map[string]string{"stream_id": opened.StreamID})
		case <-stopCancelWatch:
		}
	}()
	collector := newResponsesCollector()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := hostCall("host.model.stream_read", map[string]string{"stream_id": opened.StreamID})
		if err != nil {
			return nil, err
		}
		var chunk hostModelStreamReadResponse
		if err := json.Unmarshal(raw, &chunk); err != nil {
			return nil, fmt.Errorf("decode model stream chunk: %w", err)
		}
		if chunk.Error != "" {
			return nil, fmt.Errorf("host model stream failed: %s", chunk.Error)
		}
		if err := collector.feed(chunk.Payload); err != nil {
			return nil, err
		}
		if chunk.Done {
			return collector.result()
		}
	}
}

type responsesCollector struct {
	pending    []byte
	totalBytes int
	completed  json.RawMessage
	items      []json.RawMessage
	created    json.RawMessage
	failed     bool
}

func newResponsesCollector() *responsesCollector { return &responsesCollector{} }

func (c *responsesCollector) feed(chunk []byte) error {
	if c.totalBytes+len(chunk) > maxModelStreamBytes {
		return errors.New("host model response exceeds size limit")
	}
	c.totalBytes += len(chunk)
	c.pending = append(c.pending, chunk...)
	for {
		index := bytes.Index(c.pending, []byte("\n\n"))
		separator := 2
		if index < 0 {
			index = bytes.Index(c.pending, []byte("\r\n\r\n"))
			separator = 4
		}
		if index < 0 {
			return nil
		}
		frame := append([]byte(nil), c.pending[:index]...)
		c.pending = c.pending[index+separator:]
		if err := c.frame(frame); err != nil {
			return err
		}
	}
}

func (c *responsesCollector) frame(frame []byte) error {
	var data []byte
	for _, line := range bytes.Split(bytes.ReplaceAll(frame, []byte("\r\n"), []byte("\n")), []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data:")) {
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, bytes.TrimSpace(line[5:])...)
		}
	}
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return nil
	}
	var event struct {
		Type     string          `json:"type"`
		Response json.RawMessage `json:"response"`
		Item     json.RawMessage `json:"item"`
		Error    json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("decode host Responses event: %w", err)
	}
	switch event.Type {
	case "response.created":
		c.created = append(c.created[:0], event.Response...)
	case "response.output_item.done":
		if len(event.Item) > 0 {
			c.items = append(c.items, append(json.RawMessage(nil), event.Item...))
		}
	case "response.completed":
		c.completed = append(c.completed[:0], event.Response...)
	case "response.failed", "response.incomplete":
		c.failed = true
	}
	return nil
}

func (c *responsesCollector) result() ([]byte, error) {
	if len(bytes.TrimSpace(c.pending)) > 0 {
		if err := c.frame(c.pending); err != nil {
			return nil, err
		}
	}
	if c.failed {
		return nil, errors.New("host model response failed")
	}
	if len(c.completed) > 0 {
		var completed struct {
			Status string          `json:"status"`
			Output json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(c.completed, &completed); err != nil || completed.Status != "completed" || len(completed.Output) == 0 {
			return nil, errors.New("host model returned an invalid completed response")
		}
		return c.completed, nil
	}
	return nil, errors.New("host model stream ended without a completed response")
}

type streamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload"`
}

func executeStream(raw []byte) ([]byte, error) {
	var req executorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode executor stream request: %w", err)
	}
	if req.StreamID == "" {
		return errorEnvelope("executor_error", "stream ID is required", http.StatusBadRequest), nil
	}
	if req.Alt == "responses/compact" {
		return errorEnvelope("executor_error", "streaming compact is unsupported", http.StatusBadRequest), nil
	}
	cfg := currentConfig()
	if !cfg.Enabled || cfg.engine == nil {
		return errorEnvelope("executor_error", "DeepSeek search is disabled", http.StatusServiceUnavailable), nil
	}
	if _, ok := cfg.models[req.Model]; !ok {
		return errorEnvelope("executor_error", "model is not enabled for DeepSeek search", http.StatusBadRequest), nil
	}
	go func() {
		var closeError string
		defer func() {
			if recovered := recover(); recovered != nil {
				closeError = "stream orchestration failed"
			}
			_, _ = hostCall("host.stream.close", map[string]string{"stream_id": req.StreamID, "error": closeError})
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		heartbeatDone := make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if _, err := hostCall("host.stream.emit", streamEmitRequest{StreamID: req.StreamID, Payload: []byte(": keep-alive\n\n")}); err != nil {
						cancel()
						return
					}
				}
			}
		}()
		response, err := runEngine(ctx, cfg, req)
		cancel()
		<-heartbeatDone
		if err != nil {
			closeError = err.Error()
			return
		}
		payload, err := responseSSE(response)
		if err != nil {
			closeError = err.Error()
			return
		}
		_, err = hostCall("host.stream.emit", streamEmitRequest{StreamID: req.StreamID, Payload: payload})
		if err != nil {
			closeError = err.Error()
		}
	}()
	return okEnvelope(map[string]any{"headers": http.Header{"Content-Type": {"text/event-stream"}}})
}

func responseSSE(raw []byte) ([]byte, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}
	if string(response["status"]) != `"completed"` {
		return nil, errors.New("search response is not completed")
	}
	var output []json.RawMessage
	if err := json.Unmarshal(response["output"], &output); err != nil {
		return nil, fmt.Errorf("decode search output: %w", err)
	}
	created := make(map[string]json.RawMessage, len(response))
	for key, value := range response {
		created[key] = value
	}
	created["status"] = json.RawMessage(`"in_progress"`)
	created["output"] = json.RawMessage("[]")
	if _, ok := created["usage"]; ok {
		created["usage"] = json.RawMessage("null")
	}
	var buf bytes.Buffer
	seq := 0
	emit := func(kind string, fields map[string]any) error {
		seq++
		fields["type"] = kind
		fields["sequence_number"] = seq
		data, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		fmt.Fprintf(&buf, "event: %s\ndata: %s\n\n", kind, data)
		return nil
	}
	if err := emit("response.created", map[string]any{"response": created}); err != nil {
		return nil, err
	}
	if err := emit("response.in_progress", map[string]any{"response": created}); err != nil {
		return nil, err
	}
	for i, item := range output {
		var parsed struct {
			ID        string            `json:"id"`
			Type      string            `json:"type"`
			Arguments string            `json:"arguments"`
			Content   []json.RawMessage `json:"content"`
			Summary   []json.RawMessage `json:"summary"`
		}
		if err := json.Unmarshal(item, &parsed); err != nil {
			return nil, err
		}
		var added map[string]json.RawMessage
		if err := json.Unmarshal(item, &added); err != nil {
			return nil, err
		}
		added["status"] = json.RawMessage(`"in_progress"`)
		switch parsed.Type {
		case "function_call":
			added["arguments"] = json.RawMessage(`""`)
		case "message":
			added["content"] = json.RawMessage("[]")
		case "reasoning":
			added["summary"] = json.RawMessage("[]")
			if _, ok := added["content"]; ok {
				added["content"] = json.RawMessage("[]")
			}
		}
		if err := emit("response.output_item.added", map[string]any{"output_index": i, "item": added}); err != nil {
			return nil, err
		}
		switch parsed.Type {
		case "reasoning":
			for j, part := range parsed.Summary {
				var content map[string]json.RawMessage
				if err := json.Unmarshal(part, &content); err != nil {
					return nil, err
				}
				var kind, text string
				_ = json.Unmarshal(content["type"], &kind)
				_ = json.Unmarshal(content["text"], &text)
				if kind == "summary_text" {
					content["text"] = json.RawMessage(`""`)
				}
				if err := emit("response.reasoning_summary_part.added", map[string]any{"item_id": parsed.ID, "output_index": i, "summary_index": j, "part": content}); err != nil {
					return nil, err
				}
				if kind == "summary_text" {
					if text != "" {
						if err := emit("response.reasoning_summary_text.delta", map[string]any{"item_id": parsed.ID, "output_index": i, "summary_index": j, "delta": text}); err != nil {
							return nil, err
						}
					}
					if err := emit("response.reasoning_summary_text.done", map[string]any{"item_id": parsed.ID, "output_index": i, "summary_index": j, "text": text}); err != nil {
						return nil, err
					}
				}
				if err := emit("response.reasoning_summary_part.done", map[string]any{"item_id": parsed.ID, "output_index": i, "summary_index": j, "part": part}); err != nil {
					return nil, err
				}
			}
			for j, part := range parsed.Content {
				var content struct {
					Type string `json:"type"`
					Text string `json:"text"`
				}
				if err := json.Unmarshal(part, &content); err != nil {
					return nil, err
				}
				if content.Type != "reasoning_text" {
					continue
				}
				if content.Text != "" {
					if err := emit("response.reasoning_text.delta", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "delta": content.Text}); err != nil {
						return nil, err
					}
				}
				if err := emit("response.reasoning_text.done", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "text": content.Text}); err != nil {
					return nil, err
				}
			}
		case "function_call":
			if parsed.Arguments != "" {
				if err := emit("response.function_call_arguments.delta", map[string]any{"item_id": parsed.ID, "output_index": i, "delta": parsed.Arguments}); err != nil {
					return nil, err
				}
			}
			if err := emit("response.function_call_arguments.done", map[string]any{"item_id": parsed.ID, "output_index": i, "arguments": parsed.Arguments}); err != nil {
				return nil, err
			}
		case "message":
			for j, part := range parsed.Content {
				var content map[string]json.RawMessage
				if err := json.Unmarshal(part, &content); err != nil {
					return nil, err
				}
				var kind, text string
				_ = json.Unmarshal(content["type"], &kind)
				_ = json.Unmarshal(content["text"], &text)
				addedPart := make(map[string]json.RawMessage, len(content))
				for key, value := range content {
					addedPart[key] = value
				}
				if kind == "output_text" {
					addedPart["text"] = json.RawMessage(`""`)
				}
				if err := emit("response.content_part.added", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "part": addedPart}); err != nil {
					return nil, err
				}
				if kind == "output_text" {
					if text != "" {
						if err := emit("response.output_text.delta", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "delta": text}); err != nil {
							return nil, err
						}
					}
					if err := emit("response.output_text.done", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "text": text}); err != nil {
						return nil, err
					}
				}
				if err := emit("response.content_part.done", map[string]any{"item_id": parsed.ID, "output_index": i, "content_index": j, "part": part}); err != nil {
					return nil, err
				}
			}
		}
		if err := emit("response.output_item.done", map[string]any{"output_index": i, "item": item}); err != nil {
			return nil, err
		}
	}
	if err := emit("response.completed", map[string]any{"response": response}); err != nil {
		return nil, err
	}
	buf.WriteString("data: [DONE]\n\n")
	return buf.Bytes(), nil
}
