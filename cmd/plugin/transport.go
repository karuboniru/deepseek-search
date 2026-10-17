package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type callbackIDContextKey struct{}
type hostHTTPTransport struct{}

type hostHTTPRequest struct {
	HostCallbackID string      `json:"host_callback_id"`
	OperationID    string      `json:"operation_id"`
	Method         string      `json:"method"`
	URL            string      `json:"url"`
	Headers        http.Header `json:"headers"`
	Body           []byte      `json:"body"`
}

type hostHTTPStreamResponse struct {
	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers"`
	StreamID   string      `json:"stream_id"`
}

type hostHTTPStreamChunk struct {
	Payload []byte `json:"payload"`
	Error   string `json:"error"`
	Done    bool   `json:"done"`
}

type hostHTTPOperation struct {
	callbackID  string
	operationID string
	mu          sync.Mutex
	streamID    string
	closed      bool
	closeOnce   sync.Once
	done        chan struct{}
	watchExited chan struct{}
}

func (o *hostHTTPOperation) setStreamID(id string) {
	o.mu.Lock()
	closed := o.closed
	if !closed {
		o.streamID = id
	}
	o.mu.Unlock()
	if closed {
		_, _ = hostCall("host.http.stream_close", map[string]string{"stream_id": id})
	}
}

func (o *hostHTTPOperation) close() {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		streamID := o.streamID
		o.mu.Unlock()
		close(o.done)
		_, _ = hostCall("host.http.cancel", map[string]string{"host_callback_id": o.callbackID, "operation_id": o.operationID})
		if streamID != "" {
			_, _ = hostCall("host.http.stream_close", map[string]string{"stream_id": streamID})
		}
	})
}

func (o *hostHTTPOperation) stop() {
	o.close()
	<-o.watchExited
}

type hostHTTPBody struct {
	operation *hostHTTPOperation
	readMu    sync.Mutex
	pending   []byte
	done      bool
}

func (b *hostHTTPBody) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	b.readMu.Lock()
	defer b.readMu.Unlock()
	for {
		b.operation.mu.Lock()
		closed := b.operation.closed
		streamID := b.operation.streamID
		b.operation.mu.Unlock()
		if closed {
			return 0, io.EOF
		}
		if len(b.pending) > 0 {
			n := copy(dst, b.pending)
			b.pending = b.pending[n:]
			return n, nil
		}
		if b.done {
			b.operation.close()
			return 0, io.EOF
		}
		raw, err := hostCall("host.http.stream_read", map[string]string{"stream_id": streamID})
		if err != nil {
			b.operation.close()
			return 0, fmt.Errorf("read host HTTP stream: %w", err)
		}
		var chunk hostHTTPStreamChunk
		if err := json.Unmarshal(raw, &chunk); err != nil {
			b.operation.close()
			return 0, errors.New("decode host HTTP stream chunk")
		}
		if chunk.Error != "" {
			b.operation.close()
			return 0, errors.New("host HTTP stream failed")
		}
		b.pending = chunk.Payload
		b.done = chunk.Done
		if len(b.pending) == 0 && !b.done {
			continue
		}
	}
}

func (b *hostHTTPBody) Close() error {
	b.operation.stop()
	return nil
}

func (hostHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("missing HTTP request")
	}
	callbackID, _ := req.Context().Value(callbackIDContextKey{}).(string)
	if callbackID == "" {
		return nil, errors.New("host callback scope is required for DeepSeek HTTP")
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	rawOpen, err := hostCall("host.http.operation_open", map[string]string{"host_callback_id": callbackID})
	if err != nil {
		return nil, fmt.Errorf("open host HTTP operation: %w", err)
	}
	var opened struct {
		OperationID string `json:"operation_id"`
	}
	if err := json.Unmarshal(rawOpen, &opened); err != nil || opened.OperationID == "" {
		return nil, errors.New("host HTTP operation returned no ID")
	}
	op := &hostHTTPOperation{callbackID: callbackID, operationID: opened.OperationID, done: make(chan struct{}), watchExited: make(chan struct{})}
	go func(ctx context.Context) {
		defer close(op.watchExited)
		select {
		case <-ctx.Done():
			op.close()
		case <-op.done:
		}
	}(req.Context())
	var body []byte
	if req.Body != nil {
		body, err = io.ReadAll(io.LimitReader(req.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			op.stop()
			return nil, errors.New("DeepSeek request body exceeds size limit or could not be read")
		}
	}
	rawResponse, err := hostCall("host.http.do_stream", hostHTTPRequest{
		HostCallbackID: callbackID, OperationID: opened.OperationID,
		Method: req.Method, URL: req.URL.String(), Headers: req.Header.Clone(), Body: body,
	})
	if err != nil {
		op.stop()
		return nil, fmt.Errorf("open host HTTP stream: %w", err)
	}
	var response hostHTTPStreamResponse
	if err := json.Unmarshal(rawResponse, &response); err != nil || response.StreamID == "" || response.StatusCode < 100 || response.StatusCode > 599 {
		op.stop()
		return nil, errors.New("invalid host HTTP stream response")
	}
	op.setStreamID(response.StreamID)
	if err := req.Context().Err(); err != nil {
		op.stop()
		return nil, err
	}
	return &http.Response{
		StatusCode: response.StatusCode,
		Status:     fmt.Sprintf("%d %s", response.StatusCode, http.StatusText(response.StatusCode)),
		Header:     response.Headers, Body: &hostHTTPBody{operation: op},
		ContentLength: -1, Request: req,
	}, nil
}
