package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHostHTTPTransportStreamsPartialReadsAndCloses(t *testing.T) {
	previous := hostCall
	defer func() { hostCall = previous }()
	reads, canceled, closed := 0, 0, 0
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.http.operation_open":
			if payload.(map[string]string)["host_callback_id"] != "callback-1" {
				t.Fatal("callback scope missing")
			}
			return json.RawMessage(`{"operation_id":"operation-1"}`), nil
		case "host.http.do_stream":
			req := payload.(hostHTTPRequest)
			if req.OperationID != "operation-1" || req.HostCallbackID != "callback-1" || req.Method != "POST" || req.URL != "https://api.deepseek.com/v1/tools/search" || string(req.Body) != `{"query":"test"}` {
				t.Fatalf("request = %#v", req)
			}
			if req.Headers.Get("Authorization") != "Bearer test" {
				t.Fatal("authorization header missing")
			}
			return json.RawMessage(`{"status_code":200,"headers":{"Content-Type":["application/json"]},"stream_id":"stream-1"}`), nil
		case "host.http.stream_read":
			reads++
			if reads > 1 {
				t.Fatal("response was read past limit")
			}
			raw, _ := json.Marshal(hostHTTPStreamChunk{Payload: []byte(`{"results":[]}`), Done: false})
			return raw, nil
		case "host.http.cancel":
			canceled++
			if payload.(map[string]string)["operation_id"] != "operation-1" {
				t.Fatal("operation ID missing on cancel")
			}
			return json.RawMessage(`{}`), nil
		case "host.http.stream_close":
			closed++
			if payload.(map[string]string)["stream_id"] != "stream-1" {
				t.Fatal("stream ID missing on close")
			}
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	}
	ctx := context.WithValue(context.Background(), callbackIDContextKey{}, "callback-1")
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.deepseek.com/v1/tools/search", strings.NewReader(`{"query":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	resp, err := (hostHTTPTransport{}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	first := make([]byte, 3)
	if n, err := resp.Body.Read(first); err != nil || n != 3 || string(first) != `{"r` {
		t.Fatalf("partial read = %d %q %v", n, first, err)
	}
	limited, err := io.ReadAll(io.LimitReader(resp.Body, 2))
	if err != nil || string(limited) != "es" {
		t.Fatalf("limited read = %q %v", limited, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if reads != 1 || canceled != 1 || closed != 1 {
		t.Fatalf("reads=%d canceled=%d closed=%d", reads, canceled, closed)
	}
}

func TestHostHTTPTransportRequiresCallbackScope(t *testing.T) {
	req, err := http.NewRequest("GET", "https://api.deepseek.com/v1/tools/search", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (hostHTTPTransport{}).RoundTrip(req); err == nil {
		t.Fatal("request without callback scope was accepted")
	}
}

func TestHostHTTPTransportCancelsOnContextDone(t *testing.T) {
	previous := hostCall
	defer func() { hostCall = previous }()
	canceled := make(chan struct{})
	closed := make(chan struct{})
	hostCall = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case "host.http.operation_open":
			return json.RawMessage(`{"operation_id":"operation-1"}`), nil
		case "host.http.do_stream":
			return json.RawMessage(`{"status_code":200,"stream_id":"stream-1"}`), nil
		case "host.http.cancel":
			close(canceled)
			return json.RawMessage(`{}`), nil
		case "host.http.stream_close":
			close(closed)
			return json.RawMessage(`{}`), nil
		default:
			t.Fatalf("unexpected callback %s", method)
			return nil, nil
		}
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), callbackIDContextKey{}, "callback-1"))
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.deepseek.com/v1/tools/search", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (hostHTTPTransport{}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	<-canceled
	<-closed
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
