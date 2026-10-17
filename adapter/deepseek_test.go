package adapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestDeepSeekRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test-secret" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("invalid request headers")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		switch r.URL.Path {
		case "/v1/tools/search":
			if body["query"] != "测试 search" || body["count"] != float64(5) || body["need_content"] != false {
				t.Errorf("unexpected search body: %v", body)
			}
			_, _ = w.Write([]byte(`{"results":[{"title":"Example","url":"https://example.com","summary":"result"}]}`))
		case "/v1/tools/fetch":
			if body["url"] != "https://example.com" {
				t.Error("incorrect fetch URL")
			}
			_, _ = w.Write([]byte(`{"title":"Example","content":"# Page"}`))
		default:
			t.Error("unexpected endpoint")
		}
	}))
	defer server.Close()
	client, err := NewDeepSeekClient(ClientOptions{APIKey: "test-secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Search(context.Background(), "测试 search"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Fetch(context.Background(), "https://example.com"); err != nil {
		t.Fatal(err)
	}
}

func TestDeepSeekValidationAndSanitizedErrors(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":{"message":"test-secret"}}`))
	}))
	defer server.Close()
	client, err := NewDeepSeekClient(ClientOptions{APIKey: "test-secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"", "  ", strings.Repeat("字", 129)} {
		if _, err := client.Search(context.Background(), query); err == nil {
			t.Error("accepted invalid query")
		}
	}
	if _, err := client.Fetch(context.Background(), "https://user:pass@example.com"); err == nil {
		t.Error("accepted credentials in URL")
	}
	if requests != 0 {
		t.Fatal("invalid input reached network")
	}
	_, err = client.Search(context.Background(), "valid")
	if err == nil || strings.Contains(err.Error(), "test-secret") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unexpected sanitized error: %v", err)
	}
	if requests != 1 {
		t.Error("retried non-retryable error")
	}
}

func TestDeepSeekBoundedResponseAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(strings.Repeat("x", 100))) }))
	defer server.Close()
	client, err := NewDeepSeekClient(ClientOptions{APIKey: "test", BaseURL: server.URL, MaxResponseBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Search(context.Background(), "valid"); err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("unexpected error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Search(ctx, "valid"); err != context.Canceled {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

// Live tests are opt-in and never print credentials or response contents.
func TestDeepSeekLive(t *testing.T) {
	if os.Getenv("DEEPSEEK_LIVE_TEST") != "1" {
		t.Skip("set DEEPSEEK_LIVE_TEST=1 and DEEPSEEK to enable")
	}
	client, err := NewDeepSeekClient(ClientOptions{APIKey: os.Getenv("DEEPSEEK"), Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.Search(context.Background(), "Go programming language official documentation")
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Results []json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(raw, &data); err != nil || len(data.Results) == 0 {
		t.Fatal("live search did not return results")
	}
	t.Logf("live search succeeded with %d result(s)", len(data.Results))
}
