package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ClientOptions configures the search service, independently of model execution.
type ClientOptions struct {
	APIKey           string
	BaseURL          string
	Count            int
	NeedContent      bool
	MaxResponseBytes int64
	HTTPClient       *http.Client
}

// DeepSeekClient implements Searcher. It never adds a network timeout; callers
// own cancellation. Rate limiting applies separately to search and fetch.
type DeepSeekClient struct {
	opts       ClientOptions
	searchGate requestGate
	fetchGate  requestGate
}

type requestGate struct {
	mu   sync.Mutex
	next time.Time
}

func NewDeepSeekClient(opts ClientOptions) (*DeepSeekClient, error) {
	if strings.TrimSpace(opts.APIKey) == "" {
		return nil, errors.New("DeepSeek API key is required")
	}
	if opts.BaseURL == "" {
		opts.BaseURL = "https://api.deepseek.com"
	}
	u, err := url.Parse(opts.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("DeepSeek base_url must be an HTTP(S) URL without credentials, query or fragment")
	}
	opts.BaseURL = strings.TrimRight(opts.BaseURL, "/")
	if opts.Count == 0 {
		opts.Count = 5
	}
	if opts.Count < 1 || opts.Count > 50 {
		return nil, errors.New("DeepSeek count must be between 1 and 50")
	}
	if opts.MaxResponseBytes == 0 {
		opts.MaxResponseBytes = 8 << 20
	}
	if opts.MaxResponseBytes < 1 || opts.MaxResponseBytes > 64<<20 {
		return nil, errors.New("DeepSeek max response size must be between 1 and 67108864 bytes")
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &DeepSeekClient{opts: opts}, nil
}

func (c *DeepSeekClient) Search(ctx context.Context, query string) (json.RawMessage, error) {
	if strings.TrimSpace(query) == "" || !utf8.ValidString(query) || utf8.RuneCountInString(query) > 128 {
		return nil, errors.New("search query must contain 1 to 128 Unicode characters")
	}
	raw, err := c.request(ctx, "/v1/tools/search", map[string]any{"query": query, "count": c.opts.Count, "need_content": c.opts.NeedContent}, &c.searchGate)
	if err != nil {
		return nil, err
	}
	var result struct {
		Results json.RawMessage `json:"results"`
	}
	if json.Unmarshal(raw, &result) != nil || len(result.Results) == 0 || result.Results[0] != '[' {
		return nil, errors.New("DeepSeek search returned an invalid results array")
	}
	return raw, nil
}

func (c *DeepSeekClient) Fetch(ctx context.Context, pageURL string) (json.RawMessage, error) {
	u, err := url.Parse(pageURL)
	if err != nil || len(pageURL) > 8192 || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return nil, errors.New("fetch requires an HTTP(S) URL without credentials, at most 8192 bytes")
	}
	raw, err := c.request(ctx, "/v1/tools/fetch", map[string]string{"url": pageURL}, &c.fetchGate)
	if err != nil {
		return nil, err
	}
	var result struct {
		Content *string `json:"content"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Content == nil {
		return nil, errors.New("DeepSeek fetch returned invalid content")
	}
	return raw, nil
}

func (g *requestGate) wait(ctx context.Context) error {
	g.mu.Lock()
	now := time.Now()
	at := g.next
	if at.Before(now) {
		at = now
	}
	g.next = at.Add(210 * time.Millisecond)
	g.mu.Unlock()
	return waitContext(ctx, time.Until(at))
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *DeepSeekClient) request(ctx context.Context, path string, payload any, gate *requestGate) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("encode DeepSeek request")
	}
	for attempt := 0; attempt < 3; attempt++ {
		if errWait := gate.wait(ctx); errWait != nil {
			return nil, errWait
		}
		req, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, c.opts.BaseURL+path, bytes.NewReader(body))
		if errRequest != nil {
			return nil, errors.New("create DeepSeek request")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.opts.APIKey)
		resp, errDo := c.opts.HTTPClient.Do(req)
		if errDo != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Do not expose transport errors, which may contain request URLs.
			return nil, errors.New("DeepSeek transport failed")
		}
		raw, errRead := io.ReadAll(io.LimitReader(resp.Body, c.opts.MaxResponseBytes+1))
		errClose := resp.Body.Close()
		if errRead != nil || errClose != nil {
			return nil, errors.New("read DeepSeek response failed")
		}
		if int64(len(raw)) > c.opts.MaxResponseBytes {
			return nil, errors.New("DeepSeek response exceeds configured size limit")
		}
		if resp.StatusCode == http.StatusOK {
			if !json.Valid(raw) {
				return nil, errors.New("DeepSeek returned invalid JSON")
			}
			return json.RawMessage(raw), nil
		}
		retryable := resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500
		if !retryable || attempt == 2 {
			return nil, fmt.Errorf("DeepSeek request failed (HTTP %d)", resp.StatusCode)
		}
		delay := time.Duration(attempt+1) * time.Second
		if seconds, errParse := strconv.Atoi(resp.Header.Get("Retry-After")); errParse == nil && seconds > 0 && seconds <= 60 {
			delay = time.Duration(seconds) * time.Second
		}
		if errWait := waitContext(ctx, delay); errWait != nil {
			return nil, errWait
		}
	}
	return nil, errors.New("DeepSeek retry limit reached")
}
