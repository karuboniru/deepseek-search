package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
)

// SearchOptions overrides service defaults for one search call. Nil means use
// the configured default; a non-nil false explicitly disables webpage content.
type SearchOptions struct {
	Count       *int
	NeedContent *bool
}

// ConfigurableSearcher optionally supports model-selected search options.
// Searcher implementations without this interface remain usable with defaults.
type ConfigurableSearcher interface {
	SearchWithOptions(context.Context, string, SearchOptions) (json.RawMessage, error)
}

func (options SearchOptions) hasOverrides() bool {
	return options.Count != nil || options.NeedContent != nil
}

func (options SearchOptions) validate() error {
	if options.Count != nil && (*options.Count < 1 || *options.Count > 50) {
		return errors.New("search count must be an integer between 1 and 50")
	}
	return nil
}

func parseSearchOptions(args map[string]json.RawMessage) (SearchOptions, error) {
	var options SearchOptions
	if raw, ok := args["count"]; ok {
		var count int
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &count) != nil {
			return options, errors.New("search count must be an integer between 1 and 50")
		}
		options.Count = &count
	}
	if raw, ok := args["need_content"]; ok {
		var content bool
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &content) != nil {
			return options, errors.New("search need_content must be a boolean")
		}
		options.NeedContent = &content
	}
	return options, options.validate()
}

func (e *Engine) search(ctx context.Context, query string, options SearchOptions) (json.RawMessage, error) {
	if configurable, ok := e.searcher.(ConfigurableSearcher); ok {
		return configurable.SearchWithOptions(ctx, query, options)
	}
	if options.hasOverrides() {
		return nil, errors.New("search provider does not support per-call options")
	}
	return e.searcher.Search(ctx, query)
}
