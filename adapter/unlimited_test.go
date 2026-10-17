package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const unlimitedRequest = `{"input":"Question","tools":[{"type":"web_search"}]}`

func unlimitedSearchResponse(first, count int) string {
	items := make([]string, count)
	for i := range items {
		items[i] = fmt.Sprintf(`{"type":"function_call","name":"__deepseek_web_search","call_id":"search_%d","arguments":"{\"action\":\"search\",\"query\":\"Go\"}"}`, first+i)
	}
	return `{"output":[` + strings.Join(items, ",") + `]}`
}

func TestUnlimitedWorkExceedsDefaultLimits(t *testing.T) {
	for _, test := range []struct {
		name          string
		opts          Options
		searchRounds  int
		callsPerRound int
	}{
		{name: "rounds", opts: Options{MaxRounds: -1}, searchRounds: 9, callsPerRound: 1},
		{name: "calls", opts: Options{MaxCalls: -1}, searchRounds: 1, callsPerRound: 17},
		{name: "rounds and calls", opts: Options{MaxRounds: -1, MaxCalls: -1}, searchRounds: 18, callsPerRound: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			searcher := new(fakeSearcher)
			engine := NewEngine(searcher, test.opts)
			responses := make([]string, test.searchRounds+1)
			for round := 0; round < test.searchRounds; round++ {
				responses[round] = unlimitedSearchResponse(round*test.callsPerRound, test.callsPerRound)
			}
			responses[test.searchRounds] = `{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done"}]}]}`
			model, requests := scriptedModel(t, responses...)
			result, err := engine.Run(context.Background(), "tenant", []byte(unlimitedRequest), model)
			if err != nil {
				t.Fatal(err)
			}
			if len(*requests) != test.searchRounds+1 || len(searcher.queries) != test.searchRounds*test.callsPerRound {
				t.Fatalf("model calls=%d searches=%d", len(*requests), len(searcher.queries))
			}
			response, err := object(result)
			if err != nil {
				t.Fatal(err)
			}
			var output []json.RawMessage
			if err := json.Unmarshal(response["output"], &output); err != nil {
				t.Fatal(err)
			}
			if len(output) != test.searchRounds*test.callsPerRound+1 || !strings.Contains(string(output[len(output)-1]), "Done") {
				t.Fatalf("search output or final answer missing: %s", result)
			}
		})
	}
}

func TestUnlimitedWorkPreservesIndependentFiniteLimit(t *testing.T) {
	for _, test := range []struct {
		name string
		opts Options
		want string
	}{
		{name: "finite calls", opts: Options{MaxRounds: -1, MaxCalls: 2}, want: "web_search call limit exceeded"},
		{name: "finite rounds", opts: Options{MaxRounds: 2, MaxCalls: -1}, want: "web_search round limit exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			searcher := new(fakeSearcher)
			engine := NewEngine(searcher, test.opts)
			model, requests := scriptedModel(t, unlimitedSearchResponse(0, 1), unlimitedSearchResponse(1, 1), unlimitedSearchResponse(2, 1))
			result, err := engine.Run(context.Background(), "tenant", []byte(unlimitedRequest), model)
			if err == nil || err.Error() != test.want || result != nil {
				t.Fatalf("result=%s error=%v; want %q", result, err, test.want)
			}
			if len(searcher.queries) != 2 {
				t.Fatalf("finite limit allowed %d searches", len(searcher.queries))
			}
			wantModelCalls := 2
			if test.opts.MaxCalls == 2 {
				wantModelCalls = 3
			}
			if len(*requests) != wantModelCalls {
				t.Fatalf("model calls=%d; want %d", len(*requests), wantModelCalls)
			}
		})
	}
}

func TestUnlimitedWorkHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	searcher := new(fakeSearcher)
	engine := NewEngine(searcher, Options{MaxRounds: -1, MaxCalls: -1})
	modelCalls := 0
	model := func(context.Context, []byte) ([]byte, error) {
		modelCalls++
		if modelCalls == 11 {
			cancel()
		}
		if modelCalls > 11 {
			t.Fatal("model called after cancellation")
		}
		return []byte(unlimitedSearchResponse(modelCalls, 1)), nil
	}
	result, err := engine.Run(ctx, "tenant", []byte(unlimitedRequest), model)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("result=%s error=%v; want cancellation", result, err)
	}
	if modelCalls != 11 || len(searcher.queries) != 10 {
		t.Fatalf("model calls=%d searches=%d", modelCalls, len(searcher.queries))
	}
	if len(engine.states.items) != 0 || len(engine.states.webItems) != 0 {
		t.Fatal("cancelled request published continuation state")
	}
}

func TestUnlimitedWorkPreservesHistoryLimits(t *testing.T) {
	for _, test := range []struct {
		name string
		opts Options
	}{
		{name: "items", opts: Options{MaxRounds: -1, MaxCalls: -1, MaxHistoryItems: 5}},
		{name: "bytes", opts: Options{MaxRounds: -1, MaxCalls: -1, MaxHistoryBytes: 1500}},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := NewEngine(new(fakeSearcher), test.opts)
			modelCalls := 0
			model := func(context.Context, []byte) ([]byte, error) {
				modelCalls++
				if modelCalls > 20 {
					t.Fatal("conversation history limit was not enforced")
				}
				return []byte(unlimitedSearchResponse(modelCalls, 1)), nil
			}
			result, err := engine.Run(context.Background(), "tenant", []byte(unlimitedRequest), model)
			if err == nil || err.Error() != "conversation history limit exceeded" || result != nil {
				t.Fatalf("result=%s error=%v; want conversation history limit", result, err)
			}
			if len(engine.states.items) != 0 || len(engine.states.webItems) != 0 {
				t.Fatal("failed request published continuation state")
			}
		})
	}
}

func TestUnlimitedOptionsPreserveDefaults(t *testing.T) {
	for _, value := range []int{0, -2} {
		opts := (Options{MaxRounds: value, MaxCalls: value}).defaults()
		if opts.MaxRounds != 8 || opts.MaxCalls != 16 {
			t.Fatalf("value %d: MaxRounds=%d MaxCalls=%d; want 8 and 16", value, opts.MaxRounds, opts.MaxCalls)
		}
	}
	opts := (Options{MaxRounds: -1, MaxCalls: -1}).defaults()
	if opts.MaxRounds != -1 || opts.MaxCalls != -1 {
		t.Fatalf("unlimited options became MaxRounds=%d MaxCalls=%d", opts.MaxRounds, opts.MaxCalls)
	}
}

func TestUnlimitedCallsKeepReplayCacheBounded(t *testing.T) {
	engine := NewEngine(new(fakeSearcher), Options{MaxRounds: -1, MaxCalls: -1, MaxStates: 2})
	const capacity = 32
	if engine.states.webLimit != capacity {
		t.Fatalf("replay cache capacity=%d; want %d", engine.states.webLimit, capacity)
	}
	for i := 0; i <= capacity; i++ {
		engine.states.putWeb(fmt.Sprintf("web_%d", i), webRecord{tenant: "tenant"})
	}
	if len(engine.states.webItems) != capacity || len(engine.states.webOrder) != capacity {
		t.Fatalf("cache items=%d order=%d; want %d", len(engine.states.webItems), len(engine.states.webOrder), capacity)
	}
	if _, ok := engine.states.getWeb("tenant", "web_0"); ok {
		t.Fatal("oldest replay entry was not evicted")
	}
	if _, ok := engine.states.getWeb("tenant", "web_32"); !ok {
		t.Fatal("newest replay entry was not retained")
	}
}
