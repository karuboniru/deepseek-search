package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestConfiguredZeroBudgetsAreUnlimited(t *testing.T) {
	previous := activeConfig.Load()
	t.Cleanup(func() { activeConfig.Store(previous) })
	for _, tc := range []struct {
		name, yaml, wantError string
		rounds, calls         int
	}{
		{"omitted defaults", "", "web_search round limit exceeded", 8, 16},
		{"both unlimited", "max_rounds: 0\nmax_calls: 0\n", "", 0, 0},
		{"rounds unlimited only", "max_rounds: 0\n", "web_search call limit exceeded", 0, 16},
		{"calls unlimited only", "max_calls: 0\n", "web_search round limit exceeded", 8, 0},
		{"positive limit", "max_rounds: 2\nmax_calls: 3\n", "web_search round limit exceeded", 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yaml := "enabled: true\napi_key: test-unused-key\nmodels:\n  - name: budget-test\n" + tc.yaml
			raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml)})
			if err := configure(raw); err != nil {
				t.Fatal(err)
			}
			cfg := currentConfig()
			if cfg.MaxRounds != tc.rounds || cfg.MaxCalls != tc.calls {
				t.Fatalf("unexpected configured limits: %d/%d", cfg.MaxRounds, cfg.MaxCalls)
			}
			rounds := 0
			_, err := cfg.engine.Run(context.Background(), "tenant", []byte(`{"input":"question","tools":[{"type":"web_search"}]}`), func(context.Context, []byte) ([]byte, error) {
				rounds++
				if rounds > 18 {
					return []byte(`{"output":[]}`), nil
				}
				// Invalid empty query produces a local tool result without HTTP.
				return []byte(fmt.Sprintf(`{"output":[{"type":"function_call","name":"__deepseek_web_search","call_id":"call_%d","arguments":"{\"query\":\"\"}"}]}`, rounds)), nil
			})
			if tc.wantError == "" {
				if err != nil || rounds != 19 {
					t.Fatalf("rounds=%d error=%v", rounds, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error=%v, want %s", err, tc.wantError)
			}
		})
	}
	for _, field := range []string{"max_rounds", "max_calls"} {
		raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte(field + ": -1\n")})
		if err := configure(raw); err == nil {
			t.Fatalf("negative %s accepted", field)
		}
	}
}
