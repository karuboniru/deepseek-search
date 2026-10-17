package adapter

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const searchContextFormat = "deepseek-search-context"

// searchContext is a plaintext replay capsule in the public encrypted_content
// field. The field name follows the Responses protocol; this plugin does not
// encrypt or authenticate its contents. Treat it as client-supplied history.
type searchContext struct {
	Format  string          `json:"format"`
	Version int             `json:"version"`
	ID      string          `json:"id"`
	Tenant  string          `json:"tenant"`
	Round   string          `json:"round"`
	Items   []string        `json:"items"`
	Tail    *[]string       `json:"tail,omitempty"`
	Call    json.RawMessage `json:"call"`
	Result  json.RawMessage `json:"result"`
}

func searchContextTenant(tenant string) string {
	hash := sha256.Sum256([]byte(tenant))
	return hex.EncodeToString(hash[:])
}

func (e *Engine) encodeSearchContext(id string, record webRecord) (string, error) {
	items := make([]string, 0, len(record.items))
	for key := range record.items {
		// Membership hashes contain binary bytes, which JSON strings cannot
		// represent losslessly without encoding.
		items = append(items, base64.RawURLEncoding.EncodeToString([]byte(key)))
	}
	sort.Strings(items)
	var tail *[]string
	if record.tail != nil {
		encoded := make([]string, len(record.tail))
		for i, key := range record.tail {
			encoded[i] = base64.RawURLEncoding.EncodeToString([]byte(key))
		}
		tail = &encoded
	}
	data, err := json.Marshal(searchContext{
		Format: searchContextFormat, Version: 1, ID: id,
		Tenant: searchContextTenant(record.tenant), Round: record.round,
		Items: items, Tail: tail, Call: record.call, Result: record.result,
	})
	if err != nil {
		return "", fmt.Errorf("encode web_search replay context: %w", err)
	}
	if len(data) > e.opts.MaxHistoryBytes {
		return "", errors.New("web_search replay context limit exceeded")
	}
	return string(data), nil
}

func (e *Engine) decodeSearchContext(tenant, id, text string) (webRecord, error) {
	var capsule searchContext
	if len(text) > e.opts.MaxHistoryBytes {
		return webRecord{}, errors.New("web_search replay context limit exceeded")
	}
	if err := json.Unmarshal([]byte(text), &capsule); err != nil {
		return webRecord{}, fmt.Errorf("invalid web_search replay context: %w", err)
	}
	if capsule.Format != searchContextFormat || capsule.Version != 1 {
		return webRecord{}, errors.New("unsupported web_search replay context format or version")
	}
	if capsule.ID != id || capsule.Tenant != searchContextTenant(tenant) || capsule.Round == "" {
		return webRecord{}, errors.New("web_search replay context ID, tenant, or round mismatch")
	}
	call, errCall := object(capsule.Call)
	result, errResult := object(capsule.Result)
	var output *string
	if errCall != nil || errResult != nil || stringValue(call["type"]) != "function_call" ||
		stringValue(call["name"]) != searchFunction || stringValue(call["call_id"]) != id ||
		stringValue(result["type"]) != "function_call_output" || stringValue(result["call_id"]) != id ||
		json.Unmarshal(result["output"], &output) != nil || output == nil {
		return webRecord{}, errors.New("invalid web_search replay call or result")
	}
	if len(capsule.Items) > 2*e.opts.MaxHistoryItems {
		return webRecord{}, errors.New("web_search replay membership limit exceeded")
	}
	items := make(map[string]bool, len(capsule.Items))
	for _, encoded := range capsule.Items {
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) == 0 || items[string(key)] {
			return webRecord{}, errors.New("invalid web_search replay membership")
		}
		items[string(key)] = true
	}
	var tail []string
	if capsule.Tail != nil {
		if len(*capsule.Tail) > e.opts.MaxHistoryItems {
			return webRecord{}, errors.New("web_search replay tail limit exceeded")
		}
		tail = make([]string, len(*capsule.Tail))
		for i, encoded := range *capsule.Tail {
			if encoded == "" {
				continue
			}
			key, err := base64.RawURLEncoding.DecodeString(encoded)
			validHash := len(key) == len("json:")+sha256.Size && strings.HasPrefix(string(key), "json:")
			validID := len(key) > len("id:") && strings.HasPrefix(string(key), "id:")
			if err != nil || (!validHash && !validID) {
				return webRecord{}, errors.New("invalid web_search replay tail")
			}
			tail[i] = string(key)
		}
	}
	return webRecord{tenant: tenant, round: capsule.Round, items: items, tail: tail, call: capsule.Call, result: capsule.Result}, nil
}

func (e *Engine) publicWebRecord(tenant, id string, public map[string]json.RawMessage) (webRecord, bool, error) {
	if id == "" {
		return webRecord{}, false, errors.New("web_search_call has no ID")
	}
	if raw := public["encrypted_content"]; len(raw) > 0 && string(raw) != "null" {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return webRecord{}, false, errors.New("web_search encrypted_content must be a string")
		}
		if text != "" {
			// A supplied capsule is authoritative. Invalid or unknown versions
			// must not silently select a different cached history.
			record, err := e.decodeSearchContext(tenant, id, text)
			if err == nil {
				e.states.deleteWeb(tenant, id)
			}
			return record, false, err
		}
	}
	if record, ok := e.states.getWeb(tenant, id); ok {
		return record, true, nil
	}
	return unavailableWebRecord(tenant, id, public), false, nil
}

func unavailableWebRecord(tenant, id string, public map[string]json.RawMessage) webRecord {
	action, _ := object(public["action"])
	args := make(map[string]json.RawMessage)
	for _, key := range []string{"query", "queries", "url", "pattern"} {
		if raw := action[key]; len(raw) > 0 {
			args[key] = raw
		}
	}
	actionType := stringValue(action["type"])
	if actionType == "" {
		actionType = "search"
	}
	args["action"], _ = json.Marshal(actionType)
	arguments, _ := json.Marshal(args)
	call, _ := json.Marshal(map[string]any{
		"type": "function_call", "name": searchFunction, "call_id": id, "arguments": string(arguments),
	})
	data, _ := json.Marshal(map[string]any{"error": map[string]string{
		"type":    "historical_search_unavailable",
		"message": "Historical web search context is unavailable. No search results can be recovered from this item.",
	}})
	result, _ := json.Marshal(map[string]any{"type": "function_call_output", "call_id": id, "output": string(data)})
	return webRecord{tenant: tenant, round: "unavailable:" + id, call: call, result: result}
}
