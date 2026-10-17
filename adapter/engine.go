package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const searchFunction = "__deepseek_web_search"

// Model returns one complete Responses object from the upstream model.
// Its transport may stream while collecting that object.
type Model func(context.Context, []byte) ([]byte, error)

// Observer receives public response progress synchronously. Returning an error
// aborts the request. Search indexes refer to the final public output array.
// Each search emits in_progress before execution and completed or failed afterward.
type Observer interface {
	Start(response json.RawMessage) error
	Search(index int, item json.RawMessage) error
}

// Searcher supplies search results and fetched pages as JSON values.
type Searcher interface {
	Search(context.Context, string) (json.RawMessage, error)
	Fetch(context.Context, string) (json.RawMessage, error)
}

type Engine struct {
	searcher Searcher
	opts     Options
	states   *stateStore
}

func NewEngine(searcher Searcher, opts Options) *Engine {
	opts = opts.defaults()
	return &Engine{searcher: searcher, opts: opts, states: newStateStore(opts.MaxStates, opts.MaxStates*opts.MaxCalls)}
}

// Run processes one public Responses request. The tenant key must identify the
// authenticated caller; a response ID alone never authorizes access to history.
func (e *Engine) Run(ctx context.Context, tenant string, body []byte, model Model) ([]byte, error) {
	return e.RunObserved(ctx, tenant, body, model, nil)
}

// RunObserved processes a request while reporting public search progress. Start
// runs after request validation and before any upstream call, including requests
// that pause for outstanding client tools. Its ID is reused by the final response.
func (e *Engine) RunObserved(ctx context.Context, tenant string, body []byte, model Model, observer Observer) ([]byte, error) {
	if tenant == "" {
		return nil, errors.New("tenant is required")
	}
	if model == nil {
		return nil, errors.New("model is required")
	}
	req, err := object(body)
	if err != nil {
		return nil, fmt.Errorf("decode request: %w", err)
	}
	if len(body) > e.opts.MaxHistoryBytes {
		return nil, errors.New("request body limit exceeded")
	}
	var previous string
	if raw := req["previous_response_id"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &previous); err != nil || previous == "" {
			return nil, errors.New("invalid previous_response_id")
		}
	}
	c := conversation{tenant: tenant, pending: make(map[string]json.RawMessage)}
	if previous != "" {
		var ok bool
		c, ok = e.states.get(tenant, previous)
		if !ok {
			return nil, errors.New("previous_response_id not found for tenant")
		}
	}
	input, err := inputItems(req["input"])
	if err != nil {
		return nil, fmt.Errorf("decode input: %w", err)
	}
	input, restoredWeb, err := e.restorePublicWeb(tenant, input)
	if err != nil {
		return nil, err
	}
	fullReplay := restoredWeb || hasCompactionMarker(input) || (previous == "" && hasClientCall(input))
	if fullReplay {
		c.pending, err = replayPending(input)
		if err != nil {
			return nil, err
		}
		c.history = append([]json.RawMessage(nil), input...)
		if len(c.response) == 0 {
			c.response = json.RawMessage(`{"output":[],"status":"completed"}`)
		}
	} else {
		results := 0
		for _, item := range input {
			obj, _ := object(item)
			typ := stringValue(obj["type"])
			if typ == "function_call_output" || typ == "custom_tool_call_output" {
				results++
			}
		}
		if results > 0 && len(c.pending) == 0 {
			return nil, errors.New("tool result has no pending call in saved response state")
		}
	}
	if !fullReplay && len(c.pending) > 0 {
		seen := make(map[string]bool)
		for _, item := range input {
			obj, errObj := object(item)
			if errObj != nil || !isClientToolOutput(stringValue(obj["type"])) {
				continue
			}
			id := stringValue(obj["call_id"])
			pending, ok := c.pending[id]
			if !ok || seen[id] || !matchesToolOutput(pending, stringValue(obj["type"])) {
				return nil, fmt.Errorf("unexpected or repeated tool result for call_id %q", id)
			}
			seen[id] = true
			delete(c.pending, id)
		}
		if len(seen) == 0 {
			return nil, errors.New("pending tool results are required")
		}
	}
	if !fullReplay {
		c.history = append(c.history, input...)
	}
	if !e.historyWithinLimit(c.history) {
		return nil, errors.New("conversation history limit exceeded")
	}
	if len(req["tools"]) == 0 && len(c.tools) > 0 {
		req["tools"] = c.tools
	}
	if len(req["model"]) == 0 && len(c.model) > 0 {
		req["model"] = c.model
	}
	c.tools = req["tools"]
	c.model = req["model"]
	c.toolChoice, c.choicePresent = req["tool_choice"]
	choice := parseToolChoice(req["tool_choice"])
	searchEnabled, err := rewriteTools(req)
	if err != nil {
		return nil, err
	}
	c.searchEnabled = searchEnabled
	if searchEnabled && e.searcher == nil {
		return nil, errors.New("searcher is required for web_search")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id, err := responseID()
	if err != nil {
		return nil, err
	}
	reserved := reservedResponse{id: id}
	if observer != nil {
		reserved.createdAt = time.Now().Unix()
		start := map[string]json.RawMessage{
			"id":                 json.RawMessage(strconv.Quote(id)),
			"object":             json.RawMessage(`"response"`),
			"status":             json.RawMessage(`"in_progress"`),
			"output":             json.RawMessage(`[]`),
			"created_at":         json.RawMessage(strconv.FormatInt(reserved.createdAt, 10)),
			"background":         json.RawMessage(`false`),
			"error":              json.RawMessage(`null`),
			"incomplete_details": json.RawMessage(`null`),
			"usage":              json.RawMessage(`null`),
		}
		publicFields(c, previous, start)
		startBody, err := json.Marshal(start)
		if err != nil {
			return nil, fmt.Errorf("encode response start: %w", err)
		}
		if err := observer.Start(startBody); err != nil {
			return nil, fmt.Errorf("observe response start: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if len(c.pending) > 0 {
		return e.pause(c, previous, reserved)
	}
	// Stage private records until the request succeeds, so cancellation and
	// observer failures never publish continuation state for partial requests.
	var webRecords []savedWebRecord
	delete(req, "previous_response_id")
	req["stream"] = json.RawMessage("false")
	var publicOutput []json.RawMessage
	usageTotals := make(map[string]int64)
	callCount := 0
	for round := 0; round < e.opts.MaxRounds; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		req["input"], err = json.Marshal(c.history)
		if err != nil {
			return nil, fmt.Errorf("encode history: %w", err)
		}
		payload, errMarshal := json.Marshal(req)
		if errMarshal != nil {
			return nil, fmt.Errorf("encode model request: %w", errMarshal)
		}
		upstream, errModel := model(ctx, payload)
		if errModel != nil {
			return nil, fmt.Errorf("model request: %w", errModel)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(upstream) > e.opts.MaxHistoryBytes {
			return nil, errors.New("model response limit exceeded")
		}
		response, errResponse := object(upstream)
		if errResponse != nil {
			return nil, fmt.Errorf("decode model response: %w", errResponse)
		}
		switch stringValue(response["status"]) {
		case "failed", "incomplete", "cancelled":
			return nil, fmt.Errorf("model response is %s", stringValue(response["status"]))
		}
		addUsage(response["usage"], usageTotals)
		var output []json.RawMessage
		if err := json.Unmarshal(response["output"], &output); err != nil {
			return nil, fmt.Errorf("decode model output: %w", err)
		}
		if len(output) == 0 && string(response["output"]) != "[]" {
			return nil, errors.New("model response has no output array")
		}
		roundID, err := responseID()
		if err != nil {
			return nil, err
		}
		roundItems := roundMembership(output)
		priorCallIDs := callIDs(c.history)
		historyOutputStart := len(c.history)
		c.history = append(c.history, output...)
		if !e.historyWithinLimit(c.history) {
			return nil, errors.New("conversation history limit exceeded")
		}
		var privateCalls []json.RawMessage
		var privatePositions []int
		var privateHistoryPositions []int
		roundCallIDs := make(map[string]bool)
		for outputIndex, item := range output {
			obj, errObj := object(item)
			if errObj != nil {
				return nil, fmt.Errorf("decode output item: %w", errObj)
			}
			if isToolCall(stringValue(obj["type"])) {
				id := stringValue(obj["call_id"])
				if id == "" {
					return nil, errors.New("tool call has no call_id")
				}
				if roundCallIDs[id] {
					return nil, fmt.Errorf("duplicate tool call_id %q in model response", id)
				}
				roundCallIDs[id] = true
				if stringValue(obj["type"]) == "function_call" && stringValue(obj["name"]) == searchFunction {
					if !searchEnabled || stringValue(req["tool_choice"]) == "none" {
						return nil, errors.New("undeclared web_search call")
					}
					privateCalls = append(privateCalls, item)
					privatePositions = append(privatePositions, len(publicOutput))
					privateHistoryPositions = append(privateHistoryPositions, outputIndex)
					publicOutput = append(publicOutput, nil)
					continue
				}
				if priorCallIDs[id] {
					return nil, fmt.Errorf("client call_id %q was already used", id)
				}
				if _, exists := c.pending[id]; exists {
					return nil, fmt.Errorf("duplicate client call_id %q", id)
				}
				c.pending[id] = item
			}
			publicOutput = append(publicOutput, item)
		}
		if callCount+len(privateCalls) > e.opts.MaxCalls {
			return nil, errors.New("web_search call limit exceeded")
		}
		if round == 0 {
			if err := choice.validate(output); err != nil {
				return nil, err
			}
		}
		for i, item := range privateCalls {
			callCount++
			hiddenCall, result, webItem, errCall := e.execute(ctx, item, privatePositions[i], observer)
			if errCall != nil {
				return nil, errCall
			}
			webObj, _ := object(webItem)
			webRecords = append(webRecords, savedWebRecord{id: stringValue(webObj["id"]), record: webRecord{
				tenant: tenant, round: roundID, items: roundItems, call: hiddenCall, result: result,
			}})
			c.history[historyOutputStart+privateHistoryPositions[i]] = hiddenCall
			c.history = append(c.history, result)
			publicOutput[privatePositions[i]] = webItem
		}
		if !e.historyWithinLimit(c.history) {
			return nil, errors.New("conversation history limit exceeded")
		}
		if len(privateCalls) == 0 || len(c.pending) > 0 {
			applyUsage(response, usageTotals)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			result, err := e.finish(c, previous, response, publicOutput, reserved)
			if err != nil {
				return nil, err
			}
			for _, saved := range webRecords {
				e.states.putWeb(saved.id, saved.record)
			}
			return result, nil
		}
		// A forced tool choice applies to this public request, not every internal
		// continuation after its tool has been executed.
		req["tool_choice"] = json.RawMessage(`"auto"`)
	}
	return nil, errors.New("web_search round limit exceeded")
}

// Compact restores private web context before one upstream compaction request.
// The compacted output becomes a new tenant-scoped continuation point.
func (e *Engine) Compact(ctx context.Context, tenant string, body []byte, model Model) ([]byte, error) {
	if tenant == "" || model == nil {
		return nil, errors.New("tenant and model are required")
	}
	if len(body) > e.opts.MaxHistoryBytes {
		return nil, errors.New("request body limit exceeded")
	}
	req, err := object(body)
	if err != nil {
		return nil, fmt.Errorf("decode compact request: %w", err)
	}
	previous := stringValue(req["previous_response_id"])
	c := conversation{tenant: tenant, pending: make(map[string]json.RawMessage)}
	if len(req["previous_response_id"]) > 0 && string(req["previous_response_id"]) != "null" && previous == "" {
		return nil, errors.New("invalid previous_response_id")
	}
	if previous != "" {
		var ok bool
		c, ok = e.states.get(tenant, previous)
		if !ok {
			return nil, errors.New("previous_response_id not found for tenant")
		}
	}
	input, err := inputItems(req["input"])
	if err != nil {
		return nil, fmt.Errorf("decode compact input: %w", err)
	}
	input, restoredWeb, err := e.restorePublicWeb(tenant, input)
	if err != nil {
		return nil, err
	}
	if restoredWeb || hasCompactionMarker(input) || (previous == "" && hasClientCall(input)) {
		c.history = append([]json.RawMessage(nil), input...)
	} else {
		c.history = append(c.history, input...)
	}
	if !e.historyWithinLimit(c.history) {
		return nil, errors.New("conversation history limit exceeded")
	}
	pending, err := replayPending(c.history)
	if err != nil {
		return nil, err
	}
	if len(pending) > 0 {
		return nil, errors.New("cannot compact with pending client tool calls")
	}
	c.pending = pending
	if len(req["model"]) == 0 && len(c.model) > 0 {
		req["model"] = c.model
	}
	c.model = req["model"]
	c.toolChoice, c.choicePresent = req["tool_choice"]
	delete(req, "previous_response_id")
	req["stream"] = json.RawMessage("false")
	req["input"], err = json.Marshal(c.history)
	if err != nil {
		return nil, fmt.Errorf("encode compact history: %w", err)
	}
	upstreamBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode compact request: %w", err)
	}
	upstream, err := model(ctx, upstreamBody)
	if err != nil {
		return nil, fmt.Errorf("compact model request: %w", err)
	}
	if len(upstream) > e.opts.MaxHistoryBytes {
		return nil, errors.New("compact response limit exceeded")
	}
	response, err := object(upstream)
	if err != nil {
		return nil, fmt.Errorf("decode compact response: %w", err)
	}
	switch stringValue(response["status"]) {
	case "failed", "incomplete", "cancelled":
		return nil, fmt.Errorf("compact response is %s", stringValue(response["status"]))
	}
	var output []json.RawMessage
	if err := json.Unmarshal(response["output"], &output); err != nil || output == nil {
		return nil, errors.New("compact response has no output array")
	}
	// A compaction item replaces the expanded transcript for future turns.
	c.history = append([]json.RawMessage(nil), output...)
	if !e.historyWithinLimit(c.history) {
		return nil, errors.New("conversation history limit exceeded")
	}
	return e.finish(c, previous, response, output, reservedResponse{})
}

type toolChoice struct {
	mode string
	name string
}

func parseToolChoice(raw json.RawMessage) toolChoice {
	if len(raw) == 0 {
		return toolChoice{}
	}
	if raw[0] == '"' {
		mode := stringValue(raw)
		if strings.HasPrefix(mode, "web_search") {
			return toolChoice{mode: "named", name: searchFunction}
		}
		return toolChoice{mode: mode}
	}
	obj, err := object(raw)
	if err != nil {
		return toolChoice{}
	}
	if strings.HasPrefix(stringValue(obj["type"]), "web_search") {
		return toolChoice{mode: "named", name: searchFunction}
	}
	if stringValue(obj["type"]) == "function" {
		return toolChoice{mode: "named", name: stringValue(obj["name"])}
	}
	return toolChoice{}
}

func (choice toolChoice) validate(output []json.RawMessage) error {
	calls := 0
	matched := false
	for _, item := range output {
		obj, _ := object(item)
		if !isToolCall(stringValue(obj["type"])) {
			continue
		}
		calls++
		if choice.mode == "none" {
			return errors.New("model called a tool despite tool_choice none")
		}
		if choice.mode == "named" {
			if stringValue(obj["name"]) != choice.name {
				return fmt.Errorf("model called %q despite tool_choice %q", stringValue(obj["name"]), choice.name)
			}
			matched = true
		}
	}
	if choice.mode == "required" && calls == 0 {
		return errors.New("model returned no tool call despite tool_choice required")
	}
	if choice.mode == "named" && !matched {
		return fmt.Errorf("model returned no %q call despite tool_choice", choice.name)
	}
	return nil
}

func isToolCall(typ string) bool {
	return typ == "function_call" || typ == "custom_tool_call"
}

func isClientToolOutput(typ string) bool {
	return typ == "function_call_output" || typ == "custom_tool_call_output"
}

func matchesToolOutput(call json.RawMessage, outputType string) bool {
	obj, err := object(call)
	if err != nil {
		return false
	}
	return (stringValue(obj["type"]) == "function_call" && outputType == "function_call_output") ||
		(stringValue(obj["type"]) == "custom_tool_call" && outputType == "custom_tool_call_output")
}

func hasClientCall(items []json.RawMessage) bool {
	for _, item := range items {
		obj, err := object(item)
		if err == nil && isToolCall(stringValue(obj["type"])) && stringValue(obj["name"]) != searchFunction {
			return true
		}
	}
	return false
}

func hasCompactionMarker(items []json.RawMessage) bool {
	for _, item := range items {
		obj, err := object(item)
		if err == nil && stringValue(obj["type"]) == "compaction" {
			return true
		}
	}
	return false
}

func callIDs(items []json.RawMessage) map[string]bool {
	ids := make(map[string]bool)
	for _, item := range items {
		obj, err := object(item)
		if err == nil && isToolCall(stringValue(obj["type"])) {
			ids[stringValue(obj["call_id"])] = true
		}
	}
	return ids
}

func roundMembership(output []json.RawMessage) map[string]bool {
	items := make(map[string]bool)
	for _, item := range output {
		obj, err := object(item)
		if err != nil || (stringValue(obj["type"]) == "function_call" && stringValue(obj["name"]) == searchFunction) {
			continue
		}
		for _, key := range itemKeys(item) {
			items[key] = true
		}
	}
	return items
}

func itemKeys(item json.RawMessage) []string {
	var keys []string
	obj, err := object(item)
	if err == nil {
		if id := stringValue(obj["id"]); id != "" {
			keys = append(keys, "id:"+id)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) == nil {
		canonical, err := json.Marshal(value)
		if err == nil {
			hash := sha256.Sum256(canonical)
			keys = append(keys, "json:"+string(hash[:]))
		}
	}
	return keys
}

func belongsToRound(record webRecord, item json.RawMessage) bool {
	for _, key := range itemKeys(item) {
		if record.items[key] {
			return true
		}
	}
	return false
}

func (e *Engine) restorePublicWeb(tenant string, input []json.RawMessage) ([]json.RawMessage, bool, error) {
	var restored []json.RawMessage
	var hiddenResults []json.RawMessage
	var current webRecord
	seen := make(map[string]bool)
	found := false
	for _, item := range input {
		obj, _ := object(item)
		typ := stringValue(obj["type"])
		switch typ {
		case "web_search_call":
			id := stringValue(obj["id"])
			if seen[id] {
				return nil, false, fmt.Errorf("duplicate web_search_call ID %q", id)
			}
			seen[id] = true
			record, ok := e.states.getWeb(tenant, id)
			if !ok {
				return nil, false, fmt.Errorf("web_search_call ID %q not found for tenant", id)
			}
			if len(hiddenResults) > 0 && current.round != record.round {
				restored = append(restored, hiddenResults...)
				hiddenResults = nil
			}
			call, err := replaceCallID(record.call, id)
			if err != nil {
				return nil, false, err
			}
			result, err := replaceCallID(record.result, id)
			if err != nil {
				return nil, false, err
			}
			restored = append(restored, call)
			hiddenResults = append(hiddenResults, result)
			current = record
			found = true
		case "function_call":
			if stringValue(obj["name"]) == searchFunction {
				return nil, false, errors.New("private web_search call cannot be supplied by client")
			}
			if len(hiddenResults) > 0 && !belongsToRound(current, item) {
				restored = append(restored, hiddenResults...)
				hiddenResults = nil
			}
			restored = append(restored, item)
		default:
			if len(hiddenResults) > 0 && !belongsToRound(current, item) {
				restored = append(restored, hiddenResults...)
				hiddenResults = nil
			}
			restored = append(restored, item)
		}
	}
	restored = append(restored, hiddenResults...)
	return restored, found, nil
}

func replaceCallID(item json.RawMessage, id string) (json.RawMessage, error) {
	obj, err := object(item)
	if err != nil {
		return nil, fmt.Errorf("decode saved web_search item: %w", err)
	}
	obj["call_id"], _ = json.Marshal(id)
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encode saved web_search item: %w", err)
	}
	return out, nil
}

func replayPending(items []json.RawMessage) (map[string]json.RawMessage, error) {
	calls := make(map[string]json.RawMessage)
	private := make(map[string]bool)
	answered := make(map[string]bool)
	for _, item := range items {
		obj, _ := object(item)
		typ := stringValue(obj["type"])
		id := stringValue(obj["call_id"])
		if isToolCall(typ) {
			if id == "" || calls[id] != nil {
				return nil, fmt.Errorf("missing or duplicate replay call_id %q", id)
			}
			calls[id] = item
			private[id] = typ == "function_call" && stringValue(obj["name"]) == searchFunction
			continue
		}
		if !isClientToolOutput(typ) {
			continue
		}
		call, ok := calls[id]
		if !ok || answered[id] || !matchesToolOutput(call, typ) {
			return nil, fmt.Errorf("orphan or mismatched replay tool result for call_id %q", id)
		}
		answered[id] = true
	}
	pending := make(map[string]json.RawMessage)
	for id, call := range calls {
		if !answered[id] {
			if private[id] {
				return nil, fmt.Errorf("web_search result missing for call_id %q", id)
			}
			pending[id] = call
		}
	}
	return pending, nil
}

func addUsage(raw json.RawMessage, totals map[string]int64) {
	usage, err := object(raw)
	if err != nil {
		return
	}
	for _, key := range []string{"input_tokens", "output_tokens", "total_tokens"} {
		var count int64
		if json.Unmarshal(usage[key], &count) == nil {
			totals[key] += count
		}
	}
}

func applyUsage(response map[string]json.RawMessage, totals map[string]int64) {
	if len(totals) == 0 {
		return
	}
	usage, err := object(response["usage"])
	if err != nil {
		usage = make(map[string]json.RawMessage)
	}
	for key, count := range totals {
		usage[key], _ = json.Marshal(count)
	}
	response["usage"], _ = json.Marshal(usage)
}

func (e *Engine) historyWithinLimit(history []json.RawMessage) bool {
	if len(history) > e.opts.MaxHistoryItems {
		return false
	}
	bytes := 0
	for _, item := range history {
		bytes += len(item)
		if bytes > e.opts.MaxHistoryBytes {
			return false
		}
	}
	return true
}

func publicFields(c conversation, previous string, response map[string]json.RawMessage) {
	if len(c.tools) > 0 {
		response["tools"] = c.tools
	}
	if c.choicePresent {
		response["tool_choice"] = c.toolChoice
	} else {
		delete(response, "tool_choice")
	}
	if len(c.model) > 0 {
		response["model"] = c.model
	}
	if previous != "" {
		response["previous_response_id"], _ = json.Marshal(previous)
	}
}

// reservedResponse belongs to one public request, including a pending-tool pause.
// A zero createdAt preserves upstream metadata for unobserved and compact calls.
type reservedResponse struct {
	id        string
	createdAt int64
}

func (e *Engine) finish(c conversation, previous string, response map[string]json.RawMessage, output []json.RawMessage, reserved reservedResponse) ([]byte, error) {
	if output == nil {
		output = make([]json.RawMessage, 0)
	}
	publicFields(c, previous, response)
	var err error
	id := reserved.id
	if id == "" {
		id, err = responseID()
		if err != nil {
			return nil, err
		}
	}
	response["id"], _ = json.Marshal(id)
	if reserved.createdAt != 0 {
		response["created_at"], _ = json.Marshal(reserved.createdAt)
		response["object"] = json.RawMessage(`"response"`)
	}
	response["output"], err = json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("encode output: %w", err)
	}
	result, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	c.response = result
	e.states.put(id, c)
	return result, nil
}

func (e *Engine) pause(c conversation, previous string, reserved reservedResponse) ([]byte, error) {
	response, err := object(c.response)
	if err != nil {
		return nil, fmt.Errorf("decode paused response: %w", err)
	}
	var output []json.RawMessage
	for _, item := range c.history {
		obj, errObj := object(item)
		if errObj != nil || !isToolCall(stringValue(obj["type"])) {
			continue
		}
		if _, pending := c.pending[stringValue(obj["call_id"])]; pending {
			output = append(output, item)
		}
	}
	return e.finish(c, previous, response, output, reserved)
}

func rewriteTools(req map[string]json.RawMessage) (bool, error) {
	if len(req["tools"]) == 0 {
		return false, nil
	}
	var tools []json.RawMessage
	if err := json.Unmarshal(req["tools"], &tools); err != nil {
		return false, fmt.Errorf("decode tools: %w", err)
	}
	searchEnabled := false
	for i, raw := range tools {
		tool, err := object(raw)
		if err != nil {
			return false, fmt.Errorf("decode tool: %w", err)
		}
		switch stringValue(tool["type"]) {
		case "web_search", "web_search_preview":
			if searchEnabled {
				return false, errors.New("multiple web_search tools are unsupported")
			}
			if filters := strings.TrimSpace(string(tool["filters"])); filters != "" && filters != "null" && filters != "{}" {
				return false, errors.New("web_search filters are unsupported by this search provider")
			}
			searchEnabled = true
			tools[i] = json.RawMessage(searchToolDefinition)
		case "function":
			if stringValue(tool["name"]) == searchFunction {
				return false, errors.New("reserved search function name is already declared")
			}
		}
	}
	if searchEnabled {
		choice := req["tool_choice"]
		if strings.HasPrefix(stringValue(choice), "web_search") {
			req["tool_choice"] = json.RawMessage(`{"type":"function","name":"` + searchFunction + `"}`)
		} else if len(choice) > 0 && choice[0] == '{' {
			obj, err := object(choice)
			if err != nil {
				return false, fmt.Errorf("decode tool_choice: %w", err)
			}
			if strings.HasPrefix(stringValue(obj["type"]), "web_search") {
				req["tool_choice"] = json.RawMessage(`{"type":"function","name":"` + searchFunction + `"}`)
			}
		}
	}
	req["tools"], _ = json.Marshal(tools)
	return searchEnabled, nil
}

const searchToolDefinition = `{"type":"function","name":"__deepseek_web_search","description":"Search the web, open a result page, or find text in a page. Use action search with query or queries, open_page with url, or find_in_page with url and pattern.","parameters":{"type":"object","properties":{"action":{"type":"string","enum":["search","open_page","find_in_page"]},"query":{"type":"string"},"queries":{"type":"array","items":{"type":"string"},"maxItems":4},"url":{"type":"string"},"pattern":{"type":"string"}},"required":["action"]}}`

type savedWebRecord struct {
	id     string
	record webRecord
}

func observeSearch(ctx context.Context, observer Observer, index int, item json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if observer != nil {
		if err := observer.Search(index, item); err != nil {
			return fmt.Errorf("observe search: %w", err)
		}
	}
	return ctx.Err()
}

func (e *Engine) execute(ctx context.Context, call json.RawMessage, index int, observer Observer) (json.RawMessage, json.RawMessage, json.RawMessage, error) {
	obj, _ := object(call)
	var argText string
	var args map[string]json.RawMessage
	var searchErr error
	if err := json.Unmarshal(obj["arguments"], &argText); err != nil {
		searchErr = errors.New("arguments must be a JSON object string")
	} else {
		args, searchErr = object([]byte(argText))
	}
	action := stringValue(args["action"])
	if action == "" {
		action = "search"
	}
	var queries []string
	if searchErr == nil && action == "search" {
		query := strings.TrimSpace(stringValue(args["query"]))
		if query != "" {
			queries = []string{query}
		} else if len(args["queries"]) > 0 {
			if err := json.Unmarshal(args["queries"], &queries); err != nil {
				searchErr = errors.New("queries must be an array of strings")
			}
		}
		if searchErr == nil && (len(queries) == 0 || len(queries) > 4) {
			searchErr = errors.New("search requires 1 to 4 queries")
		}
		for i, query := range queries {
			queries[i] = strings.TrimSpace(query)
			if queries[i] == "" {
				searchErr = errors.New("search queries cannot be empty")
			}
		}
	}
	publicAction := map[string]any{"type": action}
	for _, key := range []string{"query", "url", "pattern"} {
		if value := stringValue(args[key]); value != "" {
			publicAction[key] = value
		}
	}
	if action == "search" {
		publicAction["queries"] = queries
	}
	randomID, err := responseID()
	if err != nil {
		return nil, nil, nil, err
	}
	publicID := "ws_" + strings.TrimPrefix(randomID, "resp_")
	publicItem := map[string]any{"id": publicID, "type": "web_search_call", "status": "in_progress", "action": publicAction}
	public, err := json.Marshal(publicItem)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := observeSearch(ctx, observer, index, public); err != nil {
		return nil, nil, nil, err
	}
	var data json.RawMessage
	switch {
	case searchErr != nil:
		// Return argument validation errors to the model as failed tool results.
	case action == "search":
		data, searchErr = e.searchQueries(ctx, queries)
	case action == "open_page" || action == "find_in_page":
		url := strings.TrimSpace(stringValue(args["url"]))
		if url == "" {
			searchErr = errors.New("url is required")
		} else if action == "find_in_page" && strings.TrimSpace(stringValue(args["pattern"])) == "" {
			searchErr = errors.New("pattern is required")
		} else {
			data, searchErr = e.searcher.Fetch(ctx, url)
			if searchErr == nil && action == "find_in_page" {
				data, searchErr = findInPage(data, url, stringValue(args["pattern"]))
			}
		}
	default:
		searchErr = fmt.Errorf("unsupported web_search action %q", action)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if errors.Is(searchErr, context.Canceled) || errors.Is(searchErr, context.DeadlineExceeded) {
		return nil, nil, nil, searchErr
	}
	status := "completed"
	if searchErr == nil && len(data) > e.opts.MaxHistoryBytes/2 {
		searchErr = errors.New("search result limit exceeded")
	}
	if searchErr != nil || !json.Valid(data) {
		status = "failed"
		message := "search returned invalid JSON"
		if searchErr != nil {
			message = searchErr.Error()
		}
		data, _ = json.Marshal(map[string]any{"error": map[string]string{"type": "search_error", "message": message}})
	}
	if action == "search" {
		if sources := sourcesFromResult(data); len(sources) > 0 {
			publicAction["sources"] = sources
		}
	}
	hiddenCall, err := replaceCallID(call, publicID)
	if err != nil {
		return nil, nil, nil, err
	}
	toolOutput, err := json.Marshal(map[string]any{"type": "function_call_output", "call_id": publicID, "output": string(data)})
	if err != nil {
		return nil, nil, nil, err
	}
	publicItem["status"] = status
	public, err = json.Marshal(publicItem)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := observeSearch(ctx, observer, index, public); err != nil {
		return nil, nil, nil, err
	}
	return hiddenCall, toolOutput, public, nil
}

func (e *Engine) searchQueries(ctx context.Context, queries []string) (json.RawMessage, error) {
	if len(queries) == 1 {
		return e.searcher.Search(ctx, queries[0])
	}
	type searchResult struct {
		Query  string          `json:"query"`
		Result json.RawMessage `json:"result"`
	}
	searches := make([]searchResult, 0, len(queries))
	combined := make([]json.RawMessage, 0)
	for _, query := range queries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := e.searcher.Search(ctx, query)
		if err != nil {
			return nil, err
		}
		obj, err := object(result)
		if err != nil {
			return nil, errors.New("search result must be a JSON object")
		}
		var entries []json.RawMessage
		if err := json.Unmarshal(obj["results"], &entries); err != nil {
			return nil, errors.New("search result has no results array")
		}
		combined = append(combined, entries...)
		searches = append(searches, searchResult{Query: query, Result: result})
	}
	return json.Marshal(map[string]any{"searches": searches, "results": combined})
}

func findInPage(raw json.RawMessage, pageURL, pattern string) (json.RawMessage, error) {
	obj, err := object(raw)
	if err != nil {
		return nil, errors.New("fetched page is not a JSON object")
	}
	var content string
	if err := json.Unmarshal(obj["content"], &content); err != nil {
		return nil, errors.New("fetched page has no text content")
	}
	type match struct {
		LineNumber int    `json:"line_number"`
		Text       string `json:"text"`
	}
	matches := make([]match, 0)
	total := 0
	needle := strings.ToLower(pattern)
	for i, line := range strings.Split(content, "\n") {
		if !strings.Contains(strings.ToLower(line), needle) {
			continue
		}
		total++
		if len(matches) == 20 {
			continue
		}
		runes := []rune(strings.TrimSpace(line))
		if len(runes) > 240 {
			runes = runes[:240]
		}
		matches = append(matches, match{LineNumber: i + 1, Text: string(runes)})
	}
	return json.Marshal(map[string]any{"url": pageURL, "pattern": pattern, "matches": matches, "total_matches": total, "truncated": total > len(matches)})
}

func sourcesFromResult(raw json.RawMessage) []map[string]string {
	obj, err := object(raw)
	if err != nil {
		return nil
	}
	var results []json.RawMessage
	if err := json.Unmarshal(obj["results"], &results); err != nil {
		return nil
	}
	var sources []map[string]string
	for _, result := range results {
		item, err := object(result)
		if err == nil {
			if url := stringValue(item["url"]); url != "" {
				sources = append(sources, map[string]string{"type": "url", "url": url})
			}
		}
	}
	return sources
}

func object(raw []byte) (map[string]json.RawMessage, error) {
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, errors.New("expected JSON object")
	}
	return out, nil
}

func inputItems(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, err
		}
		item, _ := json.Marshal(map[string]string{"role": "user", "content": text})
		return []json.RawMessage{item}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}
	if items == nil {
		return nil, errors.New("input must be a string or array")
	}
	for _, item := range items {
		if _, err := object(item); err != nil {
			return nil, fmt.Errorf("input item: %w", err)
		}
	}
	return items, nil
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}
