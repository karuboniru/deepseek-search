package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// liveResponse owns the public stream; individual model rounds never own its
// lifecycle, sequence numbers, or output indexes.
type liveResponse struct {
	emit                  func([]byte) error
	response              map[string]json.RawMessage
	sequence, round, base int
	active, ended         bool
	output                []*liveItem
	items                 map[int]*liveItem
	err                   error
}

type liveItem struct {
	index                            int
	id, upstreamID, kind             string
	classified, private, added, done bool
	current                          json.RawMessage
	pending                          []map[string]json.RawMessage
	seen                             map[string]bool
	deltas                           map[string]string
	settled                          map[string]json.RawMessage
}

func newLiveResponse(emit func([]byte) error) *liveResponse { return &liveResponse{emit: emit} }

func liveObject(raw []byte) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return nil, errors.New("invalid Responses object")
	}
	return obj, nil
}

// Some host translators include an empty reasoning carrier in item.done but
// omit it in response.completed. Normalize that optional field in both shapes.
func normalizeLiveItem(item map[string]json.RawMessage) {
	if liveString(item["type"]) == "reasoning" {
		raw, ok := item["encrypted_content"]
		if ok && (bytes.Equal(bytes.TrimSpace(raw), []byte(`""`)) || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			delete(item, "encrypted_content")
		}
	}
}

func liveString(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
func liveJSON(value any) json.RawMessage    { raw, _ := json.Marshal(value); return raw }

func (l *liveResponse) send(kind string, fields map[string]json.RawMessage) error {
	if l.err != nil {
		return l.err
	}
	l.sequence++
	fields["type"] = liveJSON(kind)
	fields["sequence_number"] = liveJSON(l.sequence)
	raw, err := json.Marshal(fields)
	if err == nil {
		err = l.emit([]byte(fmt.Sprintf("event: %s\ndata: %s\n\n", kind, raw)))
	}
	if err != nil {
		l.err = err
	}
	return err
}
func (l *liveResponse) Start(raw json.RawMessage) error {
	if l.response != nil || l.ended {
		return errors.New("public response already started")
	}
	response, err := liveObject(raw)
	if err != nil {
		return err
	}
	if liveString(response["id"]) == "" {
		return errors.New("public response ID is required")
	}
	response["status"] = liveJSON("in_progress")
	response["output"] = liveJSON([]any{})
	if _, ok := response["usage"]; ok {
		response["usage"] = json.RawMessage("null")
	}
	l.response = response
	if err = l.send("response.created", map[string]json.RawMessage{"response": liveJSON(response)}); err != nil {
		return err
	}
	return l.send("response.in_progress", map[string]json.RawMessage{"response": liveJSON(response)})
}
func (l *liveResponse) BeginRound() error {
	if l.err != nil {
		return l.err
	}
	if l.response == nil || l.ended || l.active {
		return errors.New("invalid model round lifecycle")
	}
	l.round++
	l.base = len(l.output)
	l.items = make(map[int]*liveItem)
	l.active = true
	return nil
}
func (l *liveResponse) item(index int) (*liveItem, error) {
	if index < 0 || index > 65535 {
		return nil, errors.New("invalid Responses output index")
	}
	if item := l.items[index]; item != nil {
		return item, nil
	}
	global := l.base + index
	for len(l.output) <= global {
		l.output = append(l.output, nil)
	}
	item := &liveItem{index: global, id: fmt.Sprintf("item_%s_%d_%d", liveString(l.response["id"]), l.round, index), seen: make(map[string]bool), deltas: make(map[string]string)}
	l.items[index] = item
	l.output[global] = item
	return item, nil
}

// Only these item events may cross the private tool boundary. In particular,
// provider diagnostics and unrecognized extension events cannot expose calls.
func liveItemEvent(kind string) bool {
	switch kind {
	case "response.output_item.added", "response.output_item.done",
		"response.content_part.added", "response.content_part.done",
		"response.output_text.delta", "response.output_text.done", "response.output_text.annotation.added",
		"response.refusal.delta", "response.refusal.done",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
		"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.reasoning_text.delta", "response.reasoning_text.done",
		"response.function_call_arguments.delta", "response.function_call_arguments.done",
		"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done",
		"response.web_search_call.in_progress", "response.web_search_call.searching", "response.web_search_call.completed":
		return true
	}
	return false
}
func liveEventIndex(event map[string]json.RawMessage) (int, error) {
	var index int
	raw, ok := event["output_index"]
	if !ok || json.Unmarshal(raw, &index) != nil || index < 0 {
		return 0, errors.New("invalid Responses event output index")
	}
	return index, nil
}
func (l *liveResponse) classify(item *liveItem, raw json.RawMessage) error {
	obj, err := liveObject(raw)
	if err != nil {
		return err
	}
	kind := liveString(obj["type"])
	id := liveString(obj["id"])
	if id != "" {
		if item.upstreamID != "" && item.upstreamID != id {
			return errors.New("inconsistent Responses item ID")
		}
		item.upstreamID = id
	}
	if item.kind != "" && kind != "" && item.kind != kind {
		return errors.New("inconsistent Responses item type")
	}
	if kind != "" {
		item.kind = kind
	}
	if kind == "" || kind == "function_call" && liveString(obj["name"]) == "" {
		return nil
	}
	private := kind == "function_call" && liveString(obj["name"]) == "__deepseek_web_search"
	if item.classified && item.private != private {
		return errors.New("inconsistent Responses function name")
	}
	item.classified = true
	item.private = private
	return nil
}
func (l *liveResponse) Event(raw json.RawMessage) error {
	if l.err != nil {
		return l.err
	}
	if !l.active || l.ended {
		return errors.New("Responses event outside model round")
	}
	event, err := liveObject(raw)
	if err != nil {
		return err
	}
	kind := liveString(event["type"])
	if kind == "" {
		return errors.New("Responses event type is required")
	}
	if kind == "response.failed" || kind == "response.incomplete" || kind == "error" {
		return errors.New("host model response failed")
	}
	if !liveItemEvent(kind) {
		return nil
	}
	index, err := liveEventIndex(event)
	if err != nil {
		return err
	}
	item, err := l.item(index)
	if err != nil {
		return err
	}
	if kind == "response.output_item.added" || kind == "response.output_item.done" {
		if err = l.classify(item, event["item"]); err != nil {
			return err
		}
	} else if item.kind == "" {
		return errors.New("Responses delta precedes output item")
	} else if id := liveString(event["item_id"]); id != "" {
		if item.upstreamID != "" && item.upstreamID != id {
			return errors.New("inconsistent Responses event item ID")
		}
		item.upstreamID = id
	}
	if !item.classified {
		item.pending = append(item.pending, event)
		return nil
	}
	if item.private {
		item.pending = nil
		return nil
	}
	for _, pending := range item.pending {
		if err = l.deliver(item, pending); err != nil {
			return err
		}
	}
	item.pending = nil
	return l.deliver(item, event)
}
func (l *liveResponse) deliver(item *liveItem, event map[string]json.RawMessage) error {
	if liveString(event["type"]) == "response.output_item.done" {
		obj, err := liveObject(event["item"])
		if err != nil {
			return err
		}
		normalizeLiveItem(obj)
		obj["id"] = liveJSON(item.id)
		return l.reconcile(item, liveJSON(obj))
	}
	return l.forward(item, event)
}
func liveEventKey(event map[string]json.RawMessage) string {
	key := liveString(event["type"])
	for _, field := range []string{"content_index", "summary_index", "annotation_index"} {
		if value, ok := event[field]; ok {
			key += "/" + field + "=" + string(value)
		}
	}
	return key
}
func liveDoneValue(event map[string]json.RawMessage) json.RawMessage {
	for _, field := range []string{"part", "text", "refusal", "arguments", "input", "item"} {
		if raw, ok := event[field]; ok {
			return raw
		}
	}
	return nil
}
func liveTextDoneField(kind string) string {
	switch kind {
	case "response.output_text.done", "response.reasoning_text.done", "response.reasoning_summary_text.done":
		return "text"
	case "response.refusal.done":
		return "refusal"
	case "response.function_call_arguments.done":
		return "arguments"
	case "response.custom_tool_call_input.done":
		return "input"
	}
	return ""
}
func (l *liveResponse) forward(item *liveItem, event map[string]json.RawMessage) error {
	kind := liveString(event["type"])
	if item.done {
		return errors.New("Responses event follows completed output item")
	}
	if strings.HasSuffix(kind, ".done") {
		key := liveEventKey(event)
		if item.settled == nil {
			item.settled = map[string]json.RawMessage{}
		}
		value := liveDoneValue(event)
		if old, ok := item.settled[key]; ok {
			if !liveEqual(old, value) {
				return errors.New("inconsistent Responses done value")
			}
			return nil
		}
		if field := liveTextDoneField(kind); field != "" {
			var text string
			if json.Unmarshal(event[field], &text) != nil {
				return errors.New("invalid Responses done text")
			}
			deltaKey := strings.Replace(key, ".done", ".delta", 1)
			previous := item.deltas[deltaKey]
			if !strings.HasPrefix(text, previous) {
				return errors.New("Responses done text differs from streamed deltas")
			}
			if suffix := strings.TrimPrefix(text, previous); suffix != "" {
				delta := map[string]json.RawMessage{"type": liveJSON(strings.TrimSuffix(kind, ".done") + ".delta"), "delta": liveJSON(suffix)}
				for _, index := range []string{"content_index", "summary_index"} {
					if raw, ok := event[index]; ok {
						delta[index] = raw
					}
				}
				if err := l.forward(item, delta); err != nil {
					return err
				}
			}
		}
		if kind == "response.content_part.done" || kind == "response.reasoning_summary_part.done" {
			part, errPart := liveObject(event["part"])
			if errPart != nil {
				return errPart
			}
			var doneKind, field string
			switch liveString(part["type"]) {
			case "output_text":
				doneKind, field = "response.output_text.done", "text"
			case "refusal":
				doneKind, field = "response.refusal.done", "refusal"
			case "summary_text":
				doneKind, field = "response.reasoning_summary_text.done", "text"
			case "reasoning_text":
				doneKind, field = "response.reasoning_text.done", "text"
			}
			if doneKind != "" {
				done := map[string]json.RawMessage{"type": liveJSON(doneKind), field: part[field]}
				for _, index := range []string{"content_index", "summary_index"} {
					if raw, ok := event[index]; ok {
						done[index] = raw
					}
				}
				if err := l.forward(item, done); err != nil {
					return err
				}
			}
		}
		item.settled[key] = value
	}
	fields := make(map[string]json.RawMessage)
	// Copy the protocol fields deliberately rather than forwarding provider fields.
	for _, key := range []string{"content_index", "summary_index", "annotation_index", "part", "delta", "text", "refusal", "arguments", "input", "annotation", "logprobs"} {
		if value, ok := event[key]; ok {
			fields[key] = value
		}
	}
	fields["output_index"] = liveJSON(item.index)
	if raw, ok := event["item"]; ok {
		obj, err := liveObject(raw)
		if err != nil {
			return err
		}
		normalizeLiveItem(obj)
		obj["id"] = liveJSON(item.id)
		fields["item"] = liveJSON(obj)
		item.current = fields["item"]
	} else {
		fields["item_id"] = liveJSON(item.id)
	}
	key := liveEventKey(event)
	if strings.HasSuffix(kind, ".delta") {
		var delta string
		if err := json.Unmarshal(event["delta"], &delta); err != nil {
			return errors.New("invalid Responses delta")
		}
		item.deltas[key] += delta
	}
	item.seen[key] = true
	if kind == "response.output_item.added" {
		if item.added {
			return errors.New("duplicate Responses output item")
		}
		item.added = true
	}
	if kind == "response.output_item.done" {
		item.done = true
	}
	if err := l.updatePartial(item, event); err != nil {
		return err
	}
	return l.send(kind, fields)
}

// Keep only the current public item, not another copy of all emitted events.
func (l *liveResponse) updatePartial(item *liveItem, event map[string]json.RawMessage) error {
	kind := liveString(event["type"])
	if kind == "response.output_item.added" || kind == "response.output_item.done" {
		return nil
	}
	if len(item.current) == 0 {
		return errors.New("Responses delta precedes output item")
	}
	obj, err := liveObject(item.current)
	if err != nil {
		return err
	}
	if strings.HasPrefix(kind, "response.function_call_arguments.") || strings.HasPrefix(kind, "response.custom_tool_call_input.") {
		field := "arguments"
		if strings.Contains(kind, "custom_tool_call_input") {
			field = "input"
		}
		if strings.HasSuffix(kind, ".delta") {
			obj[field] = liveJSON(liveString(obj[field]) + liveString(event["delta"]))
		} else {
			obj[field] = event[field]
		}
	} else {
		arrayField, indexField := "content", "content_index"
		if strings.Contains(kind, "reasoning_summary") {
			arrayField, indexField = "summary", "summary_index"
		}
		indexRaw, ok := event[indexField]
		if !ok {
			return nil
		}
		var index int
		if json.Unmarshal(indexRaw, &index) != nil || index < 0 || index > 65535 {
			return errors.New("invalid Responses content index")
		}
		var parts []map[string]json.RawMessage
		if raw := obj[arrayField]; len(raw) > 0 && string(raw) != "null" {
			if json.Unmarshal(raw, &parts) != nil {
				return errors.New("invalid Responses content")
			}
		}
		for len(parts) <= index {
			parts = append(parts, map[string]json.RawMessage{})
		}
		if part, ok := event["part"]; ok {
			parts[index], err = liveObject(part)
			if err != nil {
				return err
			}
		} else if kind == "response.output_text.annotation.added" {
			var annotationIndex int
			if json.Unmarshal(event["annotation_index"], &annotationIndex) != nil || annotationIndex < 0 || annotationIndex > 65535 {
				return errors.New("invalid Responses annotation index")
			}
			var annotations []json.RawMessage
			_ = json.Unmarshal(parts[index]["annotations"], &annotations)
			for len(annotations) <= annotationIndex {
				annotations = append(annotations, json.RawMessage("null"))
			}
			annotations[annotationIndex] = event["annotation"]
			parts[index]["annotations"] = liveJSON(annotations)
		} else if strings.HasSuffix(kind, ".delta") || strings.HasSuffix(kind, ".done") {
			field, partType := "text", "output_text"
			if strings.Contains(kind, "refusal") {
				field, partType = "refusal", "refusal"
			} else if strings.Contains(kind, "reasoning_summary") {
				partType = "summary_text"
			} else if strings.Contains(kind, "reasoning_text") {
				partType = "reasoning_text"
			}
			if len(parts[index]["type"]) == 0 {
				parts[index]["type"] = liveJSON(partType)
			}
			if strings.HasSuffix(kind, ".delta") {
				parts[index][field] = liveJSON(liveString(parts[index][field]) + liveString(event["delta"]))
			} else if value, ok := event[field]; ok {
				parts[index][field] = value
			}
		}
		obj[arrayField] = liveJSON(parts)
	}
	item.current = liveJSON(obj)
	return nil
}

func (l *liveResponse) EndRound(raw []byte) ([]byte, error) {
	if l.err != nil {
		return nil, l.err
	}
	if !l.active || l.ended {
		return nil, errors.New("no active model round")
	}
	response, err := liveObject(raw)
	if err != nil {
		return nil, err
	}
	var output []json.RawMessage
	if liveString(response["status"]) != "completed" || json.Unmarshal(response["output"], &output) != nil || output == nil {
		return nil, errors.New("invalid completed model response")
	}
	for index := range l.items {
		if index >= len(output) {
			return nil, errors.New("Responses event output index exceeds completed output")
		}
	}
	for index, rawItem := range output {
		item, errItem := l.item(index)
		if errItem != nil {
			return nil, errItem
		}
		if errItem = l.classify(item, rawItem); errItem != nil {
			return nil, errItem
		}
		if !item.classified {
			return nil, errors.New("unclassified completed Responses item")
		}
		obj, _ := liveObject(rawItem)
		normalizeLiveItem(obj)
		obj["id"] = liveJSON(item.id)
		output[index] = liveJSON(obj)
		if item.private {
			item.pending = nil
			continue
		}
		for _, pending := range item.pending {
			if errItem = l.deliver(item, pending); errItem != nil {
				return nil, errItem
			}
		}
		item.pending = nil
		if errItem = l.reconcile(item, output[index]); errItem != nil {
			return nil, errItem
		}
	}
	response["output"] = liveJSON(output)
	l.active = false
	l.items = nil
	return json.Marshal(response)
}

// Reuse the completion serializer as a source of missing events. Delta tracking
// means a final snapshot only supplies text which has not already been streamed.
func (l *liveResponse) reconcile(item *liveItem, raw json.RawMessage) error {
	if item.done {
		if !liveEqual(item.current, raw) {
			return errors.New("completed Responses item differs from streamed item")
		}
		return nil
	}
	obj, err := liveObject(raw)
	if err != nil {
		return err
	}
	single := liveJSON(map[string]any{"status": "completed", "output": []json.RawMessage{raw}})
	fallback, err := responseSSE(single)
	if err != nil {
		return err
	}
	var events []map[string]json.RawMessage
	for _, frame := range bytes.Split(fallback, []byte("\n\n")) {
		for _, line := range bytes.Split(frame, []byte("\n")) {
			if bytes.HasPrefix(line, []byte("data: {")) {
				event, errEvent := liveObject(line[6:])
				if errEvent != nil {
					return errEvent
				}
				if liveItemEvent(liveString(event["type"])) {
					events = append(events, event)
				}
			}
		}
	}
	// The shared serializer handles text, reasoning, and function calls. These
	// additional public item shapes use the same delta/done contract.
	if liveString(obj["type"]) == "custom_tool_call" {
		if len(events) > 0 {
			added, _ := liveObject(events[0]["item"])
			added["input"] = liveJSON("")
			events[0]["item"] = liveJSON(added)
		}
		extra := []map[string]json.RawMessage{}
		if input := liveString(obj["input"]); input != "" {
			extra = append(extra, map[string]json.RawMessage{"type": liveJSON("response.custom_tool_call_input.delta"), "delta": liveJSON(input)})
		}
		extra = append(extra, map[string]json.RawMessage{"type": liveJSON("response.custom_tool_call_input.done"), "input": obj["input"]})
		events = append(events[:len(events)-1], append(extra, events[len(events)-1])...)
	}
	if liveString(obj["type"]) == "message" {
		var content []map[string]json.RawMessage
		if err = json.Unmarshal(obj["content"], &content); err != nil {
			return errors.New("invalid completed message content")
		}
		var expanded []map[string]json.RawMessage
		for _, event := range events {
			kind := liveString(event["type"])
			if kind == "response.content_part.added" {
				part, errPart := liveObject(event["part"])
				if errPart != nil {
					return errPart
				}
				if liveString(part["type"]) == "refusal" {
					part["refusal"] = liveJSON("")
				}
				if _, ok := part["annotations"]; ok {
					part["annotations"] = liveJSON([]any{})
				}
				event["part"] = liveJSON(part)
			}
			if kind == "response.content_part.done" {
				var index int
				_ = json.Unmarshal(event["content_index"], &index)
				if index < 0 || index >= len(content) {
					return errors.New("invalid completed message content index")
				}
				part := content[index]
				if liveString(part["type"]) == "refusal" {
					if text := liveString(part["refusal"]); text != "" {
						expanded = append(expanded, map[string]json.RawMessage{"type": liveJSON("response.refusal.delta"), "content_index": liveJSON(index), "delta": liveJSON(text)})
					}
					expanded = append(expanded, map[string]json.RawMessage{"type": liveJSON("response.refusal.done"), "content_index": liveJSON(index), "refusal": part["refusal"]})
				}
				var annotations []json.RawMessage
				_ = json.Unmarshal(part["annotations"], &annotations)
				for ai, annotation := range annotations {
					expanded = append(expanded, map[string]json.RawMessage{"type": liveJSON("response.output_text.annotation.added"), "content_index": liveJSON(index), "annotation_index": liveJSON(ai), "annotation": annotation})
				}
			}
			expanded = append(expanded, event)
		}
		events = expanded
	}
	for _, event := range events {
		kind := liveString(event["type"])
		key := liveEventKey(event)
		if strings.HasSuffix(kind, ".delta") {
			if item.seen[strings.Replace(key, ".delta", ".done", 1)] {
				continue
			}
			text := liveString(event["delta"])
			already := item.deltas[key]
			if !strings.HasPrefix(text, already) {
				return errors.New("completed Responses text differs from streamed deltas")
			}
			event["delta"] = liveJSON(strings.TrimPrefix(text, already))
			if text == already {
				continue
			}
		} else if item.seen[key] {
			if previous, ok := item.settled[key]; ok && !liveEqual(previous, liveDoneValue(event)) {
				return errors.New("completed Responses done value differs from stream")
			}
			continue
		}
		if err = l.forward(item, event); err != nil {
			return err
		}
	}
	return nil
}
func liveEqual(a, b []byte) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && bytes.Equal(liveJSON(av), liveJSON(bv))
}

func (l *liveResponse) Search(index int, raw json.RawMessage) error {
	if l.err != nil {
		return l.err
	}
	if l.response == nil || l.ended || index < 0 || index > len(l.output) {
		return errors.New("invalid public search index")
	}
	obj, err := liveObject(raw)
	if err != nil {
		return err
	}
	if liveString(obj["type"]) != "web_search_call" || liveString(obj["id"]) == "" {
		return errors.New("invalid public search item")
	}
	var item *liveItem
	if index < len(l.output) {
		item = l.output[index]
	}
	if item == nil {
		item = &liveItem{index: index, seen: map[string]bool{}, deltas: map[string]string{}}
		if index == len(l.output) {
			l.output = append(l.output, item)
		} else {
			l.output[index] = item
		}
	}
	if item.added && item.id != liveString(obj["id"]) {
		return errors.New("public search ID changed")
	}
	item.id = liveString(obj["id"])
	item.classified = true
	item.private = false
	item.kind = "web_search_call"
	status := liveString(obj["status"])
	if status != "in_progress" && status != "completed" && status != "failed" {
		return errors.New("invalid public search status")
	}
	if !item.added {
		added := make(map[string]json.RawMessage, len(obj))
		for k, v := range obj {
			added[k] = v
		}
		added["status"] = liveJSON("in_progress")
		if err = l.forward(item, map[string]json.RawMessage{"type": liveJSON("response.output_item.added"), "item": liveJSON(added)}); err != nil {
			return err
		}
		if err = l.send("response.web_search_call.in_progress", map[string]json.RawMessage{"output_index": liveJSON(index), "item_id": obj["id"]}); err != nil {
			return err
		}
		if err = l.send("response.web_search_call.searching", map[string]json.RawMessage{"output_index": liveJSON(index), "item_id": obj["id"]}); err != nil {
			return err
		}
	}
	if status == "in_progress" {
		item.current = raw
		return nil
	}
	if item.done {
		if liveEqual(item.current, raw) {
			return nil
		}
		return errors.New("public search completed twice")
	}
	if status == "completed" {
		if err = l.send("response.web_search_call.completed", map[string]json.RawMessage{"output_index": liveJSON(index), "item_id": obj["id"]}); err != nil {
			return err
		}
	}
	return l.forward(item, map[string]json.RawMessage{"type": liveJSON("response.output_item.done"), "item": raw})
}
func (l *liveResponse) Complete(raw []byte) error {
	if l.err != nil {
		return l.err
	}
	if l.ended {
		return nil
	}
	if l.response == nil || l.active {
		return errors.New("invalid public completion lifecycle")
	}
	response, err := liveObject(raw)
	if err != nil {
		return err
	}
	if liveString(response["id"]) != liveString(l.response["id"]) || liveString(response["status"]) != "completed" {
		return errors.New("invalid public completed response")
	}
	var output []json.RawMessage
	if json.Unmarshal(response["output"], &output) != nil || output == nil || len(output) < len(l.output) {
		return errors.New("invalid public completed output")
	}
	for index, rawItem := range output {
		if index == len(l.output) {
			l.output = append(l.output, nil)
		}
		item := l.output[index]
		if item == nil {
			obj, errItem := liveObject(rawItem)
			if errItem != nil {
				return errItem
			}
			if liveString(obj["type"]) == "function_call" && liveString(obj["name"]) == "__deepseek_web_search" {
				return errors.New("private tool in public output")
			}
			item = &liveItem{index: index, id: liveString(obj["id"]), classified: true, seen: map[string]bool{}, deltas: map[string]string{}}
			l.output[index] = item
		}
		obj, errItem := liveObject(rawItem)
		if errItem != nil {
			return errItem
		}
		if item.private || liveString(obj["type"]) == "function_call" && liveString(obj["name"]) == "__deepseek_web_search" {
			return errors.New("private tool in public output")
		}
		if item.id != liveString(obj["id"]) {
			return errors.New("public completed item ID differs from streamed item")
		}
		if errItem = l.reconcile(item, rawItem); errItem != nil {
			return errItem
		}
	}
	l.ended = true
	if err = l.send("response.completed", map[string]json.RawMessage{"response": liveJSON(response)}); err != nil {
		return err
	}
	if err = l.emit([]byte("data: [DONE]\n\n")); err != nil {
		l.err = err
	}
	return err
}
func (l *liveResponse) Fail(cause error) error {
	if l.ended {
		return l.err
	}
	l.ended = true
	if l.err != nil {
		return l.err
	}
	if l.response == nil {
		return errors.New("public response has not started")
	}
	response := make(map[string]json.RawMessage, len(l.response))
	for k, v := range l.response {
		response[k] = v
	}
	output := []json.RawMessage{}
	for _, item := range l.output {
		if item != nil && item.added && !item.private && len(item.current) > 0 {
			output = append(output, item.current)
		}
	}
	response["status"] = liveJSON("failed")
	response["output"] = liveJSON(output)
	message := "stream orchestration failed"
	_ = cause // Provider errors may contain private arguments or credentials.
	response["error"] = liveJSON(map[string]string{"code": "server_error", "message": message})
	return l.send("response.failed", map[string]json.RawMessage{"response": liveJSON(response)})
}
