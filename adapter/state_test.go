package adapter

import (
	"encoding/json"
	"testing"
)

func TestStateStoreEvictsOldestAndCopiesHistory(t *testing.T) {
	store := newStateStore(1, 1)
	store.put("first", conversation{tenant: "a", history: []json.RawMessage{json.RawMessage(`{"role":"user","content":"first"}`)}})
	copy, ok := store.get("a", "first")
	if !ok {
		t.Fatal("first state missing")
	}
	copy.history[0] = []byte(`{"changed":true}`)
	stored, _ := store.get("a", "first")
	if string(stored.history[0]) != `{"role":"user","content":"first"}` {
		t.Fatal("state slice was not copied")
	}
	store.put("second", conversation{tenant: "a"})
	if _, ok := store.get("a", "first"); ok {
		t.Fatal("oldest state was not evicted")
	}
	if _, ok := store.get("b", "second"); ok {
		t.Fatal("cross-tenant state access")
	}
}
