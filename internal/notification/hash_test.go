package notification

import (
	"encoding/json"
	"testing"
)

func TestRequestHashCanonicalizesJSONObjectOrder(t *testing.T) {
	a, err := RequestHash("inventory", json.RawMessage(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatalf("hash first payload: %v", err)
	}
	b, err := RequestHash("inventory", json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatalf("hash second payload: %v", err)
	}
	if a != b {
		t.Fatalf("equivalent JSON objects produced different hashes: %x != %x", a, b)
	}
}

func TestRequestHashIncludesDestination(t *testing.T) {
	payload := json.RawMessage(`{"a":1}`)
	a, err := RequestHash("inventory", payload)
	if err != nil {
		t.Fatalf("hash inventory payload: %v", err)
	}
	b, err := RequestHash("crm", payload)
	if err != nil {
		t.Fatalf("hash crm payload: %v", err)
	}
	if a == b {
		t.Fatal("different destinations produced the same request hash")
	}
}

func TestRequestHashRejectsTrailingJSON(t *testing.T) {
	_, err := RequestHash("inventory", json.RawMessage(`{} {}`))
	if err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
}
