package task

import (
	"encoding/json"
	"testing"
)

func TestTaskRoundTrip(t *testing.T) {
	orig := NewTask("email", json.RawMessage(`{"to":"cam@gmail.com"}`))
	data, errT := orig.ToJSON()
	if errT != nil {
		t.Fatalf("ToJSON failed: %v", errT)
	}

	got, errF := FromJSON(data)
	if errF != nil {
		t.Fatalf("FromJSON failed: %v", errF)
	}

	if got.ID != orig.ID {
		t.Errorf("ID mismatch, got: %q, want :%q", got.ID, orig.ID)
	}

	if got.Type != orig.Type {
		t.Errorf("Type mismatch, got: %q, want :%q", got.Type, orig.Type)
	}

	if got.Attempt != orig.Attempt {
		t.Errorf("Attempt mismatch, got: %d, want :%d", got.Attempt, orig.Attempt)
	}

	if !got.CreatedAt.Equal(orig.CreatedAt) {
		t.Errorf("CreatedAt mismatch, got: %v, want :%v", got.CreatedAt, orig.CreatedAt)
	}

	if string(got.Payload) != string(orig.Payload) {
		t.Errorf("Payload mismatch, got: %q, want :%q", got.Payload, orig.Payload)
	}

}
