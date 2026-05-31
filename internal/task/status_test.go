package task

import (
	"encoding/json"
	"testing"
)

func TestStatusRoundTrip(t *testing.T) {
	task := NewTask("email", json.RawMessage(`{"to":"cam@gmail.com"}`))
	status := StatusQueued
	orig := NewStatusEvent(task, status, "message")
	data, errT := orig.ToJSON()
	if errT != nil {
		t.Fatalf("ToJSON failed: %v", errT)
	}

	got, errF := StatusEventFromJSON(data)
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

	if got.Status != orig.Status {
		t.Errorf("Status mismatch, got: %v, want :%v", got.Status, orig.Status)
	}

	if got.StatusMessage != orig.StatusMessage {
		t.Errorf("StatusMessage mismatch, got: %q, want :%q", got.StatusMessage, orig.StatusMessage)
	}

	if !got.OccuredAt.Equal(orig.OccuredAt) {
		t.Errorf("OccuredAt mismatch, got: %v, want :%v", got.OccuredAt, orig.OccuredAt)
	}
}
