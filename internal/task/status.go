// Package task this holds the enums for status codes internal/task/status.go
package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// TaskStatus defines enum for the status type
// using status as a string since tasks are monitored through a CLI
// easier to navigate when looking through the log

// TaskStatus using enum to encapsulate task to these options, ensure type safety
// ensures compile time safety
type TaskStatus string

const (
	StatusQueued       TaskStatus = "QUEUED"
	StatusRunning      TaskStatus = "RUNNING"
	StatusCompleted    TaskStatus = "COMPLETED"
	StatusRetrying     TaskStatus = "RETRYING"
	StatusDeadLettered TaskStatus = "DEADLETTERED"
)

// StatusEvent published to task-status topic on every state change
// this is what the minitor displays
// same as task but OccuredAt: specifies when the status transition happened
type StatusEvent struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	Attempt       int        `json:"attempt"`
	Status        TaskStatus `json:"status"`
	StatusMessage string     `json:"status_message,omitempty"`
	OccuredAt     time.Time  `json:"occurred_at"`
	RetryAt       *time.Time `json:"retry_at"`
}

func NewStatusEvent(t *Task, status TaskStatus, statusMessage string) *StatusEvent {
	statusEvent := new(StatusEvent)

	//init status Event
	statusEvent.ID = t.ID
	statusEvent.Type = t.Type
	statusEvent.Attempt = t.Attempts
	statusEvent.Status = status
	statusEvent.StatusMessage = statusMessage
	statusEvent.OccuredAt = time.Now()
	statusEvent.RetryAt = t.RetryAt

	return statusEvent

}

// ToJSON Serialize Status Event to JSON
func (s *StatusEvent) ToJSON() ([]byte, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("unable to serailize status event to json %w", err)
	}

	return data, nil
}

// StatusEventFromJSON unserializes status even json
func StatusEventFromJSON(data []byte) (*StatusEvent, error) {
	statusEvent := new(StatusEvent)

	err := json.Unmarshal(data, &statusEvent)
	if err != nil {
		return nil, fmt.Errorf("unable to unserialize status event from json %w", err)
	}

	return statusEvent, nil
}
