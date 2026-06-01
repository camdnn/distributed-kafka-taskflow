// Package task defines struct for task type
// internal/task/task.go
package task

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Task struct definitions
// ID: UUID generated at submission
// IdempotencyKey
// Type: the type of task to complete
// Payload: the data for that Task
// RetryAt: Used to find the next time to try this task at (exponential backoff by 2^n up to 60s)
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Attempt   int             `json:"attempt"`
	CreatedAt time.Time       `json:"created_at"`
	RetryAt   *time.Time      `json:"retry_at,omitempty"`
}

func NewTask(taskType string, payload json.RawMessage) *Task {
	task := new(Task)

	//init task variables
	task.ID = uuid.NewString()
	task.Type = taskType
	task.Payload = payload
	task.Attempt = 0
	task.CreatedAt = time.Now()
	task.RetryAt = nil

	return task
}

// ToJSON Serializes task into json
func (t *Task) ToJSON() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize task to json %w", err)
	}

	return b, nil
}

// FromJSON unpackages the json into a task
func FromJSON(data []byte) (*Task, error) {
	task := new(Task)
	err := json.Unmarshal(data, &task)
	if err != nil {
		return nil, fmt.Errorf("unable to unpackage json in FromJSON %w", err)
	}

	return task, nil

}

// Display prints a human-readable representation of the task to stdout.
// Useful for quick debugging or CLI output.
func (t *Task) Display() {
	fmt.Printf("Task ID: %s\n", t.ID)
	fmt.Printf("Type: %s\n", t.Type)
	fmt.Printf("Attempt: %d\n", t.Attempt)
	fmt.Printf("CreatedAt: %s\n", t.CreatedAt.Format(time.RFC3339))
	if t.RetryAt != nil {
		fmt.Printf("RetryAt: %s\n", t.RetryAt.Format(time.RFC3339))
	}
	if len(t.Payload) > 0 {
		fmt.Printf("Payload: %s\n", string(t.Payload))
	} else {
		fmt.Printf("Payload: <empty>\n")
	}
}
