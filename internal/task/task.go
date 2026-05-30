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
		return nil, fmt.Errorf("unable to serialize task to json %v", err)
	}

	return b, nil
}

// FromJSON unpackages the json into a task
func (t *Task) FromJSON(data []byte) (*Task, error) {
	task := new(Task)
	err := json.Unmarshal(data, &task)
	if err != nil {
		return nil, fmt.Errorf("unable to unpackage json in FromJSON %v", err)
	}

	return task, nil

}
