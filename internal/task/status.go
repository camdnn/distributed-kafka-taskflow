// Package task this holds the enums for status codes internal/task/status.go
package task

// TaskStatus defines enum for the status type
type TaskStatus string

const (
	StatusQueued       TaskStatus = "QUEUED"
	StatusRunning      TaskStatus = "RUNNING"
	StatusCompleted    TaskStatus = "COMPLETED"
	StatusRetrying     TaskStatus = "RETRYING"
	StatusDeadLettered TaskStatus = "DEADLETTERED"
)

type StatusEvent struct {
}
