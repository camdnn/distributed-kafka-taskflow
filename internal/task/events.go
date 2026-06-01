package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// Alert data that gets based to the
// alert kafka topic
type Alert struct {
	Hex        string    `json:"hex"`
	Flight     string    `json:"flight"`
	Squawk     string    `json:"squawk"`
	Reason     string    `json:"reason"`
	Lat        *float64  `json:"lat"`
	Lon        *float64  `json:"lon"`
	DetectedAt time.Time `json:"detected_at"`
}

// Result data for sucessful Flight
// and gets stored in the results topic
type Result struct {
	Hex         string    `json:"hex"`
	Flight      string    `json:"flight"`
	Lat         *float64  `json:"lat"`
	Lon         *float64  `json:"lon"`
	ProcessedAt time.Time `json:"processed_at"`
}

// DeadLetter is every tailed task that
// our program drops due to timeout
type DeadLetter struct {
	TaskID   string    `json:"task_id"`
	Hex      string    `json:"hex"`
	Reason   string    `json:"reason"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failed_at"`
}

// NewAlert Build a new alert struct for alert topic
func NewAlert(a *Aircraft) *Alert {
	squawk := ""
	if a.Squawk != nil {
		squawk = *a.Squawk
	}

	return &Alert{
		Hex:        a.Hex,
		Flight:     a.Flight,
		Squawk:     squawk,
		Reason:     squawkReason(squawk),
		Lat:        a.Lat,
		Lon:        a.Lon,
		DetectedAt: a.ObservedAt,
	}
}

// decodes the squawk code and returns a string
func squawkReason(squawk string) string {

	switch squawk {
	case "7500":
		return "Unlawful Interference / Hijacking"
	case "7600":
		return "Radio Failure / Lost Comms"
	case "7700":
		return "General Emergency: Critical Distress"
	default:
		return "Unrecognizeable alert squawk code"
	}

}

// NewResult builds a new resukt struct for sucessful
// aircrafts
func NewResult(a *Aircraft) *Result {
	return &Result{
		Hex:         a.Hex,
		Flight:      a.Flight,
		Lat:         a.Lat,
		Lon:         a.Lon,
		ProcessedAt: time.Now(),
	}
}

// NewDeadLetter builds a deadletter struct for failed tasks
func NewDeadLetter(taskID string, hex string, reason string, attempts int) *DeadLetter {
	return &DeadLetter{
		TaskID:   taskID,
		Hex:      hex,
		Reason:   reason,
		Attempts: attempts,
		FailedAt: time.Now(),
	}
}

// Serialize Functions --------------------

// ToJSON serializes structs to json -- used for publish function
func (d *DeadLetter) ToJSON() ([]byte, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize deadletter to json: %w", err)
	}
	return b, nil
}

func (r *Result) ToJSON() ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize deadletter to json: %w", err)
	}
	return b, nil
}

func (a *Alert) ToJSON() ([]byte, error) {
	b, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize deadletter to json: %w", err)
	}
	return b, nil
}
