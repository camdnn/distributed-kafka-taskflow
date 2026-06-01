package task

import "time"

// Alert data that gets based to the
// alert kafka topic
type Alert struct {
	Hex        string    `json:"hex"`
	Flight     string    `json:"flight,omitempty"`
	Squawk     string    `json:"squawk,omitempty"`
	Reason     string    `json:"reason"`
	Lat        *float64  `json:"lat,omitempty"`
	Lon        *float64  `json:"lon,omitempty"`
	DetectedAt time.Time `json:"detected_at"`
}

// Result data for sucessful Flight
// and gets stored in the results topic
type Result struct {
	Hex         string    `json:"hex"`
	Flight      string    `json:"flight,omitempty"`
	Lat         *float64  `json:"lat,omitempty"`
	Lon         *float64  `json:"lon,omitempty"`
	ProcessedAt time.Time `json:"processed_at"`
}

// DeadLetter is every tailed task that
// our program drops due to timeout
type DeadLetter struct {
	TaskID   string    `json:"task_id"`
	Hex      string    `json:"hex,omitempty"`
	Reason   string    `json:"reason"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failed_at"`
}
