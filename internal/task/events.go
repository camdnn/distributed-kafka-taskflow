package task

import "time"

// Alert data that gets based to the
// alert kafka topic
type Alert struct {
	Hex        string
	Flight     string
	Squawk     string
	Reason     string
	Lat        *float64
	Lon        *float64
	DetectedAt time.Time
}

// Result data for sucessful Flight
// and gets stored in the results topic
type Result struct {
	Hex         string
	Flight      string
	Lat         *float64
	Lon         *float64
	ProcessedAt time.Time
}

// DeadLetter is every tailed task that
// our program drops due to timeout
type DeadLetter struct {
	TaskID   string
	Hex      string
	Reason   string
	Attempts int
	FailedAt time.Time
}
