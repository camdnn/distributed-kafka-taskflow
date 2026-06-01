package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// Aircraft struct for aircraft data
type Aircraft struct {
	Hex         string          `json:"hex"`
	Flight      string          `json:"flight,omitempty"` // often absent; "" is fine as missing
	Lat         *float64        `json:"lat,omitempty"`
	Lon         *float64        `json:"lon,omitempty"`
	AltBaro     json.RawMessage `json:"alt_baro,omitempty"` // int OR the string "ground"
	GroundSpeed *float64        `json:"gs,omitempty"`
	Squawk      *string         `json:"squawk,omitempty"`
	Type        string          `json:"t,omitempty"`    // aircraft type
	Seen        *float64        `json:"seen,omitempty"` // seconds since last msg (from API)
	ObservedAt  time.Time       `json:"observed_at"`    // ingester sets = now - seen
}

// AircraftFromJSON decodes payload bytes into an Aircraft.
func AircraftFromJSON(data []byte) (*Aircraft, error) {
	a := new(Aircraft)
	err := json.Unmarshal(data, a)
	if err != nil {
		return nil, fmt.Errorf("unable to decode aircraft data from json: %w", err)
	}

	return a, nil
}

// ToJSON serializes the Aircraft
func (a *Aircraft) ToJSON() ([]byte, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("unable to encode aircraft data to json: %w", err)
	}

	return data, nil
}
