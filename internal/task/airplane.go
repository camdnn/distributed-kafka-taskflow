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

// Validate validates the data for each aircraft
func (a *Aircraft) Validate() error {
	if a.Hex == "" {
		return fmt.Errorf("mising hex")
	}

	if a.Lat == nil {
		return fmt.Errorf("missing latitude")
	}

	if *a.Lat < -90 || *a.Lat > 90 {
		return fmt.Errorf("latitude out of range: %v", *a.Lat)
	}

	if a.Lon == nil {
		return fmt.Errorf("missing longitude")
	}

	if a.Lon == nil {
		return fmt.Errorf("missing longitude")
	}

	if *a.Lon < -180 || *a.Lon > 180 {
		return fmt.Errorf("longitude out of range: %v", *a.Lon)
	}

	return nil
}

// IsEmergency returns a boolean if an emergency squawk code is sent
func (a *Aircraft) IsEmergency() bool {
	if a.Squawk != nil && (*a.Squawk == "7500" || *a.Squawk == "7600" || *a.Squawk == "7700") {
		return true
	}
	return false
}
