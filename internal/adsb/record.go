// contains the shape of the adsb data
package adsb

import "time"

type Record struct {
	Hex        string    `json:"hex"`
	Flight     string    `json:"flight,omitempty"`
	Squawk     string    `json:"squawk,omitempty"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	Altitude   int       `json:"alt_geom,omitempty"` // GPS altitude
	Velocity   float64   `json:"gs,omitempty"`       // ground speed
	ObservedAt time.Time `json:"observed_at"`
}
