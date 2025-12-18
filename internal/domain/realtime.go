package domain

import "time"

type RealtimeData struct {
	ParameterID    uint        `json:"parameter_id"`
	ParameterNama  string      `json:"parameter_nama"`
	ParameterType  string      `json:"parameter_type"`
	DeviceNama     string      `json:"device_nama"` // Device name for dashboard
	RawValue       interface{} `json:"raw_value"`
	ProcessedValue interface{} `json:"processed_value"`
	CustomRumus    *string     `json:"custom_rumus,omitempty"`
	Timestamp      time.Time   `json:"timestamp"`
	Status         string      `json:"status"` // online, offline, error
	Error          *string     `json:"error,omitempty"`
}

// DashboardData is a simplified structure sent to dashboard via WebSocket
type DashboardData struct {
	ParameterID   uint        `json:"parameter_id"` // Parameter ID for edit/delete operations
	DeviceNama    string      `json:"device_nama"`
	ParameterNama string      `json:"parameter_nama"`
	Value         interface{} `json:"value"` // Final processed value
	Timestamp     time.Time   `json:"timestamp"`
	Status        string      `json:"status"` // online, offline, error
}

// CleanData is the clean data format sent to frontend via WebSocket
// This is the ONLY format frontend should receive - no raw data, no protocol details
type CleanData struct {
	ParameterID uint        `json:"parameter_id"` // Formula parameter ID (not raw)
	Name        string      `json:"name"`         // Logical parameter name (not raw naming)
	Value       interface{} `json:"value"`        // Final value (after decode, scale, formula)
	Unit        string      `json:"unit"`         // Parameter unit
	Timestamp   string      `json:"timestamp"`    // RFC3339 format
}
