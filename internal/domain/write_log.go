package domain

import "time"

// WriteLog represents a log entry for write action to parameter
type WriteLog struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	ParameterID     uint      `gorm:"not null;index" json:"parameter_id"`
	ParameterNama   string    `gorm:"size:100" json:"parameter_nama"`
	DeviceID        uint      `gorm:"not null;index" json:"device_id"`
	DeviceNama      string    `gorm:"size:100" json:"device_nama"`
	WriteMode       string    `gorm:"size:20;not null" json:"write_mode"` // TEST, MANUAL, PRESET, TRIGGER
	Value           *float64  `gorm:"type:REAL" json:"value,omitempty"`   // Scalar value yang di-write
	RegisterValues  string    `gorm:"type:TEXT" json:"register_values,omitempty"` // JSON array of register values
	Success         bool      `gorm:"not null" json:"success"`
	Message         string    `gorm:"type:TEXT" json:"message,omitempty"`
	Error           *string   `gorm:"type:TEXT" json:"error,omitempty"`
	ReadBackValue   *float64  `gorm:"type:REAL" json:"read_back_value,omitempty"`
	WorkflowID      *uint     `gorm:"index" json:"workflow_id,omitempty"` // Jika write dari workflow
	WorkflowNama    *string   `gorm:"size:100" json:"workflow_nama,omitempty"`
	TriggeredBy     string    `gorm:"size:50;not null" json:"triggered_by"` // "MANUAL", "WORKFLOW", "API"
	CreatedAt       time.Time `gorm:"default:CURRENT_TIMESTAMP;index" json:"created_at"`
}

// TableName specifies the table name for WriteLog
func (WriteLog) TableName() string {
	return "write_log"
}

