package domain

import "time"

// ThresholdLog stores historical events when a value breaches threshold limits.
type ThresholdLog struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	ParameterID   uint      `gorm:"not null;index" json:"parameter_id"`
	ThresholdID   *uint     `gorm:"index" json:"threshold_id,omitempty"`
	ThresholdType string    `gorm:"size:20;not null" json:"threshold_type"`
	Value         float64   `gorm:"type:REAL;not null" json:"value"`
	AlarmStatus   string    `gorm:"size:30;not null;index" json:"alarm_status"`
	Message       string    `gorm:"type:TEXT" json:"message"`
	TriggerAt     time.Time `gorm:"index" json:"trigger_at"`
	ResolveAt     *time.Time `gorm:"index" json:"resolve_at,omitempty"`
	CreatedAt     time.Time `gorm:"default:CURRENT_TIMESTAMP;index" json:"created_at"`
}

func (ThresholdLog) TableName() string {
	return "threshold_log"
}

