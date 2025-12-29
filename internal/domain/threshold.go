package domain

import "time"

// Threshold represents alarm threshold configuration for a parameter
type Threshold struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	IDParameter   uint      `gorm:"not null;uniqueIndex" json:"id_parameter"`    // One threshold per parameter
	Unit          string    `gorm:"size:20" json:"unit"`                         // Unit for display (V, A, etc.)
	ThresholdType string    `gorm:"size:20;default:'max'" json:"threshold_type"` // "max", "min", "min-max", or "abnormal"
	MinValue      float64   `gorm:"not null" json:"min_value"`                   // Minimum normal value
	MaxValue      float64   `gorm:"not null" json:"max_value"`                   // Maximum normal value
	CriticalLow   float64   `gorm:"not null" json:"critical_low"`                // Critical low threshold
	CriticalHigh  float64   `gorm:"not null" json:"critical_high"`               // Critical high threshold
	WarningLow    float64   `gorm:"not null" json:"warning_low"`                 // Warning low threshold
	WarningHigh   float64   `gorm:"not null" json:"warning_high"`                // Warning high threshold
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	// Relations
	Parameter Parameter `gorm:"foreignKey:IDParameter" json:"parameter,omitempty"`
}

func (Threshold) TableName() string {
	return "threshold"
}

// AlarmStatus constants
const (
	AlarmStatusNormal       = "normal"
	AlarmStatusAbnormal     = "abnormal"
	AlarmStatusWarningLow   = "warning_low"
	AlarmStatusWarningHigh  = "warning_high"
	AlarmStatusCriticalLow  = "critical_low"
	AlarmStatusCriticalHigh = "critical_high"
)
